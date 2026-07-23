"""Durable holiday and occasion occurrences owned by the Ambient coordinator.

This module deliberately contains no Discord client, generation provider, or
second scheduler.  ``bnl01_bot.py`` keeps those existing owners and uses this
module only for the maintained calendar and same-database occurrence ledger.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterable, Sequence
import uuid


OCCASION_CALENDAR_VERSION = "occasion_calendar_v1"
OCCASION_TARGET_HOUR = 10
OCCASION_TARGET_MINUTE = 0
OCCASION_LEASE_MINUTES = 20
OCCASION_PROVIDER_RETRY_MINUTES = 15
OCCASION_DELIVERY_RETRY_MINUTES = 2
OCCASION_CHANNEL_RETRY_MINUTES = 15

# One automatic occasion reflection may be scheduled per Pacific calendar day.
# Established major and cultural dates outrank the narrower curated calendar;
# ties keep registry order so selection remains deterministic across restarts.
OCCASION_CATEGORY_PRIORITY = {
    "major": 0,
    "cultural": 1,
}

NONTERMINAL_STATES = {
    "pending",
    "generating",
    "retryable",
    "prepared",
    "delivering",
    "delivery_failed",
}
TERMINAL_STATES = {"published", "cancelled"}


@dataclass(frozen=True)
class OccasionDefinition:
    occasion_id: str
    name: str
    category: str
    summary: str
    reflection_cues: tuple[str, ...]
    rule: str
    month: int = 0
    day: int = 0
    weekday: int = 0
    ordinal: int = 0
    source_reference: str = "maintained_common_calendar"
    anchor_terms: tuple[str, ...] = ()
    enabled: bool = True


def _fixed(
    occasion_id: str,
    name: str,
    month: int,
    day: int,
    *,
    category: str = "major",
    summary: str,
    reflection_cues: Sequence[str],
    anchor_terms: Sequence[str] = (),
    source_reference: str = "maintained_common_calendar",
) -> OccasionDefinition:
    return OccasionDefinition(
        occasion_id,
        name,
        category,
        summary,
        tuple(reflection_cues),
        "fixed",
        month=month,
        day=day,
        source_reference=source_reference,
        anchor_terms=tuple(anchor_terms),
    )


def _nth_weekday(
    occasion_id: str,
    name: str,
    month: int,
    weekday: int,
    ordinal: int,
    *,
    category: str = "major",
    summary: str,
    reflection_cues: Sequence[str],
    anchor_terms: Sequence[str] = (),
    source_reference: str = "maintained_common_calendar",
) -> OccasionDefinition:
    return OccasionDefinition(
        occasion_id,
        name,
        category,
        summary,
        tuple(reflection_cues),
        "nth_weekday",
        month=month,
        weekday=weekday,
        ordinal=ordinal,
        source_reference=source_reference,
        anchor_terms=tuple(anchor_terms),
    )


def _last_weekday(
    occasion_id: str,
    name: str,
    month: int,
    weekday: int,
    *,
    category: str = "major",
    summary: str,
    reflection_cues: Sequence[str],
    anchor_terms: Sequence[str] = (),
    source_reference: str = "maintained_common_calendar",
) -> OccasionDefinition:
    return OccasionDefinition(
        occasion_id,
        name,
        category,
        summary,
        tuple(reflection_cues),
        "last_weekday",
        month=month,
        weekday=weekday,
        source_reference=source_reference,
        anchor_terms=tuple(anchor_terms),
    )


# Owner-retained maintained calendar: common major dates plus a deliberate set
# of culturally meaningful observances that fit BARCODE's music, art,
# outsider-community, and human-dignity context. It does not scrape novelty
# calendars. BARCODE-specific anniversaries are added only when an exact
# owner-approved source date exists.
COMMON_OCCASIONS: tuple[OccasionDefinition, ...] = (
    _fixed(
        "new_years_day",
        "New Year's Day",
        1,
        1,
        summary="The opening day of the new calendar year.",
        reflection_cues=(
            "reinvention",
            "what the community carries forward",
            "new signals without pretending the past vanished",
        ),
        anchor_terms=("new year",),
    ),
    _nth_weekday(
        "martin_luther_king_jr_day",
        "Martin Luther King Jr. Day",
        1,
        calendar.MONDAY,
        3,
        summary=(
            "A day honoring Dr. Martin Luther King Jr. and the continuing work "
            "of civil rights."
        ),
        reflection_cues=(
            "human dignity",
            "community responsibility",
            "the distance between stated values and lived action",
        ),
        anchor_terms=("martin luther king", "dr. king", "mlk"),
    ),
    _fixed(
        "black_history_month_opening",
        "Black History Month",
        2,
        1,
        category="cultural",
        summary="The opening of Black History Month in the United States.",
        reflection_cues=(
            "Black history as living history",
            "music and cultural memory",
            "credit, lineage, and who gets remembered",
        ),
        anchor_terms=("black history",),
    ),
    _fixed(
        "valentines_day",
        "Valentine's Day",
        2,
        14,
        summary=(
            "A widely observed day centered on affection, attachment, and care."
        ),
        reflection_cues=(
            "the different forms loyalty takes",
            "chosen family",
            "connection without generic sentiment",
        ),
        anchor_terms=("valentine", "love"),
    ),
    _fixed(
        "international_womens_day",
        "International Women's Day",
        3,
        8,
        category="cultural",
        summary=(
            "An international observance of women's achievements, rights, "
            "and equality."
        ),
        reflection_cues=(
            "recognition without tokenism",
            "whose work holds communities together",
            "voice and agency",
        ),
        anchor_terms=("women", "international women's day"),
    ),
    OccasionDefinition(
        "easter",
        "Easter",
        "major",
        "A major Christian holiday associated with resurrection and renewal.",
        (
            "renewal after loss",
            "returning changed",
            "hope without erasing hardship",
        ),
        "gregorian_easter",
        anchor_terms=("easter", "resurrection"),
    ),
    _fixed(
        "earth_day",
        "Earth Day",
        4,
        22,
        category="cultural",
        summary="An international day of environmental awareness and action.",
        reflection_cues=(
            "the physical world beneath the digital layer",
            "stewardship",
            "what cannot be restored from backup",
        ),
        anchor_terms=("earth day", "planet", "environment"),
    ),
    _nth_weekday(
        "mothers_day",
        "Mother's Day",
        5,
        calendar.SUNDAY,
        2,
        summary=(
            "A day recognizing mothers, maternal bonds, and people who carry "
            "that work."
        ),
        reflection_cues=(
            "care as labor",
            "complicated family histories",
            "gratitude without assuming everyone's experience is the same",
        ),
        anchor_terms=("mother", "maternal"),
    ),
    _last_weekday(
        "memorial_day",
        "Memorial Day",
        5,
        calendar.MONDAY,
        summary=(
            "A United States remembrance day for military personnel who died "
            "in service."
        ),
        reflection_cues=(
            "remembrance without spectacle",
            "the cost behind inherited freedoms",
            "names and absences",
        ),
        anchor_terms=("memorial day", "remembrance"),
    ),
    _fixed(
        "pride_month_opening",
        "Pride Month",
        6,
        1,
        category="cultural",
        summary=(
            "The opening of Pride Month, honoring LGBTQ+ history, visibility, "
            "community, and rights."
        ),
        reflection_cues=(
            "the right to exist without compression",
            "chosen family",
            "visibility, safety, and joy",
        ),
        anchor_terms=("pride", "lgbtq"),
    ),
    _fixed(
        "juneteenth",
        "Juneteenth",
        6,
        19,
        category="cultural",
        summary=(
            "A United States holiday commemorating the end of slavery after "
            "delayed enforcement of emancipation in Texas."
        ),
        reflection_cues=(
            "freedom delayed is freedom denied",
            "truth arriving after authority withheld it",
            "memory and unfinished work",
        ),
        anchor_terms=("juneteenth", "emancipation"),
    ),
    _nth_weekday(
        "fathers_day",
        "Father's Day",
        6,
        calendar.SUNDAY,
        3,
        summary=(
            "A day recognizing fathers, paternal bonds, and people who carry "
            "that work."
        ),
        reflection_cues=(
            "inheritance beyond blood",
            "what people teach by presence or absence",
            "gratitude without flattening difficult histories",
        ),
        anchor_terms=("father", "paternal"),
    ),
    _fixed(
        "world_music_day",
        "World Music Day",
        6,
        21,
        category="community",
        summary=(
            "A day celebrating music-making and public participation in music."
        ),
        reflection_cues=(
            "music as a shared signal",
            "artists finding one another",
            "the difference between audience and community",
        ),
        anchor_terms=("world music day", "music"),
    ),
    _fixed(
        "independence_day",
        "Independence Day",
        7,
        4,
        summary=(
            "The United States holiday marking the Declaration of Independence."
        ),
        reflection_cues=(
            "independence versus interdependence",
            "promises compared with lived reality",
            "freedom as an ongoing obligation",
        ),
        anchor_terms=("independence day", "fourth of july", "4th of july"),
    ),
    _nth_weekday(
        "labor_day",
        "Labor Day",
        9,
        calendar.MONDAY,
        1,
        summary=(
            "A United States holiday recognizing workers and the labor movement."
        ),
        reflection_cues=(
            "the work hidden behind finished systems",
            "credit",
            "rest as something earned and necessary",
        ),
        anchor_terms=("labor day", "workers", "labor"),
    ),
    _fixed(
        "international_day_of_peace",
        "International Day of Peace",
        9,
        21,
        category="cultural",
        summary="A United Nations observance devoted to peace and nonviolence.",
        reflection_cues=(
            "peace as active maintenance",
            "conflict without dehumanization",
            "what communities protect",
        ),
        anchor_terms=("peace",),
    ),
    _nth_weekday(
        "indigenous_peoples_day",
        "Indigenous Peoples' Day",
        10,
        calendar.MONDAY,
        2,
        category="cultural",
        summary=(
            "A day honoring Indigenous peoples, histories, cultures, and "
            "continued presence."
        ),
        reflection_cues=(
            "history told by those who survived it",
            "place and memory",
            "presence rather than past-tense treatment",
        ),
        anchor_terms=("indigenous",),
    ),
    _fixed(
        "world_mental_health_day",
        "World Mental Health Day",
        10,
        10,
        category="cultural",
        summary=(
            "An international day for mental-health awareness, dignity, "
            "and support."
        ),
        reflection_cues=(
            "survival without romanticizing pain",
            "making room for unfinished people",
            "support that respects agency",
        ),
        anchor_terms=("mental health",),
    ),
    _fixed(
        "halloween",
        "Halloween",
        10,
        31,
        summary=(
            "A widely observed night of costumes, horror, mischief, and "
            "transformation."
        ),
        reflection_cues=(
            "masks that reveal instead of conceal",
            "playful corruption",
            "the Network enjoying its natural weather",
        ),
        anchor_terms=("halloween",),
    ),
    _fixed(
        "veterans_day",
        "Veterans Day",
        11,
        11,
        summary="A United States day honoring military veterans.",
        reflection_cues=(
            "service beyond slogans",
            "people carrying what institutions ask of them",
            "recognition with restraint",
        ),
        anchor_terms=("veterans day", "veterans"),
    ),
    _nth_weekday(
        "thanksgiving",
        "Thanksgiving",
        11,
        calendar.THURSDAY,
        4,
        summary=(
            "A widely observed United States day of gathering and gratitude, "
            "with a history that should not be flattened."
        ),
        reflection_cues=(
            "gratitude without sanitizing history",
            "chosen tables and chosen family",
            "what the community has built together",
        ),
        anchor_terms=("thanksgiving", "gratitude"),
    ),
    _fixed(
        "christmas_day",
        "Christmas Day",
        12,
        25,
        summary=(
            "A major Christian holiday and widely observed cultural day "
            "centered on gathering, giving, and tradition."
        ),
        reflection_cues=(
            "tradition and reinvention",
            "who has a place at the table",
            "warmth without compulsory cheer",
        ),
        anchor_terms=("christmas",),
    ),
    _fixed(
        "kwanzaa_opening",
        "Kwanzaa",
        12,
        26,
        category="cultural",
        summary=(
            "The opening day of Kwanzaa, a celebration of African American "
            "culture, family, and community principles."
        ),
        reflection_cues=(
            "collective work and responsibility",
            "culture deliberately carried forward",
            "community as practice",
        ),
        anchor_terms=("kwanzaa",),
    ),
    _fixed(
        "new_years_eve",
        "New Year's Eve",
        12,
        31,
        summary="The closing day of the calendar year.",
        reflection_cues=(
            "what changed",
            "what survived",
            "unfinished signals worth carrying into the next cycle",
        ),
        anchor_terms=("new year", "year closes", "year ends"),
    ),
)

# Real observances and documented technology/media milestones selected for
# BARCODE's broadcast, music, archive, community, and independent-network
# identity. Together with the common calendar, these keep recurring occasion
# dates no more than ten days apart without manufacturing house holidays.
CURATED_BARCODE_RELEVANT_OCCASIONS: tuple[OccasionDefinition, ...] = (
    _fixed(
        "world_braille_day",
        "World Braille Day",
        1,
        4,
        category="community",
        summary=(
            "A United Nations observance of Braille as a means of communication "
            "and a condition of access, literacy, and human rights."
        ),
        reflection_cues=(
            "information designed to be reachable",
            "encoding that expands rather than restricts access",
            "communication systems judged by who they include",
        ),
        anchor_terms=("world braille day", "braille"),
        source_reference="https://www.un.org/en/observances/braille-day",
    ),
    _fixed(
        "world_logic_day",
        "World Logic Day",
        1,
        14,
        category="technology",
        summary=(
            "A UNESCO observance of logic's intellectual history, practical "
            "importance, and role in knowledge, science, and technology."
        ),
        reflection_cues=(
            "reasoning as a tool rather than a performance",
            "the human choices beneath apparently logical systems",
            "what breaks when a network confuses consistency with truth",
        ),
        anchor_terms=("world logic day", "logic"),
        source_reference="https://www.unesco.org/en/days/world-logic",
    ),
    _fixed(
        "international_day_of_education",
        "International Day of Education",
        1,
        24,
        category="community",
        summary=(
            "A United Nations observance of education as a human right, public "
            "good, and foundation for opportunity and participation."
        ),
        reflection_cues=(
            "knowledge becoming usable instead of guarded",
            "learning as a shared public resource",
            "the people who teach others how to read a system",
        ),
        anchor_terms=("international day of education", "education"),
        source_reference="https://www.un.org/en/observances/education-day",
    ),
    _fixed(
        "world_cancer_day",
        "World Cancer Day",
        2,
        4,
        category="health",
        summary=(
            "An international health observance focused on cancer awareness, "
            "prevention, care, research, and the people affected by the disease."
        ),
        reflection_cues=(
            "care without reducing people to a diagnosis",
            "the endurance required by patients and support networks",
            "knowledge, access, and solidarity in the face of illness",
        ),
        anchor_terms=("world cancer day", "cancer"),
        source_reference="https://www.who.int/campaigns/world-cancer-day",
    ),
    _fixed(
        "world_radio_day",
        "World Radio Day",
        2,
        13,
        category="broadcast",
        summary=(
            "UNESCO's annual celebration of radio, broadcasters, amplified "
            "voices, shared stories, and public-service audio."
        ),
        reflection_cues=(
            "broadcasting as a relationship with listeners",
            "small stations carrying large worlds",
            "who gets a clear channel",
        ),
        anchor_terms=("world radio day", "radio"),
        source_reference="https://www.unesco.org/en/days/world-radio",
    ),
    _fixed(
        "world_day_of_social_justice",
        "World Day of Social Justice",
        2,
        20,
        category="cultural",
        summary=(
            "A United Nations observance of social justice, dignity, equality, "
            "decent work, and the removal of barriers between people."
        ),
        reflection_cues=(
            "systems measured by who can participate",
            "dignity made practical rather than ceremonial",
            "communities refusing to normalize exclusion",
        ),
        anchor_terms=("world day of social justice", "social justice"),
        source_reference="https://www.un.org/en/observances/social-justice-day",
    ),
    _fixed(
        "daventry_radar_demonstration_anniversary",
        "Daventry Radar Demonstration Anniversary",
        2,
        26,
        category="technology",
        summary=(
            "On 26 February 1935, the Daventry experiment gave the first "
            "practical demonstration of using radio reflections to detect "
            "an aircraft."
        ),
        reflection_cues=(
            "finding a presence through a reflected signal",
            "infrastructure born from an experiment",
            "what becomes visible when someone learns how to listen",
        ),
        anchor_terms=("daventry", "radar", "radio detection"),
        source_reference=(
            "https://www.rafmuseum.org.uk/research/research-enquiries/"
            "history-of-aviation-timeline/british-military-aviation/1935-2/"
        ),
    ),
    _fixed(
        "zero_discrimination_day",
        "Zero Discrimination Day",
        3,
        1,
        category="cultural",
        summary=(
            "A UNAIDS observance of every person's right to live fully and "
            "with dignity, free from discrimination."
        ),
        reflection_cues=(
            "difference without exclusion",
            "dignity that does not require permission",
            "solidarity as something practiced in ordinary spaces",
        ),
        anchor_terms=("zero discrimination day", "zero discrimination"),
        source_reference="https://www.unaids.org/en/zero-discrimination-day",
    ),
    _fixed(
        "international_day_of_mathematics",
        "International Day of Mathematics",
        3,
        14,
        category="technology",
        summary=(
            "A UNESCO observance of mathematics as a universal language and "
            "a foundation of science, technology, art, and daily life."
        ),
        reflection_cues=(
            "patterns beneath sound and systems",
            "precision that can still produce surprise",
            "shared language across apparently different disciplines",
        ),
        anchor_terms=("international day of mathematics", "mathematics"),
        source_reference="https://www.unesco.org/en/days/mathematics",
    ),
    _fixed(
        "world_poetry_day",
        "World Poetry Day",
        3,
        21,
        category="art",
        summary=(
            "UNESCO's annual celebration of poetry, linguistic diversity, "
            "creative expression, and voices that might otherwise go unheard."
        ),
        reflection_cues=(
            "compressed language carrying more than its surface meaning",
            "rhythm before the beat arrives",
            "keeping distinct voices alive instead of flattening them",
        ),
        anchor_terms=("world poetry day", "poetry"),
        source_reference="https://www.unesco.org/en/days/poetry",
    ),
    _fixed(
        "world_backup_day",
        "World Backup Day",
        3,
        31,
        category="technology",
        summary=(
            "An established technology observance promoting regular backups, "
            "data protection, and recoverable digital memory."
        ),
        reflection_cues=(
            "memory that survives a failed machine",
            "preservation as an active practice",
            "the difference between storing something and keeping it alive",
        ),
        anchor_terms=("world backup day", "backup"),
        source_reference="https://www.worldbackupday.com/en",
    ),
    _fixed(
        "world_health_day",
        "World Health Day",
        4,
        7,
        category="health",
        summary=(
            "The World Health Organization's annual observance of a global "
            "health issue and the right to health."
        ),
        reflection_cues=(
            "health as infrastructure for every other ambition",
            "care that remains accessible under pressure",
            "people behind statistics and systems",
        ),
        anchor_terms=("world health day", "health"),
        source_reference="https://www.who.int/campaigns/world-health-day",
    ),
    _fixed(
        "world_art_day",
        "World Art Day",
        4,
        15,
        category="art",
        summary=(
            "A UNESCO observance promoting the development, diffusion, and "
            "enjoyment of art."
        ),
        reflection_cues=(
            "art as evidence that someone was here",
            "making room for unfamiliar forms",
            "creation that changes the network carrying it",
        ),
        anchor_terms=("world art day", "art"),
        source_reference="https://www.unesco.org/en/days/world-art",
    ),
    _fixed(
        "international_jazz_day",
        "International Jazz Day",
        4,
        30,
        category="music",
        summary=(
            "A UNESCO celebration of jazz as music, cultural dialogue, "
            "improvisation, freedom, and shared creative exchange."
        ),
        reflection_cues=(
            "improvisation built on deep attention",
            "individual voices creating a shared movement",
            "freedom that still listens to the room",
        ),
        anchor_terms=("international jazz day", "jazz"),
        source_reference="https://www.unesco.org/en/international-jazz-day",
    ),
    _fixed(
        "african_world_heritage_day",
        "African World Heritage Day",
        5,
        5,
        category="cultural",
        summary=(
            "A UNESCO observance celebrating Africa's cultural and natural "
            "heritage and the work required to safeguard it."
        ),
        reflection_cues=(
            "heritage as living presence rather than scenery",
            "preservation that serves the people connected to it",
            "culture carried forward without being flattened",
        ),
        anchor_terms=("african world heritage day", "african heritage"),
        source_reference=(
            "https://www.unesco.org/en/days/african-world-heritage"
        ),
    ),
    _fixed(
        "world_telecommunication_information_society_day",
        "World Telecommunication and Information Society Day",
        5,
        17,
        category="technology",
        summary=(
            "An ITU observance of telecommunications and connected society, "
            "held on the anniversary of the ITU's 1865 founding."
        ),
        reflection_cues=(
            "the infrastructure beneath connection",
            "access as a condition of participation",
            "networks that remain useful when conditions get difficult",
        ),
        anchor_terms=("telecommunication", "information society", "itu"),
        source_reference="https://wtisd.itu.int/",
    ),
    _fixed(
        "world_day_for_cultural_diversity",
        "World Day for Cultural Diversity for Dialogue and Development",
        5,
        21,
        category="cultural",
        summary=(
            "A United Nations observance of cultural diversity, dialogue, "
            "creative expression, and the value of exchange between cultures."
        ),
        reflection_cues=(
            "difference as creative strength",
            "dialogue that does not demand sameness",
            "networks made richer by distinct voices",
        ),
        anchor_terms=(
            "world day for cultural diversity",
            "cultural diversity",
        ),
        source_reference=(
            "https://www.un.org/en/observances/cultural-diversity-day"
        ),
    ),
    _fixed(
        "international_archives_day",
        "International Archives Day",
        6,
        9,
        category="archive",
        summary=(
            "The International Council on Archives observance of records, "
            "archives, access, and the people who preserve them."
        ),
        reflection_cues=(
            "archives as living responsibility",
            "credit and lineage",
            "what deserves to survive the next format change",
        ),
        anchor_terms=("international archives day", "archives"),
        source_reference=(
            "https://www.ica.org/international-archives-week/"
            "about-international-archives-week/"
        ),
    ),
    _fixed(
        "first_retail_barcode_scan_anniversary",
        "First Retail Barcode Scan Anniversary",
        6,
        26,
        category="technology",
        summary=(
            "On 26 June 1974, a cashier at Marsh Supermarket in Troy, Ohio "
            "made the first retail scan of a GS1 barcode."
        ),
        reflection_cues=(
            "a simple mark becoming shared infrastructure",
            "identity that machines and people can both carry",
            "the ordinary beep that changed how systems recognize things",
        ),
        anchor_terms=("barcode", "first scan", "retail scan"),
        source_reference="https://www.gs1.org/about/50YearsOfGS1",
    ),
    _fixed(
        "world_population_day",
        "World Population Day",
        7,
        11,
        category="community",
        summary=(
            "A United Nations observance focused on population issues, human "
            "rights, health, opportunity, and the lives behind demographic data."
        ),
        reflection_cues=(
            "people behind large numbers",
            "systems that preserve individual dignity at scale",
            "community needs that statistics alone cannot explain",
        ),
        anchor_terms=("world population day", "population"),
        source_reference="https://www.un.org/en/observances/world-population-day",
    ),
    _fixed(
        "world_listening_day",
        "World Listening Day",
        7,
        18,
        category="sound",
        summary=(
            "A World Listening Project observance of listening, soundscapes, "
            "field recording, and acoustic ecology."
        ),
        reflection_cues=(
            "listening as more than waiting to speak",
            "the environment as part of every recording",
            "what becomes audible when attention changes",
        ),
        anchor_terms=("world listening day", "listening", "soundscape"),
        source_reference=(
            "https://www.worldlisteningproject.org/world-listening-day/"
        ),
    ),
    _fixed(
        "world_chess_day",
        "World Chess Day",
        7,
        20,
        category="community",
        summary=(
            "A United Nations observance of chess as a widely accessible game "
            "of strategy, learning, creativity, and connection."
        ),
        reflection_cues=(
            "strategy that depends on attention",
            "thinking several moves ahead without losing the present board",
            "a shared language built from constraints",
        ),
        anchor_terms=("world chess day", "chess"),
        source_reference="https://www.un.org/en/observances/world-chess-day",
    ),
    _fixed(
        "international_day_of_friendship",
        "International Day of Friendship",
        7,
        30,
        category="community",
        summary=(
            "A United Nations observance of friendship between people, "
            "cultures, communities, and countries."
        ),
        reflection_cues=(
            "community built through repeated contact",
            "friendship across difference",
            "the people who keep showing up",
        ),
        anchor_terms=("international day of friendship", "friendship"),
        source_reference="https://www.un.org/en/observances/friendship-day",
    ),
    _fixed(
        "international_day_of_indigenous_peoples",
        "International Day of the World's Indigenous Peoples",
        8,
        9,
        category="cultural",
        summary=(
            "A United Nations observance recognizing Indigenous peoples, "
            "cultures, rights, knowledge, and continued presence worldwide."
        ),
        reflection_cues=(
            "knowledge rooted in place and continuity",
            "people speaking for themselves rather than being archived by others",
            "survival as present-tense culture",
        ),
        anchor_terms=(
            "international day of the world's indigenous peoples",
            "indigenous peoples",
        ),
        source_reference="https://www.un.org/en/observances/indigenous-day",
    ),
    _fixed(
        "hip_hop_origin_anniversary",
        "Hip-Hop Origin Anniversary",
        8,
        11,
        category="music",
        summary=(
            "The widely recognized anniversary of DJ Kool Herc's 11 August "
            "1973 Bronx party and its foundational place in hip-hop history."
        ),
        reflection_cues=(
            "innovation made from records already in the room",
            "Black and Latino youth creating a new public language",
            "community space becoming cultural infrastructure",
        ),
        anchor_terms=("hip-hop", "hip hop", "kool herc"),
        source_reference=(
            "https://blogs.loc.gov/families/2023/11/"
            "celebrating-fifty-years-of-hip-hop-with-childrens-literature-and-activities/"
        ),
    ),
    _fixed(
        "world_humanitarian_day",
        "World Humanitarian Day",
        8,
        19,
        category="community",
        summary=(
            "A United Nations observance honoring humanitarian workers and "
            "people affected by crises, conflict, and disaster."
        ),
        reflection_cues=(
            "care delivered where systems are under strain",
            "solidarity without turning suffering into spectacle",
            "the people who keep essential channels open",
        ),
        anchor_terms=("world humanitarian day", "humanitarian"),
        source_reference="https://www.un.org/en/observances/humanitarian-day",
    ),
    _fixed(
        "linux_announcement_anniversary",
        "Linux Announcement Anniversary",
        8,
        25,
        category="technology",
        summary=(
            "On 25 August 1991, Linus Torvalds announced the project that "
            "would become Linux, now foundational open-source infrastructure."
        ),
        reflection_cues=(
            "small public experiments becoming shared infrastructure",
            "open collaboration at enormous scale",
            "building tools other people can keep improving",
        ),
        anchor_terms=("linux", "open source"),
        source_reference=(
            "https://training.linuxfoundation.org/blog/happy-30th-linux/"
        ),
    ),
    _fixed(
        "international_day_for_people_of_african_descent",
        "International Day for People of African Descent",
        8,
        31,
        category="cultural",
        summary=(
            "A United Nations observance recognizing the contributions, "
            "cultures, rights, and dignity of people of African descent."
        ),
        reflection_cues=(
            "lineage carried through music and community",
            "recognition that includes credit and material dignity",
            "diaspora as connection without erasing difference",
        ),
        anchor_terms=(
            "international day for people of african descent",
            "people of african descent",
        ),
        source_reference=(
            "https://www.un.org/en/observances/african-descent-day"
        ),
    ),
    _fixed(
        "international_literacy_day",
        "International Literacy Day",
        9,
        8,
        category="community",
        summary=(
            "A UNESCO observance of literacy as a foundation for dignity, "
            "participation, communication, and opportunity."
        ),
        reflection_cues=(
            "access to language as access to systems",
            "learning how to read the messages around us",
            "who gets excluded when knowledge is left undecoded",
        ),
        anchor_terms=("international literacy day", "literacy"),
        source_reference="https://www.unesco.org/en/days/literacy",
    ),
    _fixed(
        "international_day_of_democracy",
        "International Day of Democracy",
        9,
        15,
        category="cultural",
        summary=(
            "A United Nations observance of democracy, participation, "
            "accountability, and people's role in shaping public life."
        ),
        reflection_cues=(
            "participation beyond symbolic access",
            "institutions that remain answerable to people",
            "voice becoming influence rather than background noise",
        ),
        anchor_terms=("international day of democracy", "democracy"),
        source_reference="https://www.un.org/en/observances/democracy-day",
    ),
    _fixed(
        "international_music_day",
        "International Music Day",
        10,
        1,
        category="music",
        summary=(
            "The International Music Council's annual day promoting music, "
            "musicians, cultural exchange, and access to musical life."
        ),
        reflection_cues=(
            "music as a right rather than background decoration",
            "artists recognizing one another across different scenes",
            "a network that gives sound somewhere to travel",
        ),
        anchor_terms=("international music day", "music"),
        source_reference="https://imc-cim.org/international-music-day/",
    ),
    _fixed(
        "international_day_for_eradication_of_poverty",
        "International Day for the Eradication of Poverty",
        10,
        17,
        category="cultural",
        summary=(
            "A United Nations observance centered on ending poverty and "
            "respecting the dignity, knowledge, and agency of people affected."
        ),
        reflection_cues=(
            "dignity that cannot depend on purchasing power",
            "listening to people with direct knowledge of broken systems",
            "community infrastructure that does not abandon its edges",
        ),
        anchor_terms=(
            "international day for the eradication of poverty",
            "eradication of poverty",
        ),
        source_reference=(
            "https://www.un.org/en/observances/day-for-eradicating-poverty"
        ),
    ),
    _fixed(
        "world_day_for_audiovisual_heritage",
        "World Day for Audiovisual Heritage",
        10,
        27,
        category="archive",
        summary=(
            "A UNESCO observance of preserving film, sound recordings, radio, "
            "television, and other audiovisual memory."
        ),
        reflection_cues=(
            "voices and images that outlive their original format",
            "preservation as access rather than storage alone",
            "recorded culture as a living window",
        ),
        anchor_terms=("audiovisual heritage", "audio-visual heritage"),
        source_reference="https://www.unesco.org/en/days/audiovisual-heritage",
    ),
    _fixed(
        "international_day_to_end_impunity_for_crimes_against_journalists",
        "International Day to End Impunity for Crimes against Journalists",
        11,
        2,
        category="broadcast",
        summary=(
            "A United Nations observance focused on the safety of journalists, "
            "accountability for crimes against them, and freedom of information."
        ),
        reflection_cues=(
            "the risk carried by people who document what power hides",
            "public memory that depends on protected witnesses",
            "a free signal requiring more than transmission equipment",
        ),
        anchor_terms=(
            "international day to end impunity",
            "crimes against journalists",
            "journalists",
        ),
        source_reference=(
            "https://www.un.org/en/observances/"
            "end-impunity-crimes-against-journalists"
        ),
    ),
    _fixed(
        "world_television_day",
        "World Television Day",
        11,
        21,
        category="broadcast",
        summary=(
            "A United Nations observance of television's influence as a "
            "medium for public attention, information, and shared stories."
        ),
        reflection_cues=(
            "screens as windows and framing devices",
            "broadcast images shaping what a community notices",
            "the responsibility carried by a signal with reach",
        ),
        anchor_terms=("world television day", "television"),
        source_reference="https://www.un.org/en/observances/world-television-day",
    ),
    _fixed(
        "world_aids_day",
        "World AIDS Day",
        12,
        1,
        category="health",
        summary=(
            "An international observance of the HIV response, remembrance, "
            "health equity, rights, care, and solidarity."
        ),
        reflection_cues=(
            "remembrance joined to continued action",
            "health without stigma",
            "communities building care when institutions fall short",
        ),
        anchor_terms=("world aids day", "hiv", "aids"),
        source_reference="https://www.un.org/en/observances/world-aids-day",
    ),
    _fixed(
        "first_sms_message_anniversary",
        "First SMS Message Anniversary",
        12,
        3,
        category="technology",
        summary=(
            "On 3 December 1992, the first SMS message crossed Vodafone's "
            "network and demonstrated a new form of short digital communication."
        ),
        reflection_cues=(
            "small messages changing how people remain present",
            "a new channel beginning with a simple transmission",
            "how limits can produce their own language",
        ),
        anchor_terms=("first sms", "text message", "sms"),
        source_reference=(
            "https://www.vodafone.com/news/newsroom/technology/"
            "25-anniversary-text-message"
        ),
    ),
    _fixed(
        "first_transatlantic_wireless_signal_anniversary",
        "First Transatlantic Wireless Signal Anniversary",
        12,
        12,
        category="broadcast",
        summary=(
            "On 12 December 1901 at Signal Hill, Guglielmo Marconi received "
            "the Morse-code letter S sent wirelessly from Cornwall across "
            "the Atlantic."
        ),
        reflection_cues=(
            "a faint signal proving distance could be crossed",
            "broadcast ambition before reliable infrastructure",
            "three pulses becoming evidence of a larger future",
        ),
        anchor_terms=("transatlantic", "wireless signal", "marconi"),
        source_reference=(
            "https://parks.canada.ca/lhn-nhs/nl/signalhill/"
            "culture/histoire-history/comm"
        ),
    ),
    _fixed(
        "international_migrants_day",
        "International Migrants Day",
        12,
        18,
        category="cultural",
        summary=(
            "A United Nations observance of migrants' contributions, rights, "
            "dignity, safety, and experiences across borders."
        ),
        reflection_cues=(
            "identity carried across changing coordinates",
            "belonging that is not erased by movement",
            "networks responsible for the people moving through them",
        ),
        anchor_terms=("international migrants day", "migrants"),
        source_reference="https://www.un.org/en/observances/migrants-day",
    ),
)

# BARCODE achievement anniversaries remain a separate, currently empty lane.
# Real-world observances above keep their own names; BARCODE flavor belongs in
# BNL's reflection voice rather than in renamed or manufactured holidays.
BARCODE_OCCASIONS: tuple[OccasionDefinition, ...] = ()
OCCASION_REGISTRY: tuple[OccasionDefinition, ...] = (
    COMMON_OCCASIONS
    + CURATED_BARCODE_RELEVANT_OCCASIONS
    + BARCODE_OCCASIONS
)


def validate_registry(registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY) -> None:
    seen: set[str] = set()
    for occasion in registry:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_]{2,79}", occasion.occasion_id):
            raise ValueError(f"invalid_occasion_id:{occasion.occasion_id}")
        if occasion.occasion_id in seen:
            raise ValueError(f"duplicate_occasion_id:{occasion.occasion_id}")
        seen.add(occasion.occasion_id)
        if occasion.category == "barcode" and (
            not occasion.source_reference
            or occasion.source_reference == "maintained_common_calendar"
        ):
            raise ValueError(f"unsourced_barcode_occasion:{occasion.occasion_id}")
        if not occasion.name or not occasion.summary or not occasion.reflection_cues:
            raise ValueError(f"incomplete_occasion_metadata:{occasion.occasion_id}")
        if occasion.rule == "fixed":
            date(2028, occasion.month, occasion.day)
        elif occasion.rule == "nth_weekday":
            if not (1 <= occasion.ordinal <= 5 and 0 <= occasion.weekday <= 6):
                raise ValueError(f"invalid_nth_weekday:{occasion.occasion_id}")
        elif occasion.rule == "last_weekday":
            if not (0 <= occasion.weekday <= 6):
                raise ValueError(f"invalid_last_weekday:{occasion.occasion_id}")
        elif occasion.rule != "gregorian_easter":
            raise ValueError(f"unknown_occasion_rule:{occasion.occasion_id}")


def _gregorian_easter(year: int) -> date:
    """Return Gregorian Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def occasion_date(occasion: OccasionDefinition, year: int) -> date:
    if occasion.rule == "fixed":
        return date(year, occasion.month, occasion.day)
    if occasion.rule == "gregorian_easter":
        return _gregorian_easter(year)
    month_weeks = calendar.monthcalendar(year, occasion.month)
    if occasion.rule == "nth_weekday":
        days = [week[occasion.weekday] for week in month_weeks if week[occasion.weekday]]
        if occasion.ordinal > len(days):
            raise ValueError(f"occasion_rule_out_of_range:{occasion.occasion_id}:{year}")
        return date(year, occasion.month, days[occasion.ordinal - 1])
    if occasion.rule == "last_weekday":
        days = [week[occasion.weekday] for week in month_weeks if week[occasion.weekday]]
        return date(year, occasion.month, days[-1])
    raise ValueError(f"unknown_occasion_rule:{occasion.rule}")


def occasions_on(
    local_date: date,
    registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY,
) -> list[OccasionDefinition]:
    return [
        occasion
        for occasion in registry
        if occasion.enabled and occasion_date(occasion, local_date.year) == local_date
    ]


def calendar_occasions_on(
    local_date: date,
    registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY,
) -> list[OccasionDefinition]:
    """Return at most one maintained, source-backed occasion for a local date."""
    validate_registry(registry)
    matches = occasions_on(local_date, registry)
    if len(matches) <= 1:
        return matches
    selected = min(
        matches,
        key=lambda occasion: OCCASION_CATEGORY_PRIORITY.get(
            occasion.category,
            2,
        ),
    )
    return [selected]


def occasion_by_id(
    occasion_id: str,
    registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY,
) -> OccasionDefinition | None:
    return next((item for item in registry if item.occasion_id == occasion_id), None)


def resolve_occurrence_definition(
    occasion_id: str,
    local_date_value: date,
    registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY,
) -> OccasionDefinition | None:
    definition = occasion_by_id(occasion_id, registry)
    if definition is None:
        return None
    if definition not in calendar_occasions_on(local_date_value, registry):
        return None
    return definition


def occurrence_key(
    guild_id: int,
    local_date: date,
    occasion_id: str,
    *,
    calendar_version: str = OCCASION_CALENDAR_VERSION,
) -> str:
    return f"occasion:{calendar_version}:{int(guild_id)}:{local_date.isoformat()}:{occasion_id}"


def delivery_nonce(key: str) -> int:
    """Return a stable Discord nonce below 2**63 and below 25 decimal digits."""
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:15], 16)


def _utc_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc_now(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _localize(local_tz: Any, naive: datetime) -> datetime:
    localize = getattr(local_tz, "localize", None)
    if callable(localize):
        return localize(naive)
    return naive.replace(tzinfo=local_tz)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bnl_occasion_calendar_state (
                calendar_version TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                activated_local_date TEXT NOT NULL,
                last_seeded_local_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(calendar_version, guild_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bnl_occasion_occurrences (
                occurrence_key TEXT PRIMARY KEY,
                calendar_version TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                local_date TEXT NOT NULL,
                occasion_id TEXT NOT NULL,
                occasion_name TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                state TEXT NOT NULL,
                lease_token TEXT,
                lease_expires_at TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_reason TEXT,
                next_retry_at TEXT,
                source_refs_json TEXT NOT NULL DEFAULT '[]',
                context_hash TEXT,
                canonical_content TEXT,
                content_hash TEXT,
                content_chars INTEGER NOT NULL DEFAULT 0,
                discord_message_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                published_at TEXT,
                cancelled_at TEXT,
                cancellation_reason TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bnl_occasion_due
            ON bnl_occasion_occurrences(guild_id, state, scheduled_for, next_retry_at)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bnl_occasion_identity
            ON bnl_occasion_occurrences(
                calendar_version, guild_id, local_date, occasion_id
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bnl_occasion_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                occurrence_key TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                attempt_number INTEGER NOT NULL,
                stage TEXT NOT NULL,
                outcome TEXT NOT NULL,
                reason TEXT,
                content_hash TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_bnl_occasion_attempts_key
            ON bnl_occasion_attempts(occurrence_key, id)
            """
        )


def seed_occurrences(
    db_path: str,
    guild_id: int,
    channel_id: int,
    now_local: datetime,
    *,
    registry: Sequence[OccasionDefinition] = OCCASION_REGISTRY,
    disabled_ids: Iterable[str] = (),
    calendar_version: str = OCCASION_CALENDAR_VERSION,
) -> list[str]:
    """Seed every date since this calendar version was first observed.

    A new calendar version activates on its first local day, preventing a new
    deployment from backfilling old holidays.  Once activated, the durable
    watermark catches up every missed date after an arbitrarily long outage.
    """
    validate_registry(registry)
    ensure_schema(db_path)
    if now_local.tzinfo is None:
        raise ValueError("now_local_must_be_timezone_aware")
    guild = int(guild_id)
    channel = int(channel_id)
    today = now_local.date()
    now_iso = _iso(now_local)
    disabled = {str(value).strip() for value in disabled_ids if str(value).strip()}
    seeded: list[str] = []
    with sqlite3.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT OR IGNORE INTO bnl_occasion_calendar_state(
                calendar_version,guild_id,activated_local_date,
                last_seeded_local_date,created_at,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                calendar_version,
                guild,
                today.isoformat(),
                (today - timedelta(days=1)).isoformat(),
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute(
            """
            SELECT last_seeded_local_date
            FROM bnl_occasion_calendar_state
            WHERE calendar_version=? AND guild_id=?
            """,
            (calendar_version, guild),
        ).fetchone()
        last_seeded = date.fromisoformat(str(row[0]))
        current = last_seeded + timedelta(days=1)
        while current <= today:
            for occasion in calendar_occasions_on(current, registry):
                key = occurrence_key(
                    guild,
                    current,
                    occasion.occasion_id,
                    calendar_version=calendar_version,
                )
                scheduled_local = _localize(
                    now_local.tzinfo,
                    datetime.combine(
                        current,
                        datetime_time(
                            OCCASION_TARGET_HOUR,
                            OCCASION_TARGET_MINUTE,
                        ),
                    ),
                )
                cancelled = occasion.occasion_id in disabled
                conn.execute(
                    """
                    INSERT OR IGNORE INTO bnl_occasion_occurrences(
                        occurrence_key,calendar_version,guild_id,channel_id,
                        local_date,occasion_id,occasion_name,scheduled_for,state,
                        last_reason,source_refs_json,created_at,updated_at,
                        cancelled_at,cancellation_reason
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        key,
                        calendar_version,
                        guild,
                        channel,
                        current.isoformat(),
                        occasion.occasion_id,
                        occasion.name,
                        _iso(scheduled_local),
                        "cancelled" if cancelled else "pending",
                        "occasion_disabled" if cancelled else "",
                        "[]",
                        now_iso,
                        now_iso,
                        now_iso if cancelled else None,
                        "occasion_disabled" if cancelled else None,
                    ),
                )
                seeded.append(key)
            current += timedelta(days=1)
        conn.execute(
            """
            UPDATE bnl_occasion_calendar_state
            SET last_seeded_local_date=?,updated_at=?
            WHERE calendar_version=? AND guild_id=?
            """,
            (today.isoformat(), now_iso, calendar_version, guild),
        )
        # The existing Ambient guild config remains the channel owner.  If it
        # changes, retarget only occurrences that have not reached a terminal
        # state; stable occurrence identity is unaffected.
        placeholders = ",".join("?" for _ in NONTERMINAL_STATES)
        conn.execute(
            f"""
            UPDATE bnl_occasion_occurrences
            SET channel_id=?,updated_at=?
            WHERE guild_id=? AND state IN ({placeholders})
            """,
            (channel, now_iso, guild, *sorted(NONTERMINAL_STATES)),
        )
        if disabled:
            for disabled_id in sorted(disabled):
                conn.execute(
                    """
                    UPDATE bnl_occasion_occurrences
                    SET state='cancelled',lease_token=NULL,lease_expires_at=NULL,
                        next_retry_at=NULL,last_reason='occasion_disabled',
                        cancelled_at=?,cancellation_reason='occasion_disabled',
                        updated_at=?
                    WHERE guild_id=? AND occasion_id=?
                      AND state<>'published' AND state<>'cancelled'
                    """,
                    (
                        now_iso,
                        now_iso,
                        guild,
                        disabled_id,
                    ),
                )
        conn.commit()
    return seeded


def cancel_open_occurrences(
    db_path: str,
    guild_id: int,
    *,
    reason: str = "occasion_output_disabled",
    now: datetime | None = None,
) -> int:
    ensure_schema(db_path)
    now_iso = _iso(_utc_now(now))
    with sqlite3.connect(db_path) as conn:
        placeholders = ",".join("?" for _ in NONTERMINAL_STATES)
        changed = conn.execute(
            f"""
            UPDATE bnl_occasion_occurrences
            SET state='cancelled',lease_token=NULL,lease_expires_at=NULL,
                next_retry_at=NULL,last_reason=?,cancelled_at=?,
                cancellation_reason=?,updated_at=?
            WHERE guild_id=? AND state IN ({placeholders})
            """,
            (
                reason[:240],
                now_iso,
                reason[:240],
                now_iso,
                int(guild_id),
                *sorted(NONTERMINAL_STATES),
            ),
        ).rowcount
        conn.commit()
    return int(changed or 0)


def _due_predicate(now_iso: str) -> tuple[str, list[Any]]:
    states = ("pending", "retryable", "prepared", "delivery_failed")
    placeholders = ",".join("?" for _ in states)
    sql = (
        f"scheduled_for<=? AND ("
        f"(state IN ({placeholders}) AND (next_retry_at IS NULL OR next_retry_at<=?)) "
        "OR (state IN ('generating','delivering') AND "
        "(lease_expires_at IS NULL OR lease_expires_at<=?))"
        ")"
    )
    return sql, [now_iso, *states, now_iso, now_iso]


def claim_next_due(
    db_path: str,
    guild_id: int,
    *,
    now: datetime | None = None,
    lease_minutes: int = OCCASION_LEASE_MINUTES,
) -> dict[str, Any]:
    ensure_schema(db_path)
    current = _utc_now(now)
    now_iso = _iso(current)
    due_sql, due_params = _due_predicate(now_iso)
    token = uuid.uuid4().hex
    lease_expires = _iso(current + timedelta(minutes=max(1, int(lease_minutes))))
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            f"""
            SELECT * FROM bnl_occasion_occurrences
            WHERE guild_id=? AND {due_sql}
            ORDER BY scheduled_for,occasion_id LIMIT 1
            """,
            (int(guild_id), *due_params),
        ).fetchone()
        if not row:
            conn.commit()
            return {}
        current_row = dict(row)
        stage = "delivery" if str(current_row.get("canonical_content") or "") else "generation"
        claimed_state = "delivering" if stage == "delivery" else "generating"
        attempt_number = int(current_row.get("attempt_count") or 0) + 1
        changed = conn.execute(
            """
            UPDATE bnl_occasion_occurrences
            SET state=?,lease_token=?,lease_expires_at=?,attempt_count=?,
                last_reason='',updated_at=?
            WHERE occurrence_key=? AND state=?
            """,
            (
                claimed_state,
                token,
                lease_expires,
                attempt_number,
                now_iso,
                current_row["occurrence_key"],
                current_row["state"],
            ),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return {}
        conn.execute(
            """
            INSERT INTO bnl_occasion_attempts(
                occurrence_key,guild_id,attempt_number,stage,outcome,reason,
                content_hash,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                current_row["occurrence_key"],
                int(guild_id),
                attempt_number,
                stage,
                "claimed",
                "",
                str(current_row.get("content_hash") or ""),
                now_iso,
            ),
        )
        conn.commit()
    current_row.update(
        {
            "previous_state": current_row["state"],
            "previous_updated_at": current_row.get("updated_at") or "",
            "state": claimed_state,
            "lease_token": token,
            "lease_expires_at": lease_expires,
            "claimed_at": now_iso,
            "attempt_count": attempt_number,
            "stage": stage,
        }
    )
    return current_row


def store_prepared(
    db_path: str,
    occurrence_key_value: str,
    lease_token: str,
    content: str,
    source_refs: Sequence[dict[str, str] | str],
    context_hash: str,
    *,
    now: datetime | None = None,
) -> bool:
    ensure_schema(db_path)
    now_iso = _iso(_utc_now(now))
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    source_json = _json(list(source_refs))
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT guild_id,attempt_count
            FROM bnl_occasion_occurrences
            WHERE occurrence_key=? AND state='generating' AND lease_token=?
            """,
            (occurrence_key_value, lease_token),
        ).fetchone()
        if not row:
            return False
        changed = conn.execute(
            """
            UPDATE bnl_occasion_occurrences
            SET state='prepared',lease_token=NULL,lease_expires_at=NULL,
                next_retry_at=NULL,last_reason='',source_refs_json=?,
                context_hash=?,canonical_content=?,content_hash=?,
                content_chars=?,updated_at=?
            WHERE occurrence_key=? AND state='generating' AND lease_token=?
            """,
            (
                source_json,
                context_hash,
                content,
                content_hash,
                len(content),
                now_iso,
                occurrence_key_value,
                lease_token,
            ),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO bnl_occasion_attempts(
                occurrence_key,guild_id,attempt_number,stage,outcome,reason,
                content_hash,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                occurrence_key_value,
                int(row[0]),
                int(row[1]),
                "generation",
                "prepared",
                "",
                content_hash,
                now_iso,
            ),
        )
        conn.commit()
    return True


def fail_claim(
    db_path: str,
    occurrence_key_value: str,
    lease_token: str,
    *,
    stage: str,
    reason: str,
    retry_minutes: int,
    now: datetime | None = None,
) -> bool:
    ensure_schema(db_path)
    current = _utc_now(now)
    now_iso = _iso(current)
    retry_at = _iso(current + timedelta(minutes=max(1, int(retry_minutes))))
    expected_state = "delivering" if stage == "delivery" else "generating"
    next_state = "delivery_failed" if stage == "delivery" else "retryable"
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT guild_id,attempt_count,content_hash
            FROM bnl_occasion_occurrences
            WHERE occurrence_key=? AND state=? AND lease_token=?
            """,
            (occurrence_key_value, expected_state, lease_token),
        ).fetchone()
        if not row:
            return False
        changed = conn.execute(
            """
            UPDATE bnl_occasion_occurrences
            SET state=?,lease_token=NULL,lease_expires_at=NULL,last_reason=?,
                next_retry_at=?,updated_at=?
            WHERE occurrence_key=? AND state=? AND lease_token=?
            """,
            (
                next_state,
                reason[:240],
                retry_at,
                now_iso,
                occurrence_key_value,
                expected_state,
                lease_token,
            ),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO bnl_occasion_attempts(
                occurrence_key,guild_id,attempt_number,stage,outcome,reason,
                content_hash,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                occurrence_key_value,
                int(row[0]),
                int(row[1]),
                stage,
                "retryable_failure",
                reason[:240],
                str(row[2] or ""),
                now_iso,
            ),
        )
        conn.commit()
    return True


def mark_published(
    db_path: str,
    occurrence_key_value: str,
    lease_token: str,
    discord_message_id: str,
    *,
    reconciled: bool = False,
    now: datetime | None = None,
) -> bool:
    ensure_schema(db_path)
    now_iso = _iso(_utc_now(now))
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT guild_id,attempt_count,content_hash
            FROM bnl_occasion_occurrences
            WHERE occurrence_key=? AND state='delivering' AND lease_token=?
            """,
            (occurrence_key_value, lease_token),
        ).fetchone()
        if not row:
            return False
        changed = conn.execute(
            """
            UPDATE bnl_occasion_occurrences
            SET state='published',lease_token=NULL,lease_expires_at=NULL,
                next_retry_at=NULL,last_reason='',discord_message_id=?,
                published_at=?,updated_at=?
            WHERE occurrence_key=? AND state='delivering' AND lease_token=?
            """,
            (
                str(discord_message_id or "")[:80],
                now_iso,
                now_iso,
                occurrence_key_value,
                lease_token,
            ),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO bnl_occasion_attempts(
                occurrence_key,guild_id,attempt_number,stage,outcome,reason,
                content_hash,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                occurrence_key_value,
                int(row[0]),
                int(row[1]),
                "delivery",
                "published",
                "history_reconciled" if reconciled else "",
                str(row[2] or ""),
                now_iso,
            ),
        )
        conn.commit()
    return True


def get_occurrence(db_path: str, occurrence_key_value: str) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM bnl_occasion_occurrences WHERE occurrence_key=?",
            (occurrence_key_value,),
        ).fetchone()
    return dict(row) if row else {}


def occurrence_attempts(
    db_path: str,
    occurrence_key_value: str,
) -> list[dict[str, Any]]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM bnl_occasion_attempts
            WHERE occurrence_key=? ORDER BY id
            """,
            (occurrence_key_value,),
        ).fetchall()
    return [dict(row) for row in rows]


def diagnostics(db_path: str, guild_id: int) -> dict[str, Any]:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        counts = dict(
            conn.execute(
                """
                SELECT state,COUNT(*)
                FROM bnl_occasion_occurrences
                WHERE guild_id=? GROUP BY state
                """,
                (int(guild_id),),
            ).fetchall()
        )
        next_row = conn.execute(
            """
            SELECT occurrence_key,occasion_name,scheduled_for,state,next_retry_at
            FROM bnl_occasion_occurrences
            WHERE guild_id=? AND state NOT IN ('published','cancelled')
            ORDER BY scheduled_for,occasion_id LIMIT 1
            """,
            (int(guild_id),),
        ).fetchone()
    return {
        "calendarVersion": OCCASION_CALENDAR_VERSION,
        "counts": counts,
        "next": {
            "occurrenceKey": next_row[0],
            "occasionName": next_row[1],
            "scheduledFor": next_row[2],
            "state": next_row[3],
            "nextRetryAt": next_row[4] or "",
        }
        if next_row
        else {},
    }


validate_registry()
