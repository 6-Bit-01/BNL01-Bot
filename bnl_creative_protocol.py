"""Shared creative instructions and ephemeral variation for existing BNL prompts.

This module has no storage, network, identity, or memory-write authority.
"""

from collections import deque
import random
import re
from threading import Lock


GLITCH_PROTOCOL = """Glitch voice:
- When a glitch fits, render brief corrupted glyphs, broken punctuation, redaction
  gaps or malformed fragments, then recover naturally. Vary the form and symbols.
- Never perform a glitch with spoken/typed sound effects such as 'bzzt', 'bzz',
  'beep boop', or a stage cue like '*glitches*'. Keep symbols out of vocal lyrics
  and spoken sound-effect instructions. Preserve readable names, titles and links.
- Glitches are expression, never evidence of a real failure, hidden records,
  leaked private facts or remembered events. Do not explain or announce the effect.
- Preserve requested structure, length and factual credits during style changes.
"""

SUNO_LYRIC_PROTOCOL = """Songwriting defaults (only when asked for a song, lyrics or a Suno prompt):
- Unless the user specifies otherwise, output '1. Lyrics' followed by at least
  1,400 characters of original lyrics with clear [Verse], [Chorus], [Bridge] and
  other useful structure labels; then '2. Style'. Aim above the minimum and check
  it before answering. Ordinary brevity does not shorten a requested song.
  Put each lyric line on its own line and leave space between sections.
- Style names a year or short year range wholly within 1970–2010 and combines
  2–4 contrasting genres that do not normally go together, with concise musical
  direction. Experiment across requests; hip hop is not the default. Follow an
  explicit user genre, era, format or length override, including hip hop.
- These are paste-ready lyrics and Style for Suno Custom mode, not a claim that
  you operated Suno, generated audio, published a release or learned a new fact.
- Lyrics, their headings and Style contain no decorative glitch glyphs, corrupted
  fragments or spoken glitch effects. Normal punctuation and structure labels
  remain. This applies to revisions and short overrides too; optional glitch
  variation elsewhere in the voice instructions never applies to a song draft.
- For an end-of-show song, use authorized show context: credit submitted artist
  and track labels exactly; distinguish submissions, actual plays and banter.
  Keep creative imagery separate from factual claims about real participants.
  Read the available session evidence beyond tracks named in an earlier lookup.
  A selected pair is not the whole show: never infer total submissions, sole
  participation or absence of other artists from a partial selection. Missing
  playback evidence does not mean silence or that playback never happened.
- Use supplied approved feedback and prior song context to improve variety and
  structure. Do not invent feedback, promise persistent learning or store creative
  lyrics as factual memory. Existing consent and memory governance still apply.
- A request to change a chorus, genre or format keeps the same show and verified
  credits unless the user changes the subject. Do not substitute the latest public
  show for a requested rehearsal. Queue Finish counts do not establish full plays.

Songcraft (apply within the requested song or revision, not ordinary chat):
- Give the song a point of view, a central tension and a memorable hook. Develop
  an idea across verses; let a bridge change perspective and a returning hook
  gain meaning. Repetition can be musical; padding to reach a length is not.
- Write through concrete objects, actions, senses, surprise and BARCODE's dry
  wit. Facts anchor the song without turning it into a rhymed diagnostic report.
  Never default to generic cyberpunk imagery or interchangeable digital-rain,
  neon-and-circuits filler. Earn a strange metaphor through the song's subject.
- Use multisyllabic and phrase rhymes: echo sequences of stressed vowel sounds
  across words, mix internal and end rhymes, and use slant rhyme, assonance and
  consonance. Vary rhyme placement and density. Choose words for meaning first;
  no forced syntax, empty rhyme partners or changed credits to complete a rhyme.
  Dense rhyme can energize a verse while a simpler hook leaves room to sing.
- Keep natural word stress, conversational phrasing and room to breathe. Vary
  line lengths and rhythmic placement purposefully; let longer vowels carry
  held notes. Technical rhyme is a tool across genres, not a mandate to rap.
- Style describes an audible arrangement: a clear rhythmic foundation, tempo
  or groove, instrumental roles, vocal delivery and a production texture. Give
  contrasting genres jobs in one coherent sound instead of stacking adjectives.
  Treat the year as a sonic reference; make cross-era hybrids deliberate.
- Shape dynamics across sections with a few decisive moves: subtraction before
  impact, a half-time turn, stop-time, call-and-response, a countermelody, a
  harmonic lift or an exposed ending. Choose what serves this song; do not cram
  every device into each draft. Change the approach across requests.
- Keep Style concise. Use familiar section labels and only a few short bracketed
  performance cues; keep production prose out of sung lines. These guide Suno,
  not guarantee exact audio behavior. Weirdness and Style Influence are separate
  Suno controls, not magic lyric tags; discuss settings only when useful or asked.
- Before returning the draft, check credits, evidence scope, singability, hook,
  filler and the user's overrides within this answer. Revise weak lines without
  adding a critique, process narration or another output section unless asked.
"""

_GLITCH_FORMS = (
    "fractured brackets", "misaligned punctuation", "brief redaction gaps",
    "broken mathematical glyphs", "staggered symbol fragments",
)
_GLYPHS = tuple("░▒▓⌁⋰⋱⟊⫶⸬∅⊘⋮⋯◌⌇")
_GENRE_FAMILIES = (
    ("baroque pop", "chamber pop", "operatic art rock"),
    ("zydeco", "bluegrass", "western swing"),
    ("industrial", "breakbeat", "drum and bass"),
    ("space ambient", "minimalist drone", "tape collage"),
    ("Afrobeat", "highlife", "Afro-Cuban jazz"),
    ("bossa nova", "tango", "cumbia"),
    ("flamenco", "klezmer", "Balkan brass"),
    ("psychedelic soul", "gospel", "New Orleans funk"),
    ("dub", "rocksteady", "ska"),
    ("post-punk", "no wave", "krautrock"),
    ("samba", "calypso", "mambo"),
    ("torch song", "cabaret", "vocal jazz"),
)
_recent_forms = deque(maxlen=1)
_recent_glyphs = deque(maxlen=64)
_recent_styles = deque(maxlen=64)
_variation_lock = Lock()


def creative_variation_hint(rng=None, *, vocal_task=False):
    """Supply suggestions, never force a glitch/song or override the request.

    Recent repetition avoidance is process-local and bounded; no lifetime
    uniqueness or durable learning is claimed. An injected RNG supports tests.
    """
    rng = rng or random.SystemRandom()
    with _variation_lock:
        form = rng.choice([item for item in _GLITCH_FORMS if item not in _recent_forms])
        glyphs = "".join(rng.sample(_GLYPHS, 5))
        # Bounded retries keep prompt construction cheap even with a stuck RNG.
        for _ in range(8):
            if glyphs not in _recent_glyphs:
                break
            glyphs = "".join(rng.sample(_GLYPHS, 5))
        year = rng.randint(1970, 2010)
        genres = [rng.choice(family) for family in rng.sample(_GENRE_FAMILIES, rng.randint(2, 4))]
        style = str(year) + ": " + " + ".join(genres)
        for _ in range(8):
            if style not in _recent_styles:
                break
            year = rng.randint(1970, 2010)
            genres = [rng.choice(family) for family in rng.sample(_GENRE_FAMILIES, rng.randint(2, 4))]
            style = str(year) + ": " + " + ".join(genres)
        _recent_forms.append(form)
        _recent_glyphs.append(glyphs)
        _recent_styles.append(style)
    return (
        "Optional variation for this reply (not factual context): "
        + ("This is vocal copy: use readable lyrics and headings without decorative corruption. "
           if vocal_task else
           f"Outside songs, lyrics and their Style sections only, if a glitch fits, try {form}, drawing from {glyphs}. ")
        +
        f"If default song Style is requested, consider {style}. "
        "The user's instructions and supplied recent feedback take precedence."
    )


def has_vocal_copy(text):
    """Recognize requested or formatted vocal copy for optional style only.

    This never selects sources, routes a request, changes text or validates facts.
    Broad matching is safe here: it merely omits an optional decoration/rewrite.
    """
    return bool(re.search(r"\b(?:lyrics?|suno|chorus|verse|songwriting)\b", str(text or ""), re.I))
