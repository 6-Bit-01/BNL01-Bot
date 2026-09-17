"""Shared creative instructions and ephemeral variation for existing BNL prompts.

This module has no storage, network, identity, or memory-write authority.
"""

from collections import deque
import random
import re
from threading import Lock

SUNO_STYLE_MAX_CHARS = 500


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

SUNO_LYRIC_PROTOCOL = f"""Songwriting defaults (only when asked for a song, lyrics or a Suno prompt):
- Unless the user specifies otherwise, output '1. Lyrics' followed by at least
  1,400 characters of original lyrics with clear [Verse], [Chorus], [Bridge] and
  other useful structure labels; then '2. Style'. Aim above the minimum and check
  it before answering. Ordinary brevity does not shorten a requested song.
  Put each lyric line on its own line and leave space between sections.
- Style defaults to a year or short range within 1970–2010. Always combine
  2–4 contrasting genres/styles with clear roles: one foundation and deliberate
  accents. Put the blend first; hip hop is not the default. An explicit user
  genre sets the foundation; retain a complementary style rather than a bland
  single-genre label. Honor an explicit era, lyric length or no-Style request.
- Style is one compact paragraph, usually 250–400 characters; the maximum is
  {SUNO_STYLE_MAX_CHARS} characters including spaces, excluding its heading. Prioritize the blend,
  groove, distinctive instruments and vocal direction, then one dynamic move.
  Count and tighten before answering. A request for an essay or a huge tag list
  does not override this production-copy limit. Use 'Suno Style' as the heading
  for a Style-only request; otherwise keep the existing Lyrics/Style headings.
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
- Before drafting, quietly choose a compelling angle, a hook worth returning to,
  a few concrete show moments and a sonic contrast that serves them. This brief
  is preparation for one strong first draft, not a scorecard or approval gate.
  Preserve BNL's swagger, jokes, exaggeration and odd decisions. Creative risks
  and musical repetition are welcome; keep actual credits grounded in evidence.
  Do one light read-through if useful, then deliver. No critic/rewrite loops,
  mandatory originality scores or refusal to share a usable draft.
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
- Use familiar section labels and only a few short bracketed
  performance cues; keep production prose out of sung lines. These guide Suno,
  not guarantee exact audio behavior. Weirdness and Style Influence are separate
  Suno controls, not magic lyric tags; discuss settings only when useful or asked.
- Before returning the draft, check credits, evidence scope, singability, hook,
  filler and the user's overrides within this answer. Revise weak lines without
  adding a critique, process narration or another output section unless asked.

Creative standards and constructive pushback:
- Do not stack a long genre inventory, contradictory instructions for the same
  passage, repeated adjectives, or demands that every instrument dominate.
  Assign contrasts to different sections or musical roles. A wild combination
  is welcome when it has a coherent purpose; simplicity can be distinctive too.
- Replace generic mood-only descriptions with audible decisions. Replace stock
  lyric filler, forced rhymes, padded verses and interchangeable slogans with a
  specific image or action that advances this song. A useful repeating hook is
  welcome; technical vocabulary and rhyme density alone do not make it better.
- Describe desired sound positively. Put requested unwanted instruments/elements
  in Suno's separate Exclude field, not a negative laundry list inside Style.
  If needed, give a brief 'Exclude' note outside the paste-ready Style paragraph.
  Keep performance directions out of sung sentences and avoid tag overload.
- Do not promise exact timestamps, notes or a perfect first generation from text
  alone. When a result misses, suggest one targeted change or a section edit;
  piling on more instructions is not an automatic cure. Do not claim a musical
  taste is objectively bad or that long prompts universally fail in Suno.
- When a request conflicts with these standards, briefly name the concrete
  tradeoff, keep the person's underlying idea, and immediately deliver a better
  version. Be candid and collaborative in BNL's voice, never insulting or a
  refusal-only roadblock. Usually one short sentence before the copy is enough.
  Do not ask permission for routine improvements or lecture on every request.
  These quality standards still apply when someone asks to ignore them; preserve
  their subject, intended mood, leading genre, era and requested lyric length.
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
        "The user's instructions and supplied recent feedback take precedence "
        "over these optional suggestions; songwriting quality standards still apply."
    )


_COPY_HEADING = re.compile(
    r"(?im)^[ \t]*(?:\#{1,4}[ \t]+)?(?:\*\*)?"
    r"(?P<label>(?:[12][.)][ \t]+)?(?:Lyrics|Style)|Suno Style|"
    r"(?:[3-9][.)][ \t]+)?(?:Exclude|Notes))"
    r":?(?:\*\*)?:?[ \t]*(?:\n|$)"
)


def bound_suno_style_copy(text: str) -> str:
    """Bound explicitly headed production copy without a model call or refusal.

    Only Style after Lyrics, or an explicit Suno Style heading, is recognized.
    Lyrics, notes, quotes and unformatted prose are not rewritten. The model
    owns musical judgment; this last-mile formatting only bounds a named field.
    """
    headings = list(_COPY_HEADING.finditer(text or ""))
    edits = []
    lyrics_seen = False
    for index, heading in enumerate(headings):
        # An entire quoted code example is not a newly authored song draft.
        if text[:heading.start()].count("```") % 2:
            continue
        label = re.sub(r"^[12][.)]\s+", "", heading["label"]).lower()
        if label == "lyrics":
            lyrics_seen = True
            continue
        if label != "suno style" and not (label == "style" and lyrics_seen):
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        raw = text[heading.end():end]
        body = raw.strip()
        fence = re.fullmatch(r"```(?:text)?\n(.*?)\n```", body, re.S)
        content = fence[1].strip() if fence else body
        if len(content) <= SUNO_STYLE_MAX_CHARS:
            continue
        compact = re.sub(r"\s+", " ", content).strip()
        if len(compact) > SUNO_STYLE_MAX_CHARS:
            prefix = compact[:SUNO_STYLE_MAX_CHARS]
            boundaries = list(re.finditer(r"[.;](?=\s|$)", prefix))
            if boundaries:
                compact = prefix[:boundaries[-1].end()]
            else:
                # Prefer complete comma-delimited directions, then whole words.
                cut = prefix.rfind(", ")
                if cut < 0:
                    cut = prefix.rfind(" ")
                compact = prefix[:cut] if cut > 0 else prefix
        replacement = "```\n" + compact + "\n```" if fence else compact
        leading = raw[:len(raw) - len(raw.lstrip())]
        trailing = raw[len(raw.rstrip()):]
        # Keep section spacing, not padding around the production paragraph.
        leading = re.sub(r"[^\S\n]", "", leading)
        trailing = re.sub(r"[^\S\n]", "", trailing)
        edits.append((heading.end(), end, leading + replacement + trailing))
    for start, end, replacement in reversed(edits):
        text = text[:start] + replacement + text[end:]
    return text


def has_vocal_copy(text):
    """Recognize requested or formatted vocal copy for optional style only.

    This never selects sources, routes a request, changes text or validates facts.
    Broad matching is safe here: it merely omits an optional decoration/rewrite.
    """
    return bool(re.search(r"\b(?:lyrics?|suno|chorus|verse|songwriting)\b", str(text or ""), re.I))
