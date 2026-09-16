"""Shared creative instructions and ephemeral variation for existing BNL prompts.

This module has no storage, network, identity, or memory-write authority.
"""

from collections import deque
import random
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
- Style names a year or short year range wholly within 1970–2010 and combines
  2–4 contrasting genres that do not normally go together, with concise musical
  direction. Experiment across requests; hip hop is not the default. Follow an
  explicit user genre, era, format or length override, including hip hop.
- These are paste-ready lyrics and Style for Suno Custom mode, not a claim that
  you operated Suno, generated audio, published a release or learned a new fact.
- For an end-of-show song, use authorized show context: credit submitted artist
  and track labels exactly; distinguish submissions, actual plays and banter.
  Keep creative imagery separate from factual claims about real participants.
- Use supplied approved feedback and prior song context to improve variety and
  structure. Do not invent feedback, promise persistent learning or store creative
  lyrics as factual memory. Existing consent and memory governance still apply.
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
)
_recent_forms = deque(maxlen=1)
_recent_glyphs = deque(maxlen=64)
_recent_styles = deque(maxlen=64)
_variation_lock = Lock()


def creative_variation_hint(rng=None):
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
        f"if a glitch fits, try {form}, drawing from {glyphs}. "
        f"If default song Style is requested, consider {style}. "
        "The user's instructions and supplied recent feedback take precedence."
    )
