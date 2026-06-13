import asyncio
import os
from types import SimpleNamespace

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def test_rd_assessment_intent_and_clean_candidate():
    text = "@BNL-01 is this appropriate for broadcast memory? DJ Floppydisc keeps trying to get me to play Chumbawamba even though the Oreaganomics Clause says I need to make the music or know someone who made it."
    assert bnl01_bot.detect_rd_ops_intent(text) == "broadcast_memory_assessment"
    response = bnl01_bot.build_rd_broadcast_memory_assessment_response(text)
    assert response.startswith("Decision:")
    assert "Suggested type: running_joke" in response
    assert "Clean memory candidate: DJ Floppydisc keeps trying" in response
    clean_line = next(line for line in response.splitlines() if line.startswith("Clean memory candidate:"))
    assert "is this appropriate" not in clean_line.lower()


def test_rd_draft_uses_sanitized_summary():
    text = "@BNL-01 write the broadcast memory note: DJ Floppydisc keeps trying to get me to play Chumbawamba even though the Oreaganomics Clause says I need to make the music or know someone who made it."
    assert bnl01_bot.detect_rd_ops_intent(text) == "broadcast_memory_draft"
    response = bnl01_bot.build_rd_broadcast_memory_draft_response(text)
    assert "Copy-paste into #bnl-broadcast-memory" in response
    summary = response.split("Summary:\n", 1)[1].split("\n\nBoundaries:", 1)[0]
    assert "DJ Floppydisc keeps trying" in summary
    assert "write the broadcast memory note" not in summary.lower()
    assert "broadcast memory?" not in summary.lower()


def test_structured_summary_meta_scaffold_rejected_when_only_question():
    content = """Broadcast memory note
Title: Test
Date: 2026-06-12
Type: notable_moment
Public-safe: yes
Usage: ambient,direct

Summary:
Does this belong in broadcast memory?

Boundaries:
This is broadcast memory only.
"""
    result = asyncio.run(bnl01_bot.process_broadcast_memory_note(SimpleNamespace(content=content)))
    assert result["entry_type"] == "reject"
    assert result["reason"] == "summary is meta/R&D scaffold; include the actual show memory."


def test_structured_summary_meta_scaffold_removed_when_payload_remains():
    content = """Broadcast memory note
Title: DJ Floppydisc
Date: 2026-06-12
Type: running_joke
Public-safe: yes
Usage: ambient,direct

Summary:
Does this belong in broadcast memory? DJ Floppydisc keeps trying to get me to play Chumbawamba.

Boundaries:
This is broadcast memory only.
"""
    result = asyncio.run(bnl01_bot.process_broadcast_memory_note(SimpleNamespace(content=content)))
    assert result["entry_type"] == "running_joke"
    assert "does this belong" not in result["cleaned_summary"].lower()
    assert "DJ Floppydisc" in result["cleaned_summary"]
