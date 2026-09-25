"""Read scope is independent of Discord participation and public reuse."""
from __future__ import annotations


PRIVATE_POLICIES = frozenset({
    "sealed_test", "internal_controlled", "broadcast_memory",
    "protected_system", "reference_canon",
})


def is_community_image_channel(channel) -> bool:
    """Threads inherit the community tool's strict no-output boundary."""
    seen = set()
    # Discord nesting is thread -> channel -> category. Keep malformed adapters
    # (including dynamic proxy objects) from creating an unbounded parent walk.
    for _ in range(4):
        if channel is None or id(channel) in seen:
            break
        seen.add(id(channel))
        if str(getattr(channel, "name", "") or "").strip().lower() == "ai-image-generator":
            return True
        channel = getattr(channel, "parent", None) or getattr(channel, "parent_channel", None)
    return False


def channel_is_publicly_readable(channel) -> bool:
    """Bot access never implies community access; absent evidence stays private."""
    if channel is None:
        return False
    try:
        if str(getattr(channel, "type", "")).lower() == "private_thread":
            return False
        is_private = getattr(channel, "is_private", None)
        if callable(is_private) and is_private():
            return False
        guild = getattr(channel, "guild", None)
        everyone = getattr(guild, "default_role", None)
        if everyone is None:
            return False
        perms = channel.permissions_for(everyone)
        return bool(perms.view_channel and perms.read_message_history)
    except Exception:
        return False


def public_observation_policy(channel, participation_policy: str) -> str:
    """Classify new observation sources without granting any reply permission."""
    if participation_policy in PRIVATE_POLICIES:
        return ""
    if not channel_is_publicly_readable(channel):
        return ""
    return "public_selective"
