"""Normalize public TikTok LIVE Webcast observations into bounded NDJSON payloads."""

from __future__ import annotations

from typing import Any, Dict, Optional

from scripts.tiktok_live_telemetry_common import (
    MAX_COMMENT_CHARS,
    MAX_GIFT_NAME_CHARS,
    MAX_QUESTION_CHARS,
    _bounded_text,
    _coerce_bool,
    _first_nonempty,
    _mapping,
    _nonnegative_int,
    _observation_base,
    _user_fields,
)

def build_comment_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    user = _mapping(data.get("user"))
    comment_text = _bounded_text(data.get("content"), MAX_COMMENT_CHARS)
    if not comment_text:
        return None
    user_fields = _user_fields(user)
    payload = _observation_base(
        "comment",
        event,
        data,
        [user_fields["unique_id"], comment_text],
    )
    payload.update(user_fields)
    payload["comment_text"] = comment_text
    return payload


def build_like_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    like_count = _nonnegative_int(data.get("count"), maximum=10**9)
    like_total = _nonnegative_int(data.get("total"), maximum=10**12)
    if like_count <= 0 and like_total <= 0:
        return None
    user_fields = _user_fields(_mapping(data.get("user")))
    payload = _observation_base(
        "like",
        event,
        data,
        [user_fields["unique_id"], like_count, like_total],
    )
    payload.update(user_fields)
    payload.update({"like_count": like_count, "like_total": like_total})
    return payload


def build_viewer_snapshot_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    viewer_count = _nonnegative_int(
        data.get("viewerCount", data.get("viewer_count")), maximum=10**9
    )
    payload = _observation_base(
        "viewer_snapshot",
        event,
        data,
        [viewer_count, data.get("popStr"), data.get("pop_str")],
    )
    payload["viewer_count"] = viewer_count
    return payload


def build_social_payload(event_type: str, event: Any) -> Optional[Dict[str, Any]]:
    if event_type not in {"share", "follow"}:
        raise ValueError("unsupported social event type")
    data = _mapping(getattr(event, "data", {}))
    user_fields = _user_fields(_mapping(data.get("user")))
    payload = _observation_base(
        event_type,
        event,
        data,
        [
            user_fields["unique_id"],
            data.get("action"),
            data.get("shareType"),
            data.get("share_type"),
        ],
    )
    payload.update(user_fields)
    if event_type == "share":
        payload["share_type"] = _nonnegative_int(
            data.get("shareType", data.get("share_type")), maximum=1000
        )
    return payload


def build_gift_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    is_combo = _coerce_bool(data.get("is_combo", data.get("isCombo")))
    is_streak_over = _coerce_bool(
        data.get("is_streak_over", data.get("isStreakOver"))
    )
    if is_combo and not is_streak_over:
        return None

    gift = _mapping(data.get("gift"))
    user_fields = _user_fields(_mapping(data.get("user")))
    gift_id = _nonnegative_int(
        data.get("giftId", data.get("gift_id", gift.get("id"))), maximum=10**12
    )
    gift_name = _bounded_text(
        _first_nonempty(
            gift.get("name"),
            data.get("giftName"),
            data.get("gift_name"),
            "Gift",
            max_chars=MAX_GIFT_NAME_CHARS,
        ),
        MAX_GIFT_NAME_CHARS,
    )
    gift_count = max(
        1,
        _nonnegative_int(
            data.get("repeatCount", data.get("repeat_count", 1)), maximum=10**9
        ),
    )
    diamond_count = _nonnegative_int(
        gift.get("diamondCount", gift.get("diamond_count")), maximum=10**9
    )
    diamond_total = _nonnegative_int(
        data.get("diamond_total", data.get("diamondTotal")), maximum=10**12
    )
    if diamond_total <= 0 and diamond_count > 0:
        diamond_total = min(10**12, diamond_count * gift_count)

    payload = _observation_base(
        "gift",
        event,
        data,
        [user_fields["unique_id"], gift_id, gift_count, diamond_total],
    )
    payload.update(user_fields)
    payload.update(
        {
            "gift_id": gift_id,
            "gift_name": gift_name,
            "gift_count": gift_count,
            "diamond_count": diamond_count,
            "diamond_total": diamond_total,
            "combo": is_combo,
            "streak_over": is_streak_over or not is_combo,
        }
    )
    return payload


def build_question_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    details = _mapping(data.get("details"))
    question_text = _bounded_text(
        details.get("questionText", details.get("question_text")),
        MAX_QUESTION_CHARS,
    )
    if not question_text:
        return None
    user_fields = _user_fields(_mapping(details.get("user")))
    question_id = _bounded_text(
        _first_nonempty(
            details.get("questionId"),
            details.get("question_id"),
            max_chars=120,
        ),
        120,
    )
    payload = _observation_base(
        "question",
        event,
        data,
        [question_id, user_fields["unique_id"], question_text],
    )
    payload.update(user_fields)
    payload.update(
        {
            "question_id": question_id,
            "question_text": question_text,
            "answer_status": _nonnegative_int(
                details.get("answerStatus", details.get("answer_status")),
                maximum=1000,
            ),
        }
    )
    return payload


def build_join_payload(event: Any) -> Optional[Dict[str, Any]]:
    data = _mapping(getattr(event, "data", {}))
    user_fields = _user_fields(_mapping(data.get("user")))
    payload = _observation_base(
        "join",
        event,
        data,
        [user_fields["unique_id"], data.get("action")],
    )
    payload.update(user_fields)
    payload["join_count"] = 1
    return payload


