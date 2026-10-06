"""Read-only preview of a separately reviewed exact retained-source manifest.

This command cannot apply a repair. Live application requires a separately reviewed
current channel-policy adapter and the existing stored source/member controls.
The manifest is private operator input; do not commit it or include it in a pack.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    started = time.monotonic()
    result = {"status": "preview_incomplete", "database_writes": 0, "provider_calls": 0,
              "repair_executed": False, "source_text_or_identities_emitted": False}
    try:
        db = Path(args.db).resolve(strict=True)
        manifest_path = Path(args.manifest).resolve(strict=True)
        if manifest_path.stat().st_size > 65536:
            raise ValueError("manifest_file_bound")
        payload = manifest_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != args.manifest_sha256:
            raise ValueError("manifest_file_binding_changed")
        manifest = json.loads(payload.decode("utf-8"))
        uri = db.as_uri() + "?mode=ro"
        def audit(event, values):
            if event.startswith("socket."):
                raise PermissionError("network_denied")
            if event == "sqlite3.connect" and os.fsdecode(values[0]) != uri:
                raise PermissionError("unexpected_database_connection")
        sys.addaudithook(audit)
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from bnl_memory_ledger import _retained_repair_digest, run_retained_conversation_repair
        # File approval binds exact reviewed bytes. The owner separately binds
        # parsed canonical content, so harmless JSON formatting is not conflated.
        result.update(run_retained_conversation_repair(str(db), manifest,
            expected_manifest_sha256=_retained_repair_digest(manifest)))
        result["reviewed_manifest_file_sha256"] = args.manifest_sha256
        result["manifest_content_sha256"] = _retained_repair_digest(manifest)
        result["scope"] = "Stored originals, exact Journal bindings and scoped ledger/member controls only"
        result["apply_ready"] = False
    except Exception as error:
        result.update(status="preview_incomplete", error_type=type(error).__name__,
                      sqlite_errorcode=getattr(error, "sqlite_errorcode", None))
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"stored_eligibility_preview", "deferred_privacy_fence_busy"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
