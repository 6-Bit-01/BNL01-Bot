"""Operator acceptance tool. No user prompt, scheduler, or public sender."""
from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, help="A new private directory under an existing parent")
    parser.add_argument("--generate", action="store_true", help="Explicitly allow a concept call and, if BNL chooses, one image call")
    args = parser.parse_args()
    import bnl01_bot as bot
    from bnl_own_art import prepare_private_preview
    try:
        result = prepare_private_preview(bot, args.output_dir, generate=args.generate)
        print(json.dumps({key: result[key] for key in ("status", "published", "conceptCalls", "imageCalls")}))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "preview_failed", "published": False, "errorType": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
