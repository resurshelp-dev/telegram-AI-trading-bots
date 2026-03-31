from __future__ import annotations

import json
from pathlib import Path


SUMMARY_PATH = Path(__file__).resolve().parents[1] / "runtime" / "summary" / "latest_summary.json"


def main() -> None:
    if not SUMMARY_PATH.exists():
        print(f"summary_missing={SUMMARY_PATH}")
        return
    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    print(f"generated_at={summary.get('generated_at')}")
    for bot in summary.get("bots", []):
        print(
            f"{bot['bot']}: paper={bot['paper_mode']} "
            f"lock={bot['lock_present']} "
            f"age={bot['last_run_age_sec']} "
            f"error={bot['last_error']}"
        )


if __name__ == "__main__":
    main()
