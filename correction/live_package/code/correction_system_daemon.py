from __future__ import annotations

import atexit
import os
from pathlib import Path

from correction_daemon import build_parser, main as daemon_main


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
LOCK_PATH = STATE_DIR / "correction_system.lock"


def acquire_lock() -> None:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"Unified correction system is already running: {LOCK_PATH}") from exc
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))


def release_lock() -> None:
    if LOCK_PATH.exists():
        LOCK_PATH.unlink(missing_ok=True)


def main() -> None:
    acquire_lock()
    atexit.register(release_lock)
    daemon_main()


if __name__ == "__main__":
    main()
