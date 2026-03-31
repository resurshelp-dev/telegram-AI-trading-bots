from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
for candidate in (str(BASE_DIR), str(PROJECT_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)
RUNTIME_ROOT = Path(os.getenv("THREE_BAR_RUNTIME_ROOT", str(BASE_DIR))).resolve()
LOGS_DIR = RUNTIME_ROOT / "logs"
STATE_DIR = RUNTIME_ROOT / "state"
REPORTS_DIR = RUNTIME_ROOT / "reports"
ENV_PATH = Path(os.getenv("THREE_BAR_ENV_PATH", str(BASE_DIR / ".env"))).resolve()


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file(ENV_PATH)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_event(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, bot_tag: str) -> None:
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self.bot_tag = bot_tag.strip() or "3bar-live"

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, title: str, details: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return {"ok": False, "msg": "Telegram disabled"}
        lines = [f"[{self.bot_tag}] {title}"]
        for key, value in details.items():
            if value in (None, "", [], {}):
                continue
            if isinstance(value, float):
                lines.append(f"{key}: {value:.4f}")
            else:
                lines.append(f"{key}: {value}")
        payload = {"chat_id": self.chat_id, "text": "\n".join(lines), "disable_web_page_preview": True}
        request = Request(
            f"https://api.telegram.org/bot{self.token}/sendMessage",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except URLError as exc:
            return {"ok": False, "msg": str(exc)}


def parse_bool(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def print_status(title: str, details: Dict[str, Any]) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fields = []
    for key, value in details.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, float):
            fields.append(f"{key}={value:.4f}")
        else:
            fields.append(f"{key}={value}")
    suffix = " | " + ", ".join(fields) if fields else ""
    print(f"[{stamp}] {title}{suffix}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Continuous live daemon for 3bar package.")
    parser.add_argument("--paper", choices=["true", "false"], default=None)
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--data-mode", choices=["cache", "live"], default=os.getenv("DATA_MODE", "live"))
    parser.add_argument("--days", type=int, default=int(os.getenv("LIVE_DAYS", "180")))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "ETH-USDT"))
    parser.add_argument("--initial-capital", type=float, default=float(os.getenv("INITIAL_CAPITAL", "10000")))
    parser.add_argument("--risk-percent", type=float, default=float(os.getenv("RISK_PERCENT", "1")))
    parser.add_argument("--lookback-bars", type=int, default=int(os.getenv("LOOKBACK_BARS", "6")))
    parser.add_argument("--max-signal-age-minutes", type=int, default=int(os.getenv("MAX_SIGNAL_AGE_MINUTES", "180")))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("POLL_SECONDS", "60")))
    parser.add_argument("--heartbeat-minutes", type=int, default=int(os.getenv("HEARTBEAT_MINUTES", "30")))
    parser.add_argument("--max-loops", type=int, default=0)
    parser.add_argument("--state-file", default=str(STATE_DIR / "live_state.json"))
    parser.add_argument("--output-file", default=str(REPORTS_DIR / "live" / "latest_scan.json"))
    parser.add_argument("--daemon-state-file", default=str(STATE_DIR / "daemon_state.json"))
    parser.add_argument("--event-log-file", default=str(LOGS_DIR / "daemon_events.jsonl"))
    return parser


def main() -> None:
    from three_bar_live import execute_plan, scan_plans

    parser = build_parser()
    args = parser.parse_args()
    daemon_state_path = Path(args.daemon_state_file)
    event_log_path = Path(args.event_log_file)
    notifier = TelegramNotifier(
        token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        bot_tag=os.getenv("BOT_TAG", "3bar-live"),
    )
    paper_mode = parse_bool(args.paper if args.paper is not None else os.getenv("PAPER", "true"))
    last_heartbeat = 0.0
    loop_index = 0

    append_event(
        event_log_path,
        {
            "time": utc_now_iso(),
            "type": "daemon_start",
            "symbol": args.symbol,
            "paper": paper_mode,
            "data_mode": args.data_mode,
            "poll_seconds": args.poll_seconds,
        },
    )
    print_status(
        "system started",
        {
            "symbol": args.symbol,
            "paper": paper_mode,
            "data_mode": args.data_mode,
            "capital": args.initial_capital,
            "risk_percent": args.risk_percent,
            "heartbeat_minutes": args.heartbeat_minutes,
        },
    )
    notifier.send(
        "system started",
        {"symbol": args.symbol, "paper": paper_mode, "data_mode": args.data_mode, "poll_seconds": args.poll_seconds},
    )

    while True:
        loop_index += 1
        try:
            scan_payload = scan_plans(args)
            execution_payload = None
            if scan_payload.get("selected_plan") is not None:
                selected_plan = scan_payload["selected_plan"]
                print_status(
                    "signal detected",
                    {
                        "loop": loop_index,
                        "symbol": selected_plan.get("symbol"),
                        "direction": selected_plan.get("direction"),
                        "entry": selected_plan.get("entry_price"),
                        "stop": selected_plan.get("stop_price"),
                        "age_min": selected_plan.get("signal_age_minutes"),
                    },
                )
                notifier.send(
                    "signal detected",
                    {
                        "symbol": selected_plan.get("symbol"),
                        "direction": selected_plan.get("direction"),
                        "entry": selected_plan.get("entry_price"),
                        "stop": selected_plan.get("stop_price"),
                        "age_min": selected_plan.get("signal_age_minutes"),
                    },
                )
                execution_payload = execute_plan(args, scan_payload)
                print_status(
                    "execution result",
                    {
                        "loop": loop_index,
                        "ok": execution_payload.get("ok"),
                        "msg": execution_payload.get("msg"),
                        "symbol": execution_payload.get("symbol"),
                        "direction": execution_payload.get("direction"),
                        "quantity": execution_payload.get("quantity"),
                        "paper_trading": execution_payload.get("paper_trading"),
                    },
                )
                notifier.send(
                    "execution result",
                    {
                        "ok": execution_payload.get("ok"),
                        "msg": execution_payload.get("msg"),
                        "symbol": execution_payload.get("symbol"),
                        "direction": execution_payload.get("direction"),
                        "quantity": execution_payload.get("quantity"),
                        "paper_trading": execution_payload.get("paper_trading"),
                    },
                )
            payload = {
                "time": utc_now_iso(),
                "type": "loop",
                "loop": loop_index,
                "scan": scan_payload,
                "execution": execution_payload,
            }
            append_event(event_log_path, payload)
            save_json(
                daemon_state_path,
                {
                    "last_loop": loop_index,
                    "last_run_at": utc_now_iso(),
                    "last_scan": scan_payload,
                    "last_execution": execution_payload,
                    "poll_seconds": args.poll_seconds,
                },
            )
        except Exception as exc:
            error_payload = {"time": utc_now_iso(), "type": "error", "loop": loop_index, "error": str(exc)}
            append_event(event_log_path, error_payload)
            save_json(daemon_state_path, {"last_loop": loop_index, "last_error": error_payload, "last_run_at": utc_now_iso()})
            print_status("error", {"loop": loop_index, "symbol": args.symbol, "error": str(exc)})
            notifier.send("error", {"loop": loop_index, "error": str(exc), "symbol": args.symbol})

        now_ts = time.time()
        if now_ts - last_heartbeat >= args.heartbeat_minutes * 60:
            heartbeat = {
                "time": utc_now_iso(),
                "type": "heartbeat",
                "loop": loop_index,
                "symbol": args.symbol,
                "paper": paper_mode,
                "status": "running",
            }
            append_event(event_log_path, heartbeat)
            save_json(Path(args.output_file), heartbeat)
            print_status(
                "heartbeat",
                {"loop": loop_index, "symbol": args.symbol, "paper": paper_mode, "status": "running"},
            )
            notifier.send("heartbeat", {"loop": loop_index, "symbol": args.symbol, "paper": paper_mode, "status": "running"})
            last_heartbeat = now_ts

        if args.max_loops and loop_index >= args.max_loops:
            break
        time.sleep(max(args.poll_seconds, 5))


if __name__ == "__main__":
    main()
