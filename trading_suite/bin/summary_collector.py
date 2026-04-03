from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


SUITE_DIR = Path(__file__).resolve().parents[1]
RUNTIME_DIR = SUITE_DIR / "runtime"
SUMMARY_DIR = RUNTIME_DIR / "summary"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def read_json(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def count_jsonl_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def count_json_items(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    return len(payload) if isinstance(payload, list) else 0


def file_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def age_seconds(iso_value: str | None) -> float | None:
    if not iso_value:
        return None
    try:
        dt = datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((utc_now() - dt).total_seconds(), 2)


def summarize_correction() -> Dict[str, Any]:
    root = RUNTIME_DIR / "correction"
    daemon_state = read_json(root / "state" / "daemon_state.json") or {}
    paper_report = read_json(root / "reports" / "paper_trading" / "paper_report.json") or {}
    paper_state = read_json(root / "state" / "paper_state.json") or {}
    last_scan = daemon_state.get("last_scan") or {}
    return {
        "bot": "correction",
        "paper_mode": True,
        "lock_present": (root / "state" / "correction_system.lock").exists(),
        "last_run_at": daemon_state.get("last_run_at"),
        "last_run_age_sec": age_seconds(daemon_state.get("last_run_at")),
        "last_error": (daemon_state.get("last_error") or {}).get("error"),
        "symbol": ((last_scan.get("selected_plan") or {}).get("symbol") or "ETH-USDT"),
        "open_trade": paper_state.get("open_trade"),
        "paper_summary": paper_report,
        "logs": {
            "daemon_events": file_meta(root / "logs" / "daemon_events.jsonl"),
            "stdout": file_meta(root / "logs" / "stdout.log"),
            "stderr": file_meta(root / "logs" / "stderr.log"),
        },
    }


def summarize_three_bar() -> Dict[str, Any]:
    root = RUNTIME_DIR / "3bar"
    daemon_state = read_json(root / "state" / "daemon_state.json") or {}
    live_state = read_json(root / "state" / "live_state.json") or {}
    latest_scan = read_json(root / "reports" / "live" / "latest_scan.json") or {}
    trades_path = root / "reports" / "live" / "paper_trades.jsonl"
    return {
        "bot": "3bar",
        "paper_mode": True,
        "lock_present": (root / "state" / "three_bar_system.lock").exists(),
        "last_run_at": daemon_state.get("last_run_at"),
        "last_run_age_sec": age_seconds(daemon_state.get("last_run_at")),
        "last_error": (daemon_state.get("last_error") or {}).get("error"),
        "symbol": latest_scan.get("symbol") or (live_state.get("active_trade") or {}).get("symbol") or "ETH-USDT",
        "open_trade": live_state.get("active_trade"),
        "paper_summary": {
            "trades": count_jsonl_rows(trades_path),
            "last_execution": daemon_state.get("last_execution"),
        },
        "logs": {
            "daemon_events": file_meta(root / "logs" / "daemon_events.jsonl"),
            "paper_trades": file_meta(trades_path),
            "stdout": file_meta(root / "logs" / "stdout.log"),
            "stderr": file_meta(root / "logs" / "stderr.log"),
        },
    }


def summarize_svechi() -> Dict[str, Any]:
    root = RUNTIME_DIR / "svechi"
    state = read_json(root / "state.json") or {}
    final_summary = read_json(root / "reports" / "final_summary.json") or {}
    latest_signal = read_json(root / "reports" / "latest_signal.json") or {}
    return {
        "bot": "svechi",
        "paper_mode": True,
        "lock_present": (root / "svechi_final.lock").exists(),
        "last_run_at": state.get("updated_at") or state.get("time"),
        "last_run_age_sec": age_seconds(state.get("updated_at") or state.get("time")),
        "last_error": state.get("last_error"),
        "symbol": latest_signal.get("symbol") or "ETH-USDT",
        "open_trade": latest_signal if latest_signal else None,
        "paper_summary": final_summary,
        "logs": {
            "stdout": file_meta(root / "logs" / "stdout.log"),
            "stderr": file_meta(root / "logs" / "stderr.log"),
            "telegram_message": file_meta(root / "reports" / "telegram_message.txt"),
        },
    }


def summarize_kaktak() -> Dict[str, Any]:
    root = RUNTIME_DIR / "kaktak"
    state = read_json(root / "runtime_state.json") or {}
    return {
        "bot": "kaktak",
        "paper_mode": True,
        "lock_present": False,
        "last_run_at": state.get("updated_at") or state.get("last_loop_at") or state.get("timestamp"),
        "last_run_age_sec": age_seconds(state.get("updated_at") or state.get("last_loop_at") or state.get("timestamp")),
        "last_error": state.get("last_error"),
        "symbol": state.get("symbol") or "BTCUSDT",
        "open_trade": state.get("position") or state.get("open_trade"),
        "paper_summary": {
            "trade_log_rows": count_json_items(root / "trade_log.json"),
            "state": state,
        },
        "logs": {
            "app": file_meta(root / "bot.log"),
            "events": file_meta(root / "events.jsonl"),
            "stdout": file_meta(root / "stdout.log"),
            "stderr": file_meta(root / "stderr.log"),
        },
    }


def summarize_fixed() -> Dict[str, Any]:
    root = RUNTIME_DIR / "fixed"
    state = read_json(root / "state" / "runtime_state.json") or {}
    return {
        "bot": "fixed",
        "paper_mode": True,
        "lock_present": False,
        "last_run_at": state.get("updated_at") or state.get("last_loop_at") or state.get("timestamp"),
        "last_run_age_sec": age_seconds(state.get("updated_at") or state.get("last_loop_at") or state.get("timestamp")),
        "last_error": state.get("last_error"),
        "symbol": state.get("symbol") or "BTCUSDT",
        "open_trade": state.get("position") or state.get("open_trade"),
        "paper_summary": {
            "trade_log_rows": count_json_items(root / "reports" / "trade_log_contrarian.json"),
            "summary": read_json(root / "reports" / "backtest_last_month_summary.json"),
        },
        "logs": {
            "app": file_meta(root / "logs" / "bot.log"),
            "events": file_meta(root / "logs" / "events.jsonl"),
            "stdout": file_meta(root / "logs" / "stdout.log"),
            "stderr": file_meta(root / "logs" / "stderr.log"),
        },
    }


def build_summary() -> Dict[str, Any]:
    bots = [
        summarize_correction(),
        summarize_three_bar(),
        summarize_svechi(),
        summarize_kaktak(),
        summarize_fixed(),
    ]
    return {
        "generated_at": iso_now(),
        "runtime_dir": str(RUNTIME_DIR),
        "bots": bots,
    }


def render_markdown(summary: Dict[str, Any]) -> str:
    lines = [
        f"# Trading Summary",
        "",
        f"Generated at: {summary['generated_at']}",
        "",
    ]
    for bot in summary["bots"]:
        lines.append(f"## {bot['bot']}")
        lines.append(f"- paper_mode: {bot['paper_mode']}")
        lines.append(f"- lock_present: {bot['lock_present']}")
        lines.append(f"- last_run_at: {bot['last_run_at']}")
        lines.append(f"- last_run_age_sec: {bot['last_run_age_sec']}")
        lines.append(f"- symbol: {bot['symbol']}")
        lines.append(f"- last_error: {bot['last_error']}")
        lines.append(f"- open_trade: {json.dumps(bot['open_trade'], ensure_ascii=False, default=str)}")
        lines.append(f"- paper_summary: {json.dumps(bot['paper_summary'], ensure_ascii=False, default=str)}")
        lines.append("")
    return "\n".join(lines)


def write_summary() -> Dict[str, Any]:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    summary = build_summary()
    (SUMMARY_DIR / "latest_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    (SUMMARY_DIR / "latest_summary.md").write_text(render_markdown(summary), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect unified trading suite summary.")
    parser.add_argument("--loop-seconds", type=int, default=0)
    args = parser.parse_args()

    if args.loop_seconds <= 0:
        write_summary()
        return

    while True:
        write_summary()
        time.sleep(max(args.loop_seconds, 5))


if __name__ == "__main__":
    main()
