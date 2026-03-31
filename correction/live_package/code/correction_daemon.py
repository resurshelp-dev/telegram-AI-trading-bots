from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
STATE_DIR = BASE_DIR / "state"
REPORTS_DIR = BASE_DIR / "reports" / "paper_trading"
ENV_PATH = BASE_DIR / ".env"


def load_env_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_bool(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, bot_tag: str, sender: Optional[Any] = None) -> None:
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self.bot_tag = bot_tag.strip() or "correction-live"
        self.sender = sender or _post_json

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, text: str) -> Dict[str, Any]:
        if not self.enabled:
            return {"ok": False, "msg": "Telegram is disabled"}
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            data = self.sender(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                payload,
            )
            return {"ok": bool(data.get("ok")), "raw": data}
        except Exception as exc:
            return {"ok": False, "msg": str(exc)}


def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except URLError as exc:
        raise RuntimeError(str(exc)) from exc


def build_notifier() -> TelegramNotifier:
    return TelegramNotifier(
        token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        bot_tag=os.getenv("BOT_TAG", "correction-live"),
    )


def format_telegram_message(title: str, details: Dict[str, Any]) -> str:
    lines = [f"[{os.getenv('BOT_TAG', 'correction-live')}] {title}"]
    for key, value in details.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, float):
            rendered = f"{value:.4f}"
        else:
            rendered = str(value)
        lines.append(f"{key}: {rendered}")
    return "\n".join(lines)


def notify_telegram(notifier: TelegramNotifier, title: str, details: Dict[str, Any]) -> Dict[str, Any]:
    if not notifier.enabled:
        return {"ok": False, "msg": "Telegram is disabled"}
    return notifier.send(format_telegram_message(title, details))


def default_paper_state(initial_capital: float) -> Dict[str, Any]:
    return {
        "initial_capital": initial_capital,
        "current_capital": initial_capital,
        "closed_trades": [],
        "open_trade": None,
        "last_mark_price": None,
        "updated_at": utc_now_iso(),
    }


def load_paper_state(path: Path, initial_capital: float) -> Dict[str, Any]:
    if not path.exists():
        return default_paper_state(initial_capital)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("initial_capital", initial_capital)
    payload.setdefault("current_capital", payload["initial_capital"])
    payload.setdefault("closed_trades", [])
    payload.setdefault("open_trade", None)
    payload.setdefault("last_mark_price", None)
    payload.setdefault("updated_at", utc_now_iso())
    return payload


def paper_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    trades = state.get("closed_trades", [])
    if not trades:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "net_pnl": 0.0,
            "current_capital": state.get("current_capital", state.get("initial_capital", 0.0)),
            "open_trade": state.get("open_trade"),
        }
    wins = sum(1 for item in trades if float(item.get("net_pnl", 0.0)) > 0)
    losses = len(trades) - wins
    net_pnl = sum(float(item.get("net_pnl", 0.0)) for item in trades)
    return {
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(trades) * 100.0,
        "net_pnl": net_pnl,
        "current_capital": state.get("current_capital", state.get("initial_capital", 0.0)),
        "open_trade": state.get("open_trade"),
    }


def open_paper_trade(
    state: Dict[str, Any],
    execution_payload: Dict[str, Any],
    max_hold_minutes: int,
    fee_per_side: float,
) -> Optional[Dict[str, Any]]:
    plan = execution_payload.get("plan")
    quantity = execution_payload.get("quantity")
    if not isinstance(plan, dict) or quantity in (None, 0):
        return None
    entry_time = utc_now_iso()
    open_trade = {
        "signal_key": f"{plan.get('source')}|{plan.get('module_name')}|{plan.get('direction')}|{plan.get('signal_time')}",
        "source": plan.get("source"),
        "strategy_name": plan.get("strategy_name"),
        "module_name": plan.get("module_name"),
        "symbol": plan.get("symbol"),
        "direction": plan.get("direction"),
        "entry_time": entry_time,
        "entry_price": float(plan.get("entry_price", 0.0)),
        "stop_price": float(plan.get("stop_price", 0.0)),
        "tp1_price": float(plan.get("tp1_price", 0.0)),
        "tp2_price": float(plan.get("tp2_price", 0.0)),
        "quantity_initial": float(quantity),
        "quantity_open": float(quantity),
        "realized_pnl": 0.0,
        "tp1_taken": False,
        "fee_per_side": fee_per_side,
        "risk_points": abs(float(plan.get("entry_price", 0.0)) - float(plan.get("stop_price", 0.0))),
        "expires_at": datetime.now(timezone.utc).timestamp() + max_hold_minutes * 60.0,
    }
    state["open_trade"] = open_trade
    state["updated_at"] = utc_now_iso()
    return open_trade


def maybe_close_paper_trade(state: Dict[str, Any], candle: Dict[str, Any], trades_file: Path) -> Optional[Dict[str, Any]]:
    trade = state.get("open_trade")
    if not isinstance(trade, dict):
        return None
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])
    direction = trade["direction"]
    entry_price = float(trade["entry_price"])
    quantity_initial = float(trade["quantity_initial"])
    quantity_open = float(trade["quantity_open"])
    stop_price = float(trade["stop_price"])
    tp1_price = float(trade["tp1_price"])
    tp2_price = float(trade["tp2_price"])
    realized_pnl = float(trade["realized_pnl"])
    tp1_taken = bool(trade["tp1_taken"])
    risk_points = max(float(trade["risk_points"]), 1e-9)
    now_ts = datetime.now(timezone.utc).timestamp()
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None

    if direction == "long":
        if (not tp1_taken) and high >= tp1_price:
            realized_pnl += quantity_initial * 0.5 * (tp1_price - entry_price)
            quantity_open = quantity_initial * 0.5
            stop_price = max(stop_price, entry_price + 0.08 * risk_points)
            tp1_taken = True
        if low <= stop_price:
            exit_price = stop_price
            exit_reason = "stop"
        elif high >= tp2_price:
            exit_price = tp2_price
            exit_reason = "tp2"
    else:
        if (not tp1_taken) and low <= tp1_price:
            realized_pnl += quantity_initial * 0.5 * (entry_price - tp1_price)
            quantity_open = quantity_initial * 0.5
            stop_price = min(stop_price, entry_price - 0.08 * risk_points)
            tp1_taken = True
        if high >= stop_price:
            exit_price = stop_price
            exit_reason = "stop"
        elif low <= tp2_price:
            exit_price = tp2_price
            exit_reason = "tp2"

    if exit_price is None and now_ts >= float(trade["expires_at"]):
        exit_price = close
        exit_reason = "time_stop"

    trade["quantity_open"] = quantity_open
    trade["realized_pnl"] = realized_pnl
    trade["tp1_taken"] = tp1_taken
    trade["stop_price"] = stop_price
    state["last_mark_price"] = close

    if exit_price is None or exit_reason is None:
        state["updated_at"] = utc_now_iso()
        return None

    gross = realized_pnl + quantity_open * ((exit_price - entry_price) if direction == "long" else (entry_price - exit_price))
    fees = quantity_initial * entry_price * float(trade["fee_per_side"]) * 2.0
    net_pnl = gross - fees
    closed = {
        "time": utc_now_iso(),
        "symbol": trade["symbol"],
        "source": trade["source"],
        "strategy_name": trade["strategy_name"],
        "module_name": trade["module_name"],
        "direction": direction,
        "entry_time": trade["entry_time"],
        "exit_time": utc_now_iso(),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "stop_price": stop_price,
        "tp1_price": tp1_price,
        "tp2_price": tp2_price,
        "quantity": quantity_initial,
        "net_pnl": net_pnl,
        "r_multiple": net_pnl / max(quantity_initial * risk_points, 1e-9),
        "exit_reason": exit_reason,
        "tp1_taken": tp1_taken,
    }
    state["closed_trades"].append(closed)
    state["current_capital"] = float(state.get("current_capital", state.get("initial_capital", 0.0))) + net_pnl
    state["open_trade"] = None
    state["updated_at"] = utc_now_iso()
    append_jsonl(trades_file, closed)
    return closed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Continuous live daemon for correction package.")
    parser.add_argument("--paper", choices=["true", "false"], default=None)
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--data-mode", choices=["cache", "live"], default=os.getenv("DATA_MODE", "live"))
    parser.add_argument("--days", type=int, default=int(os.getenv("LIVE_DAYS", "30")))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "ETH-USDT"))
    parser.add_argument("--initial-capital", type=float, default=float(os.getenv("INITIAL_CAPITAL", "10000")))
    parser.add_argument("--risk-percent", type=float, default=float(os.getenv("RISK_PERCENT", "1")))
    parser.add_argument("--qty", type=float, default=None)
    parser.add_argument("--lookback-bars", type=int, default=int(os.getenv("LOOKBACK_BARS", "6")))
    parser.add_argument("--entry-tolerance-r", type=float, default=float(os.getenv("ENTRY_TOLERANCE_R", "0.25")))
    parser.add_argument("--max-signal-age-minutes", type=int, default=int(os.getenv("MAX_SIGNAL_AGE_MINUTES", "180")))
    parser.add_argument("--trend-profile", default=os.getenv("TREND_PROFILE", "profit_max_locked"))
    parser.add_argument("--sweep-profile", default=os.getenv("SWEEP_PROFILE", "off"))
    parser.add_argument("--poll-seconds", type=int, default=int(os.getenv("POLL_SECONDS", "60")))
    parser.add_argument("--heartbeat-minutes", type=int, default=int(os.getenv("HEARTBEAT_MINUTES", "30")))
    parser.add_argument("--max-loops", type=int, default=0, help="0 means run forever")
    parser.add_argument("--state-file", default=str(STATE_DIR / "live_state.json"))
    parser.add_argument("--output-file", default=str(BASE_DIR / "reports" / "live_scan" / "latest_signal.json"))
    parser.add_argument("--daemon-state-file", default=str(STATE_DIR / "daemon_state.json"))
    parser.add_argument("--event-log-file", default=str(LOGS_DIR / "daemon_events.jsonl"))
    parser.add_argument("--paper-state-file", default=str(STATE_DIR / "paper_state.json"))
    parser.add_argument("--paper-report-file", default=str(REPORTS_DIR / "paper_report.json"))
    parser.add_argument("--paper-trades-file", default=str(REPORTS_DIR / "paper_trades.jsonl"))
    parser.add_argument("--paper-max-hold-minutes", type=int, default=int(os.getenv("PAPER_MAX_HOLD_MINUTES", "480")))
    parser.add_argument("--paper-fee-per-side", type=float, default=float(os.getenv("PAPER_FEE_PER_SIDE", "0.0005")))
    return parser


def main() -> None:
    from correction_exchange import BingXExchange, ExchangeConfig
    from correction_live import execute_plan, scan_plans

    parser = build_parser()
    args = parser.parse_args()
    daemon_state_path = Path(args.daemon_state_file)
    event_log_path = Path(args.event_log_file)
    paper_state_path = Path(args.paper_state_file)
    paper_report_path = Path(args.paper_report_file)
    paper_trades_path = Path(args.paper_trades_file)
    notifier = build_notifier()
    paper_mode = parse_bool(args.paper if args.paper is not None else os.getenv("PAPER", "true"))
    daemon_state_path.parent.mkdir(parents=True, exist_ok=True)
    event_log_path.parent.mkdir(parents=True, exist_ok=True)
    paper_state_path.parent.mkdir(parents=True, exist_ok=True)
    paper_report_path.parent.mkdir(parents=True, exist_ok=True)
    paper_trades_path.parent.mkdir(parents=True, exist_ok=True)
    last_heartbeat = 0.0
    loop_index = 0
    paper_state = load_paper_state(paper_state_path, args.initial_capital)
    market_exchange = BingXExchange(ExchangeConfig())

    start_event = {
        "time": utc_now_iso(),
        "type": "daemon_start",
        "symbol": args.symbol,
        "paper": paper_mode,
        "data_mode": args.data_mode,
        "trend_profile": args.trend_profile,
        "sweep_profile": args.sweep_profile,
        "poll_seconds": args.poll_seconds,
    }
    append_event(event_log_path, start_event)
    save_json(paper_state_path, paper_state)
    save_json(paper_report_path, paper_summary(paper_state))
    notify_telegram(
        notifier,
        "system started",
        {
            "symbol": args.symbol,
            "paper": paper_mode,
            "data_mode": args.data_mode,
            "trend_profile": args.trend_profile,
            "sweep_profile": args.sweep_profile,
            "poll_seconds": args.poll_seconds,
        },
    )

    while True:
        loop_index += 1
        loop_started = time.time()
        try:
            if paper_mode and paper_state.get("open_trade") is not None and args.data_mode == "live":
                frame = market_exchange.get_klines(args.symbol, interval="1m", limit=1)
                if not frame.empty:
                    latest = frame.iloc[-1]
                    closed_trade = maybe_close_paper_trade(
                        paper_state,
                        {
                            "high": float(latest["high"]),
                            "low": float(latest["low"]),
                            "close": float(latest["close"]),
                        },
                        paper_trades_path,
                    )
                    save_json(paper_state_path, paper_state)
                    save_json(paper_report_path, paper_summary(paper_state))
                    if closed_trade is not None:
                        notify_telegram(
                            notifier,
                            "paper trade closed",
                            {
                                "symbol": closed_trade["symbol"],
                                "source": closed_trade["source"],
                                "direction": closed_trade["direction"],
                                "net_pnl": closed_trade["net_pnl"],
                                "exit_reason": closed_trade["exit_reason"],
                                "capital": paper_state["current_capital"],
                            },
                        )

            scan_payload = scan_plans(args)
            event_payload: Dict[str, Any] = {
                "time": utc_now_iso(),
                "type": "scan",
                "loop": loop_index,
                "plans_found": scan_payload.get("plans_found", 0),
                "selected_plan": scan_payload.get("selected_plan"),
                "paper_mode": paper_mode,
            }
            execution_payload = None

            if paper_mode and paper_state.get("open_trade") is not None:
                event_payload["paper_open_trade"] = paper_state.get("open_trade")
            elif scan_payload.get("selected_plan") is not None:
                selected_plan = scan_payload["selected_plan"]
                notify_telegram(
                    notifier,
                    "signal detected",
                    {
                        "loop": loop_index,
                        "symbol": selected_plan.get("symbol"),
                        "source": selected_plan.get("source"),
                        "module": selected_plan.get("module_name"),
                        "direction": selected_plan.get("direction"),
                        "entry": selected_plan.get("entry_price"),
                        "stop": selected_plan.get("stop_price"),
                        "tp1": selected_plan.get("tp1_price"),
                        "confidence": selected_plan.get("confidence"),
                    },
                )
                execution_payload = execute_plan(args, scan_payload)
                event_payload["execution"] = execution_payload
                if paper_mode and execution_payload.get("ok"):
                    event_payload["paper_open_trade"] = open_paper_trade(
                        paper_state,
                        execution_payload,
                        args.paper_max_hold_minutes,
                        args.paper_fee_per_side,
                    )
                    save_json(paper_state_path, paper_state)
                    save_json(paper_report_path, paper_summary(paper_state))
                notify_telegram(
                    notifier,
                    "execution result",
                    {
                        "loop": loop_index,
                        "ok": execution_payload.get("ok"),
                        "msg": execution_payload.get("msg"),
                        "symbol": execution_payload.get("symbol") or selected_plan.get("symbol"),
                        "quantity": execution_payload.get("quantity"),
                        "paper_trading": execution_payload.get("paper_trading"),
                        "order_id": (execution_payload.get("order_result") or {}).get("order_id"),
                    },
                )

            append_event(event_log_path, event_payload)
            save_json(
                daemon_state_path,
                {
                    "last_loop": loop_index,
                    "last_run_at": utc_now_iso(),
                    "last_scan": scan_payload,
                    "last_execution": execution_payload,
                    "paper_mode": paper_mode,
                    "paper_summary": paper_summary(paper_state) if paper_mode else None,
                    "poll_seconds": args.poll_seconds,
                    "trend_profile": args.trend_profile,
                    "sweep_profile": args.sweep_profile,
                },
            )
        except Exception as exc:
            error_payload = {
                "time": utc_now_iso(),
                "type": "error",
                "loop": loop_index,
                "error": str(exc),
            }
            append_event(event_log_path, error_payload)
            save_json(
                daemon_state_path,
                {
                    "last_loop": loop_index,
                    "last_run_at": utc_now_iso(),
                    "last_error": error_payload,
                    "paper_mode": paper_mode,
                    "paper_summary": paper_summary(paper_state) if paper_mode else None,
                    "poll_seconds": args.poll_seconds,
                    "trend_profile": args.trend_profile,
                    "sweep_profile": args.sweep_profile,
                },
            )
            notify_telegram(
                notifier,
                "error",
                {
                    "loop": loop_index,
                    "error": str(exc),
                    "symbol": args.symbol,
                },
            )

        now_ts = time.time()
        if now_ts - last_heartbeat >= args.heartbeat_minutes * 60:
            heartbeat_payload = {
                "time": utc_now_iso(),
                "type": "heartbeat",
                "loop": loop_index,
                "status": "running",
                "paper_mode": paper_mode,
                "poll_seconds": args.poll_seconds,
                "trend_profile": args.trend_profile,
                "sweep_profile": args.sweep_profile,
            }
            append_event(event_log_path, heartbeat_payload)
            notify_telegram(
                notifier,
                "heartbeat",
                {
                    "loop": loop_index,
                    "status": "running",
                    "symbol": args.symbol,
                    "trend_profile": args.trend_profile,
                    "sweep_profile": args.sweep_profile,
                    "paper_mode": paper_mode,
                    "paper_trades": paper_summary(paper_state)["trades"] if paper_mode else None,
                },
            )
            last_heartbeat = now_ts

        if args.max_loops > 0 and loop_index >= args.max_loops:
            break

        elapsed = time.time() - loop_started
        time.sleep(max(args.poll_seconds - elapsed, 0.0))


if __name__ == "__main__":
    main()
