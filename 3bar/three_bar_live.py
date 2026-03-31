from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
for candidate in (str(BASE_DIR), str(PROJECT_ROOT), str(PROJECT_ROOT / "correction")):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

RUNTIME_ROOT = Path(os.getenv("THREE_BAR_RUNTIME_ROOT", str(BASE_DIR))).resolve()
from correction_exchange import (
    BingXExchange,
    ExchangeConfig,
    OrderRequest,
    ProtectionRequest,
    parse_bool,
    require_live_confirmation,
)
from bingx_regime_fib_backtest import BingXClient, atr, find_pivots
from three_bar_level_system import HourSimpleLevelSystem, HourSystemConfig

STATE_DIR = RUNTIME_ROOT / "state"
REPORTS_DIR = RUNTIME_ROOT / "reports" / "live"
ENV_PATH = Path(os.getenv("THREE_BAR_ENV_PATH", str(BASE_DIR / ".env"))).resolve()
PAPER_TRADES_PATH = REPORTS_DIR / "paper_trades.jsonl"


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


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


class LiveThreeBarSystem(HourSimpleLevelSystem):
    def prepare_live_frame(self, symbol: str, days: int) -> pd.DataFrame:
        end_time = now_utc()
        start_time = end_time - timedelta(days=days)
        df = self.client.fetch_klines(symbol, "1h", start_time, end_time)
        df["atr"] = atr(df, 14)
        df["ema_fast"] = df["close"].ewm(span=self.config.ema_fast_period, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.config.ema_slow_period, adjust=False).mean()
        pivots = find_pivots(df, width=self.config.pivot_width)
        df["pivot_high"] = pivots["pivot_high"]
        df["pivot_low"] = pivots["pivot_low"]
        return df.dropna(subset=["atr"]).copy()

    def prepare_cached_frame(self, symbol: str, days: int) -> pd.DataFrame:
        cache_path = BASE_DIR / "data_cache" / f"{symbol.replace('-', '_')}_1h_{days}d.csv"
        if not cache_path.exists():
            raise FileNotFoundError(f"Cached frame not found: {cache_path}")
        df = pd.read_csv(cache_path, parse_dates=["time"])
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.set_index("time").sort_index()
        df["atr"] = atr(df, 14)
        df["ema_fast"] = df["close"].ewm(span=self.config.ema_fast_period, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.config.ema_slow_period, adjust=False).mean()
        pivots = find_pivots(df, width=self.config.pivot_width)
        df["pivot_high"] = pivots["pivot_high"]
        df["pivot_low"] = pivots["pivot_low"]
        return df.dropna(subset=["atr"]).copy()


def build_exchange(args: argparse.Namespace) -> BingXExchange:
    paper_value = os.getenv("PAPER", "true") if args.paper is None else args.paper
    config = ExchangeConfig(
        paper_trading=parse_bool(paper_value),
        default_symbol=args.symbol,
    )
    return BingXExchange(config)


def build_system(args: argparse.Namespace) -> LiveThreeBarSystem:
    config = HourSystemConfig(
        days=args.days,
        initial_capital=args.initial_capital,
        risk_per_trade=args.risk_percent / 100.0,
        level_mode=os.getenv("LEVEL_MODE", "rolling_window"),
        level_lookback_bars=int(os.getenv("LEVEL_LOOKBACK_BARS", "6")),
        level_tolerance_atr=float(os.getenv("LEVEL_TOLERANCE_ATR", "0.8")),
        long_trend_filter=os.getenv("LONG_TREND_FILTER", "ema"),
        short_trend_filter=os.getenv("SHORT_TREND_FILTER", "none"),
        min_pin_wick_body_ratio=float(os.getenv("MIN_PIN_WICK_BODY_RATIO", "0.35")),
        min_pin_range_atr=float(os.getenv("MIN_PIN_RANGE_ATR", "0.60")),
        trail_activation_r=float(os.getenv("TRAIL_ACTIVATION_R", "1.4")),
        trail_lookback_bars=int(os.getenv("TRAIL_LOOKBACK_BARS", "4")),
        max_hold_bars=int(os.getenv("MAX_HOLD_BARS", "48")),
    )
    client = BingXClient()
    return LiveThreeBarSystem(config, client, cache=None, output_dir=REPORTS_DIR)  # type: ignore[arg-type]


def signal_age_minutes(signal_time: str) -> float:
    signal_dt = pd.Timestamp(signal_time).to_pydatetime()
    return (now_utc() - signal_dt).total_seconds() / 60.0


def compute_quantity(initial_capital: float, risk_percent: float, entry_price: float, stop_price: float, qty_precision: int) -> float:
    risk_points = abs(entry_price - stop_price)
    if risk_points <= 0:
        return 0.0
    quantity = (initial_capital * (risk_percent / 100.0)) / risk_points
    return round(max(quantity, 0.0), qty_precision)


def paper_positions_exist(state: Dict[str, Any], symbol: str) -> bool:
    active = state.get("active_trade")
    return bool(active and active.get("symbol") == symbol)


def paper_orders_exist(state: Dict[str, Any], symbol: str) -> bool:
    active = state.get("active_trade")
    return bool(active and active.get("symbol") == symbol)


def finalize_paper_trade(
    args: argparse.Namespace,
    state: Dict[str, Any],
    active_trade: Dict[str, Any],
    exit_price: float,
    exit_time: str,
    exit_reason: str,
) -> Dict[str, Any]:
    direction = active_trade["direction"]
    quantity = float(active_trade["quantity"])
    entry_price = float(active_trade["entry_price"])
    gross_move = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
    gross_pnl = gross_move * quantity
    fee_per_side = 0.0005
    fees = quantity * (entry_price + exit_price) * fee_per_side
    net_pnl = gross_pnl - fees
    record = {
        "time": now_utc().isoformat(),
        "symbol": args.symbol,
        "direction": direction,
        "entry_time": active_trade["entry_time"],
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "quantity": quantity,
        "stop_price": float(active_trade["stop_price"]),
        "trail_activation_r": float(active_trade["trail_activation_r"]),
        "trail_lookback_bars": int(active_trade["trail_lookback_bars"]),
        "exit_reason": exit_reason,
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "paper_trading": True,
        "signal_key": active_trade.get("signal_key"),
    }
    append_jsonl(PAPER_TRADES_PATH, record)
    state["last_paper_trade"] = record
    state["active_trade"] = None
    return {"ok": True, "msg": f"Paper trade closed: {exit_reason}", "paper_trade": record}


def scan_latest_signal(system: LiveThreeBarSystem, symbol: str, lookback_bars: int, data_mode: str) -> Optional[Dict[str, Any]]:
    df = system.prepare_cached_frame(symbol, system.config.days) if data_mode == "cache" else system.prepare_live_frame(symbol, system.config.days)
    start_idx = max(system.config.pivot_width * 2 + 2, 20)
    latest = None
    for idx in range(max(start_idx, len(df) - max(lookback_bars, 8)), len(df) - 1):
        signal = system.build_signal(symbol, df, idx)
        if signal is None:
            continue
        trigger_candle = df.iloc[idx + 1]
        if signal.direction == "long" and float(trigger_candle["low"]) <= signal.stop_price:
            continue
        if signal.direction == "short" and float(trigger_candle["high"]) >= signal.stop_price:
            continue
        latest = {
            "signal": signal,
            "entry_idx": idx + 1,
            "data": df,
        }
    if latest is None:
        return None
    signal = latest["signal"]
    entry_idx = latest["entry_idx"]
    frame = latest["data"]
    return {
        "symbol": signal.symbol,
        "direction": signal.direction,
        "signal_time": signal.setup_time,
        "signal_age_minutes": signal_age_minutes(signal.setup_time),
        "entry_price": signal.entry_price,
        "stop_price": signal.stop_price,
        "target_price": signal.target_price,
        "risk_points": signal.risk_points,
        "entry_idx": entry_idx,
        "entry_time": str(frame.index[entry_idx]),
        "signal_key": f"{signal.symbol}|{signal.direction}|{signal.setup_time}",
        "level_kind": signal.level_kind,
        "level_price": signal.level_price,
        "signal": asdict(signal),
    }


def scan_plans(args: argparse.Namespace) -> Dict[str, Any]:
    exchange = build_exchange(args)
    state_path = Path(args.state_file)
    state = load_json(state_path)
    system = build_system(args)
    selected_plan = scan_latest_signal(system, args.symbol, args.lookback_bars, args.data_mode)
    if selected_plan is not None:
        if selected_plan["signal_age_minutes"] > args.max_signal_age_minutes:
            selected_plan = None
        elif state.get("last_signal_key") == selected_plan["signal_key"]:
            selected_plan = None
    payload = {
        "time": now_utc().isoformat(),
        "symbol": args.symbol,
        "paper_trading": exchange.paper_trading,
        "selected_plan": selected_plan,
        "state_file": str(state_path),
        "active_trade": state.get("active_trade"),
    }
    save_json(Path(args.output_file), payload)
    return payload


def update_trailing_stop(args: argparse.Namespace, exchange: BingXExchange, active_trade: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    direction = active_trade["direction"]
    position = exchange.get_open_position(args.symbol, direction)
    if position is None and not exchange.paper_trading:
        return {"ok": False, "msg": "No open position found for active trade management"}
    last_close_time = pd.Timestamp(active_trade["last_bar_time"]) if active_trade.get("last_bar_time") else None
    current_bar_time = df.index[-1]
    if last_close_time is not None and current_bar_time <= last_close_time:
        return {"ok": True, "msg": "No new closed bar yet", "updated": False}
    entry_price = float(active_trade["entry_price"])
    stop_price = float(active_trade["stop_price"])
    risk_points = abs(entry_price - stop_price)
    if not active_trade.get("armed_trail"):
        if direction == "long" and float(df["high"].iloc[-1]) >= entry_price + risk_points * active_trade["trail_activation_r"]:
            active_trade["armed_trail"] = True
            stop_price = max(stop_price, entry_price)
        elif direction == "short" and float(df["low"].iloc[-1]) <= entry_price - risk_points * active_trade["trail_activation_r"]:
            active_trade["armed_trail"] = True
            stop_price = min(stop_price, entry_price)
    if active_trade.get("armed_trail"):
        window = df.iloc[-active_trade["trail_lookback_bars"] :]
        if direction == "long":
            stop_price = max(stop_price, float(window["low"].min()))
            fail_safe_tp = entry_price + risk_points * 20.0
        else:
            stop_price = min(stop_price, float(window["high"].max()))
            fail_safe_tp = entry_price - risk_points * 20.0
        protection = exchange.set_protection_orders(
            ProtectionRequest(
                symbol=args.symbol,
                direction=direction,
                stop_price=stop_price,
                take_profit_price=fail_safe_tp,
                quantity=float(active_trade["quantity"]),
            )
        )
        active_trade["stop_price"] = stop_price
        active_trade["last_bar_time"] = str(current_bar_time)
        active_trade["protection_result"] = protection
        return {"ok": protection.get("ok", False), "msg": "Trailing protection updated", "updated": True, "stop_price": stop_price}
    active_trade["last_bar_time"] = str(current_bar_time)
    return {"ok": True, "msg": "Trade not armed for trailing yet", "updated": False, "stop_price": stop_price}


def maybe_close_paper_trade(
    args: argparse.Namespace,
    state: Dict[str, Any],
    active_trade: Dict[str, Any],
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    current_bar = df.iloc[-1]
    high = float(current_bar["high"])
    low = float(current_bar["low"])
    stop_price = float(active_trade["stop_price"])
    if active_trade["direction"] == "long" and low <= stop_price:
        return finalize_paper_trade(args, state, active_trade, stop_price, str(df.index[-1]), "paper_stop")
    if active_trade["direction"] == "short" and high >= stop_price:
        return finalize_paper_trade(args, state, active_trade, stop_price, str(df.index[-1]), "paper_stop")
    entry_time = pd.Timestamp(active_trade["entry_time"])
    if pd.Timestamp(df.index[-1]) >= entry_time + pd.Timedelta(hours=int(active_trade["max_hold_bars"])):
        exit_price = float(current_bar["close"])
        return finalize_paper_trade(args, state, active_trade, exit_price, str(df.index[-1]), "paper_timeout")
    return None


def execute_plan(args: argparse.Namespace, scan_payload: Dict[str, Any]) -> Dict[str, Any]:
    exchange = build_exchange(args)
    state_path = Path(args.state_file)
    state = load_json(state_path)
    active_trade = state.get("active_trade")
    system = build_system(args)
    live_df = system.prepare_cached_frame(args.symbol, system.config.days) if args.data_mode == "cache" else system.prepare_live_frame(args.symbol, system.config.days)

    if active_trade is not None:
        open_position = exchange.get_open_position(args.symbol, active_trade["direction"])
        if exchange.paper_trading:
            manage_payload = update_trailing_stop(args, exchange, active_trade, live_df)
            state["active_trade"] = active_trade
            closed_payload = maybe_close_paper_trade(args, state, active_trade, live_df)
            save_json(state_path, state)
            return closed_payload or manage_payload
        if open_position is None and not exchange.paper_trading:
            state["active_trade"] = None
            save_json(state_path, state)
        else:
            entry_time = pd.Timestamp(active_trade["entry_time"])
            if pd.Timestamp(live_df.index[-1]) >= entry_time + pd.Timedelta(hours=int(active_trade["max_hold_bars"])):
                require_live_confirmation(exchange, args.confirm_live, "close")
                close_payload = exchange.close_position_market(args.symbol, active_trade["direction"])
                state["active_trade"] = None
                save_json(state_path, state)
                return {"ok": close_payload.get("ok", False), "msg": "Timeout close executed", "close_result": close_payload}
            manage_payload = update_trailing_stop(args, exchange, active_trade, live_df)
            state["active_trade"] = active_trade
            save_json(state_path, state)
            return manage_payload

    selected_plan = scan_payload.get("selected_plan")
    if selected_plan is None:
        return {"ok": True, "msg": "No fresh signal"}

    if (exchange.paper_trading and paper_positions_exist(state, args.symbol)) or (not exchange.paper_trading and exchange.get_positions(args.symbol)):
        return {"ok": False, "msg": "Existing position found, skip new signal"}
    if (exchange.paper_trading and paper_orders_exist(state, args.symbol)) or (not exchange.paper_trading and exchange.get_open_orders(args.symbol)):
        return {"ok": False, "msg": "Existing open orders found, skip new signal"}

    quantity = compute_quantity(args.initial_capital, args.risk_percent, selected_plan["entry_price"], selected_plan["stop_price"], exchange.config.qty_precision)
    if quantity <= 0:
        return {"ok": False, "msg": "Quantity resolved to zero"}

    require_live_confirmation(exchange, args.confirm_live, "execute")
    side = "BUY" if selected_plan["direction"] == "long" else "SELL"
    order_result = exchange.place_market_order(OrderRequest(symbol=args.symbol, side=side, quantity=quantity))
    risk_points = abs(float(selected_plan["entry_price"]) - float(selected_plan["stop_price"]))
    fail_safe_tp = (
        float(selected_plan["entry_price"]) + risk_points * 20.0
        if selected_plan["direction"] == "long"
        else float(selected_plan["entry_price"]) - risk_points * 20.0
    )
    protection_result = exchange.set_protection_orders(
        ProtectionRequest(
            symbol=args.symbol,
            direction=selected_plan["direction"],
            stop_price=float(selected_plan["stop_price"]),
            take_profit_price=fail_safe_tp,
            quantity=quantity,
        )
    )
    state["last_signal_key"] = selected_plan["signal_key"]
    state["active_trade"] = {
        "symbol": args.symbol,
        "direction": selected_plan["direction"],
        "entry_time": selected_plan["entry_time"],
        "signal_time": selected_plan["signal_time"],
        "entry_price": float(selected_plan["entry_price"]),
        "stop_price": float(selected_plan["stop_price"]),
        "quantity": quantity,
        "armed_trail": False,
        "trail_activation_r": system.config.trail_activation_r,
        "trail_lookback_bars": system.config.trail_lookback_bars,
        "max_hold_bars": system.config.max_hold_bars,
        "last_bar_time": str(live_df.index[-1]),
        "signal_key": selected_plan["signal_key"],
        "paper_trading": exchange.paper_trading,
    }
    save_json(state_path, state)
    return {
        "ok": bool(order_result.get("ok")) and bool(protection_result.get("ok")),
        "msg": "Signal executed",
        "symbol": args.symbol,
        "direction": selected_plan["direction"],
        "quantity": quantity,
        "paper_trading": exchange.paper_trading,
        "order_result": order_result,
        "protection_result": protection_result,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="3bar live scan and execution.")
    parser.add_argument("command", choices=["scan", "execute"], nargs="?", default="scan")
    parser.add_argument("--paper", choices=["true", "false"], default=None)
    parser.add_argument("--confirm-live", action="store_true")
    parser.add_argument("--data-mode", choices=["cache", "live"], default="live")
    parser.add_argument("--days", type=int, default=int(os.getenv("LIVE_DAYS", "180")))
    parser.add_argument("--symbol", default=os.getenv("SYMBOL", "ETH-USDT"))
    parser.add_argument("--initial-capital", type=float, default=float(os.getenv("INITIAL_CAPITAL", "10000")))
    parser.add_argument("--risk-percent", type=float, default=float(os.getenv("RISK_PERCENT", "1")))
    parser.add_argument("--lookback-bars", type=int, default=int(os.getenv("LOOKBACK_BARS", "6")))
    parser.add_argument("--max-signal-age-minutes", type=int, default=int(os.getenv("MAX_SIGNAL_AGE_MINUTES", "180")))
    parser.add_argument("--state-file", default=str(STATE_DIR / "live_state.json"))
    parser.add_argument("--output-file", default=str(REPORTS_DIR / "latest_signal.json"))
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    scan_payload = scan_plans(args)
    if args.command == "scan":
        print(json.dumps(scan_payload, indent=2, ensure_ascii=False))
        return
    result = execute_plan(args, scan_payload)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
