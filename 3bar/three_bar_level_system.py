from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from bingx_regime_fib_backtest import BingXClient, DataCache, atr, find_pivots, max_drawdown


def candle_color(candle: pd.Series) -> str:
    if float(candle["close"]) > float(candle["open"]):
        return "bull"
    if float(candle["close"]) < float(candle["open"]):
        return "bear"
    return "flat"


def body_low(candle: pd.Series) -> float:
    return float(min(candle["open"], candle["close"]))


def body_high(candle: pd.Series) -> float:
    return float(max(candle["open"], candle["close"]))


def candle_parts(candle: pd.Series) -> dict:
    candle_range = max(float(candle["high"]) - float(candle["low"]), 1e-9)
    return {
        "range": candle_range,
        "body": abs(float(candle["close"]) - float(candle["open"])),
        "upper_wick": float(candle["high"]) - body_high(candle),
        "lower_wick": body_low(candle) - float(candle["low"]),
    }


@dataclass
class HourSystemConfig:
    days: int = 180
    fee_per_side: float = 0.0005
    initial_capital: float = 10000.0
    risk_per_trade: float = 0.01
    level_mode: str = "rolling_window"
    pivot_width: int = 3
    ema_fast_period: int = 50
    ema_slow_period: int = 200
    level_lookback_bars: int = 6
    level_tolerance_atr: float = 0.80
    level_cluster_tolerance_atr: float = 0.20
    min_level_pivots: int = 2
    min_level_touches: int = 3
    long_trend_filter: str = "ema"
    short_trend_filter: str = "none"
    engulf_mode: str = "body_break"
    min_pin_wick_body_ratio: float = 0.35
    min_pin_range_atr: float = 0.60
    exit_mode: str = "trail_after_r"
    trail_activation_r: float = 1.40
    trail_lookback_bars: int = 4
    stop_buffer_atr: float = 0.00
    trigger_buffer_atr: float = 0.00
    target_r: float = 1.0
    max_hold_bars: int = 48


@dataclass
class HourSignal:
    symbol: str
    direction: str
    setup_time: str
    pin_time: str
    engulf_time: str
    trigger_time: str
    level_price: float
    level_kind: str
    level_touches: int
    entry_price: float
    stop_price: float
    target_price: float
    risk_points: float
    ema_fast: float
    ema_slow: float
    pin_open: float
    pin_high: float
    pin_low: float
    pin_close: float
    engulf_open: float
    engulf_high: float
    engulf_low: float
    engulf_close: float
    trigger_open: float
    trigger_high: float
    trigger_low: float
    trigger_close: float


@dataclass
class HourTrade:
    symbol: str
    direction: str
    setup_time: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    stop_price: float
    target_price: float
    risk_points: float
    r_multiple: float
    net_pnl: float
    hold_bars: int
    exit_reason: str
    level_kind: str
    level_price: float
    level_touches: int


class HourSimpleLevelSystem:
    def __init__(self, config: HourSystemConfig, client: BingXClient, cache: DataCache, output_dir: Path) -> None:
        self.config = config
        self.client = client
        self.cache = cache
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def prepare_frame(self, symbol: str) -> pd.DataFrame:
        df = self.cache.load_or_fetch(self.client, symbol, "1h", self.config.days).copy()
        df["atr"] = atr(df, 14)
        df["ema_fast"] = df["close"].ewm(span=self.config.ema_fast_period, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.config.ema_slow_period, adjust=False).mean()
        pivots = find_pivots(df, width=self.config.pivot_width)
        df["pivot_high"] = pivots["pivot_high"]
        df["pivot_low"] = pivots["pivot_low"]
        return df.dropna(subset=["atr"]).copy()

    def pivot_cluster_level(self, df: pd.DataFrame, idx: int, direction: str, atr_now: float) -> Optional[tuple[str, float, int]]:
        if idx <= 0:
            return None
        start = max(0, idx - self.config.level_lookback_bars)
        window = df.iloc[start:idx]
        if window.empty:
            return None
        if direction == "short":
            pivots = window["pivot_high"].dropna()
            level_kind = "resistance"
            touch_col = "high"
        else:
            pivots = window["pivot_low"].dropna()
            level_kind = "support"
            touch_col = "low"
        if pivots.empty:
            return None
        cluster_tolerance = atr_now * self.config.level_cluster_tolerance_atr
        touch_tolerance = atr_now * self.config.level_tolerance_atr
        recent_candidates = list(reversed([float(value) for value in pivots.iloc[-8:]]))
        best_match: Optional[tuple[str, float, int]] = None
        best_score = -1.0
        for candidate in recent_candidates:
            cluster = pivots[(pivots - candidate).abs() <= cluster_tolerance]
            if len(cluster) < self.config.min_level_pivots:
                continue
            level_price = float(cluster.mean())
            touch_count = int(((window[touch_col] - level_price).abs() <= touch_tolerance).sum())
            if touch_count < self.config.min_level_touches:
                continue
            score = touch_count * 10.0 + len(cluster)
            if score > best_score:
                best_score = score
                best_match = (level_kind, level_price, touch_count)
        return best_match

    def recent_level(self, df: pd.DataFrame, idx: int, direction: str, atr_now: float) -> Optional[tuple[str, float, int]]:
        if idx <= 0:
            return None
        start = max(0, idx - self.config.level_lookback_bars)
        window = df.iloc[start:idx]
        if window.empty:
            return None
        if self.config.level_mode == "pivot_cluster":
            return self.pivot_cluster_level(df, idx, direction, atr_now)
        if direction == "long":
            return "support", float(window["low"].min()), 1
        return "resistance", float(window["high"].max()), 1

    @staticmethod
    def trend_allows(candle: pd.Series, direction: str, trend_filter: str) -> bool:
        if trend_filter == "none":
            return True
        ema_fast = float(candle["ema_fast"])
        ema_slow = float(candle["ema_slow"])
        close = float(candle["close"])
        if not np.isfinite(ema_fast) or not np.isfinite(ema_slow):
            return False
        if direction == "long":
            return ema_fast > ema_slow and close > ema_fast
        return ema_fast < ema_slow and close < ema_fast

    def pin_bar_near_level(self, candle: pd.Series, atr_now: float, direction: str, level_price: float) -> bool:
        parts = candle_parts(candle)
        if parts["body"] <= 0:
            return False
        if parts["range"] < atr_now * self.config.min_pin_range_atr:
            return False
        tolerance = atr_now * self.config.level_tolerance_atr
        wick_ratio = self.config.min_pin_wick_body_ratio
        if direction == "short":
            return (
                parts["upper_wick"] >= parts["body"] * wick_ratio
                and parts["upper_wick"] > parts["lower_wick"]
                and abs(float(candle["high"]) - level_price) <= tolerance
            )
        return (
            parts["lower_wick"] >= parts["body"] * wick_ratio
            and parts["lower_wick"] > parts["upper_wick"]
            and abs(float(candle["low"]) - level_price) <= tolerance
        )

    @staticmethod
    def engulf_body(previous: pd.Series, current: pd.Series, direction: str, engulf_mode: str) -> bool:
        previous_color = candle_color(previous)
        current_color = candle_color(current)
        if direction == "long":
            if engulf_mode == "strict":
                return (
                    previous_color == "bear"
                    and current_color == "bull"
                    and body_low(current) <= body_low(previous)
                    and body_high(current) >= body_high(previous)
                )
            previous_body = max(abs(float(previous["close"]) - float(previous["open"])), 1e-9)
            return (
                previous_color == "bear"
                and current_color == "bull"
                and float(current["close"]) >= body_high(previous)
                and float(current["open"]) <= float(previous["close"]) + previous_body * 0.5
            )
        if engulf_mode == "strict":
            return (
                previous_color == "bull"
                and current_color == "bear"
                and body_low(current) <= body_low(previous)
                and body_high(current) >= body_high(previous)
            )
        previous_body = max(abs(float(previous["close"]) - float(previous["open"])), 1e-9)
        return (
            previous_color == "bull"
            and current_color == "bear"
            and float(current["close"]) <= body_low(previous)
            and float(current["open"]) >= float(previous["close"]) - previous_body * 0.5
        )

    def trigger_entry(self, trigger: pd.Series, atr_now: float, direction: str) -> Optional[float]:
        buffer = atr_now * self.config.trigger_buffer_atr
        open_price = float(trigger["open"])
        if direction == "long":
            trigger_price = open_price + buffer
            if float(trigger["high"]) >= trigger_price and float(trigger["close"]) > open_price:
                return trigger_price
            return None
        trigger_price = open_price - buffer
        if float(trigger["low"]) <= trigger_price and float(trigger["close"]) < open_price:
            return trigger_price
        return None

    def build_signal(self, symbol: str, df: pd.DataFrame, idx: int) -> Optional[HourSignal]:
        if idx < 3 or idx >= len(df) - 1:
            return None
        pin = df.iloc[idx - 1]
        engulf = df.iloc[idx]
        trigger = df.iloc[idx + 1]
        atr_now = float(engulf["atr"])
        if not np.isfinite(atr_now) or atr_now <= 0:
            return None

        for direction in ("long", "short"):
            trend_filter = self.config.long_trend_filter if direction == "long" else self.config.short_trend_filter
            if not self.trend_allows(engulf, direction, trend_filter):
                continue
            level = self.recent_level(df, idx, direction, atr_now)
            if level is None:
                continue
            level_kind, level_price, level_touches = level
            if not self.pin_bar_near_level(pin, atr_now, direction, level_price):
                continue
            if not self.engulf_body(pin, engulf, direction, self.config.engulf_mode):
                continue
            entry_price = self.trigger_entry(trigger, atr_now, direction)
            if entry_price is None:
                continue
            if direction == "long":
                stop_price = float(pin["low"]) - atr_now * self.config.stop_buffer_atr
                risk_points = entry_price - stop_price
                target_price = entry_price + risk_points * self.config.target_r
            else:
                stop_price = float(pin["high"]) + atr_now * self.config.stop_buffer_atr
                risk_points = stop_price - entry_price
                target_price = entry_price - risk_points * self.config.target_r
            if risk_points <= 0:
                continue
            return HourSignal(
                symbol=symbol,
                direction=direction,
                setup_time=str(df.index[idx + 1]),
                pin_time=str(df.index[idx - 1]),
                engulf_time=str(df.index[idx]),
                trigger_time=str(df.index[idx + 1]),
                level_price=level_price,
                level_kind=level_kind,
                level_touches=level_touches,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                risk_points=risk_points,
                ema_fast=float(engulf["ema_fast"]),
                ema_slow=float(engulf["ema_slow"]),
                pin_open=float(pin["open"]),
                pin_high=float(pin["high"]),
                pin_low=float(pin["low"]),
                pin_close=float(pin["close"]),
                engulf_open=float(engulf["open"]),
                engulf_high=float(engulf["high"]),
                engulf_low=float(engulf["low"]),
                engulf_close=float(engulf["close"]),
                trigger_open=float(trigger["open"]),
                trigger_high=float(trigger["high"]),
                trigger_low=float(trigger["low"]),
                trigger_close=float(trigger["close"]),
            )
        return None

    def execute_trade(self, signal: HourSignal, df: pd.DataFrame, entry_idx: int) -> HourTrade:
        position_size = max((self.config.initial_capital * self.config.risk_per_trade) / signal.risk_points, 0.0001)
        exit_price = float(df.iloc[entry_idx]["close"])
        exit_time = df.index[entry_idx]
        exit_reason = "timeout"
        last_idx = min(len(df) - 1, entry_idx + self.config.max_hold_bars)
        stop_price = signal.stop_price
        armed_trail = False

        for idx in range(entry_idx, last_idx + 1):
            candle = df.iloc[idx]
            high = float(candle["high"])
            low = float(candle["low"])
            close = float(candle["close"])

            if armed_trail and idx > entry_idx:
                start = max(entry_idx, idx - self.config.trail_lookback_bars)
                trail_window = df.iloc[start:idx]
                if not trail_window.empty:
                    if signal.direction == "long":
                        stop_price = max(stop_price, float(trail_window["low"].min()))
                    else:
                        stop_price = min(stop_price, float(trail_window["high"].max()))

            if signal.direction == "long":
                if low <= stop_price:
                    exit_price = stop_price
                    exit_time = df.index[idx]
                    exit_reason = "trail_stop" if armed_trail else "stop"
                    break
                if self.config.exit_mode == "fixed_target":
                    if high >= signal.target_price:
                        exit_price = signal.target_price
                        exit_time = df.index[idx]
                        exit_reason = "target"
                        break
                elif (not armed_trail) and high >= signal.entry_price + signal.risk_points * self.config.trail_activation_r:
                    armed_trail = True
                    stop_price = max(stop_price, signal.entry_price)
            else:
                if high >= stop_price:
                    exit_price = stop_price
                    exit_time = df.index[idx]
                    exit_reason = "trail_stop" if armed_trail else "stop"
                    break
                if self.config.exit_mode == "fixed_target":
                    if low <= signal.target_price:
                        exit_price = signal.target_price
                        exit_time = df.index[idx]
                        exit_reason = "target"
                        break
                elif (not armed_trail) and low <= signal.entry_price - signal.risk_points * self.config.trail_activation_r:
                    armed_trail = True
                    stop_price = min(stop_price, signal.entry_price)
            exit_price = close
            exit_time = df.index[idx]

        gross_move = (exit_price - signal.entry_price) if signal.direction == "long" else (signal.entry_price - exit_price)
        gross_pnl = gross_move * position_size
        fees = position_size * (signal.entry_price + exit_price) * self.config.fee_per_side
        net_pnl = gross_pnl - fees
        r_multiple = net_pnl / max(self.config.initial_capital * self.config.risk_per_trade, 1e-9)
        return HourTrade(
            symbol=signal.symbol,
            direction=signal.direction,
            setup_time=signal.setup_time,
            entry_time=str(df.index[entry_idx]),
            exit_time=str(exit_time),
            entry_price=signal.entry_price,
            exit_price=exit_price,
            stop_price=stop_price,
            target_price=signal.target_price,
            risk_points=signal.risk_points,
            r_multiple=r_multiple,
            net_pnl=net_pnl,
            hold_bars=max(0, df.index.get_loc(exit_time) - entry_idx),
            exit_reason=exit_reason,
            level_kind=signal.level_kind,
            level_price=signal.level_price,
            level_touches=signal.level_touches,
        )

    def run_symbol(self, symbol: str) -> dict:
        df = self.prepare_frame(symbol)
        signals: List[HourSignal] = []
        trades: List[HourTrade] = []
        equity = self.config.initial_capital
        equity_curve = [equity]
        idx = max(self.config.pivot_width * 2 + 2, 20)

        while idx < len(df) - 1:
            signal = self.build_signal(symbol, df, idx)
            if signal is None:
                idx += 1
                continue
            trigger_candle = df.iloc[idx + 1]
            if signal.direction == "long" and float(trigger_candle["low"]) <= signal.stop_price:
                idx += 1
                continue
            if signal.direction == "short" and float(trigger_candle["high"]) >= signal.stop_price:
                idx += 1
                continue
            signals.append(signal)
            entry_idx = idx + 1
            if entry_idx >= len(df):
                break
            trade = self.execute_trade(signal, df, entry_idx)
            trades.append(trade)
            equity += trade.net_pnl
            equity_curve.append(equity)
            idx += 1

        signals_df = pd.DataFrame(asdict(item) for item in signals)
        trades_df = pd.DataFrame(asdict(item) for item in trades)
        if not signals_df.empty:
            signals_df.to_csv(self.output_dir / f"{symbol.replace('-', '_')}_signals.csv", index=False)
        if not trades_df.empty:
            trades_df.to_csv(self.output_dir / f"{symbol.replace('-', '_')}_trades.csv", index=False)

        wins = int((trades_df["net_pnl"] > 0).sum()) if not trades_df.empty else 0
        summary = {
            "symbol": symbol,
            "days": self.config.days,
            "signals": len(signals),
            "trades": len(trades),
            "wins": wins,
            "win_rate": float(wins / len(trades)) if trades else 0.0,
            "net_pnl": float(trades_df["net_pnl"].sum()) if not trades_df.empty else 0.0,
            "avg_r": float(trades_df["r_multiple"].mean()) if not trades_df.empty else 0.0,
            "profit_factor": float(
                trades_df.loc[trades_df["net_pnl"] > 0, "net_pnl"].sum()
                / max(abs(trades_df.loc[trades_df["net_pnl"] < 0, "net_pnl"].sum()), 1e-9)
            )
            if not trades_df.empty and (trades_df["net_pnl"] < 0).any()
            else 0.0,
            "max_drawdown": float(max_drawdown(equity_curve)),
            "final_equity": float(equity),
        }
        (self.output_dir / f"{symbol.replace('-', '_')}_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple hourly level-pin-engulfing system.")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--risk-percent", type=float, default=1.0)
    parser.add_argument("--cache-dir", type=Path, default=Path("3bar") / "data_cache")
    parser.add_argument("--output-dir", type=Path, default=Path("3bar") / "reports")
    parser.add_argument("--level-mode", choices=["rolling_window", "pivot_cluster"], default="rolling_window")
    parser.add_argument("--level-lookback-bars", type=int, default=6)
    parser.add_argument("--level-tolerance-atr", type=float, default=0.80)
    parser.add_argument("--level-cluster-tolerance-atr", type=float, default=0.20)
    parser.add_argument("--min-level-pivots", type=int, default=2)
    parser.add_argument("--min-level-touches", type=int, default=3)
    parser.add_argument("--long-trend-filter", choices=["none", "ema"], default="ema")
    parser.add_argument("--short-trend-filter", choices=["none", "ema"], default="none")
    parser.add_argument("--ema-fast-period", type=int, default=50)
    parser.add_argument("--ema-slow-period", type=int, default=200)
    parser.add_argument("--engulf-mode", choices=["body_break", "strict"], default="body_break")
    parser.add_argument("--min-pin-wick-body-ratio", type=float, default=0.35)
    parser.add_argument("--min-pin-range-atr", type=float, default=0.60)
    parser.add_argument("--exit-mode", choices=["fixed_target", "trail_after_r"], default="trail_after_r")
    parser.add_argument("--trail-activation-r", type=float, default=1.40)
    parser.add_argument("--trail-lookback-bars", type=int, default=4)
    parser.add_argument("--trigger-buffer-atr", type=float, default=0.00)
    parser.add_argument("--stop-buffer-atr", type=float, default=0.00)
    parser.add_argument("--target-r", type=float, default=1.0)
    parser.add_argument("--max-hold-bars", type=int, default=48)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = HourSystemConfig(
        days=args.days,
        initial_capital=args.initial_capital,
        risk_per_trade=args.risk_percent / 100.0,
        level_mode=args.level_mode,
        ema_fast_period=args.ema_fast_period,
        ema_slow_period=args.ema_slow_period,
        level_lookback_bars=args.level_lookback_bars,
        level_tolerance_atr=args.level_tolerance_atr,
        level_cluster_tolerance_atr=args.level_cluster_tolerance_atr,
        min_level_pivots=args.min_level_pivots,
        min_level_touches=args.min_level_touches,
        long_trend_filter=args.long_trend_filter,
        short_trend_filter=args.short_trend_filter,
        engulf_mode=args.engulf_mode,
        min_pin_wick_body_ratio=args.min_pin_wick_body_ratio,
        min_pin_range_atr=args.min_pin_range_atr,
        exit_mode=args.exit_mode,
        trail_activation_r=args.trail_activation_r,
        trail_lookback_bars=args.trail_lookback_bars,
        trigger_buffer_atr=args.trigger_buffer_atr,
        stop_buffer_atr=args.stop_buffer_atr,
        target_r=args.target_r,
        max_hold_bars=args.max_hold_bars,
    )
    client = BingXClient()
    cache = DataCache(args.cache_dir)
    system = HourSimpleLevelSystem(config, client, cache, args.output_dir)
    summary = system.run_symbol(args.symbol)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
