from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction_regime import BingXClient, DataCache, atr, candle_metrics, find_pivots, max_drawdown, resample_ohlcv


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain.divide(avg_loss.replace(0.0, np.nan))
    return 100.0 - (100.0 / (1.0 + rs))


def money_flow_index(df: pd.DataFrame, period: int = 14) -> pd.Series:
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    raw_flow = typical_price * df["volume"]
    direction = typical_price.diff()
    positive = raw_flow.where(direction > 0.0, 0.0)
    negative = raw_flow.where(direction < 0.0, 0.0).abs()
    pos_sum = positive.rolling(period).sum()
    neg_sum = negative.rolling(period).sum()
    ratio = pos_sum.divide(neg_sum.replace(0.0, np.nan))
    return 100.0 - (100.0 / (1.0 + ratio))


def dmi_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0.0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0.0), down_move, 0.0), index=df.index)
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_smoothed = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    plus_smoothed = plus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    minus_smoothed = minus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    plus_di = 100.0 * plus_smoothed.divide(atr_smoothed.replace(0.0, np.nan))
    minus_di = 100.0 * minus_smoothed.divide(atr_smoothed.replace(0.0, np.nan))
    dx = 100.0 * (plus_di - minus_di).abs().divide((plus_di + minus_di).replace(0.0, np.nan))
    adx = dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return pd.DataFrame({"plus_di": plus_di, "minus_di": minus_di, "adx": adx}, index=df.index)


@dataclass
class VariantConfig:
    name: str
    divergence_mode: str = "dual"
    confirm_interval: str = "15m"
    entry_fib: float = 0.382
    min_rr: float = 1.8
    tp1_r: float = 0.9
    tp2_r: float = 1.8
    require_both_exhaustion: bool = False
    min_wick_fraction: float = 0.26
    adx_drop_min: float = 1.1


@dataclass
class ExhaustionConfig:
    days: int = 365
    end_time: Optional[datetime] = None
    fee_per_side: float = 0.0005
    initial_capital: float = 10000.0
    risk_per_trade: float = 0.01
    signal_cooldown_bars_15m: int = 4
    max_setup_bars_15m: int = 20
    max_retrace_bars_5m: int = 72
    max_hold_bars_5m: int = 144
    breakout_lookback_1h: int = 12
    min_breakout_atr_1h: float = 0.03
    min_reversal_leg_atr_15m: float = 0.35
    entry_zone_min: float = 0.236
    entry_zone_max: float = 0.618
    stop_buffer_atr_15m: float = 0.18
    breakeven_buffer_r: float = 0.04
    rsi_period: int = 14
    mfi_period: int = 14
    adx_period: int = 14
    min_adx: float = 13.0
    confirm_volume_factor: float = 0.15


@dataclass
class ReversalSignal:
    symbol: str
    time: pd.Timestamp
    exhaustion_time: str
    direction: str
    divergence_mode: str
    breakout_level: float
    exhaustion_price: float
    reversal_extreme: float
    structure_level: float
    confirmation_close: float
    entry_zone_low: float
    entry_zone_high: float
    planned_entry: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    risk_points: float
    adx_now: float
    adx_drop: float
    rsi_now: float
    rsi_ref: float
    mfi_now: float
    mfi_ref: float
    note: str


@dataclass
class ReversalTrade:
    symbol: str
    direction: str
    setup_time: str
    entry_time: str
    exit_time: str
    exhaustion_time: str
    divergence_mode: str
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    exit_price: float
    r_multiple: float
    net_pnl: float
    hold_hours: float
    exit_reason: str
    entry_zone_low: float
    entry_zone_high: float
    note: str


def default_variants() -> List[VariantConfig]:
    return [
        VariantConfig(name="rsi_382_active", divergence_mode="rsi", confirm_interval="15m", entry_fib=0.382, min_rr=1.55, tp1_r=0.8, tp2_r=1.6),
        VariantConfig(name="rsi_500_active", divergence_mode="rsi", confirm_interval="15m", entry_fib=0.500, min_rr=1.45, tp1_r=0.75, tp2_r=1.5),
        VariantConfig(name="dual_500_active", divergence_mode="dual", confirm_interval="15m", entry_fib=0.500, min_rr=1.40, tp1_r=0.75, tp2_r=1.45, min_wick_fraction=0.22, adx_drop_min=1.0),
    ]


class HourlyExhaustionFibStrategy:
    def __init__(self, config: ExhaustionConfig, variant: VariantConfig, client: BingXClient, cache: DataCache, output_root: Path) -> None:
        self.config = config
        self.variant = variant
        self.client = client
        self.cache = cache
        self.output_root = output_root
        self.output_root.mkdir(parents=True, exist_ok=True)

    def prepare_frames(self, symbol: str) -> Dict[str, pd.DataFrame]:
        base = self.cache.load_or_fetch(self.client, symbol, "5m", self.config.days, self.config.end_time)
        frames = {"5m": base, "15m": resample_ohlcv(base, "15m"), "30m": resample_ohlcv(base, "30m"), "1h": resample_ohlcv(base, "1h")}
        for tf in ["15m", "30m", "1h"]:
            frames[tf]["atr"] = atr(frames[tf], 14)
        frames["15m"]["volume_median"] = frames["15m"]["volume"].rolling(20).median()
        frames["30m"]["volume_median"] = frames["30m"]["volume"].rolling(20).median()
        dmi = dmi_adx(frames["1h"], self.config.adx_period)
        frames["1h"]["rsi"] = rsi(frames["1h"]["close"], self.config.rsi_period)
        frames["1h"]["mfi"] = money_flow_index(frames["1h"], self.config.mfi_period)
        frames["1h"]["adx"] = dmi["adx"]
        frames["1h"]["rolling_high"] = frames["1h"]["high"].shift(1).rolling(self.config.breakout_lookback_1h).max()
        frames["1h"]["rolling_low"] = frames["1h"]["low"].shift(1).rolling(self.config.breakout_lookback_1h).min()
        pivots = find_pivots(frames["1h"], width=2)
        frames["1h"]["pivot_high"] = pivots["pivot_high"]
        frames["1h"]["pivot_low"] = pivots["pivot_low"]
        return frames

    @staticmethod
    def slice_frame(df: pd.DataFrame, timestamp: pd.Timestamp, min_bars: int) -> Optional[pd.DataFrame]:
        position = df.index.searchsorted(timestamp, side="right")
        if position < min_bars:
            return None
        return df.iloc[:position]

    def divergence_flags(self, h1: pd.DataFrame, direction: str, current: pd.Series) -> Optional[Dict[str, float]]:
        pivots = h1["pivot_high"].dropna() if direction == "short" else h1["pivot_low"].dropna()
        if pivots.empty:
            return None
        ref = h1.loc[pivots.index[-1]]
        if direction == "short":
            rsi_ok = float(current["rsi"]) < float(ref["rsi"])
            mfi_ok = float(current["mfi"]) < float(ref["mfi"])
            price_ok = float(current["high"]) > float(ref["high"])
        else:
            rsi_ok = float(current["rsi"]) > float(ref["rsi"])
            mfi_ok = float(current["mfi"]) > float(ref["mfi"])
            price_ok = float(current["low"]) < float(ref["low"])
        if not price_ok:
            return None
        if self.variant.divergence_mode == "rsi":
            ok = rsi_ok
        elif self.variant.divergence_mode == "mfi":
            ok = mfi_ok
        else:
            ok = (rsi_ok and mfi_ok) if self.variant.require_both_exhaustion else (rsi_ok or mfi_ok)
        if not ok:
            return None
        return {"rsi_now": float(current["rsi"]), "rsi_ref": float(ref["rsi"]), "mfi_now": float(current["mfi"]), "mfi_ref": float(ref["mfi"])}

    def build_signal(self, symbol: str, frames: Dict[str, pd.DataFrame], timestamp: pd.Timestamp) -> Optional[ReversalSignal]:
        h1 = self.slice_frame(frames["1h"], timestamp, 80)
        confirm = self.slice_frame(frames[self.variant.confirm_interval], timestamp, 120)
        if h1 is None or confirm is None:
            return None
        current_h1 = h1.iloc[-1]
        atr_h1 = float(current_h1["atr"])
        if np.isnan(atr_h1) or atr_h1 <= 0:
            return None
        metrics = candle_metrics(current_h1)
        direction: Optional[str] = None
        breakout_level = np.nan
        exhaustion_price = np.nan
        if (
            not np.isnan(float(current_h1["rolling_high"]))
            and float(current_h1["high"]) > float(current_h1["rolling_high"])
            and float(current_h1["close"]) < float(current_h1["rolling_high"])
            and float(current_h1["high"] - current_h1["rolling_high"]) >= self.config.min_breakout_atr_1h * atr_h1
            and metrics["upper_wick"] / metrics["range"] >= self.variant.min_wick_fraction
        ):
            direction = "short"
            breakout_level = float(current_h1["rolling_high"])
            exhaustion_price = float(current_h1["high"])
        elif (
            not np.isnan(float(current_h1["rolling_low"]))
            and float(current_h1["low"]) < float(current_h1["rolling_low"])
            and float(current_h1["close"]) > float(current_h1["rolling_low"])
            and float(current_h1["rolling_low"] - current_h1["low"]) >= self.config.min_breakout_atr_1h * atr_h1
            and metrics["lower_wick"] / metrics["range"] >= self.variant.min_wick_fraction
        ):
            direction = "long"
            breakout_level = float(current_h1["rolling_low"])
            exhaustion_price = float(current_h1["low"])
        if direction is None:
            return None
        flags = self.divergence_flags(h1.iloc[:-1], direction, current_h1)
        if flags is None:
            return None
        adx_now = float(current_h1["adx"])
        adx_prev = float(h1["adx"].iloc[-2]) if len(h1) > 1 else np.nan
        adx_drop = adx_prev - adx_now if not np.isnan(adx_prev) else 0.0
        if np.isnan(adx_now) or adx_now < self.config.min_adx or adx_drop < self.variant.adx_drop_min:
            return None

        exhaustion_time = h1.index[-1]
        segment = confirm.loc[(confirm.index >= exhaustion_time) & (confirm.index <= timestamp)]
        if len(segment) < 3 or len(segment) > self.config.max_setup_bars_15m:
            return None
        current = segment.iloc[-1]
        atr_now = float(current["atr"])
        if np.isnan(atr_now) or atr_now <= 0:
            return None
        volume_median = float(current.get("volume_median", np.nan))
        if not np.isnan(volume_median) and float(current["volume"]) < self.config.confirm_volume_factor * volume_median:
            return None

        if direction == "short":
            pivot_idx = segment["high"].idxmax()
            if pivot_idx == segment.index[-1]:
                return None
            post = segment.loc[pivot_idx:]
            structure_level = float(post["low"].iloc[:-1].min())
            reversal_extreme = float(post["low"].min())
            reversal_leg = float(post["high"].iloc[0]) - reversal_extreme
            structure_ok = float(current["close"]) < structure_level and float(current["close"]) < float(current["open"])
            zone_low = reversal_extreme + self.config.entry_zone_min * reversal_leg
            zone_high = reversal_extreme + self.config.entry_zone_max * reversal_leg
            planned_entry = reversal_extreme + self.variant.entry_fib * reversal_leg
            stop_price = float(post["high"].iloc[0]) + self.config.stop_buffer_atr_15m * atr_now
            tp1_price = planned_entry - self.variant.tp1_r * abs(planned_entry - stop_price)
            tp2_price = planned_entry - self.variant.tp2_r * abs(planned_entry - stop_price)
        else:
            pivot_idx = segment["low"].idxmin()
            if pivot_idx == segment.index[-1]:
                return None
            post = segment.loc[pivot_idx:]
            structure_level = float(post["high"].iloc[:-1].max())
            reversal_extreme = float(post["high"].max())
            reversal_leg = reversal_extreme - float(post["low"].iloc[0])
            structure_ok = float(current["close"]) > structure_level and float(current["close"]) > float(current["open"])
            zone_low = reversal_extreme - self.config.entry_zone_max * reversal_leg
            zone_high = reversal_extreme - self.config.entry_zone_min * reversal_leg
            planned_entry = reversal_extreme - self.variant.entry_fib * reversal_leg
            stop_price = float(post["low"].iloc[0]) - self.config.stop_buffer_atr_15m * atr_now
            tp1_price = planned_entry + self.variant.tp1_r * abs(planned_entry - stop_price)
            tp2_price = planned_entry + self.variant.tp2_r * abs(planned_entry - stop_price)

        if reversal_leg < self.config.min_reversal_leg_atr_15m * atr_now or not structure_ok:
            return None
        risk_points = abs(planned_entry - stop_price)
        if risk_points <= 0:
            return None
        if abs(tp2_price - planned_entry) / risk_points < self.variant.min_rr:
            return None

        return ReversalSignal(symbol, timestamp, exhaustion_time.isoformat(), direction, self.variant.divergence_mode, breakout_level, exhaustion_price, reversal_extreme, structure_level, float(current["close"]), min(zone_low, zone_high), max(zone_low, zone_high), planned_entry, stop_price, tp1_price, tp2_price, risk_points, adx_now, adx_drop, flags["rsi_now"], flags["rsi_ref"], flags["mfi_now"], flags["mfi_ref"], f"active:{self.variant.name}")


def execute_trade(config: ExhaustionConfig, signal: ReversalSignal, base: pd.DataFrame) -> Optional[ReversalTrade]:
    start_position = base.index.searchsorted(signal.time, side="right")
    if start_position >= len(base) - 1:
        return None
    entry_time: Optional[pd.Timestamp] = None
    entry_price: Optional[float] = None
    entry_idx: Optional[int] = None
    end_search = min(start_position + config.max_retrace_bars_5m, len(base) - 1)
    for idx in range(start_position, end_search + 1):
        candle = base.iloc[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        overlap = max(low, signal.entry_zone_low) <= min(high, signal.entry_zone_high)
        if low <= signal.planned_entry <= high:
            entry_time = base.index[idx]
            entry_price = signal.planned_entry
            entry_idx = idx
            break
        if overlap:
            metrics = candle_metrics(candle)
            if signal.direction == "short" and float(candle["close"]) < float(candle["open"]) and metrics["close_position"] <= 0.50:
                entry_time = base.index[idx]
                entry_price = min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high)
                entry_idx = idx
                break
            if signal.direction == "long" and float(candle["close"]) > float(candle["open"]) and metrics["close_position"] >= 0.50:
                entry_time = base.index[idx]
                entry_price = min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high)
                entry_idx = idx
                break
    if entry_time is None or entry_price is None or entry_idx is None:
        return None

    risk_points = abs(entry_price - signal.stop_price)
    quantity_initial = max((config.initial_capital * config.risk_per_trade) / risk_points, 0.0001)
    quantity_open = quantity_initial
    stop_price = signal.stop_price
    realized = 0.0
    took_tp1 = False
    exit_price: Optional[float] = None
    exit_time: Optional[pd.Timestamp] = None
    exit_reason: Optional[str] = None
    last_idx = min(entry_idx + config.max_hold_bars_5m, len(base) - 1)
    for idx in range(entry_idx, last_idx + 1):
        candle = base.iloc[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        if signal.direction == "long":
            if (not took_tp1) and high >= signal.tp1_price:
                realized += quantity_initial * 0.5 * (signal.tp1_price - entry_price)
                quantity_open = quantity_initial * 0.5
                stop_price = max(stop_price, entry_price + config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if low <= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if high >= signal.tp2_price:
                exit_price, exit_time, exit_reason = signal.tp2_price, base.index[idx], "tp2"
                break
        else:
            if (not took_tp1) and low <= signal.tp1_price:
                realized += quantity_initial * 0.5 * (entry_price - signal.tp1_price)
                quantity_open = quantity_initial * 0.5
                stop_price = min(stop_price, entry_price - config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if high >= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if low <= signal.tp2_price:
                exit_price, exit_time, exit_reason = signal.tp2_price, base.index[idx], "tp2"
                break
        if idx == last_idx:
            exit_price, exit_time, exit_reason = close, base.index[idx], "time_stop"
    if exit_price is None or exit_time is None or exit_reason is None:
        return None
    gross = realized + quantity_open * ((exit_price - entry_price) if signal.direction == "long" else (entry_price - exit_price))
    fees = quantity_initial * entry_price * config.fee_per_side * 2.0
    net_pnl = gross - fees
    theoretical_risk = quantity_initial * risk_points
    r_multiple = net_pnl / theoretical_risk if theoretical_risk > 0 else 0.0
    return ReversalTrade(signal.symbol, signal.direction, signal.time.isoformat(), entry_time.isoformat(), exit_time.isoformat(), signal.exhaustion_time, signal.divergence_mode, entry_price, signal.stop_price, signal.tp1_price, signal.tp2_price, exit_price, r_multiple, net_pnl, (exit_time - entry_time).total_seconds() / 3600.0, exit_reason, signal.entry_zone_low, signal.entry_zone_high, signal.note)


def summarize(config: ExhaustionConfig, trades: List[ReversalTrade]) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "win_rate": 0.0, "expectancy_r": 0.0, "profit_factor": 0.0, "net_pnl": 0.0, "max_drawdown_pct": 0.0, "avg_hold_hours": 0.0}
    pnl = np.array([trade.net_pnl for trade in trades], dtype=float)
    r_values = np.array([trade.r_multiple for trade in trades], dtype=float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    profit_factor = float(winners.sum() / abs(losers.sum())) if losers.size and abs(losers.sum()) > 0 else (float("inf") if winners.size else 0.0)
    equity = [config.initial_capital]
    balance = config.initial_capital
    for trade in trades:
        balance += trade.net_pnl
        equity.append(balance)
    return {"trades": int(len(trades)), "win_rate": float((r_values > 0).mean() * 100.0), "expectancy_r": float(r_values.mean()), "profit_factor": profit_factor, "net_pnl": float(pnl.sum()), "max_drawdown_pct": float(max_drawdown(equity) * 100.0), "avg_hold_hours": float(np.mean([trade.hold_hours for trade in trades]))}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly exhaustion fib backtest.")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--symbols", nargs="+", default=["BTC-USDT", "ETH-USDT"])
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/hourly_exhaustion_fib_backtest")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = ExhaustionConfig(days=args.days)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, object]] = []
    for variant in default_variants():
        strategy = HourlyExhaustionFibStrategy(config, variant, client, cache, output_root)
        for symbol in args.symbols:
            frames = strategy.prepare_frames(symbol)
            trades: List[ReversalTrade] = []
            next_available_time: Optional[pd.Timestamp] = None
            last_signal_time: Optional[pd.Timestamp] = None
            bar_minutes = 30 if variant.confirm_interval == "30m" else 15
            for timestamp in frames[variant.confirm_interval].index[120:]:
                if next_available_time is not None and timestamp <= next_available_time:
                    continue
                if last_signal_time is not None:
                    bars_since = int((timestamp - last_signal_time).total_seconds() // (bar_minutes * 60))
                    if bars_since < config.signal_cooldown_bars_15m:
                        continue
                signal = strategy.build_signal(symbol, frames, timestamp)
                if signal is None:
                    continue
                trade = execute_trade(config, signal, frames["5m"])
                last_signal_time = timestamp
                if trade is None:
                    continue
                trades.append(trade)
                next_available_time = pd.Timestamp(trade.exit_time)
            results.append({"variant": variant.name, "symbol": symbol, "summary": summarize(config, trades)})
    with (output_root / "comparison.json").open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
