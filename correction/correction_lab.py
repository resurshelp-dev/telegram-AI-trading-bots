from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction_regime import BingXClient, DataCache, atr, candle_metrics, find_pivots, max_drawdown, resample_ohlcv
from correction_hourly import money_flow_index, rsi


def obv(df: pd.DataFrame) -> pd.Series:
    return (np.sign(df["close"].diff().fillna(0.0)) * df["volume"]).cumsum()


def accumulation_distribution(df: pd.DataFrame) -> pd.Series:
    span = (df["high"] - df["low"]).replace(0.0, np.nan)
    multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])).divide(span).fillna(0.0)
    return (multiplier * df["volume"]).cumsum()


def chaikin_money_flow(df: pd.DataFrame, period: int = 20) -> pd.Series:
    span = (df["high"] - df["low"]).replace(0.0, np.nan)
    multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])).divide(span).fillna(0.0)
    flow = multiplier * df["volume"]
    return flow.rolling(period).sum().divide(df["volume"].rolling(period).sum().replace(0.0, np.nan))


def bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    middle = series.rolling(period).mean()
    sigma = series.rolling(period).std()
    upper = middle + std_dev * sigma
    lower = middle - std_dev * sigma
    width = (upper - lower).divide(middle.replace(0.0, np.nan))
    pct_b = (series - lower).divide((upper - lower).replace(0.0, np.nan))
    return pd.DataFrame({"bb_middle": middle, "bb_upper": upper, "bb_lower": lower, "bb_width": width, "bb_pct_b": pct_b}, index=series.index)


def anchored_vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    return (typical * df["volume"]).cumsum().divide(df["volume"].cumsum().replace(0.0, np.nan))


@dataclass
class LabConfig:
    days: int = 365
    end_time: Optional[datetime] = None
    fee_per_side: float = 0.0005
    initial_capital: float = 10000.0
    risk_per_trade: float = 0.01
    signal_cooldown_bars_confirm: int = 4
    max_setup_bars_confirm: int = 20
    max_retrace_bars_5m: int = 72
    max_hold_bars_5m: int = 144
    breakout_lookback_1h: int = 12
    sfp_buffer_atr_1h: float = 0.03
    min_reversal_leg_atr_confirm: float = 0.35
    entry_zone_min: float = 0.236
    entry_zone_max: float = 0.618
    stop_buffer_atr_confirm: float = 0.18
    breakeven_r: float = 0.85
    breakeven_buffer_r: float = 0.04
    divergence_threshold_rsi: float = 0.75
    min_wick_fraction: float = 0.18
    cmf_trigger_abs: float = 0.02
    avwap_tolerance_atr: float = 0.30
    bb_reentry_min_width: float = 0.012


@dataclass
class LabVariant:
    name: str
    confirm_interval: str = "15m"
    indicator_mode: str = "obv"
    require_avwap: bool = True
    require_cmf_shift: bool = True
    require_bb_reentry: bool = False
    entry_fib: float = 0.500
    tp1_r: float = 0.80
    tp2_r: float = 1.60
    min_rr: float = 1.50


@dataclass
class LabSignal:
    symbol: str
    time: pd.Timestamp
    direction: str
    variant_name: str
    exhaustion_time: str
    breakout_level: float
    exhaustion_price: float
    structure_level: float
    reversal_extreme: float
    avwap_value: float
    entry_zone_low: float
    entry_zone_high: float
    planned_entry: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    risk_points: float
    note: str


@dataclass
class LabTrade:
    symbol: str
    direction: str
    variant_name: str
    setup_time: str
    entry_time: str
    exit_time: str
    exhaustion_time: str
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


class PredictiveReversalLab:
    def __init__(self, config: LabConfig, variant: LabVariant, client: BingXClient, cache: DataCache, output_root: Path):
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
            frames[tf]["rsi"] = rsi(frames[tf]["close"], 14)
            frames[tf]["mfi"] = money_flow_index(frames[tf], 14)
            frames[tf]["obv"] = obv(frames[tf])
            frames[tf]["adl"] = accumulation_distribution(frames[tf])
            frames[tf]["cmf"] = chaikin_money_flow(frames[tf], 20)
            bands = bollinger_bands(frames[tf]["close"], 20, 2.0)
            for col in bands.columns:
                frames[tf][col] = bands[col]
        frames["15m"]["volume_median"] = frames["15m"]["volume"].rolling(20).median()
        frames["30m"]["volume_median"] = frames["30m"]["volume"].rolling(20).median()
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

    def divergence_ok(self, h1: pd.DataFrame, direction: str, current: pd.Series) -> bool:
        indicator_name = {"obv": "obv", "adl": "adl", "hybrid": "obv"}.get(self.variant.indicator_mode, "obv")
        pivots = h1["pivot_high"].dropna() if direction == "short" else h1["pivot_low"].dropna()
        if pivots.empty:
            return False
        pivot_time = pivots.index[-1]
        ref = h1.loc[pivot_time]
        if direction == "short":
            indicator_ok = float(current[indicator_name]) < float(ref[indicator_name]) and float(current["rsi"]) <= float(ref["rsi"])
            if self.variant.indicator_mode == "hybrid":
                indicator_ok = indicator_ok and float(current["adl"]) < float(ref["adl"])
            return indicator_ok and float(current["high"]) > float(ref["high"])
        indicator_ok = float(current[indicator_name]) > float(ref[indicator_name]) and float(current["rsi"]) >= float(ref["rsi"])
        if self.variant.indicator_mode == "hybrid":
            indicator_ok = indicator_ok and float(current["adl"]) > float(ref["adl"])
        return indicator_ok and float(current["low"]) < float(ref["low"])

    def latest_h1_sfp(self, h1: pd.DataFrame) -> Optional[Dict[str, object]]:
        current = h1.iloc[-1]
        atr_now = float(current["atr"])
        if np.isnan(atr_now) or atr_now <= 0:
            return None
        metrics = candle_metrics(current)
        if (
            not np.isnan(float(current["rolling_high"]))
            and float(current["high"]) >= float(current["rolling_high"]) + self.config.sfp_buffer_atr_1h * atr_now
            and float(current["close"]) < float(current["rolling_high"])
            and metrics["upper_wick"] / max(metrics["range"], 1e-12) >= self.config.min_wick_fraction
            and self.divergence_ok(h1.iloc[:-1], "short", current)
        ):
            return {"time": h1.index[-1], "direction": "short", "breakout_level": float(current["rolling_high"]), "exhaustion_price": float(current["high"])}
        if (
            not np.isnan(float(current["rolling_low"]))
            and float(current["low"]) <= float(current["rolling_low"]) - self.config.sfp_buffer_atr_1h * atr_now
            and float(current["close"]) > float(current["rolling_low"])
            and metrics["lower_wick"] / max(metrics["range"], 1e-12) >= self.config.min_wick_fraction
            and self.divergence_ok(h1.iloc[:-1], "long", current)
        ):
            return {"time": h1.index[-1], "direction": "long", "breakout_level": float(current["rolling_low"]), "exhaustion_price": float(current["low"])}
        return None

    def build_signal(self, symbol: str, frames: Dict[str, pd.DataFrame], timestamp: pd.Timestamp) -> Optional[LabSignal]:
        h1 = self.slice_frame(frames["1h"], timestamp, 80)
        confirm = self.slice_frame(frames[self.variant.confirm_interval], timestamp, 120)
        if h1 is None or confirm is None:
            return None
        sfp = self.latest_h1_sfp(h1)
        if sfp is None:
            return None
        exhaustion_time = pd.Timestamp(sfp["time"])
        segment = confirm.loc[(confirm.index >= exhaustion_time) & (confirm.index <= timestamp)]
        if len(segment) < 3 or len(segment) > self.config.max_setup_bars_confirm:
            return None
        current = segment.iloc[-1]
        atr_now = float(current["atr"])
        if np.isnan(atr_now) or atr_now <= 0:
            return None
        avwap_now = float(anchored_vwap(segment).iloc[-1])
        if np.isnan(avwap_now):
            return None
        direction = str(sfp["direction"])

        if direction == "short":
            pivot_idx = segment["high"].idxmax()
            if pivot_idx == segment.index[-1]:
                return None
            post = segment.loc[pivot_idx:]
            if len(post) < 3:
                return None
            structure_level = float(post["low"].iloc[:-1].min())
            reversal_extreme = float(post["low"].min())
            reversal_leg = float(post["high"].iloc[0]) - reversal_extreme
            structure_ok = float(current["close"]) < structure_level and float(current["close"]) < float(current["open"])
            cmf_ok = (not self.variant.require_cmf_shift) or float(current["cmf"]) <= -self.config.cmf_trigger_abs
            avwap_ok = (not self.variant.require_avwap) or float(current["close"]) < avwap_now
            bb_ok = (not self.variant.require_bb_reentry) or (float(post.iloc[0]["high"]) >= float(post.iloc[0]["bb_upper"]) and float(current["close"]) <= float(current["bb_upper"]) and float(current["bb_width"]) >= self.config.bb_reentry_min_width)
            if not (structure_ok and cmf_ok and avwap_ok and bb_ok):
                return None
            zone_low = reversal_extreme + self.config.entry_zone_min * reversal_leg
            zone_high = reversal_extreme + self.config.entry_zone_max * reversal_leg
            planned_entry = avwap_now if self.variant.require_avwap and min(zone_low, zone_high) <= avwap_now <= max(zone_low, zone_high) else reversal_extreme + self.variant.entry_fib * reversal_leg
            stop_price = float(post["high"].iloc[0]) + self.config.stop_buffer_atr_confirm * atr_now
            tp1_price = planned_entry - self.variant.tp1_r * abs(planned_entry - stop_price)
            tp2_price = planned_entry - self.variant.tp2_r * abs(planned_entry - stop_price)
        else:
            pivot_idx = segment["low"].idxmin()
            if pivot_idx == segment.index[-1]:
                return None
            post = segment.loc[pivot_idx:]
            if len(post) < 3:
                return None
            structure_level = float(post["high"].iloc[:-1].max())
            reversal_extreme = float(post["high"].max())
            reversal_leg = reversal_extreme - float(post["low"].iloc[0])
            structure_ok = float(current["close"]) > structure_level and float(current["close"]) > float(current["open"])
            cmf_ok = (not self.variant.require_cmf_shift) or float(current["cmf"]) >= self.config.cmf_trigger_abs
            avwap_ok = (not self.variant.require_avwap) or float(current["close"]) > avwap_now
            bb_ok = (not self.variant.require_bb_reentry) or (float(post.iloc[0]["low"]) <= float(post.iloc[0]["bb_lower"]) and float(current["close"]) >= float(current["bb_lower"]) and float(current["bb_width"]) >= self.config.bb_reentry_min_width)
            if not (structure_ok and cmf_ok and avwap_ok and bb_ok):
                return None
            zone_low = reversal_extreme - self.config.entry_zone_max * reversal_leg
            zone_high = reversal_extreme - self.config.entry_zone_min * reversal_leg
            planned_entry = avwap_now if self.variant.require_avwap and min(zone_low, zone_high) <= avwap_now <= max(zone_low, zone_high) else reversal_extreme - self.variant.entry_fib * reversal_leg
            stop_price = float(post["low"].iloc[0]) - self.config.stop_buffer_atr_confirm * atr_now
            tp1_price = planned_entry + self.variant.tp1_r * abs(planned_entry - stop_price)
            tp2_price = planned_entry + self.variant.tp2_r * abs(planned_entry - stop_price)

        if reversal_leg < self.config.min_reversal_leg_atr_confirm * atr_now:
            return None
        risk_points = abs(planned_entry - stop_price)
        if risk_points <= 0 or abs(tp2_price - planned_entry) / risk_points < self.variant.min_rr:
            return None
        return LabSignal(symbol, timestamp, direction, self.variant.name, exhaustion_time.isoformat(), float(sfp["breakout_level"]), float(sfp["exhaustion_price"]), structure_level, reversal_extreme, avwap_now, min(zone_low, zone_high), max(zone_low, zone_high), planned_entry, stop_price, tp1_price, tp2_price, risk_points, f"sfp + {self.variant.indicator_mode} + avwap={self.variant.require_avwap} + cmf={self.variant.require_cmf_shift} + bb={self.variant.require_bb_reentry} + tf={self.variant.confirm_interval}")


def execute_trade(config: LabConfig, signal: LabSignal, base: pd.DataFrame) -> Optional[LabTrade]:
    start_position = base.index.searchsorted(signal.time, side="right")
    if start_position >= len(base) - 1:
        return None
    entry_time: Optional[pd.Timestamp] = None
    entry_price: Optional[float] = None
    entry_idx: Optional[int] = None
    search_end = min(start_position + config.max_retrace_bars_5m, len(base) - 1)
    for idx in range(start_position, search_end + 1):
        candle = base.iloc[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        overlap = max(low, signal.entry_zone_low) <= min(high, signal.entry_zone_high)
        if low <= signal.planned_entry <= high:
            entry_time, entry_price, entry_idx = base.index[idx], signal.planned_entry, idx
            break
        if overlap:
            metrics = candle_metrics(candle)
            if signal.direction == "short" and float(candle["close"]) < float(candle["open"]) and metrics["close_position"] <= 0.50:
                entry_time, entry_price, entry_idx = base.index[idx], min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high), idx
                break
            if signal.direction == "long" and float(candle["close"]) > float(candle["open"]) and metrics["close_position"] >= 0.50:
                entry_time, entry_price, entry_idx = base.index[idx], min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high), idx
                break
    if entry_time is None or entry_price is None or entry_idx is None:
        return None
    risk_points = abs(entry_price - signal.stop_price)
    quantity_initial = max((config.initial_capital * config.risk_per_trade) / risk_points, 0.0001)
    quantity_open = quantity_initial
    stop_price = signal.stop_price
    realized = 0.0
    took_tp1 = False
    last_idx = min(entry_idx + config.max_hold_bars_5m, len(base) - 1)
    exit_price: Optional[float] = None
    exit_time: Optional[pd.Timestamp] = None
    exit_reason: Optional[str] = None
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
    return LabTrade(signal.symbol, signal.direction, signal.variant_name, signal.time.isoformat(), entry_time.isoformat(), exit_time.isoformat(), signal.exhaustion_time, entry_price, signal.stop_price, signal.tp1_price, signal.tp2_price, exit_price, r_multiple, net_pnl, (exit_time - entry_time).total_seconds() / 3600.0, exit_reason, signal.entry_zone_low, signal.entry_zone_high, signal.note)


def summarize(config: LabConfig, trades: List[LabTrade]) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "trades_per_month": 0.0, "win_rate": 0.0, "expectancy_r": 0.0, "profit_factor": 0.0, "net_pnl": 0.0, "max_drawdown_pct": 0.0, "avg_hold_hours": 0.0}
    pnl = np.array([trade.net_pnl for trade in trades], dtype=float)
    r_values = np.array([trade.r_multiple for trade in trades], dtype=float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    profit_factor = float(winners.sum() / abs(losers.sum())) if losers.size and abs(losers.sum()) > 0 else (float("inf") if winners.size else 0.0)
    balance = config.initial_capital
    equity = [balance]
    for trade in trades:
        balance += trade.net_pnl
        equity.append(balance)
    return {"trades": int(len(trades)), "trades_per_month": float(len(trades) / max(config.days / 30.0, 1.0)), "win_rate": float((r_values > 0).mean() * 100.0), "expectancy_r": float(r_values.mean()), "profit_factor": profit_factor, "net_pnl": float(pnl.sum()), "max_drawdown_pct": float(max_drawdown(equity) * 100.0), "avg_hold_hours": float(np.mean([trade.hold_hours for trade in trades]))}


def recommendation_score(summary: Dict[str, float]) -> float:
    return summary["win_rate"] * 0.36 + summary["expectancy_r"] * 50.0 + min(summary["profit_factor"], 3.0) * 10.0 - summary["max_drawdown_pct"] * 0.60 + min(summary["trades"], 40) * 0.25


def default_variants() -> List[LabVariant]:
    return [
        LabVariant(name="btc_obv_bb_plus_avwap", confirm_interval="15m", indicator_mode="obv", require_avwap=True, require_cmf_shift=False, require_bb_reentry=True, entry_fib=0.500, tp1_r=0.70, tp2_r=1.45, min_rr=1.35),
        LabVariant(name="sfp_obv_bb_15m", confirm_interval="15m", indicator_mode="obv", require_avwap=False, require_cmf_shift=True, require_bb_reentry=True, entry_fib=0.500, tp1_r=0.75, tp2_r=1.55, min_rr=1.45),
        LabVariant(name="eth_adl_15m_avwap", confirm_interval="15m", indicator_mode="adl", require_avwap=True, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.500, tp1_r=0.70, tp2_r=1.45, min_rr=1.35),
        LabVariant(name="eth_adl_30m_no_avwap", confirm_interval="30m", indicator_mode="adl", require_avwap=False, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.500, tp1_r=0.70, tp2_r=1.45, min_rr=1.35),
        LabVariant(name="sfp_adl_avwap_30m", confirm_interval="30m", indicator_mode="adl", require_avwap=True, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.500, tp1_r=0.70, tp2_r=1.50, min_rr=1.40),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predictive reversal lab.")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--symbols", nargs="+", default=["BTC-USDT", "ETH-USDT"])
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/predictive_reversal_lab")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = LabConfig(days=args.days)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    results: List[Dict[str, object]] = []
    for variant in default_variants():
        strategy = PredictiveReversalLab(config, variant, client, cache, output_root)
        for symbol in args.symbols:
            frames = strategy.prepare_frames(symbol)
            trades: List[LabTrade] = []
            next_available_time: Optional[pd.Timestamp] = None
            last_signal_time: Optional[pd.Timestamp] = None
            bars_per_signal = 2 if variant.confirm_interval == "30m" else 4
            for timestamp in frames[variant.confirm_interval].index[120:]:
                if next_available_time is not None and timestamp <= next_available_time:
                    continue
                if last_signal_time is not None:
                    bars_since = int((timestamp - last_signal_time).total_seconds() // (bars_per_signal * 15 * 60))
                    if bars_since < config.signal_cooldown_bars_confirm:
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
            results.append({"variant": variant.name, "symbol": symbol, "summary": summarize(config, trades), "recommendation_score": recommendation_score(summarize(config, trades))})
    with (output_root / "comparison.json").open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
