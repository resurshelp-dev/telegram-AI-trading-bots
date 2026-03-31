from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction_regime import BingXClient, DataCache, candle_metrics, max_drawdown, parse_end_time
from correction_hourly import ExhaustionConfig, HourlyExhaustionFibStrategy, default_variants as active_default_variants, execute_trade as execute_active_trade
from correction_lab import LabConfig, LabSignal, LabTrade, PredictiveReversalLab, default_variants as lab_default_variants, execute_trade as execute_lab_trade


@dataclass
class RecentModule:
    kind: str
    name: str
    priority: int
    base_size: float = 1.0
    config_preset: str = "default"
    adaptive_shallow_threshold: Optional[float] = None
    second_chance_min_quality_score: Optional[float] = None
    second_chance_min_primary_r: Optional[float] = None
    second_chance_confirm_bars: int = 0


@dataclass
class RecentTrade:
    symbol: str
    module_kind: str
    module_name: str
    priority: int
    direction: str
    entry_time: str
    exit_time: str
    net_pnl: float
    r_multiple: float
    hold_hours: float
    confidence_score: float
    size_multiplier: float
    adjusted_net_pnl: float
    adjusted_r_multiple: float
    note: str


def active_variant_by_name(name: str):
    return next(v for v in active_default_variants() if v.name == name)


def lab_variant_by_name(name: str):
    defaults = {variant.name: variant for variant in lab_default_variants()}
    if name in defaults:
        return defaults[name]
    custom = {
        "adl15_avwap": dict(confirm_interval="15m", indicator_mode="adl", require_avwap=True, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.500, tp1_r=0.70, tp2_r=1.45, min_rr=1.20),
        "adl15_noavwap": dict(confirm_interval="15m", indicator_mode="adl", require_avwap=False, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.382, tp1_r=0.65, tp2_r=1.30, min_rr=1.00),
        "obv15_avwap": dict(confirm_interval="15m", indicator_mode="obv", require_avwap=True, require_cmf_shift=False, require_bb_reentry=False, entry_fib=0.382, tp1_r=0.60, tp2_r=1.20, min_rr=0.95),
    }
    if name not in custom:
        raise KeyError(f"Unknown lab variant: {name}")
    return lab_default_variants()[0].__class__(name=name, **custom[name])


def build_lab_config(days: int, preset: str, initial_capital: float, risk_per_trade: float, end_time: Optional[datetime] = None) -> LabConfig:
    config = LabConfig(days=days, end_time=end_time, initial_capital=initial_capital, risk_per_trade=risk_per_trade)
    if preset == "relaxed":
        config.breakout_lookback_1h = 8
        config.sfp_buffer_atr_1h = 0.015
        config.min_reversal_leg_atr_confirm = 0.24
        config.max_setup_bars_confirm = 28
        config.max_retrace_bars_5m = 96
        config.max_hold_bars_5m = 96
    return config


def impulse_quality_score(signal, confirm_row: pd.Series) -> float:
    bar_range = max(float(confirm_row["high"]) - float(confirm_row["low"]), 1e-9)
    atr_now = max(float(confirm_row["atr"]), 1e-9)
    body_ratio = abs(float(confirm_row["close"]) - float(confirm_row["open"])) / bar_range
    structure_push = (
        (float(signal.structure_level) - float(confirm_row["close"])) / atr_now
        if signal.direction == "short"
        else (float(confirm_row["close"]) - float(signal.structure_level)) / atr_now
    )
    zone_width = abs(float(signal.entry_zone_high) - float(signal.entry_zone_low))
    reversal_leg = zone_width / max(0.618 - 0.236, 1e-9)
    reversal_leg_atr = reversal_leg / atr_now
    score = 1.30 * min(max(body_ratio / 0.55, 0.0), 2.0)
    score += 1.10 * min(max(structure_push / 0.45, 0.0), 2.0)
    score += 1.00 * min(max(reversal_leg_atr / 0.90, 0.0), 2.0)
    return float(score)


def default_routes(profile: str = "quality") -> List[RecentModule]:
    quality = [
        RecentModule(kind="active", name="rsi_382_active", priority=4, base_size=1.0),
        RecentModule(kind="active", name="dual_500_active", priority=3, base_size=1.0),
        RecentModule(kind="lab", name="adl15_noavwap", priority=3, base_size=1.0, config_preset="default", adaptive_shallow_threshold=2.50, second_chance_min_quality_score=3.5, second_chance_min_primary_r=0.20, second_chance_confirm_bars=3),
        RecentModule(kind="lab", name="adl15_avwap", priority=2, base_size=1.0, config_preset="relaxed"),
    ]
    balanced = quality + [
        RecentModule(kind="lab", name="obv15_avwap", priority=1, base_size=0.9, config_preset="default"),
    ]
    profiles = {
        "quality": quality,
        "balanced": balanced,
    }
    if profile not in profiles:
        raise KeyError(f"Unknown profile: {profile}")
    return profiles[profile]


def module_confidence(route: RecentModule) -> float:
    return {
        "rsi_382_active": 3.0,
        "dual_500_active": 2.8,
        "adl15_noavwap": 3.1,
        "adl15_avwap": 2.8,
        "obv15_avwap": 2.1,
    }.get(route.name, 1.0)


def size_multiplier(route: RecentModule) -> float:
    mult = {
        "rsi_382_active": 1.00,
        "dual_500_active": 1.00,
        "adl15_noavwap": 1.00,
        "adl15_avwap": 1.00,
        "obv15_avwap": 0.90,
    }.get(route.name, 1.0)
    return float(min(max(route.base_size * mult, 0.5), 1.2))


def simulate_second_chance_trade(config: LabConfig, signal: LabSignal, base: pd.DataFrame, entry_idx: int, entry_price: float) -> Optional[LabTrade]:
    risk_points = abs(entry_price - signal.stop_price)
    if risk_points <= 0:
        return None
    if signal.direction == "long":
        tp1_price = entry_price + (signal.tp1_price - signal.planned_entry) * (risk_points / max(signal.risk_points, 1e-9))
        tp2_price = entry_price + (signal.tp2_price - signal.planned_entry) * (risk_points / max(signal.risk_points, 1e-9))
    else:
        tp1_price = entry_price - (signal.planned_entry - signal.tp1_price) * (risk_points / max(signal.risk_points, 1e-9))
        tp2_price = entry_price - (signal.planned_entry - signal.tp2_price) * (risk_points / max(signal.risk_points, 1e-9))
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
            if (not took_tp1) and high >= tp1_price:
                realized += quantity_initial * 0.5 * (tp1_price - entry_price)
                quantity_open = quantity_initial * 0.5
                stop_price = max(stop_price, entry_price + config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if low <= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if high >= tp2_price:
                exit_price, exit_time, exit_reason = tp2_price, base.index[idx], "tp2"
                break
        else:
            if (not took_tp1) and low <= tp1_price:
                realized += quantity_initial * 0.5 * (entry_price - tp1_price)
                quantity_open = quantity_initial * 0.5
                stop_price = min(stop_price, entry_price - config.breakeven_buffer_r * risk_points)
                took_tp1 = True
            if high >= stop_price:
                exit_price, exit_time, exit_reason = stop_price, base.index[idx], "stop"
                break
            if low <= tp2_price:
                exit_price, exit_time, exit_reason = tp2_price, base.index[idx], "tp2"
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
    return LabTrade(signal.symbol, signal.direction, signal.variant_name, signal.time.isoformat(), base.index[entry_idx].isoformat(), exit_time.isoformat(), signal.exhaustion_time, float(entry_price), float(signal.stop_price), float(tp1_price), float(tp2_price), float(exit_price), float(r_multiple), float(net_pnl), (exit_time - base.index[entry_idx]).total_seconds() / 3600.0, exit_reason, float(signal.entry_zone_low), float(signal.entry_zone_high), signal.note)


def build_second_chance_trade(config: LabConfig, signal: LabSignal, primary_trade: LabTrade, base: pd.DataFrame, quality_score: float, min_primary_r: float, min_quality_score: float, confirm_bars: int) -> Optional[LabTrade]:
    if primary_trade.r_multiple <= min_primary_r or quality_score < min_quality_score:
        return None
    start_idx = base.index.searchsorted(pd.Timestamp(primary_trade.exit_time), side="right")
    end_idx = min(start_idx + confirm_bars * 3, len(base) - 1)
    if start_idx >= end_idx:
        return None
    for idx in range(start_idx, end_idx + 1):
        candle = base.iloc[idx]
        high = float(candle["high"])
        low = float(candle["low"])
        overlap = max(low, signal.entry_zone_low) <= min(high, signal.entry_zone_high)
        if not overlap:
            continue
        metrics = candle_metrics(candle)
        if signal.direction == "short":
            if float(candle["close"]) >= float(candle["open"]) or metrics["close_position"] > 0.55:
                continue
            entry_price = min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high)
        else:
            if float(candle["close"]) <= float(candle["open"]) or metrics["close_position"] < 0.45:
                continue
            entry_price = min(max(float(candle["close"]), signal.entry_zone_low), signal.entry_zone_high)
        return simulate_second_chance_trade(config, signal, base, idx, entry_price)
    return None


def summarize(initial_capital: float, trades: List[RecentTrade], adjusted: bool = True) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "win_rate": 0.0, "expectancy_r": 0.0, "profit_factor": 0.0, "net_pnl": 0.0, "max_drawdown_pct": 0.0, "avg_hold_hours": 0.0}
    pnl = np.array([trade.adjusted_net_pnl if adjusted else trade.net_pnl for trade in trades], dtype=float)
    r_values = np.array([trade.adjusted_r_multiple if adjusted else trade.r_multiple for trade in trades], dtype=float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    profit_factor = float(winners.sum() / abs(losers.sum())) if losers.size and abs(losers.sum()) > 0 else (float("inf") if winners.size else 0.0)
    equity = [initial_capital]
    balance = initial_capital
    for trade in trades:
        balance += trade.net_pnl
        equity.append(balance)
    return {"trades": int(len(trades)), "win_rate": float((r_values > 0).mean() * 100.0), "expectancy_r": float(r_values.mean()), "profit_factor": profit_factor, "net_pnl": float(pnl.sum()), "max_drawdown_pct": float(max_drawdown(equity) * 100.0), "avg_hold_hours": float(np.mean([trade.hold_hours for trade in trades]))}


def run_active(symbol: str, route: RecentModule, cache: DataCache, client: BingXClient, output_root: Path, days: int, initial_capital: float, risk_per_trade: float, end_time: Optional[datetime] = None) -> List[RecentTrade]:
    config = ExhaustionConfig(days=days, end_time=end_time, initial_capital=initial_capital, risk_per_trade=risk_per_trade)
    variant = active_variant_by_name(route.name)
    strategy = HourlyExhaustionFibStrategy(config, variant, client, cache, output_root)
    frames = strategy.prepare_frames(symbol)
    trades: List[RecentTrade] = []
    next_available: Optional[pd.Timestamp] = None
    last_signal: Optional[pd.Timestamp] = None
    bar_minutes = 30 if variant.confirm_interval == "30m" else 15
    for timestamp in frames[variant.confirm_interval].index[120:]:
        if next_available is not None and timestamp <= next_available:
            continue
        if last_signal is not None:
            bars_since = int((timestamp - last_signal).total_seconds() // (bar_minutes * 60))
            if bars_since < config.signal_cooldown_bars_15m:
                continue
        signal = strategy.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        trade = execute_active_trade(config, signal, frames["5m"])
        last_signal = timestamp
        if trade is None:
            continue
        mult = size_multiplier(route)
        trades.append(RecentTrade(symbol, "active", route.name, route.priority, trade.direction, trade.entry_time, trade.exit_time, float(trade.net_pnl), float(trade.r_multiple), float(trade.hold_hours), module_confidence(route), mult, float(trade.net_pnl) * mult, float(trade.r_multiple) * mult, str(trade.note)))
        next_available = pd.Timestamp(trade.exit_time)
    return trades


def run_lab(symbol: str, route: RecentModule, cache: DataCache, client: BingXClient, output_root: Path, days: int, initial_capital: float, risk_per_trade: float, end_time: Optional[datetime] = None) -> List[RecentTrade]:
    config = build_lab_config(days, route.config_preset, initial_capital, risk_per_trade, end_time)
    variant = lab_variant_by_name(route.name)
    strategy = PredictiveReversalLab(config, variant, client, cache, output_root)
    shallow_strategy: Optional[PredictiveReversalLab] = None
    if route.adaptive_shallow_threshold is not None and route.name == "adl15_noavwap":
        shallow_variant = variant.__class__(
            name="adl15_shallow_noavwap",
            confirm_interval="15m",
            indicator_mode="adl",
            require_avwap=False,
            require_cmf_shift=False,
            require_bb_reentry=False,
            entry_fib=0.236,
            tp1_r=0.62,
            tp2_r=1.28,
            min_rr=0.95,
        )
        shallow_strategy = PredictiveReversalLab(config, shallow_variant, client, cache, output_root)
    frames = strategy.prepare_frames(symbol)
    trades: List[RecentTrade] = []
    next_available: Optional[pd.Timestamp] = None
    last_signal: Optional[pd.Timestamp] = None
    bar_minutes = 30 if variant.confirm_interval == "30m" else 15
    for timestamp in frames[variant.confirm_interval].index[120:]:
        if next_available is not None and timestamp <= next_available:
            continue
        if last_signal is not None:
            bars_since = int((timestamp - last_signal).total_seconds() // (bar_minutes * 60))
            if bars_since < config.signal_cooldown_bars_confirm:
                continue
        signal = strategy.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        quality_score = 0.0
        if shallow_strategy is not None:
            quality_score = impulse_quality_score(signal, frames[variant.confirm_interval].loc[timestamp])
            if quality_score >= route.adaptive_shallow_threshold:
                shallow_signal = shallow_strategy.build_signal(symbol, frames, timestamp)
                if shallow_signal is not None:
                    signal = shallow_signal
        trade = execute_lab_trade(config, signal, frames["5m"])
        last_signal = timestamp
        if trade is None:
            continue
        mult = size_multiplier(route)
        trades.append(RecentTrade(symbol, "lab", route.name, route.priority, trade.direction, trade.entry_time, trade.exit_time, float(trade.net_pnl), float(trade.r_multiple), float(trade.hold_hours), module_confidence(route), mult, float(trade.net_pnl) * mult, float(trade.r_multiple) * mult, str(trade.note)))
        latest_exit = pd.Timestamp(trade.exit_time)
        if route.second_chance_confirm_bars > 0 and route.second_chance_min_primary_r is not None and route.second_chance_min_quality_score is not None and route.name == "adl15_noavwap":
            retry = build_second_chance_trade(config, signal, trade, frames["5m"], quality_score, route.second_chance_min_primary_r, route.second_chance_min_quality_score, route.second_chance_confirm_bars)
            if retry is not None:
                trades.append(RecentTrade(symbol, "lab", f"{route.name}_retry", route.priority - 1, retry.direction, retry.entry_time, retry.exit_time, float(retry.net_pnl), float(retry.r_multiple), float(retry.hold_hours), module_confidence(route) - 0.4, mult, float(retry.net_pnl) * mult, float(retry.r_multiple) * mult, str(retry.note)))
                latest_exit = pd.Timestamp(retry.exit_time)
        next_available = latest_exit
    return trades


def select_non_overlapping(candidates: List[RecentTrade]) -> List[RecentTrade]:
    ordered = sorted(candidates, key=lambda trade: (pd.Timestamp(trade.entry_time), -trade.confidence_score, -trade.priority, -trade.size_multiplier))
    selected: List[RecentTrade] = []
    busy_until: Optional[pd.Timestamp] = None
    for trade in ordered:
        entry_time = pd.Timestamp(trade.entry_time)
        exit_time = pd.Timestamp(trade.exit_time)
        if busy_until is not None and entry_time <= busy_until:
            continue
        selected.append(trade)
        busy_until = exit_time
    return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recent-focused ETH router for the last 2-3 months.")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--end-date", default=None, help="UTC end date for the test window, for example 2026-01-20")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/baseline_run")
    parser.add_argument("--initial-capital", type=float, default=10.0)
    parser.add_argument("--risk-percent", type=float, default=1.0)
    parser.add_argument("--profile", choices=["quality", "balanced"], default="quality")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    routes = default_routes(args.profile)
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    risk_per_trade = args.risk_percent / 100.0
    end_time = parse_end_time(args.end_date)
    candidates: List[RecentTrade] = []
    for route in routes:
        if route.kind == "active":
            candidates.extend(run_active(args.symbol, route, cache, client, output_root, args.days, args.initial_capital, risk_per_trade, end_time))
        else:
            candidates.extend(run_lab(args.symbol, route, cache, client, output_root, args.days, args.initial_capital, risk_per_trade, end_time))
    selected = select_non_overlapping(candidates)
    payload = {
        "symbol": args.symbol,
        "profile": args.profile,
        "routes": [asdict(route) for route in routes],
        "run_config": {"days": args.days, "end_date": args.end_date, "initial_capital": args.initial_capital, "risk_per_trade": risk_per_trade},
        "raw_summary": summarize(args.initial_capital, selected, adjusted=False),
        "adjusted_summary": summarize(args.initial_capital, selected, adjusted=True),
        "selected_trades": [asdict(item) for item in selected],
    }
    with (output_root / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    pd.DataFrame([asdict(item) for item in selected]).to_csv(output_root / "selected_trades.csv", index=False)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
