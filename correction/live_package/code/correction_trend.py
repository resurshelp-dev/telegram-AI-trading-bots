from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction_trend_pullback import (
    PullbackConfig,
    PullbackTrade,
    EthTrendPullbackLab,
    default_variants,
    execute_trade,
)
from correction_regime import BingXClient, DataCache, candle_metrics, max_drawdown, parse_end_time


@dataclass
class TrendHqRule:
    name: str
    base_variant_name: str
    session: Optional[str] = None
    direction: Optional[str] = None
    min_er: Optional[float] = None
    min_body_ratio: Optional[float] = None
    min_t1h: Optional[float] = None
    max_stop_atr: Optional[float] = None
    priority: int = 1


@dataclass
class TrendProfileLeg:
    rule_name: str
    min_confidence: float = 0.0
    direction: Optional[str] = None
    min_t1h: Optional[float] = None
    max_stop_atr: Optional[float] = None
    tp1_r: Optional[float] = None
    tp2_r: Optional[float] = None
    breakeven_buffer_r: Optional[float] = None
    max_hold_bars_5m: Optional[int] = None


@dataclass
class TrendHqTrade:
    symbol: str
    rule_name: str
    base_variant_name: str
    direction: str
    setup_time: str
    entry_time: str
    exit_time: str
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    exit_price: float
    r_multiple: float
    net_pnl: float
    hold_hours: float
    exit_reason: str
    session: str
    er: float
    body_ratio: float
    t1h: float
    stop_atr: float
    priority: int
    confidence: float
    note: str


def curated_rules() -> List[TrendHqRule]:
    return [
        TrendHqRule(
            name="hq_micro_euus_er36",
            base_variant_name="micro_pullback_15m",
            session="euus",
            min_er=0.36,
            priority=2,
        ),
        TrendHqRule(
            name="hq_micro_euus_body45_er30_stop12",
            base_variant_name="micro_pullback_15m",
            session="euus",
            min_er=0.30,
            min_body_ratio=0.45,
            max_stop_atr=1.20,
            priority=3,
        ),
        TrendHqRule(
            name="hq_avwap_body45_stop12",
            base_variant_name="ema_avwap_pullback_15m",
            min_body_ratio=0.45,
            max_stop_atr=1.20,
            priority=4,
        ),
        TrendHqRule(
            name="hq_avwap_euus_er36",
            base_variant_name="ema_avwap_pullback_15m",
            session="euus",
            min_er=0.36,
            priority=3,
        ),
        TrendHqRule(
            name="hq_micro_short_euus_er36",
            base_variant_name="micro_pullback_15m",
            session="euus",
            direction="short",
            min_er=0.36,
            priority=3,
        ),
    ]


def default_trend_profile() -> str:
    return "profit_max_locked"


def trend_profiles() -> Dict[str, List[TrendProfileLeg]]:
    return {
        "combo_hq": [],
        "profit_max_locked": [
            TrendProfileLeg(rule_name="hq_micro_euus_er36", min_confidence=4.5, direction="long", min_t1h=5.0),
            TrendProfileLeg(rule_name="hq_avwap_body45_stop12", min_confidence=3.5),
            TrendProfileLeg(rule_name="hq_avwap_euus_er36", min_confidence=3.4, direction="long", min_t1h=4.8),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=4.8, direction="long", min_t1h=7.0),
        ],
        "profit_locked_extreme": [
            TrendProfileLeg(rule_name="hq_micro_euus_er36", min_confidence=4.5, direction="long", min_t1h=5.0),
            TrendProfileLeg(rule_name="hq_avwap_body45_stop12", min_confidence=3.5),
        ],
        "profit_combo_filtered": [
            TrendProfileLeg(rule_name="hq_micro_euus_er36", min_confidence=3.5, direction="long", min_t1h=5.0),
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=2.5, direction="short", min_t1h=3.0),
            TrendProfileLeg(rule_name="hq_avwap_body45_stop12", min_confidence=3.0),
        ],
        "defensive_hq": [
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=4.0, direction="short"),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=3.0, direction="short"),
        ],
        "defensive_hq_t1h": [
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=4.0, direction="short", min_t1h=5.8),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=3.0, direction="short", min_t1h=5.8),
        ],
        "profit_hq_t1h": [
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=4.0, direction="short", min_t1h=5.8, tp1_r=0.72, tp2_r=1.60, breakeven_buffer_r=0.10, max_hold_bars_5m=120),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=3.0, direction="short", min_t1h=5.8, tp1_r=0.78, tp2_r=1.65, breakeven_buffer_r=0.08, max_hold_bars_5m=120),
        ],
        "profit_body_hq_t1h": [
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=4.0, direction="short", min_t1h=5.8),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=3.0, direction="short", min_t1h=5.8, tp1_r=0.78, tp2_r=1.65, breakeven_buffer_r=0.08, max_hold_bars_5m=120),
        ],
        "balanced_body_hq_t1h": [
            TrendProfileLeg(rule_name="hq_micro_short_euus_er36", min_confidence=3.8, direction="short", min_t1h=5.0),
            TrendProfileLeg(rule_name="hq_micro_euus_body45_er30_stop12", min_confidence=3.0, direction="short", min_t1h=5.0, tp1_r=0.78, tp2_r=1.65, breakeven_buffer_r=0.08, max_hold_bars_5m=120),
        ],
    }


def summarize(initial_capital: float, trades: List[TrendHqTrade], days: int) -> Dict[str, float]:
    if not trades:
        return {"trades": 0, "trades_per_month": 0.0, "win_rate": 0.0, "expectancy_r": 0.0, "profit_factor": 0.0, "net_pnl": 0.0, "max_drawdown_pct": 0.0, "avg_hold_hours": 0.0}
    pnl = np.array([trade.net_pnl for trade in trades], dtype=float)
    r_values = np.array([trade.r_multiple for trade in trades], dtype=float)
    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    profit_factor = float(winners.sum() / abs(losers.sum())) if losers.size and abs(losers.sum()) > 0 else (float("inf") if winners.size else 0.0)
    balance = initial_capital
    equity = [balance]
    for trade in trades:
        balance += trade.net_pnl
        equity.append(balance)
    return {
        "trades": int(len(trades)),
        "trades_per_month": float(len(trades) / max(days / 30.0, 1.0)),
        "win_rate": float((r_values > 0).mean() * 100.0),
        "expectancy_r": float(r_values.mean()),
        "profit_factor": profit_factor,
        "net_pnl": float(pnl.sum()),
        "max_drawdown_pct": float(max_drawdown(equity) * 100.0),
        "avg_hold_hours": float(np.mean([trade.hold_hours for trade in trades])),
    }


def select_non_overlapping(trades: List[TrendHqTrade]) -> List[TrendHqTrade]:
    ordered = sorted(
        trades,
        key=lambda item: (
            pd.Timestamp(item.entry_time),
            -item.priority,
            -item.confidence,
        ),
    )
    selected: List[TrendHqTrade] = []
    busy_until: Optional[pd.Timestamp] = None
    for trade in ordered:
        entry_time = pd.Timestamp(trade.entry_time)
        exit_time = pd.Timestamp(trade.exit_time)
        if busy_until is not None and entry_time <= busy_until:
            continue
        selected.append(trade)
        busy_until = exit_time
    return selected


def feature_snapshot(base_lab: EthTrendPullbackLab, frames: Dict[str, pd.DataFrame], timestamp: pd.Timestamp, variant) -> Optional[Dict[str, float | str]]:
    h1 = base_lab.slice_frame(frames["1h"], timestamp, 60)
    confirm = base_lab.slice_frame(frames["15m"], timestamp, 80)
    if h1 is None or confirm is None:
        return None
    direction = base_lab.trend_direction(h1)
    if direction is None:
        return None
    recent = confirm.iloc[-(variant.pullback_bars + 2) :]
    if len(recent) < variant.pullback_bars + 2:
        return None
    current = recent.iloc[-1]
    atr_now = float(current["atr"])
    if np.isnan(atr_now) or atr_now <= 0:
        return None
    signal = base_lab.build_signal("ETH-USDT", frames, timestamp)
    if signal is None:
        return None
    metrics = candle_metrics(current)
    h1_current = h1.iloc[-1]
    entry_hour = pd.Timestamp(timestamp).hour
    session = "euus" if 8 <= entry_hour <= 20 else "asia"
    stop_atr = abs(signal.planned_entry - signal.stop_price) / atr_now
    return {
        "direction": direction,
        "session": session,
        "er": float(current["er"]),
        "body_ratio": metrics["body"] / max(metrics["range"], 1e-9),
        "t1h": abs(float(h1_current["trend_t"])),
        "stop_atr": stop_atr,
    }


def passes_rule(rule: TrendHqRule, features: Dict[str, float | str]) -> bool:
    if rule.session is not None and features["session"] != rule.session:
        return False
    if rule.direction is not None and features["direction"] != rule.direction:
        return False
    if rule.min_er is not None and float(features["er"]) < rule.min_er:
        return False
    if rule.min_body_ratio is not None and float(features["body_ratio"]) < rule.min_body_ratio:
        return False
    if rule.min_t1h is not None and float(features["t1h"]) < rule.min_t1h:
        return False
    if rule.max_stop_atr is not None and float(features["stop_atr"]) > rule.max_stop_atr:
        return False
    return True


def confidence_score(features: Dict[str, float | str]) -> float:
    return float(features["er"]) * 1.8 + float(features["body_ratio"]) * 1.3 + float(features["t1h"]) * 0.4 - float(features["stop_atr"]) * 0.25


def run_rule(
    rule: TrendHqRule,
    days: int,
    symbol: str,
    client: BingXClient,
    cache: DataCache,
    output_root: Path,
    end_time: Optional[datetime] = None,
    tp1_r_override: Optional[float] = None,
    tp2_r_override: Optional[float] = None,
    breakeven_buffer_r_override: Optional[float] = None,
    max_hold_bars_5m_override: Optional[int] = None,
) -> List[TrendHqTrade]:
    variant = next(item for item in default_variants() if item.name == rule.base_variant_name)
    config = PullbackConfig(days=days, end_time=end_time)
    if breakeven_buffer_r_override is not None or max_hold_bars_5m_override is not None:
        config = replace(
            config,
            breakeven_buffer_r=breakeven_buffer_r_override if breakeven_buffer_r_override is not None else config.breakeven_buffer_r,
            max_hold_bars_5m=max_hold_bars_5m_override if max_hold_bars_5m_override is not None else config.max_hold_bars_5m,
        )
    lab = EthTrendPullbackLab(config, variant, client, cache, output_root)
    frames = lab.prepare_frames(symbol)
    trades: List[TrendHqTrade] = []
    next_available: Optional[pd.Timestamp] = None
    last_signal: Optional[pd.Timestamp] = None
    for timestamp in frames["15m"].index[120:]:
        if next_available is not None and timestamp <= next_available:
            continue
        if last_signal is not None:
            bars_since = int((timestamp - last_signal).total_seconds() // (15 * 60))
            if bars_since < config.signal_cooldown_bars_15m:
                continue
        features = feature_snapshot(lab, frames, timestamp, variant)
        if features is None or not passes_rule(rule, features):
            continue
        signal = lab.build_signal(symbol, frames, timestamp)
        if signal is None:
            continue
        if tp1_r_override is not None or tp2_r_override is not None:
            signal = replace(
                signal,
                tp1_r=tp1_r_override if tp1_r_override is not None else signal.tp1_r,
                tp2_r=tp2_r_override if tp2_r_override is not None else signal.tp2_r,
            )
        trade = execute_trade(config, signal, frames["5m"])
        last_signal = timestamp
        if trade is None:
            continue
        next_available = pd.Timestamp(trade.exit_time)
        trades.append(
            TrendHqTrade(
                symbol=trade.symbol,
                rule_name=rule.name,
                base_variant_name=rule.base_variant_name,
                direction=trade.direction,
                setup_time=trade.setup_time,
                entry_time=trade.entry_time,
                exit_time=trade.exit_time,
                entry_price=trade.entry_price,
                stop_price=trade.stop_price,
                tp1_price=trade.tp1_price,
                tp2_price=trade.tp2_price,
                exit_price=trade.exit_price,
                r_multiple=trade.r_multiple,
                net_pnl=trade.net_pnl,
                hold_hours=trade.hold_hours,
                exit_reason=trade.exit_reason,
                session=str(features["session"]),
                er=float(features["er"]),
                body_ratio=float(features["body_ratio"]),
                t1h=float(features["t1h"]),
                stop_atr=float(features["stop_atr"]),
                priority=rule.priority,
                confidence=confidence_score(features),
                note=trade.note,
            )
        )
    return trades


def run_combo(days: int, symbol: str, client: BingXClient, cache: DataCache, output_root: Path, end_time: Optional[datetime] = None) -> List[TrendHqTrade]:
    candidates: List[TrendHqTrade] = []
    for rule in curated_rules():
        candidates.extend(run_rule(rule, days, symbol, client, cache, output_root, end_time))
    return select_non_overlapping(candidates)


def run_curated_profile(profile_name: str, days: int, symbol: str, client: BingXClient, cache: DataCache, output_root: Path, end_time: Optional[datetime] = None) -> List[TrendHqTrade]:
    legs = trend_profiles()[profile_name]
    rules_by_name = {rule.name: rule for rule in curated_rules()}
    candidates: List[TrendHqTrade] = []
    for leg in legs:
        rule = rules_by_name[leg.rule_name]
        trades = run_rule(
            rule,
            days,
            symbol,
            client,
            cache,
            output_root,
            end_time,
            leg.tp1_r,
            leg.tp2_r,
            leg.breakeven_buffer_r,
            leg.max_hold_bars_5m,
        )
        for trade in trades:
            if trade.confidence < leg.min_confidence:
                continue
            if leg.direction is not None and trade.direction != leg.direction:
                continue
            if leg.min_t1h is not None and trade.t1h < leg.min_t1h:
                continue
            if leg.max_stop_atr is not None and trade.stop_atr > leg.max_stop_atr:
                continue
            candidates.append(trade)
    return select_non_overlapping(candidates)


def run_trend_profile(profile: str, days: int, symbol: str, client: BingXClient, cache: DataCache, output_root: Path, end_time: Optional[datetime] = None) -> List[TrendHqTrade]:
    if profile == "combo_hq":
        return run_combo(days, symbol, client, cache, output_root, end_time)
    if profile in trend_profiles():
        return run_curated_profile(profile, days, symbol, client, cache, output_root, end_time)
    rule = next((item for item in curated_rules() if item.name == profile), None)
    if rule is None:
        raise KeyError(f"Unknown trend profile: {profile}")
    return run_rule(rule, days, symbol, client, cache, output_root, end_time)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recent ETH HQ trend lab.")
    parser.add_argument("--days", nargs="+", type=int, default=[60, 90])
    parser.add_argument("--end-date", default=None, help="UTC end date for the test window, for example 2026-01-20")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/trend_run")
    parser.add_argument("--profile", default=default_trend_profile(), choices=list(trend_profiles().keys()) + [rule.name for rule in curated_rules()])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    end_time = parse_end_time(args.end_date)
    results: List[Dict[str, object]] = []
    for days in args.days:
        trades = run_trend_profile(args.profile, days, args.symbol, client, cache, output_root, end_time)
        summary = summarize(10.0, trades, days)
        profile_dir = output_root / f"{args.profile}_{days}d"
        profile_dir.mkdir(parents=True, exist_ok=True)
        with (profile_dir / "summary.json").open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "days": days,
                    "end_date": args.end_date,
                    "symbol": args.symbol,
                    "profile": args.profile,
                    "profile_legs": [asdict(item) for item in trend_profiles().get(args.profile, [])],
                    "rules": [asdict(rule) for rule in curated_rules()],
                    "summary": summary,
                    "trades": [asdict(item) for item in trades],
                },
                handle,
                indent=2,
                ensure_ascii=False,
            )
        results.append({"days": days, "mode": "profile", "name": args.profile, "summary": summary})

    results.sort(key=lambda item: (item["days"], item["summary"]["net_pnl"], item["summary"]["win_rate"]), reverse=True)
    with (output_root / "comparison.json").open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, ensure_ascii=False)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
