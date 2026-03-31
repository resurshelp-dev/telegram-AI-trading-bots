from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from correction import RecentTrade, default_routes, run_active, run_lab
from correction_liquidity_sweep import SweepTrade, default_sweep_profile, run_sweep_profile
from correction_regime import BingXClient, DataCache, max_drawdown, parse_end_time
from correction_trend import TrendHqTrade, default_trend_profile, run_trend_profile


@dataclass
class SupervisorTrade:
    symbol: str
    source: str
    source_name: str
    direction: str
    entry_time: str
    exit_time: str
    net_pnl: float
    r_multiple: float
    hold_hours: float
    priority: int
    confidence: float
    note: str


def summarize(initial_capital: float, trades: List[SupervisorTrade], days: int) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0,
            "trades_per_month": 0.0,
            "win_rate": 0.0,
            "expectancy_r": 0.0,
            "profit_factor": 0.0,
            "net_pnl": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_hold_hours": 0.0,
        }
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


def convert_correction_trade(trade: RecentTrade) -> SupervisorTrade:
    return SupervisorTrade(
        symbol=trade.symbol,
        source="correction",
        source_name=trade.module_name,
        direction=trade.direction,
        entry_time=trade.entry_time,
        exit_time=trade.exit_time,
        net_pnl=trade.adjusted_net_pnl,
        r_multiple=trade.adjusted_r_multiple,
        hold_hours=trade.hold_hours,
        priority=5,
        confidence=trade.confidence_score,
        note=trade.note,
    )


def convert_trend_trade(trade: TrendHqTrade) -> SupervisorTrade:
    return SupervisorTrade(
        symbol=trade.symbol,
        source="trend",
        source_name=trade.rule_name,
        direction=trade.direction,
        entry_time=trade.entry_time,
        exit_time=trade.exit_time,
        net_pnl=trade.net_pnl,
        r_multiple=trade.r_multiple,
        hold_hours=trade.hold_hours,
        priority=trade.priority,
        confidence=trade.confidence,
        note=trade.note,
    )


def convert_sweep_trade(trade: SweepTrade) -> SupervisorTrade:
    return SupervisorTrade(
        symbol=trade.symbol,
        source="sweep",
        source_name=trade.variant_name,
        direction=trade.direction,
        entry_time=trade.entry_time,
        exit_time=trade.exit_time,
        net_pnl=trade.net_pnl,
        r_multiple=trade.r_multiple,
        hold_hours=trade.hold_hours,
        priority=4,
        confidence=trade.score,
        note=trade.note,
    )


def select_non_overlapping(trades: List[SupervisorTrade]) -> List[SupervisorTrade]:
    ordered = sorted(
        trades,
        key=lambda item: (
            pd.Timestamp(item.entry_time),
            -item.priority,
            -item.confidence,
        ),
    )
    selected: List[SupervisorTrade] = []
    busy_until: Optional[pd.Timestamp] = None
    for trade in ordered:
        entry_time = pd.Timestamp(trade.entry_time)
        exit_time = pd.Timestamp(trade.exit_time)
        if busy_until is not None and entry_time <= busy_until:
            continue
        selected.append(trade)
        busy_until = exit_time
    return selected


def run_correction_baseline(symbol: str, days: int, initial_capital: float, risk_per_trade: float, cache: DataCache, client: BingXClient, output_root: Path, end_time=None) -> List[RecentTrade]:
    candidates: List[RecentTrade] = []
    for route in default_routes("quality"):
        if route.kind == "active":
            candidates.extend(run_active(symbol, route, cache, client, output_root, days, initial_capital, risk_per_trade, end_time))
        else:
            candidates.extend(run_lab(symbol, route, cache, client, output_root, days, initial_capital, risk_per_trade, end_time))
    ordered = sorted(
        candidates,
        key=lambda trade: (
            pd.Timestamp(trade.entry_time),
            -trade.confidence_score,
            -trade.priority,
            -trade.size_multiplier,
        ),
    )
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
    parser = argparse.ArgumentParser(description="ETH supervisor for correction + trend + sweep reclaim.")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--end-date", default=None, help="UTC end date for the test window, for example 2026-01-20")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default="correction/data_cache")
    parser.add_argument("--output-dir", default="correction/reports/supervisor_run")
    parser.add_argument("--initial-capital", type=float, default=10.0)
    parser.add_argument("--risk-percent", type=float, default=1.0)
    parser.add_argument("--trend-profile", default=default_trend_profile())
    parser.add_argument("--sweep-profile", default="off", choices=["off", default_sweep_profile()])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    client = BingXClient()
    cache = DataCache(Path(args.cache_dir))
    risk_per_trade = args.risk_percent / 100.0
    end_time = parse_end_time(args.end_date)

    correction_trades = run_correction_baseline(args.symbol, args.days, args.initial_capital, risk_per_trade, cache, client, output_root, end_time)
    trend_trades = run_trend_profile(args.trend_profile, args.days, args.symbol, client, cache, output_root, end_time)
    sweep_trades = []
    if args.sweep_profile != "off":
        sweep_trades = run_sweep_profile(args.sweep_profile, args.days, args.symbol, client, cache, output_root, end_time)

    combined_candidates = [convert_correction_trade(item) for item in correction_trades]
    combined_candidates.extend(convert_trend_trade(item) for item in trend_trades)
    combined_candidates.extend(convert_sweep_trade(item) for item in sweep_trades)
    combined_selected = select_non_overlapping(combined_candidates)

    payload = {
        "symbol": args.symbol,
        "days": args.days,
        "end_date": args.end_date,
        "initial_capital": args.initial_capital,
        "risk_per_trade": risk_per_trade,
        "trend_profile": args.trend_profile,
        "sweep_profile": args.sweep_profile,
        "correction_summary": summarize(args.initial_capital, [convert_correction_trade(item) for item in correction_trades], args.days),
        "trend_summary": summarize(args.initial_capital, [convert_trend_trade(item) for item in trend_trades], args.days),
        "sweep_summary": summarize(args.initial_capital, [convert_sweep_trade(item) for item in sweep_trades], args.days),
        "combined_summary": summarize(args.initial_capital, combined_selected, args.days),
        "counts_by_source": {
            "correction": int(sum(1 for item in combined_selected if item.source == "correction")),
            "trend": int(sum(1 for item in combined_selected if item.source == "trend")),
            "sweep": int(sum(1 for item in combined_selected if item.source == "sweep")),
        },
        "combined_trades": [asdict(item) for item in combined_selected],
    }
    with (output_root / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    pd.DataFrame([asdict(item) for item in combined_selected]).to_csv(output_root / "combined_trades.csv", index=False)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
