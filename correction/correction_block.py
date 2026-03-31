from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from correction import (
    RecentTrade,
    default_routes,
    run_active,
    run_lab,
    select_non_overlapping as select_correction_non_overlapping,
    summarize as summarize_correction,
)
from correction_liquidity_sweep import (
    SweepTrade,
    default_sweep_profile,
    default_variants as sweep_variants,
    run_sweep_profile,
    summarize as summarize_sweep,
)
from correction_regime import BingXClient, DataCache, parse_end_time
from correction_trend import (
    TrendHqTrade,
    curated_rules,
    default_trend_profile,
    run_trend_profile,
    summarize as summarize_trend,
    trend_profiles,
)
from eth_supervisor import (
    convert_correction_trade,
    convert_sweep_trade,
    convert_trend_trade,
    select_non_overlapping as select_supervisor_non_overlapping,
    summarize as summarize_supervisor,
)


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CACHE_DIR = BASE_DIR / "data_cache"
DEFAULT_OUTPUT_DIR = BASE_DIR / "reports" / "block_run"
BASIC_CORRECTION_PROFILE = "quality"


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def build_correction_payload(
    symbol: str,
    days: int,
    end_date: Optional[str],
    initial_capital: float,
    risk_per_trade: float,
    correction_strategy: str,
    cache: DataCache,
    client: BingXClient,
    output_root: Path,
) -> tuple[Dict[str, Any], List[RecentTrade]]:
    profile = BASIC_CORRECTION_PROFILE
    if correction_strategy != "basic":
        raise KeyError(f"Unknown correction strategy: {correction_strategy}")
    routes = default_routes(profile)
    end_time = parse_end_time(end_date)
    candidates: List[RecentTrade] = []
    for route in routes:
        if route.kind == "active":
            candidates.extend(run_active(symbol, route, cache, client, output_root, days, initial_capital, risk_per_trade, end_time))
        else:
            candidates.extend(run_lab(symbol, route, cache, client, output_root, days, initial_capital, risk_per_trade, end_time))
    selected = select_correction_non_overlapping(candidates)
    payload = {
        "module": "correction",
        "symbol": symbol,
        "strategy": correction_strategy,
        "internal_profile": profile,
        "routes": [asdict(route) for route in routes],
        "run_config": {
            "days": days,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "risk_per_trade": risk_per_trade,
        },
        "raw_summary": summarize_correction(initial_capital, selected, adjusted=False),
        "adjusted_summary": summarize_correction(initial_capital, selected, adjusted=True),
        "selected_trades": [asdict(item) for item in selected],
    }
    write_json(output_root / "summary.json", payload)
    write_csv(output_root / "selected_trades.csv", [asdict(item) for item in selected])
    return payload, selected


def build_trend_payload(
    symbol: str,
    days: int,
    end_date: Optional[str],
    initial_capital: float,
    profile: str,
    cache: DataCache,
    client: BingXClient,
    output_root: Path,
) -> tuple[Dict[str, Any], List[TrendHqTrade]]:
    end_time = parse_end_time(end_date)
    trades = run_trend_profile(profile, days, symbol, client, cache, output_root, end_time)
    payload = {
        "module": "trend",
        "symbol": symbol,
        "profile": profile,
        "run_config": {
            "days": days,
            "end_date": end_date,
            "initial_capital": initial_capital,
        },
        "profile_legs": [asdict(item) for item in trend_profiles().get(profile, [])],
        "rules": [asdict(item) for item in curated_rules()],
        "summary": summarize_trend(initial_capital, trades, days),
        "trades": [asdict(item) for item in trades],
    }
    write_json(output_root / "summary.json", payload)
    write_csv(output_root / "trades.csv", [asdict(item) for item in trades])
    return payload, trades


def build_sweep_payload(
    symbol: str,
    days: int,
    end_date: Optional[str],
    initial_capital: float,
    profile: str,
    cache: DataCache,
    client: BingXClient,
    output_root: Path,
) -> tuple[Dict[str, Any], List[SweepTrade]]:
    end_time = parse_end_time(end_date)
    trades = run_sweep_profile(profile, days, symbol, client, cache, output_root, end_time)
    payload = {
        "module": "sweep",
        "symbol": symbol,
        "profile": profile,
        "run_config": {
            "days": days,
            "end_date": end_date,
            "initial_capital": initial_capital,
        },
        "variants": [asdict(item) for item in sweep_variants()],
        "summary": summarize_sweep(initial_capital, trades, days),
        "trades": [asdict(item) for item in trades],
    }
    write_json(output_root / "summary.json", payload)
    write_csv(output_root / "trades.csv", [asdict(item) for item in trades])
    return payload, trades


def build_supervisor_payload(
    symbol: str,
    days: int,
    end_date: Optional[str],
    initial_capital: float,
    risk_per_trade: float,
    trend_profile: str,
    sweep_profile: str,
    correction_trades: List[RecentTrade],
    trend_trades: List[TrendHqTrade],
    sweep_trades: List[SweepTrade],
    output_root: Path,
) -> Dict[str, Any]:
    combined_candidates = [convert_correction_trade(item) for item in correction_trades]
    combined_candidates.extend(convert_trend_trade(item) for item in trend_trades)
    combined_candidates.extend(convert_sweep_trade(item) for item in sweep_trades)
    combined_selected = select_supervisor_non_overlapping(combined_candidates)
    payload = {
        "module": "supervisor",
        "symbol": symbol,
        "days": days,
        "end_date": end_date,
        "initial_capital": initial_capital,
        "risk_per_trade": risk_per_trade,
        "trend_profile": trend_profile,
        "sweep_profile": sweep_profile,
        "correction_summary": summarize_supervisor(initial_capital, [convert_correction_trade(item) for item in correction_trades], days),
        "trend_summary": summarize_supervisor(initial_capital, [convert_trend_trade(item) for item in trend_trades], days),
        "sweep_summary": summarize_supervisor(initial_capital, [convert_sweep_trade(item) for item in sweep_trades], days),
        "combined_summary": summarize_supervisor(initial_capital, combined_selected, days),
        "counts_by_source": {
            "correction": int(sum(1 for item in combined_selected if item.source == "correction")),
            "trend": int(sum(1 for item in combined_selected if item.source == "trend")),
            "sweep": int(sum(1 for item in combined_selected if item.source == "sweep")),
        },
        "combined_trades": [asdict(item) for item in combined_selected],
    }
    write_json(output_root / "summary.json", payload)
    write_csv(output_root / "combined_trades.csv", [asdict(item) for item in combined_selected])
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified working block for ETH correction, trend, sweep, and supervisor.")
    parser.add_argument("--mode", choices=["correction", "trend", "sweep", "supervisor", "all"], default="all")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--end-date", default=None, help="UTC end date for the test window, for example 2026-01-20")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--initial-capital", type=float, default=10.0)
    parser.add_argument("--risk-percent", type=float, default=1.0)
    parser.add_argument("--correction-strategy", choices=["basic"], default="basic")
    parser.add_argument(
        "--trend-profile",
        default=default_trend_profile(),
        choices=list(trend_profiles().keys()) + [rule.name for rule in curated_rules()],
    )
    parser.add_argument("--sweep-profile", default="off", choices=["off"] + [item.name for item in sweep_variants()])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    cache = DataCache(Path(args.cache_dir))
    client = BingXClient()
    risk_per_trade = args.risk_percent / 100.0

    correction_payload: Optional[Dict[str, Any]] = None
    trend_payload: Optional[Dict[str, Any]] = None
    sweep_payload: Optional[Dict[str, Any]] = None
    supervisor_payload: Optional[Dict[str, Any]] = None
    correction_trades: List[RecentTrade] = []
    trend_trades: List[TrendHqTrade] = []
    sweep_trades: List[SweepTrade] = []

    if args.mode in {"correction", "supervisor", "all"}:
        correction_payload, correction_trades = build_correction_payload(
            args.symbol,
            args.days,
            args.end_date,
            args.initial_capital,
            risk_per_trade,
            args.correction_strategy,
            cache,
            client,
            output_root / "correction",
        )

    if args.mode in {"trend", "supervisor", "all"}:
        trend_payload, trend_trades = build_trend_payload(
            args.symbol,
            args.days,
            args.end_date,
            args.initial_capital,
            args.trend_profile,
            cache,
            client,
            output_root / "trend",
        )

    if args.mode in {"sweep", "supervisor", "all"} and args.sweep_profile != "off":
        sweep_payload, sweep_trades = build_sweep_payload(
            args.symbol,
            args.days,
            args.end_date,
            args.initial_capital,
            args.sweep_profile,
            cache,
            client,
            output_root / "sweep",
        )

    if args.mode == "supervisor":
        supervisor_payload = build_supervisor_payload(
            args.symbol,
            args.days,
            args.end_date,
            args.initial_capital,
            risk_per_trade,
            args.trend_profile,
            args.sweep_profile,
            correction_trades,
            trend_trades,
            sweep_trades,
            output_root / "supervisor",
        )
    elif args.mode == "all":
        supervisor_payload = build_supervisor_payload(
            args.symbol,
            args.days,
            args.end_date,
            args.initial_capital,
            risk_per_trade,
            args.trend_profile,
            args.sweep_profile,
            correction_trades,
            trend_trades,
            sweep_trades,
            output_root / "supervisor",
        )

    bundle_payload: Dict[str, Any] = {
        "mode": args.mode,
        "symbol": args.symbol,
        "days": args.days,
        "end_date": args.end_date,
        "initial_capital": args.initial_capital,
        "risk_per_trade": risk_per_trade,
        "correction_strategy": args.correction_strategy,
        "trend_profile": args.trend_profile,
        "sweep_profile": args.sweep_profile,
        "components": {},
    }

    if correction_payload is not None:
        bundle_payload["components"]["correction"] = {
            "output_dir": str((output_root / "correction").resolve()),
            "summary": correction_payload["adjusted_summary"],
        }
    if trend_payload is not None:
        bundle_payload["components"]["trend"] = {
            "output_dir": str((output_root / "trend").resolve()),
            "summary": trend_payload["summary"],
        }
    if sweep_payload is not None:
        bundle_payload["components"]["sweep"] = {
            "output_dir": str((output_root / "sweep").resolve()),
            "summary": sweep_payload["summary"],
        }
    if supervisor_payload is not None:
        bundle_payload["components"]["supervisor"] = {
            "output_dir": str((output_root / "supervisor").resolve()),
            "summary": supervisor_payload["combined_summary"],
            "counts_by_source": supervisor_payload["counts_by_source"],
        }

    write_json(output_root / "bundle_summary.json", bundle_payload)
    print(json.dumps(bundle_payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
