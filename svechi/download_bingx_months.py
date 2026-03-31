from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bingx_regime_fib_backtest import BingXClient


def parse_month(value: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(value, "%Y-%m").replace(tzinfo=timezone.utc)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def main() -> None:
    parser = argparse.ArgumentParser(description="Download BingX ETH monthly 5m data.")
    parser.add_argument("--months", nargs="+", required=True, help="Months in YYYY-MM format")
    parser.add_argument("--symbol", default="ETH-USDT")
    parser.add_argument("--interval", default="5m")
    parser.add_argument("--out-dir", default="svechi/month_datasets")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    client = BingXClient()

    for month in args.months:
        start, end = parse_month(month)
        df = client.fetch_klines(args.symbol, args.interval, start, end, limit=1000)
        out_path = out_dir / f"{args.symbol.replace('-', '_')}_{args.interval}_{month}.csv"
        df.reset_index().to_csv(out_path, index=False)
        print(f"saved {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()
