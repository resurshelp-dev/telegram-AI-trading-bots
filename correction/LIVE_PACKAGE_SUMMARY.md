# Correction Live Package

Current live bundle is fixed to:

- correction strategy: `basic`
- trend profile: `profit_max_locked`
- supervisor: `basic correction + profit_max_locked`

Current validated month result:

- window: `2026-02-18` to `2026-03-20`
- supervisor net pnl: `+0.8917952949444196`
- supervisor win rate: `100%`
- supervisor trades: `15`

Key entrypoints:

- `run_block.ps1`
- `run_exchange.ps1`
- `run_live_block.ps1`

Key files:

- `correction_block.py`
- `correction_exchange.py`
- `correction_live.py`
- `.env`

Primary report:

- `reports/month_test_profit_max_locked_2026_03_20/bundle_summary.json`

Real market smoke test:

- `LIVE_REAL_MARKET_TEST.md`
