# ETH Unified Block

This folder is organized around four top-level components:

- `correction.py` is the basic correction strategy.
- `correction_trend.py` is the separate trend strategy.
- `correction_liquidity_sweep.py` is the liquidity sweep + reclaim strategy.
- `eth_supervisor.py` combines `basic correction + trend + liquidity sweep` without overlapping trades.

Current high-profit working pair:

- correction strategy: `basic`
- trend profile: `profit_max_locked`
- sweep profile for research: `sweep_15m_round_reclaim_combo`

The only user-facing launcher is:

```powershell
.\correction\launch_system_window.cmd
```

Modes:

- `--mode correction` runs only the basic correction strategy.
- `--mode trend` runs only the trend strategy.
- `--mode sweep` runs only the liquidity sweep + reclaim strategy.
- `--mode supervisor` builds the supervisor output from basic correction + trend + sweep.
- `--mode all` runs the full block and writes all component reports together.

Useful options:

- `--correction-strategy basic`
- `--trend-profile profit_max_locked`
- `--sweep-profile off`
- `--end-date 2026-01-20`
- `--cache-dir <path>`
- `--output-dir <path>`

Default paths for the unified block:

- cache: `correction/data_cache`
- reports root: `correction/reports/block_run`
- bundle summary: `correction/reports/block_run/bundle_summary.json`

Output layout for `--mode all`:

- `correction/summary.json`
- `correction/selected_trades.csv`
- `trend/summary.json`
- `trend/trades.csv`
- `sweep/summary.json`
- `sweep/trades.csv`
- `supervisor/summary.json`
- `supervisor/combined_trades.csv`
- `bundle_summary.json`

Launcher layout:

- `launch_system_window.cmd` is the only file you should launch manually.
- `run_system.ps1` is the only technical runner kept in the root.
- old helper launchers were moved to `legacy_launchers/`.

Liquidity sweep + reclaim lab:

- `correction_liquidity_sweep.py` tests reclaim entries after liquidity sweeps beyond daily and round-number liquidity.
- Best monthly profile on the latest cached 30-day window is `sweep_15m_round_reclaim_combo`.
- That profile keeps only:
  - long reclaims from `round_low` and `prev_day_low`
  - short reclaims from `round_high`
- `sweep` is not enabled by default in the unified block or live system.
- It improved the March 2026 window, but degraded January 2026 windows, so it remains an optional research module until it gets a regime filter.

Example:

```powershell
powershell -ExecutionPolicy Bypass -File .\correction\run_liquidity_sweep.ps1 --days 30 --symbol ETH-USDT --cache-dir C:\Users\User\PycharmProjects\neirosystems\correction\data_cache --output-dir C:\Users\User\PycharmProjects\neirosystems\correction\reports\liquidity_sweep_month
```

Exchange wrapper:

- `correction_exchange.py` is a safe BingX wrapper for balance, positions, orders, price, market entry, market close and protection orders.
- `run_exchange.ps1` launches it through the project virtualenv.
- `.env.example` shows the required BingX configuration.

Examples:

```powershell
.\correction\run_exchange.ps1 health --symbol ETH-USDT
```

```powershell
.\correction\run_exchange.ps1 price --symbol ETH-USDT
```

```powershell
.\correction\run_exchange.ps1 buy --symbol ETH-USDT --qty 0.01 --paper true
```

Live order-changing commands require explicit confirmation:

```powershell
.\correction\run_exchange.ps1 buy --symbol ETH-USDT --qty 0.01 --paper false --confirm-live
```

Unified live system:

- `correction_live.py` scans the fixed pair `basic correction + trend` and can execute the freshest plan through the exchange wrapper.
- `correction_daemon.py` is the continuous execution loop.
- `correction_system_daemon.py` wraps the daemon with a single-instance lock.
- `run_system.ps1` is the only real live entrypoint for this system.
- `launch_system_window.cmd` opens its own dedicated terminal window for the unified system.
- `flatten_eth_position.ps1` is a quick emergency close helper for `ETH-USDT` long.
- Telegram notifications are sent automatically when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` exist in `.env`.
- Notifications cover daemon start, detected signals, execution results, errors and heartbeats.

Examples:

```powershell
powershell -ExecutionPolicy Bypass -File .\correction\run_live_block.ps1 --paper true --data-mode cache scan
```

```powershell
powershell -ExecutionPolicy Bypass -File .\correction\run_live_block.ps1 --paper true --data-mode cache execute
```

```powershell
powershell -ExecutionPolicy Bypass -File .\correction\run_system.ps1
```

Daemon runtime files:

- `state/daemon_state.json`
- `state/live_state.json`
- `state/correction_system.lock`
- `logs/daemon_events.jsonl`
- Telegram channel smoke test result is recorded in `TELEGRAM_TEST.md`

Conflict protection:

- only one instance of this unified system can run at a time
- if any existing live position or open order already exists on `ETH-USDT`, the system skips new entries to avoid conflicts with other trading systems
