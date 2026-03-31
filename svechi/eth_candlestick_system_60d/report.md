# ETH candlestick pattern analysis

- source: `C:\Users\User\PycharmProjects\neirosystems\correction\data_cache_current_supervisor_fixed\ETH_USDT_5m_60d_20260320T235959Z.csv`
- train/test split: `2026-03-02T16:45:00+00:00`
- timeframes reviewed: `5m`, `15m`, `30m`, `1h`

## Best entry patterns by timeframe

- `15m` `falling_three_methods` (short) count=1, mean60=2.246%, win60=100.0%, use=entry_hold
- `1h` `gravestone_doji` (short) count=4, mean60=0.202%, win60=75.0%, use=entry_hold
- `30m` `three_black_crows` (short) count=2, mean60=0.750%, win60=100.0%, use=entry_hold
- `5m` `three_black_crows` (short) count=30, mean60=0.178%, win60=56.7%, use=entry_hold
- `1h` `inverted_hammer` (long) count=9, mean60=0.361%, win60=66.7%, use=entry_hold
- `15m` `shooting_star` (short) count=68, mean60=0.193%, win60=66.2%, use=entry_hold
- `1h` `bullish_engulfing` (long) count=46, mean60=0.283%, win60=56.5%, use=entry_scalp_or_exit
- `15m` `gravestone_doji` (short) count=26, mean60=0.262%, win60=65.4%, use=entry_scalp_or_exit
- `30m` `inverted_hammer` (long) count=20, mean60=0.170%, win60=60.0%, use=entry_hold
- `30m` `dragonfly_doji` (long) count=13, mean60=0.132%, win60=69.2%, use=entry_hold
- `30m` `gravestone_doji` (short) count=14, mean60=0.090%, win60=57.1%, use=entry_hold
- `1h` `dragonfly_doji` (long) count=9, mean60=0.118%, win60=55.6%, use=entry_hold

## Exit / caution patterns

- `15m` `long_legged_doji` count=211, 30m=0.405%, 60m=0.517%, 180m=0.950%
- `15m` `spinning_top` count=587, 30m=0.386%, 60m=0.528%, 180m=0.932%
- `15m` `doji` count=636, 30m=0.377%, 60m=0.547%, 180m=0.996%
- `15m` `gravestone_doji` count=26, 30m=0.220%, 60m=0.262%, 180m=0.050%
- `15m` `bullish_marubozu` count=154, 30m=0.034%, 60m=0.023%, 180m=0.003%
- `15m` `bullish_engulfing` count=205, 30m=0.033%, 60m=-0.066%, 180m=-0.141%
- `15m` `bullish_harami` count=152, 30m=0.008%, 60m=0.039%, 180m=-0.035%
- `15m` `hanging_man` count=60, 30m=0.004%, 60m=0.024%, 180m=-0.016%
- `1h` `doji` count=168, 30m=0.515%, 60m=0.515%, 180m=0.832%
- `1h` `spinning_top` count=168, 30m=0.501%, 60m=0.501%, 180m=0.824%

## Derived trading system

- `15m` `shooting_star` short entry on next bar open, `12` bars hold (180 min), stop `0.8 ATR`, min body `0.2`, volume filter=off
- `30m` `bearish_engulfing` short entry on next bar open, `8` bars hold (240 min), stop `1.2 ATR`, min body `0.2`, volume filter=on
- execution: next bar open after signal, fee model `0.05%` per side, no overlap between positions
- cleanup: low-frequency and weak-expectancy patterns are excluded by train/test screening before portfolio assembly

## Strategy summary

- full sample: trades=77, win_rate=44.16%, expectancy=0.735R, net=56.57R, PF=2.18
- train sample: trades=46, win_rate=43.48%, expectancy=0.692R, net=31.83R, PF=2.09
- test sample: trades=31, win_rate=45.16%, expectancy=0.798R, net=24.75R, PF=2.34

## Practical reading

- strongest patterns on this month of ETH should be treated as local tendencies, not universal market laws
- continuation patterns that keep positive 180m edge are better for fresh entries
- indecision patterns and short-lived bursts are better as exit or risk-reduction cues
