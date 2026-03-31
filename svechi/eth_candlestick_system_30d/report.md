# ETH candlestick pattern analysis

- source: `correction\data_cache\ETH_USDT_5m_30d.csv`
- train/test split: `2026-03-11T10:30:00+00:00`
- timeframes reviewed: `5m`, `15m`, `30m`, `1h`

## Best entry patterns by timeframe

- `1h` `gravestone_doji` (short) count=1, mean60=0.950%, win60=100.0%, use=entry_hold
- `30m` `inverted_hammer` (long) count=6, mean60=0.458%, win60=66.7%, use=entry_hold
- `30m` `three_black_crows` (short) count=1, mean60=0.963%, win60=100.0%, use=entry_scalp_or_exit
- `5m` `morning_star` (long) count=37, mean60=0.376%, win60=62.2%, use=entry_hold
- `15m` `shooting_star` (short) count=42, mean60=0.175%, win60=61.9%, use=entry_hold
- `30m` `gravestone_doji` (short) count=9, mean60=0.121%, win60=55.6%, use=entry_hold
- `30m` `bullish_engulfing` (long) count=51, mean60=0.197%, win60=56.9%, use=entry_hold
- `1h` `bearish_harami` (short) count=16, mean60=0.028%, win60=56.2%, use=entry_hold
- `1h` `dark_cloud_cover` (short) count=6, mean60=0.187%, win60=50.0%, use=weak_or_context_only
- `15m` `gravestone_doji` (short) count=15, mean60=-0.050%, win60=60.0%, use=entry_hold
- `1h` `inverted_hammer` (long) count=5, mean60=0.021%, win60=80.0%, use=entry_hold
- `1h` `dragonfly_doji` (long) count=7, mean60=0.145%, win60=57.1%, use=entry_scalp_or_exit

## Exit / caution patterns

- `15m` `doji` count=306, 30m=0.343%, 60m=0.464%, 180m=0.859%
- `15m` `long_legged_doji` count=96, 30m=0.342%, 60m=0.405%, 180m=0.807%
- `15m` `spinning_top` count=284, 30m=0.334%, 60m=0.452%, 180m=0.852%
- `15m` `bullish_engulfing` count=101, 30m=0.025%, 60m=-0.094%, 180m=-0.167%
- `15m` `dark_cloud_cover` count=23, 30m=0.007%, 60m=-0.141%, 180m=-0.621%
- `1h` `doji` count=94, 30m=0.512%, 60m=0.512%, 180m=0.780%
- `1h` `spinning_top` count=77, 30m=0.508%, 60m=0.508%, 180m=0.838%
- `1h` `long_legged_doji` count=31, 30m=0.478%, 60m=0.478%, 180m=0.720%
- `1h` `dragonfly_doji` count=7, 30m=0.145%, 60m=0.145%, 180m=0.009%
- `1h` `bullish_engulfing` count=22, 30m=0.090%, 60m=0.090%, 180m=0.016%

## Derived trading system

- `5m` `morning_star` long entry on next bar open, `36` bars hold (180 min), stop `0.8 ATR`, min body `0.0`, volume filter=off
- `15m` `shooting_star` short entry on next bar open, `12` bars hold (180 min), stop `0.8 ATR`, min body `0.2`, volume filter=off
- execution: next bar open after signal, fee model `0.05%` per side, no overlap between positions
- cleanup: low-frequency and weak-expectancy patterns are excluded by train/test screening before portfolio assembly

## Strategy summary

- full sample: trades=56, win_rate=44.64%, expectancy=1.418R, net=79.42R, PF=3.18
- train sample: trades=36, win_rate=41.67%, expectancy=1.252R, net=45.08R, PF=2.87
- test sample: trades=20, win_rate=50.00%, expectancy=1.717R, net=34.33R, PF=3.78

## Practical reading

- strongest patterns on this month of ETH should be treated as local tendencies, not universal market laws
- continuation patterns that keep positive 180m edge are better for fresh entries
- indecision patterns and short-lived bursts are better as exit or risk-reduction cues
