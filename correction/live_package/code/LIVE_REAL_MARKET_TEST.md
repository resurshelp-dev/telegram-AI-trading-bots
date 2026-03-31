# Live Real Market Test

This file records the real BingX smoke test executed from the `correction` package.

Status:

- live connectivity: confirmed
- real market order: confirmed
- close order: confirmed
- final open position state: empty

Test instrument:

- symbol: `ETH-USDT`
- side opened: `BUY`
- direction closed: `LONG`
- quantity: `0.01`

Observed exchange details:

- open order id: `2035035615335247873`
- open avg price: `2134.75`
- close order id: `2035035770235088897`
- close avg price: `2134.86`

Observed account state:

- balance before test: `4.19729721 USDT`
- balance after test: `4.17705118 USDT`
- positions after close: `[]`

Notes:

- exchange rejected `0.001 ETH` with message: `The minimum order amount is 0.01 ETH.`
- the successful real position opened as isolated `LONG` with leverage `50`
- package default remains:
  - correction strategy: `basic`
  - trend profile: `profit_max_locked`
