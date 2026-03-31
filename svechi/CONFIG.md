# Svechi Config

Главный файл настроек:

- `svechi.env`

Шаблон:

- `svechi.env.example`

## Что можно менять в одном месте

- `PAPER=true/false`
  - `true` — тестовый режим
  - `false` — live-режим
- `BINGX_API_KEY`
- `BINGX_SECRET_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SEND_TELEGRAM=true/false`
- `SYMBOL`
- `INITIAL_CAPITAL`
- `RISK_PERCENT`
- `QTY_PRECISION`
- `PRICE_PRECISION`
- `RECV_WINDOW`

## Как пользоваться

1. Откройте `svechi.env`
2. Поменяйте нужные параметры
3. Сохраните файл
4. Запускайте систему через `launch_svechi_live.cmd`

## Главное

Для безопасной проверки:

- держите `PAPER=true`

Для реальной торговли:

- переключайте `PAPER=false` только когда уверены в ключах и балансе
