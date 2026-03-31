param(
    [string]$Side = "BUY",
    [double]$Qty = 0.0001,
    [string]$Symbol = "BTCUSDT"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = "C:\Users\User\PycharmProjects\neirosystems\maintest\.venv\Scripts\python.exe"
$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Set-Location (Split-Path -Parent $root)
& $python ".\fixed\contrarian_bot.py" --mode smoke-test --paper false --symbol $Symbol --smoke-side $Side --smoke-qty $Qty *>> (Join-Path $logDir "smoke_test.launch.log")
