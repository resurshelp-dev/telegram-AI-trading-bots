param(
    [bool]$Paper = $true,
    [string]$Symbol = "BTCUSDT",
    [string]$Timeframe = "15min"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = "C:\Users\User\PycharmProjects\neirosystems\maintest\.venv\Scripts\python.exe"
$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Set-Location (Split-Path -Parent $root)
& $python ".\fixed\contrarian_bot.py" --mode live --paper $Paper --symbol $Symbol --timeframe $Timeframe *>> (Join-Path $logDir "live.launch.log")
