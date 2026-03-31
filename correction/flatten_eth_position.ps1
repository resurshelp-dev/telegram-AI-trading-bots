$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
$runner = Join-Path $root "run_exchange.ps1"

if (-not (Test-Path $runner)) {
    throw "Exchange runner not found at $runner"
}

powershell -ExecutionPolicy Bypass -File $runner --paper false --confirm-live close --symbol ETH-USDT --direction long
