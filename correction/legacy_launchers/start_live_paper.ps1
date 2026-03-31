$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
$runner = Join-Path $root "run_live_block.ps1"

if (-not (Test-Path $runner)) {
    throw "Live runner not found at $runner"
}

powershell -ExecutionPolicy Bypass -File $runner --paper true --data-mode live execute
