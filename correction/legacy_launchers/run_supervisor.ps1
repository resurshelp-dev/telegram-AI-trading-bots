$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot "..\maintest\.venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Python interpreter not found at $python"
}

& $python (Join-Path $PSScriptRoot "eth_supervisor.py") @args
