$ErrorActionPreference = "Stop"

$python = Join-Path $PSScriptRoot "..\maintest\.venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = "C:\Users\User\PycharmProjects\neirosystems\maintest\.venv\Scripts\python.exe"
}
if (-not (Test-Path $python)) {
    throw "Python interpreter not found at $python"
}

& $python (Join-Path $PSScriptRoot "correction_system_daemon.py") --data-mode live --confirm-live
 
