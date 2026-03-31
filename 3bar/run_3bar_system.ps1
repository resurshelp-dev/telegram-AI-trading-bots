$ErrorActionPreference = "Stop"

$root = $PSScriptRoot
$python = Join-Path $root "..\maintest\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    $python = "C:\Users\User\PycharmProjects\neirosystems\maintest\.venv\Scripts\python.exe"
}

if (-not (Test-Path $python)) {
    throw "Python interpreter not found at $python"
}

& $python (Join-Path $root "three_bar_system_daemon.py")
