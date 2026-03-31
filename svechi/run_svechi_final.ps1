$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$pythonCandidates = @(
    (Join-Path $projectRoot "maintest\.venv\Scripts\python.exe"),
    (Join-Path $projectRoot ".venv\Scripts\python.exe"),
    (Join-Path $projectRoot "telegram_content_mvp\.venv\Scripts\python.exe")
)

$pythonExe = $pythonCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not (Test-Path $pythonExe)) {
    throw "Python interpreter not found in expected locations."
}

& $pythonExe (Join-Path $scriptDir "svechi_final_automation.py")
