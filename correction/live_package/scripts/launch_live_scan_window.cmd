@echo off
setlocal
cd /d "%~dp0"
start "Correction Live Scan" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0run_live_block.ps1" --paper true --data-mode live scan
