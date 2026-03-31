@echo off
setlocal
cd /d "%~dp0"
start "Correction Real Daemon" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0start_daemon_real.ps1"
