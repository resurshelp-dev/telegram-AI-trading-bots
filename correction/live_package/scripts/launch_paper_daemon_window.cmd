@echo off
setlocal
cd /d "%~dp0"
start "Correction Paper Daemon" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0start_daemon_paper.ps1"
