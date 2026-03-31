@echo off
setlocal
cd /d "%~dp0"
start "3bar Live System" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0run_3bar_system.ps1"
