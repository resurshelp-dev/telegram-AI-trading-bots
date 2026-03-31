@echo off
setlocal
cd /d "%~dp0"
start "Unified Correction System" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0run_system.ps1"
