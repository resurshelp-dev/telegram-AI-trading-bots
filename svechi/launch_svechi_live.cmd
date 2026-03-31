@echo off
setlocal
cd /d "%~dp0"
start "Svechi Live System" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0run_svechi_final.ps1"
