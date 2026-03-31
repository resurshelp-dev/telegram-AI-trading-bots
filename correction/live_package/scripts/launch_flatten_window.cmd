@echo off
setlocal
cd /d "%~dp0"
start "Correction Flatten ETH" powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0flatten_eth_position.ps1"
