@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE="

if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"
if not defined PYTHON_EXE if exist "..\maintest\.venv\Scripts\python.exe" set "PYTHON_EXE=..\maintest\.venv\Scripts\python.exe"
if not defined PYTHON_EXE if exist "..\.venv\Scripts\python.exe" set "PYTHON_EXE=..\.venv\Scripts\python.exe"
if not defined PYTHON_EXE if exist "C:\Python313\python.exe" set "PYTHON_EXE=C:\Python313\python.exe"
if not defined PYTHON_EXE (
  where py >nul 2>nul
  if not errorlevel 1 set "PYTHON_EXE=py -3"
)
if not defined PYTHON_EXE set "PYTHON_EXE=python"

echo Starting bot from "%cd%"
%PYTHON_EXE% contrarian_bot.py --config bot_config.json --mode live
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Bot stopped with exit code %EXIT_CODE%.
  echo Check runtime\bot.log and runtime\notifications_fallback.log for details.
  pause
)

endlocal
