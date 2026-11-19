@echo off
cd /d "%~dp0"
title POI Collection - probe running

if not exist ".venv\Scripts\python.exe" (
  echo.
  echo [X] Not set up yet on this machine. Run setup.bat first.
  echo.
  pause
  exit /b 1
)

echo ================================================================
echo   POI COLLECTION - Google probe
echo   Machine: %COMPUTERNAME%
echo.
echo   Leave this window open. Press Ctrl-C to stop at any time;
echo   progress is saved and START.bat picks up where it left off.
echo ================================================================
echo.

".venv\Scripts\python.exe" -u run_probe.py %*

echo.
echo ================================================================
echo   Probe stopped. Run collect.bat to package the results.
echo ================================================================
echo.
pause
