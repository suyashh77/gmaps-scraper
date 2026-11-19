@echo off
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set "N=%~1"
if "%N%"=="" (
  echo usage: START.bat ^<machine number^>      e.g.  START.bat 3
  echo        runs the batch in batches\ whose name ends in that number
  pause
  exit /b 1
)
if not exist ".venv\Scripts\python.exe" (
  echo [X] Not set up yet on this machine. Run setup.bat first.
  pause
  exit /b 1
)
set "BATCH="
for %%f in ("batches\*%N%.db") do set "BATCH=%%~nxf"
if "%BATCH%"=="" (
  echo [X] no batches\*%N%.db found for machine %N%
  pause
  exit /b 1
)
title NC grocery reviews - machine %N% - %BATCH%
echo ================================================================
echo   NC GROCERY REVIEW SCRAPER
echo   Machine %N%   batch %BATCH%   host %COMPUTERNAME%
echo.
echo   Leave this window open. Ctrl-C stops it; everything is saved
echo   and START.bat %N% picks up exactly where it stopped.
echo ================================================================
echo.
".venv\Scripts\python.exe" -u run_continuous.py --batch "batches\%BATCH%"
echo.
echo ================================================================
echo   Scraper stopped. Run collect.bat to package the results.
echo ================================================================
pause
