@echo off
setlocal
cd /d "%~dp0"
echo ================================================================
echo   NC GROCERY REVIEW SCRAPER - one-time setup
echo ================================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo [X] Python is not on PATH.
  echo     Install Python 3.10 or newer from python.org and tick
  echo     "Add python.exe to PATH" during the install, then rerun this.
  echo.
  pause
  exit /b 1
)

echo [1/3] Creating the virtual environment...
if exist ".venv\Scripts\python.exe" (
  echo       already there, skipping
) else (
  python -m venv .venv
  if errorlevel 1 goto :failed
)

echo [2/3] Installing Python packages...
".venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto :failed

echo [3/3] Installing the Chromium browser Playwright drives...
".venv\Scripts\python.exe" -m playwright install chromium
if errorlevel 1 goto :failed

echo.
dir /b auth\google_auth*.json >nul 2>nul || echo [!] no auth\google_auth_*.json found - runs unauthenticated, rate limited sooner
echo ================================================================
echo   Setup finished. Run:  START.bat 3      (your machine number)
echo ================================================================
echo.
pause
exit /b 0

:failed
echo.
echo [X] Setup failed. The error is above this line.
echo.
pause
exit /b 1
