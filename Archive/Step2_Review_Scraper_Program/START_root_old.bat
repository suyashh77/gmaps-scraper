@echo off
title Google Maps Review Scraper
echo.
echo  Google Maps Review Scraper
echo  ==========================
echo.

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found.
    echo  Install Python from: https://www.python.org/downloads/
    echo  Make sure to check "Add Python to PATH" during install.
    echo.
    pause
    exit /b 1
)

:: Install / update dependencies silently
echo  Checking dependencies...
python -m pip install -r requirements.txt -q --disable-pip-version-check
if errorlevel 1 (
    echo.
    echo  WARNING: Some dependencies may not have installed correctly.
    echo  Try running: pip install -r requirements.txt
    echo.
)
echo  Dependencies OK.
echo.

:: Install Playwright browsers if needed
python -m playwright install chromium >nul 2>&1

:: Launch the scraper
echo  Starting scraper...
echo.
python -m review_scraper
if errorlevel 1 (
    echo.
    echo  ERROR: Scraper crashed. Check the log above for details.
    echo.
)
pause
