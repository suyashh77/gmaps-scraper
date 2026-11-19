@echo off
setlocal
title Google Maps Review Scraper
cd /d "%~dp0"

echo.
echo  Google Maps Review Scraper
echo  ==========================
echo.

:: -- Find Python 3.8+ --
set "PYTHON="

python -c "import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)" 2>nul
if not errorlevel 1 set "PYTHON=python"
if defined PYTHON goto :found_python

py -3 -c "import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)" 2>nul
if not errorlevel 1 set "PYTHON=py -3"
if defined PYTHON goto :found_python

python3 -c "import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)" 2>nul
if not errorlevel 1 set "PYTHON=python3"
if defined PYTHON goto :found_python

echo.
echo  ERROR: Python 3.8 or higher not found.
echo.
echo  Install Python from: https://www.python.org/downloads/
echo  IMPORTANT: Check the box Add Python to PATH during installation.
echo  Then close this window and double-click START.bat again.
echo.
goto :done

:found_python
for /f "tokens=*" %%v in ('%PYTHON% --version 2^>^&1') do echo  Found: %%v
echo.

:: -- Verify pip --
%PYTHON% -m pip --version 2>nul 1>nul
if errorlevel 1 goto :no_pip
goto :pip_ok

:no_pip
echo  ERROR: pip is not available.
echo  Reinstall Python from https://www.python.org/downloads/
echo.
goto :done

:pip_ok
:: -- Install dependencies --
echo  Installing dependencies...
if not exist requirements.txt goto :no_reqs
goto :reqs_ok

:no_reqs
echo  ERROR: requirements.txt not found.
echo  Make sure ALL files were extracted from the ZIP.
echo.
goto :done

:reqs_ok
%PYTHON% -m pip install -r requirements.txt -q --disable-pip-version-check 2>nul
if errorlevel 1 goto :pip_retry
goto :deps_ok

:pip_retry
echo  Retrying with --user flag...
%PYTHON% -m pip install -r requirements.txt -q --disable-pip-version-check --user 2>nul
if errorlevel 1 goto :deps_fail
goto :deps_ok

:deps_fail
echo.
echo  ERROR: Failed to install dependencies.
echo  Try running manually: %PYTHON% -m pip install -r requirements.txt
echo.
goto :done

:deps_ok
echo  Dependencies OK.
echo.

:: -- Install Playwright Chromium --
echo  Checking browser...
echo  NOTE: If your antivirus asks about Chromium, click Allow.
%PYTHON% -m playwright install chromium 2>nul
if errorlevel 1 goto :browser_fail
goto :browser_ok

:browser_fail
echo.
echo  ERROR: Failed to install Chromium browser.
echo  Check your internet connection and try again.
echo  If antivirus blocked it, temporarily disable it and retry.
echo.
goto :done

:browser_ok
echo  Browser OK.
echo.

:: -- Launch --
echo  Starting scraper...
echo.
%PYTHON% -m review_scraper
if errorlevel 1 echo  Scraper encountered an error. Re-run START.bat to resume.

:done
echo.
pause
