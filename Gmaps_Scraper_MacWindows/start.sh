#!/bin/bash
set -e
cd "$(dirname "$0")"

echo ""
echo "  Google Maps Review Scraper"
echo "  =========================="
echo ""

# ── Find Python 3.8+ ─────────────────────────────────────────────────────────
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        if "$cmd" -c "import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)" 2>/dev/null; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "  ERROR: Python 3.8+ not found."
    echo "  Install Python from: https://www.python.org/downloads/"
    echo ""
    read -p "  Press Enter to close..."
    exit 1
fi

echo "  Found: $($PYTHON --version 2>&1)"
echo ""

# ── Install dependencies ─────────────────────────────────────────────────────
echo "  Installing dependencies..."
if [ ! -f requirements.txt ]; then
    echo "  ERROR: requirements.txt not found."
    echo "  Make sure ALL files were extracted from the ZIP."
    read -p "  Press Enter to close..."
    exit 1
fi

$PYTHON -m pip install -r requirements.txt -q --disable-pip-version-check 2>/dev/null || \
$PYTHON -m pip install -r requirements.txt -q --disable-pip-version-check --user 2>/dev/null || {
    echo ""
    echo "  ERROR: Failed to install dependencies."
    echo "  Try: $PYTHON -m pip install -r requirements.txt"
    read -p "  Press Enter to close..."
    exit 1
}
echo "  Dependencies OK."
echo ""

# ── Install Playwright browser ────────────────────────────────────────────────
echo "  Checking browser (first run downloads ~150MB)..."
$PYTHON -m playwright install chromium 2>/dev/null || {
    echo ""
    echo "  ERROR: Failed to install Chromium browser."
    echo "  Check your internet connection and try again."
    read -p "  Press Enter to close..."
    exit 1
}
echo "  Browser OK."
echo ""

# ── Launch ────────────────────────────────────────────────────────────────────
echo "  Starting scraper..."
echo ""
$PYTHON -m review_scraper || {
    echo ""
    echo "  The scraper encountered an error."
    echo "  You can re-run start.sh to resume where it left off."
}
echo ""
read -p "  Press Enter to close..."
