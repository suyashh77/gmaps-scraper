#!/usr/bin/env bash
# setup.sh — Linux setup for Google Maps Review Scraper (headless)
# Run once on a fresh machine before first use.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "========================================"
echo "  Google Maps Review Scraper — Setup"
echo "========================================"
echo ""

# ── Find Python 3.8+ ─────────────────────────────────────────────────────────
PYTHON=""
for cmd in python3.12 python3.11 python3.10 python3.9 python3.8 python3 python; do
    if command -v "$cmd" &>/dev/null; then
        version=$("$cmd" -c 'import sys; print(sys.version_info[:2])' 2>/dev/null || echo "")
        if [[ "$version" > "(3, 7)" ]]; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [[ -z "$PYTHON" ]]; then
    echo "ERROR: Python 3.8+ not found. Install it first:"
    echo "  sudo apt-get install python3 python3-pip"
    exit 1
fi

echo "Using Python: $PYTHON ($($PYTHON --version))"
echo ""

# ── System dependencies for headless Chromium ────────────────────────────────
echo "Installing system dependencies for headless Chromium..."
if command -v apt-get &>/dev/null; then
    sudo apt-get install -y --no-install-recommends \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
        libxfixes3 libxrandr2 libgbm1 libasound2 \
        libpangocairo-1.0-0 libpango-1.0-0 libcairo2 \
        fonts-liberation libappindicator3-1 xdg-utils \
        2>/dev/null || echo "  (some packages may have been skipped — continuing)"
else
    echo "  apt-get not found — skipping system dependency install."
    echo "  If Playwright Chromium fails to launch, install its deps manually."
fi
echo ""

# ── Python dependencies ───────────────────────────────────────────────────────
# Look for requirements.txt in script dir first, then parent
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"
if [[ ! -f "$REQUIREMENTS" ]]; then
    REQUIREMENTS="$PROJECT_DIR/requirements.txt"
fi
if [[ -f "$REQUIREMENTS" ]]; then
    echo "Installing Python requirements from $REQUIREMENTS..."
    "$PYTHON" -m pip install --upgrade pip --quiet
    "$PYTHON" -m pip install -r "$REQUIREMENTS" --quiet
else
    echo "requirements.txt not found at $REQUIREMENTS — skipping pip install."
fi
echo ""

# ── Playwright Chromium ───────────────────────────────────────────────────────
echo "Installing Playwright Chromium..."
if "$PYTHON" -m playwright install chromium --with-deps 2>/dev/null; then
    echo "  Playwright Chromium installed (with system deps)."
else
    echo "  Retrying without --with-deps..."
    "$PYTHON" -m playwright install chromium
fi
echo ""

# ── Done ──────────────────────────────────────────────────────────────────────
echo "========================================"
echo "  Setup complete!"
echo "========================================"
echo ""
echo "Usage:"
echo "  python -m linux_scraper --csv YOUR_FILE.csv"
echo "  python -m linux_scraper --csv YOUR_FILE.csv --auth google_auth_1.json"
echo "  python -m linux_scraper --csv YOUR_FILE.csv --partition 1/3   # machine 1 of 3"
echo "  python -m linux_scraper stats YOUR_FILE.db"
echo "  python -m linux_scraper stats YOUR_FILE.db --monitor"
echo ""
echo "Multi-machine deployment (3 machines):"
echo "  Machine 1: python -m linux_scraper --csv stores.xlsx --partition 1/3"
echo "  Machine 2: python -m linux_scraper --csv stores.xlsx --partition 2/3"
echo "  Machine 3: python -m linux_scraper --csv stores.xlsx --partition 3/3"
echo ""
echo "Tip: run inside tmux so it keeps going if you disconnect:"
echo "  tmux new -s scraper"
echo "  python -m linux_scraper --csv YOUR_FILE.csv --partition 1/3"
echo "  # detach: Ctrl-b d   re-attach: tmux attach -t scraper"
echo ""
