#!/usr/bin/env bash
# deploy.sh — One-command setup + run on a remote Linux machine.
#
# Usage (run FROM YOUR LOCAL MACHINE):
#   ssh user@machine1 'bash -s' < deploy.sh 1 3 GITHUB_REPO_URL
#   ssh user@machine2 'bash -s' < deploy.sh 2 3 GITHUB_REPO_URL
#   ssh user@machine3 'bash -s' < deploy.sh 3 3 GITHUB_REPO_URL
#
# Or run directly on the remote machine:
#   bash deploy.sh 1 3 https://github.com/YOU/gmaps-scraper.git
#
# Arguments:
#   $1 = machine number (1, 2, or 3)
#   $2 = total machines (3)
#   $3 = GitHub repo URL
set -euo pipefail

MACHINE_NUM="${1:?Usage: deploy.sh MACHINE_NUM TOTAL_MACHINES REPO_URL}"
TOTAL="${2:?Usage: deploy.sh MACHINE_NUM TOTAL_MACHINES REPO_URL}"
REPO="${3:?Usage: deploy.sh MACHINE_NUM TOTAL_MACHINES REPO_URL}"

WORKDIR="$HOME/gmaps-scraper"

echo "=========================================="
echo "  Deploy: machine $MACHINE_NUM of $TOTAL"
echo "=========================================="

# ── Clone or update repo ─────────────────────────────────────────────────
if [ -d "$WORKDIR/.git" ]; then
    echo "Repo exists — pulling latest..."
    cd "$WORKDIR"
    git pull --ff-only
else
    echo "Cloning repo..."
    git clone "$REPO" "$WORKDIR"
    cd "$WORKDIR"
fi

# ── Run setup ────────────────────────────────────────────────────────────
echo ""
echo "Running setup..."
bash linux_scraper/setup.sh

# ── Find input file ──────────────────────────────────────────────────────
CSV=""
for f in stores_to_scrape.xlsx stores_to_scrape.csv; do
    if [ -f "$f" ]; then
        CSV="$f"
        break
    fi
done

if [ -z "$CSV" ]; then
    echo ""
    echo "ERROR: No stores_to_scrape.xlsx or .csv found in repo."
    echo "Run export_for_scraping.py first, commit the file, and push."
    exit 1
fi

# ── Check for auth files ────────────────────────────────────────────────
AUTH_COUNT=$(ls google_auth*.json 2>/dev/null | wc -l)
if [ "$AUTH_COUNT" -eq 0 ]; then
    echo ""
    echo "WARNING: No google_auth*.json files found."
    echo "SCP your auth file to: $WORKDIR/google_auth_1.json"
    echo "Then re-run this script or start manually."
fi

# ── Start scraper in tmux ────────────────────────────────────────────────
SESSION="scraper"
CMD="cd $WORKDIR && python -m linux_scraper --csv $CSV --partition $MACHINE_NUM/$TOTAL"

echo ""
echo "Starting scraper in tmux session '$SESSION'..."
echo "Command: $CMD"

# Kill existing session if any
tmux kill-session -t "$SESSION" 2>/dev/null || true

tmux new-session -d -s "$SESSION" "$CMD"

echo ""
echo "=========================================="
echo "  Scraper running in tmux!"
echo "=========================================="
echo ""
echo "  Attach:   tmux attach -t $SESSION"
echo "  Detach:   Ctrl-b d"
echo "  Monitor:  python -m linux_scraper stats *.db --monitor"
echo "  Stop:     tmux send-keys -t $SESSION C-c"
echo ""
