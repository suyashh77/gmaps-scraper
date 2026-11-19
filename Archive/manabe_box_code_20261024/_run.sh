#!/usr/bin/env bash
# Shared launcher. Called by run_machine1.sh / run_machine2.sh / run_machine3.sh.
#   $1 = machine number   $2 = batch filename in batches/ (optional)

set -euo pipefail
cd "$(dirname "$0")"

N="${1:?machine number required}"
# Batch: the file named in $2, else the one batch in batches/ whose name ends in
# the machine number (grocery1.db for machine 1, and so on).
if [ -n "${2:-}" ]; then
  BATCH="$2"
else
  BATCH="$(cd batches 2>/dev/null && ls -1 *"${N}".db 2>/dev/null | head -1 || true)"
  [ -n "$BATCH" ] || { printf 'no batches/*%s.db for machine %s
' "$N" "$N"; exit 1; }
fi
SESSION="scrape${N}"
BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; OFF=$'\033[0m'

[ -f "batches/${BATCH}" ] || { printf '%sbatches/%s not found%s\n' "$RED" "$BATCH" "$OFF"; exit 1; }
[ -d .venv ] || { printf '%sRun ./setup.sh first%s\n' "$RED" "$OFF"; exit 1; }

mkdir -p logs results auth

# The scraper looks in auth/ itself; nothing needs linking into place.
authcount=$(ls -1 auth/google_auth*.json 2>/dev/null | wc -l | tr -d ' ')
if [ "$authcount" = "0" ]; then
  printf '%s  no auth sessions found; running unauthenticated (rate limits sooner)%s\n' "$YEL" "$OFF"
else
  printf '%s  %s auth session(s) linked%s\n' "$GRN" "$authcount" "$OFF"
fi

# Refuse to run two scrapers against one batch. Both would claim the same store
# and, because Google hands out different review ids per session, both copies
# would be kept as if they were different reviews.
if pgrep -f "run_continuous.py --batch .*${BATCH}" >/dev/null 2>&1; then
  printf '%sAlready running against %s. Attach with: tmux attach -t %s%s\n' \
         "$YEL" "$BATCH" "$SESSION" "$OFF"
  exit 0
fi

# A machine id that survives reboots, so every review can be traced to the host
# that collected it.
if [ ! -f machine_id.txt ]; then
  (hostname; date +%s) | md5sum | cut -c1-6 > machine_id.txt
fi
MID="$(cat machine_id.txt)"

printf '%sMachine %s — %s — id %s%s\n' "$BOLD" "$N" "$BATCH" "$MID" "$OFF"

CMD="source .venv/bin/activate && python run_continuous.py --batch batches/${BATCH} 2>&1 | tee -a logs/run.log"

if command -v tmux >/dev/null 2>&1; then
  if tmux has-session -t "$SESSION" 2>/dev/null; then
    printf '%stmux session %s already exists. Attach: tmux attach -t %s%s\n' \
           "$YEL" "$SESSION" "$SESSION" "$OFF"
    exit 0
  fi
  tmux new-session -d -s "$SESSION" "$CMD"
  printf '%s  started in tmux session %s%s\n' "$GRN" "$SESSION" "$OFF"
  printf '\n  attach   tmux attach -t %s\n' "$SESSION"
  printf '  detach   Ctrl-b then d\n'
  printf '  watch    tail -f batches/scraper.log\n'
  printf '  progress source .venv/bin/activate && python -m linux_scraper stats batches/%s --monitor\n' "$BATCH"
  printf '  stop     tmux kill-session -t %s\n\n' "$SESSION"
else
  printf '%s  tmux not installed, falling back to nohup%s\n' "$YEL" "$OFF"
  nohup bash -c "$CMD" >/dev/null 2>&1 &
  printf '%s  started, pid %s%s\n' "$GRN" "$!" "$OFF"
  printf '  watch  tail -f batches/scraper.log\n'
  printf '  stop   pkill -f "run_continuous.py --batch batches/%s"\n\n' "$BATCH"
fi
