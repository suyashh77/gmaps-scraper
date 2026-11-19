#!/usr/bin/env bash
# One-time setup for a Linux collection machine.
#
#   bash setup.sh
#
# Installs Python dependencies and the Chromium build Playwright drives, then
# checks the things that actually stop a run: a missing browser, a missing auth
# session, and a batch file that was copied without its write-ahead log.

set -euo pipefail
cd "$(dirname "$0")"

BOLD=$'\033[1m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; OFF=$'\033[0m'
say()  { printf '%s\n' "$*"; }
ok()   { printf '%s  ok %s %s\n' "$GRN" "$OFF" "$*"; }
warn() { printf '%s  !! %s %s\n' "$YEL" "$OFF" "$*"; }
die()  { printf '%s  XX %s %s\n' "$RED" "$OFF" "$*"; exit 1; }

say "${BOLD}NC grocery review scraper — machine setup${OFF}"
say ""

# ── Python ────────────────────────────────────────────────────────────────
PY=""
for c in python3.12 python3.11 python3.10 python3; do
  if command -v "$c" >/dev/null 2>&1; then
    v=$("$c" -c 'import sys; print("%d%02d" % sys.version_info[:2])' 2>/dev/null || echo 0)
    if [ "$v" -ge 310 ]; then PY="$c"; break; fi
  fi
done
[ -n "$PY" ] || die "Python 3.10 or newer is required. Install it and re-run."
ok "using $($PY --version)"

# ── venv ──────────────────────────────────────────────────────────────────
if [ ! -d .venv ]; then
  say "  creating virtualenv .venv"
  "$PY" -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
ok "virtualenv active"

python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt
ok "python dependencies installed"

# ── Chromium ──────────────────────────────────────────────────────────────
# --with-deps pulls the shared libraries headless Chromium needs. On a bare
# cloud image without them the browser fails to launch with an error that names
# a missing .so rather than the real problem, which wastes an afternoon.
say "  installing Chromium (this is the slow step)"
if ! python -m playwright install --with-deps chromium 2>/dev/null; then
  warn "install --with-deps failed, likely no sudo. Trying without system deps."
  python -m playwright install chromium
  warn "if the browser will not launch, run: sudo python -m playwright install-deps chromium"
fi
ok "chromium installed"

# ── Preflight ─────────────────────────────────────────────────────────────
say ""
say "${BOLD}Preflight${OFF}"

shopt -s nullglob
auth=(auth/google_auth*.json)
if [ ${#auth[@]} -eq 0 ]; then
  warn "no auth sessions in auth/. The scraper will run unauthenticated, which"
  warn "works but gets rate limited sooner. Copy your google_auth_*.json files"
  warn "into auth/ before a long run. They are live session cookies: never commit"
  warn "them and never share them between machines you do not control."
else
  ok "${#auth[@]} auth session(s) found"
fi

batches=(batches/*.db)
[ ${#batches[@]} -gt 0 ] || die "no batch database in batches/"
for b in "${batches[@]}"; do
  if [ -f "${b}-wal" ]; then
    warn "$(basename "$b") has an unmerged -wal sidecar. Checkpointing it now."
    python - "$b" <<'PYEOF'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.execute("PRAGMA wal_checkpoint(TRUNCATE)"); c.close()
PYEOF
  fi
  python - "$b" <<'PYEOF'
import sqlite3, sys, os
p = sys.argv[1]
c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
n = c.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
pend = c.execute("SELECT COUNT(*) FROM stores WHERE status='pending'").fetchone()[0]
keys = c.execute("SELECT COUNT(*) FROM known_review_keys").fetchone()[0]
work = c.execute("SELECT COALESCE(SUM(target_reviews-master_reviews),0) "
                 "FROM stores WHERE status='pending'").fetchone()[0]
c.close()
print(f"  ok  {os.path.basename(p)}: {n} stores, {pend} pending, "
      f"{work:,} reviews of work, {keys:,} skip-list keys")
if keys == 0:
    print("  !!  skip-list is empty: resumed stores will re-walk reviews already held")
PYEOF
done

say ""
ok "setup complete"
say ""
say "Next:  ${BOLD}./run_machine1.sh${OFF}   (or run_machine2.sh on the second box)"
say "Watch: ${BOLD}tail -f logs/scraper.log${OFF}"
