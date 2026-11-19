#!/usr/bin/env bash
# Log in all three Google accounts on THIS PC, then — only if all three check out
# signed in — run dewey9 headed on a virtual display (same browser as the login).
cd "$(dirname "$0")"
PY=.venv/bin/python
echo "$(date '+%F %T') starting login browsers on 127.0.0.1:9222/9223/9224"
for slot in 1 2 3; do
  xvfb-run -a $PY make_auth_remote.py --slot $slot --port $((9221 + slot)) --timeout-min 240 \
    > logs/make_auth_$slot.log 2>&1 &
done
wait
for slot in 1 2 3; do echo "slot $slot: $(tail -1 logs/make_auth_$slot.log)"; done

echo "$(date '+%F %T') checking sessions"
if ! xvfb-run -a $PY check_auth.py 2>&1 | grep -E "SIGNED (IN|OUT)$" | tee logs/check_auth.log | grep -q .; then
  echo "check_auth produced no result — not starting dewey9"; exit 1
fi
if grep -q "SIGNED OUT" logs/check_auth.log; then
  echo "an account is still signed out — not starting dewey9"; exit 1
fi
echo "$(date '+%F %T') all signed in — starting dewey9"
exec xvfb-run -a $PY run_continuous.py --batch dewey9.db --no-headless
