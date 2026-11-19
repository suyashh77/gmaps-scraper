#!/usr/bin/env bash
# After the current dewey9 run ends, keep retrying its failed stores, signed in or
# not: requeue every failed store that Google says has reviews (live count > 0,
# so the "Reviews: 0" placeholders are left alone) and run the batch again.
# Stops after MAX_ROUNDS, or once a round adds fewer than MIN_GAIN reviews.
cd "$(dirname "$0")"
PY=.venv/bin/python
DB=batches/dewey9.db
MAX_ROUNDS=${MAX_ROUNDS:-3}
MIN_GAIN=${MIN_GAIN:-50}

count_reviews() { $PY -c "import sqlite3;print(sqlite3.connect('file:$DB?mode=ro',uri=True).execute('select count(*) from reviews').fetchone()[0])"; }

echo "$(date '+%F %T') waiting for the running dewey9 scrape to finish"
while ps -eo args | grep -qE "^(\.venv/bin/)?python[0-9.]* run_continuous\.py --batch dewey9\.db"; do sleep 60; done

for round in $(seq 1 "$MAX_ROUNDS"); do
  n=$($PY - <<EOF
import sqlite3
c = sqlite3.connect("$DB", timeout=30)
n = c.execute("""update stores set status='pending', attempts=0, error_message=NULL, worker_id=NULL, last_sort_order=NULL, last_review_id=NULL
                 where status='failed' and coalesce(gmaps_reviews_count,0) > 0""").rowcount
c.commit(); print(n)
EOF
)
  if [ "$n" = "0" ]; then echo "$(date '+%F %T') round $round: no retryable failed stores — done"; break; fi
  before=$(count_reviews)
  echo "$(date '+%F %T') round $round: requeued $n failed stores, starting run ($before reviews so far)"
  xvfb-run -a $PY run_continuous.py --batch dewey9.db --no-headless >> logs/relogin_then_dewey9.log 2>&1
  gain=$(( $(count_reviews) - before ))
  echo "$(date '+%F %T') round $round finished: +$gain reviews"
  if [ "$gain" -lt "$MIN_GAIN" ]; then echo "gain below $MIN_GAIN — stopping"; break; fi
done
echo "$(date '+%F %T') rounds complete"
