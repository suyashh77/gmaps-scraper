#!/usr/bin/env bash
# MACHINE 3 (Holling, or any third box) — runs the batch in batches/ whose name ends in 3
#
#   ./run_machine3.sh
#
# Starts the orchestrator inside tmux so an SSH disconnect does not take the
# scrape with it. Safe to run again after a reboot: all state lives in the batch
# database, so restarting is the whole recovery procedure. One batch per
# machine, one machine per IP address.

set -euo pipefail
cd "$(dirname "$0")"
exec ./_run.sh 3
