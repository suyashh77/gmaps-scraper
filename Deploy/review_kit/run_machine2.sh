#!/usr/bin/env bash
# MACHINE 2 (Manabe) — runs the batch in batches/ whose name ends in 2
#
#   ./run_machine2.sh
#
# Starts the orchestrator inside tmux so an SSH disconnect does not take the
# scrape with it. Safe to run again after a reboot: all state lives in the batch
# database, so restarting is the whole recovery procedure. One batch per
# machine, one machine per IP address.

set -euo pipefail
cd "$(dirname "$0")"
exec ./_run.sh 2
