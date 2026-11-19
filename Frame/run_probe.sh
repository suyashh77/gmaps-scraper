#!/usr/bin/env bash
# Run the Google probe pass. Safe to run on both VMs at once against a shared
# probe.db, or on each with its own copy and merge after: stores are claimed
# with BEGIN IMMEDIATE, so two machines never take the same one.
#   ./Frame/run_probe.sh machine1
cd "$(dirname "$0")/.."
M="${1:-$(hostname)}"
exec python -u Frame/probe.py --run --machine "$M"
