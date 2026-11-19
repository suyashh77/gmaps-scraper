#!/usr/bin/env bash
# Package this machine's batch database for the trip back. See collect.py.
set -euo pipefail
cd "$(dirname "$0")"
[ -d .venv ] && source .venv/bin/activate
exec python collect.py
