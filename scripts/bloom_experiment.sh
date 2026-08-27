#!/usr/bin/env bash
# Compatibility entry point; implementation lives under experiments/.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$ROOT_DIR/scripts/experiments/bloom_experiment.sh" "$@"
