#!/usr/bin/env bash
# Current build entry point. Historical behavior is in legacy/build.sh.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="${1:-build}"
if [ "$#" -gt 0 ]; then shift; fi
case "$ACTION" in
    build|test|vet|fmt|fmt-check|all) ;;
    help|--help|-h) ACTION=help ;;
    *) echo "Unsupported action: $ACTION. Run make help." >&2; exit 2 ;;
esac
exec make -C "$ROOT_DIR" "$ACTION" "$@"
