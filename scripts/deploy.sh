#!/usr/bin/env bash
# Only operates the local Compose project; no Kubernetes or remote push.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case "${1:-help}" in
    up|deploy) ACTION=up ;;
    down|undeploy) ACTION=down ;;
    status) ACTION=status ;;
    logs) ACTION=logs ;;
    build) ACTION=image ;;
    check) ACTION=compose-check ;;
    help|--help|-h) ACTION=help ;;
    *) echo "Usage: $0 {up|down|status|logs|build|check|help}" >&2; exit 2 ;;
esac
exec make -C "$ROOT_DIR" "$ACTION"
