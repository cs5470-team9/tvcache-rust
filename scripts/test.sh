#!/usr/bin/env bash
# Runs the Rust unit tests and every Python test against fresh servers.
#
# Usage: scripts/test.sh            (from anywhere)
# Env:   TVCACHE_PORT  test server port (default 7879, so a dev server on 7878 is untouched)
#        PYTHON        interpreter with tvclient installed (default: ../.venv/bin/python)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${TVCACHE_PORT:-7879}"
PYTHON="${PYTHON:-$ROOT/../.venv/bin/python}"
SERVER="$ROOT/target/debug/tvcache"
SERVER_PID=""

export TVCACHE_PORT="$PORT"

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}
trap stop_server EXIT

start_server() {
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
        echo "port $PORT is already in use" >&2
        exit 1
    fi
    RUST_LOG=warn "$SERVER" --port="$PORT" "$@" &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        nc -z 127.0.0.1 "$PORT" 2>/dev/null && return
        sleep 0.1
    done
    echo "server did not start on port $PORT" >&2
    exit 1
}

run_python() {
    echo "--- $1"
    (cd "$ROOT/tests" && "$PYTHON" "$@")
}

cd "$ROOT"
echo "=== cargo test"
cargo test --quiet
cargo build --quiet

echo "=== python tests (no eviction)"
start_server --eviction-policy=none
run_python test_client.py --port "$PORT"
for test in tests/test_*.py; do
    case "$(basename "$test")" in
        test_client.py | test_eviction.py | test_ttl_eviction.py | test_sandbox_lifecycle.py) ;;
        *) run_python "$(basename "$test")" ;;
    esac
done
stop_server

# The remaining tests rely on the eviction loop, and each needs its own fresh
# server (see tests/README.md).
echo "=== test_sandbox_lifecycle.py (TTL eviction)"
start_server --eviction-policy=ttl_and_watermark
run_python test_sandbox_lifecycle.py
stop_server

for test in test_eviction.py test_ttl_eviction.py; do
    echo "=== $test (watermark eviction)"
    start_server --eviction-policy=ttl_and_watermark --high-watermark-mb=2 --low-watermark-mb=1
    run_python "$test"
    stop_server
done

echo "=== all tests passed"
