#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

UV_BIN="${UV_BIN:-$(command -v uv || echo "$HOME/.local/bin/uv")}"

if [[ ! -f .env ]]; then
    echo "Missing .env file. Run: cp .env.example .env and configure POSTGRES_PASSWORD, DATABASE_URL, POLYPULSE_API_KEY." >&2
    exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

echo "[polypulse] Starting PostgreSQL..."
docker compose up -d postgres

echo "[polypulse] Running Alembic migrations..."
"$UV_BIN" run --env-file .env alembic upgrade head

PIDS=()
cleanup() {
    echo ""
    echo "[polypulse] Stopping API and worker..."
    for pid in "${PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

API_PORT="${APP_PORT:-8000}"
echo "[polypulse] Launching API on http://localhost:${API_PORT} and background worker..."
"$UV_BIN" run --env-file .env uvicorn app.main:app --host 0.0.0.0 --port "$API_PORT" --reload &
PIDS+=($!)

"$UV_BIN" run --env-file .env python -m app.worker &
PIDS+=($!)

wait -n "${PIDS[@]}"

