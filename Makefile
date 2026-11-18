.PHONY: db-up db-down up down run worker soak test bench lint migrate clean format

UV := $(shell command -v uv 2>/dev/null || echo $$HOME/.local/bin/uv)

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

up:
	bash scripts/dev.sh

down:
	docker compose down

run:
	$(UV) run --env-file .env uvicorn app.main:app --host 0.0.0.0 --port $${PORT:-8000} --reload

worker:
	$(UV) run --env-file .env python -m app.worker

soak:
	$(UV) run --env-file .env python -m scripts.soak_worker --duration 900

test:
	$(UV) run python -m scripts.guard_test_database
	$(UV) run pytest -m "not benchmark"

bench:
	-$(UV) run --env-file .env python -m scripts.bench_serving

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

migrate:
	$(UV) run --env-file .env alembic upgrade head

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__

