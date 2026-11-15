.PHONY: db-up run worker soak test bench lint migrate clean format

db-up:
	docker compose up -d postgres

run:
	~/.local/bin/uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

worker:
	~/.local/bin/uv run python -m app.worker

soak:
	~/.local/bin/uv run python -m scripts.soak_worker --duration 900

test:
	~/.local/bin/uv run python -m scripts.guard_test_database
	~/.local/bin/uv run pytest -m "not benchmark"

bench:
	-~/.local/bin/uv run python -m scripts.bench_serving

lint:
	~/.local/bin/uv run ruff check .
	~/.local/bin/uv run ruff format --check .

format:
	~/.local/bin/uv run ruff format .
	~/.local/bin/uv run ruff check --fix .

migrate:
	~/.local/bin/uv run alembic upgrade head

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__

