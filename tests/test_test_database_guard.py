import os
import subprocess
import sys


def test_database_guard_rejects_production_and_accepts_scratch():
    for name, expected in (("polypulse", 1), ("polypulse_scratch_rehearsal", 0)):
        env = dict(os.environ, DATABASE_URL=f"postgresql+psycopg://localhost/{name}")
        result = subprocess.run(
            [sys.executable, "-m", "scripts.guard_test_database"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == expected
        if expected:
            assert "Refusing to run tests" in result.stderr


def test_destructive_scripts_reject_production_before_connecting():
    for module in ("measure_backfill", "chaos_ingest", "replay_fixtures_twice"):
        env = dict(os.environ, DATABASE_URL="postgresql+psycopg://localhost/polypulse")
        result = subprocess.run(
            [sys.executable, "-m", f"scripts.{module}"],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 1
        assert "Refusing to run tests" in result.stderr
