"""Prevent the test suite from targeting the production database."""

import sys

from sqlalchemy.engine import make_url

from app.config import get_settings


def main() -> int:
    if make_url(get_settings().database_url).database == "polypulse":
        print(
            "Refusing to run tests: DATABASE_URL points to database 'polypulse'.", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
