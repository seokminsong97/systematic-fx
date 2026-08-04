"""Load and guard explicit machine-local integration boundaries."""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from systematic_fx.db.bootstrap import (
    TEST_DATABASE_NAME,
    DatabaseBootstrapError,
    _url_database_name,
)

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Refuse to run integration tests against the research control database."""

    if not any("integration" in item.path.parts for item in items):
        return
    test_database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
    if not test_database_url:
        return
    try:
        database_name = _url_database_name(
            test_database_url,
            label="SYSTEMATIC_FX_TEST_DATABASE_URL",
        )
    except DatabaseBootstrapError as error:
        raise pytest.UsageError(str(error)) from error
    if database_name != TEST_DATABASE_NAME:
        raise pytest.UsageError(
            "SYSTEMATIC_FX_TEST_DATABASE_URL must explicitly target "
            f"{TEST_DATABASE_NAME!r}; refusing integration tests against "
            f"{database_name!r}"
        )
