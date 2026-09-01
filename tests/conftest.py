"""Fixtures for the pure-logic tests.

Anything touching the database is exercised by the in-container integration
tests in fritz (extensions/skyportal/skyportal/tests/api/circex/).
"""

from pathlib import Path

import pytest

FLURRY = Path(__file__).parent / "fixtures" / "flurry"


@pytest.fixture
def cfg():
    return {
        "skyportal": {"user_id": 1, "group_ids": [1988], "default_instrument_id": 4},
        "resolver": {"order": ["alias", "designation", "trigger"], "window_hours": 12},
        "writes": {"live": False, "alias": True, "source": True, "comment": True},
        "extractor": {"kind": "regex"},
    }


@pytest.fixture
def flurry():
    return FLURRY
