"""The shipped defaults must stay safe and internally consistent."""

from pathlib import Path

import yaml

DEFAULTS = Path(__file__).parent.parent / "config.yaml.defaults"


def params():
    cfg = yaml.safe_load(DEFAULTS.read_text())
    return cfg["services"]["external"]["circex"]["params"]


def test_ships_switched_off():
    p = params()
    assert p["writes"]["live"] is False
    assert p["consumer"]["enabled"] is False


def test_no_rest_credentials():
    """The service writes through the database; it has no HTTP surface."""
    p = params()
    assert "api_token" not in p["skyportal"]
    assert "base_url" not in p["skyportal"]
    assert "auth" not in p


def test_writes_are_attributed_to_a_user():
    assert params()["skyportal"]["user_id"] >= 1


def test_require_fields_matches_the_model():
    """Mispairing silently yields fabricated rows or nothing at all."""
    e = params()["extractor"]
    model = (e.get("llama_model") or "").lower()
    if "mistral" in model:
        assert e["llama_require_fields"] is False
    elif "qwen" in model:
        assert e["llama_require_fields"] is True
