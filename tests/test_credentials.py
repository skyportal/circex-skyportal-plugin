"""GCN credentials fall back to SkyPortal's, so there is one copy to rotate."""

import main


def test_configured_credentials_win():
    assert main.gcn_credentials({"client_id": "a", "client_secret": "b"}) == ("a", "b")


def test_falls_back_when_unset(monkeypatch):
    monkeypatch.setattr(main, "gcn_credentials", main.gcn_credentials)  # keep the real one
    import sys
    import types

    env = types.ModuleType("baselayer.app.env")
    env.load_env = lambda: (None, {"gcn.client_id": "x", "gcn.client_secret": "y"})
    monkeypatch.setitem(sys.modules, "baselayer", types.ModuleType("baselayer"))
    monkeypatch.setitem(sys.modules, "baselayer.app", types.ModuleType("baselayer.app"))
    monkeypatch.setitem(sys.modules, "baselayer.app.env", env)
    assert main.gcn_credentials({}) == ("x", "y")


def test_partial_config_still_falls_back(monkeypatch):
    """A client_id with no secret is not usable; don't half-use it."""
    import sys
    import types

    env = types.ModuleType("baselayer.app.env")
    env.load_env = lambda: (None, {"gcn.client_id": "x", "gcn.client_secret": "y"})
    monkeypatch.setitem(sys.modules, "baselayer", types.ModuleType("baselayer"))
    monkeypatch.setitem(sys.modules, "baselayer.app", types.ModuleType("baselayer.app"))
    monkeypatch.setitem(sys.modules, "baselayer.app.env", env)
    assert main.gcn_credentials({"client_id": "only-id"}) == ("x", "y")


def test_none_when_nothing_is_available():
    assert main.gcn_credentials({}) == (None, None)
