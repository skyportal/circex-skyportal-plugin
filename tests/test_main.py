"""Config loading, the HTTP surface, and the unresolved-event retry queue."""

import json

import pytest
import tornado.testing

import main
import pipeline
from tests.conftest import EVENT, FLURRY, FakeSkyPortal, requires_extractions


@pytest.fixture(autouse=True)
def clean_globals():
    main.RESULTS.clear()
    main.PENDING.clear()
    main.STATE = pipeline.SessionState()
    yield


def test_load_config_unwraps_the_skyportal_service_block(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        json.dumps({"services": {"external": {"circex": {"params": {"listener": {"port": 1}}}}}})
    )
    assert main.load_config(path) == {"listener": {"port": 1}}


def test_load_config_accepts_a_bare_mapping(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps({"listener": {"port": 2}}))
    assert main.load_config(path) == {"listener": {"port": 2}}


def test_build_client_stays_dry_unless_writes_live(cfg):
    cfg["skyportal"]["api_token"] = "t"
    assert not main.build_client(cfg).enabled
    cfg["writes"]["live"] = True
    assert main.build_client(cfg).enabled


@requires_extractions
def test_unresolved_circular_is_parked(cfg):
    from circex.consume.sources import dir_fetch

    ctx = {
        "cfg": cfg,
        "client": FakeSkyPortal(events=[]),
        "fetch": dir_fetch(FLURRY),
        "extractor": pipeline.build_extractor(cfg),
    }
    result = main.handle_record({"circularId": 44834}, ctx)
    assert result.status == "unresolved-event"
    assert len(main.PENDING) == 1


class HandlerTest(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        from circex.consume.sources import dir_fetch

        self.cfg = {
            "skyportal": {"group_ids": [1], "default_instrument_id": 4},
            "resolver": {"order": ["alias", "designation", "trigger"]},
            "writes": {"live": False, "source": True, "alias": True, "comment": True},
            "extractor": {"kind": "regex"},
            "auth": {"incoming_bearer_token": "secret"},
        }
        self.ctx = {
            "cfg": self.cfg,
            "client": FakeSkyPortal(),
            "fetch": dir_fetch(FLURRY),
            "extractor": pipeline.build_extractor(self.cfg),
        }
        return main.build_app(self.ctx)

    def test_health_requires_the_bearer_token(self):
        assert self.fetch("/health").code == 401

    def test_health_reports_dry_run(self):
        body = json.loads(self.fetch("/health", headers={"Authorization": "Bearer secret"}).body)
        assert body["status"] == "ok"
        assert body["live"] is False

    def test_posting_an_unknown_circular_is_a_404(self):
        resp = self.fetch(
            "/circular/999999",
            method="POST",
            body="",
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.code == 404

    @requires_extractions
    def test_posting_a_circular_returns_the_planned_writes(self):
        resp = self.fetch(
            "/circular/44834",
            method="POST",
            body="",
            headers={"Authorization": "Bearer secret"},
        )
        body = json.loads(resp.body)
        assert body["dateobs"] == EVENT["dateobs"]
        assert body["live"] is False
        assert any(w["path"] == "/photometry" for w in body["writes"])
