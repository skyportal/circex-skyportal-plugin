"""The write gate and SkyPortal's two failure conventions."""

from unittest.mock import patch

import pytest

from skyportal_client import SkyPortalClient, SkyPortalError, render_plan


class Response:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code, self._body, self.text = status_code, body, text

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


def test_dry_run_plans_but_sends_nothing():
    client = SkyPortalClient(token="t", live=False)
    with patch("requests.request") as request:
        assert client.request("POST", "/sources", {"id": "X"}) is None
        request.assert_not_called()
    assert client.plan == [{"method": "POST", "path": "/sources", "payload": {"id": "X"}}]


def test_live_without_a_token_still_does_not_send():
    """Both switches are required; `live` alone is not enough."""
    client = SkyPortalClient(token=None, live=True)
    assert not client.enabled
    with patch("requests.request") as request:
        client.request("POST", "/sources", {"id": "X"})
        request.assert_not_called()


def test_live_with_a_token_sends():
    client = SkyPortalClient(token="t", live=True, base_url="http://sp/api")
    with patch(
        "requests.request", return_value=Response(body={"status": "success", "data": {}})
    ) as request:
        client.request("POST", "/sources", {"id": "X"})
    assert request.call_args.args == ("POST", "http://sp/api/sources")


def test_error_status_inside_a_200_is_a_failure():
    client = SkyPortalClient(token="t", live=True, continue_on_error=False)
    with (
        patch(
            "requests.request", return_value=Response(body={"status": "error", "message": "nope"})
        ),
        pytest.raises(SkyPortalError, match="nope"),
    ):
        client.request("POST", "/sources", {})


def test_http_error_is_a_failure():
    client = SkyPortalClient(token="t", live=True, continue_on_error=False)
    with (
        patch("requests.request", return_value=Response(400, {"message": "bad"})),
        pytest.raises(SkyPortalError, match="bad"),
    ):
        client.request("POST", "/sources", {})


def test_continue_on_error_swallows_a_failure():
    """One bad row must not kill the stream."""
    client = SkyPortalClient(token="t", live=True, continue_on_error=True)
    with patch("requests.request", return_value=Response(400, {"message": "bad"})):
        assert client.request("POST", "/photometry", {}) is None
    assert len(client.plan) == 1


def test_reads_work_in_dry_run():
    """Resolution needs GETs even when nothing may be written."""
    client = SkyPortalClient(token="t", live=False)
    with patch(
        "requests.get", return_value=Response(body={"status": "success", "data": {"events": []}})
    ):
        assert client.get("/gcn_event", {"partialdateobs": "x"}) == {"events": []}


def test_reads_without_a_token_return_none():
    assert SkyPortalClient(token=None).get("/gcn_event") is None


def test_render_plan_is_readable():
    out = render_plan([{"method": "POST", "path": "/sources", "payload": {"id": "X"}}])
    assert "POST /sources" in out and '"id": "X"' in out
