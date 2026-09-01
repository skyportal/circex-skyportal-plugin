"""Thin SkyPortal REST client: plan requests, then optionally send them.

Every write goes through `request`, which is a no-op unless `live` is set AND a
token is present — writing to a shared SkyPortal instance is outward-facing and
hard to reverse, so it takes two deliberate switches. Planned requests are
recorded on `plan` either way, which is what the dry-run output and the tests
assert against.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("circex_plugin.skyportal")


class SkyPortalError(RuntimeError):
    """A SkyPortal request failed, by HTTP status or by status='error' in a 200."""


@dataclass
class SkyPortalClient:
    base_url: str = "http://localhost:5000/api"
    token: str | None = None
    live: bool = False
    timeout: float = 30.0
    # Unattended use: log and carry on rather than killing the stream over one
    # bad row (a filter the instrument doesn't have, a duplicate alias, ...).
    continue_on_error: bool = True
    plan: list[dict[str, Any]] = field(default_factory=list)

    @property
    def enabled(self) -> bool:
        return bool(self.live and self.token)

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Send one request, or record it and return None in dry-run."""
        entry = {"method": method.upper(), "path": path, "payload": payload or {}}
        self.plan.append(entry)
        if not self.enabled:
            log.info("dry-run %s %s %s", entry["method"], path, json.dumps(entry["payload"]))
            return None
        try:
            return self._send(entry)
        except SkyPortalError as exc:
            if not self.continue_on_error:
                raise
            log.warning("%s %s failed: %s", entry["method"], path, exc)
            return None

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Reads are not gated by `live` — a dry run still needs to resolve events."""
        if not self.token:
            return None
        import requests

        try:
            resp = requests.get(
                self._url(path), headers=self._headers(), params=params, timeout=self.timeout
            )
            return self._unwrap(resp)
        except Exception as exc:
            if not self.continue_on_error:
                raise SkyPortalError(str(exc)) from exc
            log.warning("GET %s failed: %s", path, exc)
            return None

    def existing_photometry(self, obj_id: str) -> list[tuple[str, str, float]]:
        """(obj_id, filter, mjd) already on a source, for seeding dedup.

        Without this the dedup set is only as old as the process: a restart
        re-aggregates each event from its circulars and re-posts the whole light
        curve. Reads are not gated by `live`, so a dry run sees real duplicates too.
        """
        data = self.get(f"/sources/{obj_id}/photometry")
        points = data.get("data") if isinstance(data, dict) else None
        if not isinstance(points, list):
            return []
        return [
            (obj_id, point["filter"], point["mjd"])
            for point in points
            if isinstance(point, dict) and point.get("filter") and point.get("mjd") is not None
        ]

    def _url(self, path: str) -> str:
        return self.base_url.rstrip("/") + "/" + path.lstrip("/")

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"token {self.token}", "Content-Type": "application/json"}

    def _send(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        import requests

        try:
            resp = requests.request(
                entry["method"],
                self._url(entry["path"]),
                headers=self._headers(),
                data=json.dumps(entry["payload"]),
                timeout=self.timeout,
            )
        except Exception as exc:
            raise SkyPortalError(f"transport: {exc}") from exc
        return self._unwrap(resp)

    @staticmethod
    def _unwrap(resp: Any) -> dict[str, Any] | None:
        """SkyPortal signals failure both as HTTP status and as status='error' in a 200."""
        try:
            body = resp.json()
        except Exception:
            body = None
        if resp.status_code >= 400:
            message = (body or {}).get("message") if isinstance(body, dict) else None
            raise SkyPortalError(f"HTTP {resp.status_code}: {message or resp.text[:200]}")
        if isinstance(body, dict) and body.get("status") == "error":
            raise SkyPortalError(str(body.get("message", "skyportal error"))[:200])
        data = body.get("data") if isinstance(body, dict) else None
        return data if isinstance(data, dict) else ({"data": data} if data is not None else {})


def render_plan(plan: list[dict[str, Any]]) -> str:
    """Human-readable dump of planned requests, for dry-run output."""
    lines: list[str] = []
    for req in plan:
        lines.append(f"{req['method']} {req['path']}")
        if req["payload"]:
            lines.append("  " + json.dumps(req["payload"], ensure_ascii=False))
    return "\n".join(lines)
