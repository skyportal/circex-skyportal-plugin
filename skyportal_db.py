"""Write Circex extractions through SkyPortal's own models.

The plugin runs inside SkyPortal, so it writes through the same functions
SkyPortal uses on itself rather than over the REST API. `post_source_async` and
`add_external_photometry` carry the permission checks, the default-share groups
and the photometry deduplication index, none of which we want to reimplement.

Nothing is written unless `live` is set. In dry run the planned operations are
recorded on `plan` and logged, which is what the tests assert against.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("circex_plugin.skyportal")

# Names this producer in GcnEventExtraction.origin and in Photometry.origin.
ORIGIN = "circex"


@dataclass
class SkyPortalWriter:
    # Writes are attributed to this user; 1 is the super admin a fresh
    # deployment provisions. Point it at a bot user to keep the provenance clean.
    user_id: int = 1
    live: bool = False
    plan: list[dict[str, Any]] = field(default_factory=list)

    def _record(self, op: str, payload: dict[str, Any]) -> None:
        self.plan.append({"op": op, "payload": payload})
        if not self.live:
            log.info("dry-run %s %s", op, payload)

    async def write_source(self, session: Any, source: Any, group_ids: list[int]) -> None:
        payload = {"id": source.id, "ra": source.ra, "dec": source.dec}
        if group_ids:
            payload["group_ids"] = group_ids
        self._record("source", payload)
        if not self.live:
            return
        from skyportal.handlers.api.source import post_source_async

        await post_source_async(payload, self.user_id, session, refresh_source=False)

    async def write_photometry(self, session: Any, points: list[Any], group_ids: list[int]) -> int:
        """Post photometry, one call per instrument.

        The payload is column-oriented (PhotMagFlexible), and duplicates are
        resolved against SkyPortal's unique deduplication index rather than by
        tracking what this process has already sent.
        """
        # Mag-space and flux-space rows carry different required keys, so they are
        # grouped separately: one payload cannot be validated as both.
        by_instrument: dict[tuple[str, int, bool], list[Any]] = defaultdict(list)
        for point in points:
            flux_space = getattr(point, "is_flux_space", False)
            by_instrument[(point.obj_id, point.instrument_id, flux_space)].append(point)

        written = 0
        for (obj_id, instrument_id, flux_space), rows in by_instrument.items():
            payload: dict[str, Any] = {
                "obj_id": obj_id,
                "instrument_id": instrument_id,
                "mjd": [r.mjd for r in rows],
                "filter": [r.filter for r in rows],
                "magsys": [r.magsys for r in rows],
                "altdata": [r.altdata for r in rows],
                "origin": [ORIGIN] * len(rows),
            }
            if flux_space:
                payload["flux"] = [r.flux for r in rows]
                payload["fluxerr"] = [r.fluxerr for r in rows]
                payload["zp"] = [r.zp for r in rows]
            else:
                payload["mag"] = [r.mag for r in rows]
                payload["magerr"] = [r.magerr for r in rows]
                payload["limiting_mag"] = [r.limiting_mag for r in rows]
            if group_ids:
                payload["group_ids"] = group_ids
            self._record("photometry", {**payload, "n": len(rows)})
            written += len(rows)
            if not self.live:
                continue
            import sqlalchemy as sa
            from skyportal.handlers.api.photometry import add_external_photometry
            from skyportal.models import User

            user = await session.scalar(sa.select(User).where(User.id == self.user_id))
            await add_external_photometry(
                payload, user, session, duplicates="update", refresh=False
            )
        return written

    async def write_extraction(
        self, session: Any, dateobs: Any, circular_id: int | None, data: dict[str, Any]
    ) -> None:
        """Store the structured extraction alongside the values derived from it.

        The derived writes lose everything the SkyPortal schema has no place for
        — provenance spans, redshift bounds, per-row telescopes — so keep the
        extraction itself for anything that wants to read it back.
        """
        self._record("extraction", {"dateobs": str(dateobs), "circular_id": circular_id})
        if not self.live:
            return
        import sqlalchemy as sa
        from skyportal.models import GcnEventExtraction

        existing = await session.scalar(
            sa.select(GcnEventExtraction).where(
                GcnEventExtraction.dateobs == dateobs,
                GcnEventExtraction.origin == ORIGIN,
                GcnEventExtraction.circular_id == circular_id,
            )
        )
        if existing is not None:
            existing.data = data
            return
        session.add(
            GcnEventExtraction(
                dateobs=dateobs,
                origin=ORIGIN,
                circular_id=circular_id,
                data=data,
                sent_by_id=self.user_id,
            )
        )

    async def set_redshift(self, session: Any, obj_id: str, z: float, z_err: float | None) -> None:
        self._record("redshift", {"obj_id": obj_id, "redshift": z, "redshift_error": z_err})
        if not self.live:
            return
        import sqlalchemy as sa
        from skyportal.models import Obj

        obj = await session.scalar(sa.select(Obj).where(Obj.id == obj_id))
        if obj is None:
            log.warning("cannot set redshift: no source %s", obj_id)
            return
        obj.redshift = z
        obj.redshift_error = z_err


def render_plan(plan: list[dict[str, Any]]) -> str:
    """Human-readable dump of planned writes, for dry-run output."""
    return "\n".join(f"{p['op']}: {p['payload']}" for p in plan)
