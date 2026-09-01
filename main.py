"""Circex -> SkyPortal GcnEvent service.

Two ways in, one path through. A background consumer reads the live GCN circular
stream (or replays a directory), and an HTTP listener lets an operator push a
single circular through by hand. Both land in `pipeline.process_circular`, which
extracts the event and writes it onto its SkyPortal GcnEvent.

Circulars whose event isn't in SkyPortal yet are parked on a retry queue rather
than dropped — a circular routinely beats the notice that creates the event.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import functools
import json
import logging
import signal
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import tornado.web
import yaml

import pipeline
from skyportal_client import SkyPortalClient, render_plan

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("circex_plugin")

# Extraction is slow under grammar-constrained decoding (minutes on a dense
# circular), so it never runs on the event loop.
_WORK_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="circex")

STATE = pipeline.SessionState()
RESULTS: list[dict[str, Any]] = []
# circular records whose GcnEvent did not resolve, with the time we parked them.
PENDING: list[tuple[datetime, dict[str, Any]]] = []
MAX_RESULTS = 500


def load_config(path: Path | None) -> dict[str, Any]:
    """Config from an explicit YAML file, else from SkyPortal's own config stack."""
    if path is not None:
        raw = yaml.safe_load(path.read_text())
        node = raw
        for key in ("services", "external", "circex", "params"):
            if not isinstance(node, dict) or key not in node:
                return raw if isinstance(raw, dict) else {}
            node = node[key]
        return node
    from baselayer.app.env import load_env

    _, app_cfg = load_env()
    return app_cfg["services.external.circex.params"]


def build_client(cfg: dict[str, Any]) -> SkyPortalClient:
    spcfg = cfg.get("skyportal") or {}
    return SkyPortalClient(
        base_url=spcfg.get("base_url") or "http://localhost:5000/api",
        token=spcfg.get("api_token"),
        live=bool((cfg.get("writes") or {}).get("live", False)),
        timeout=float(spcfg.get("timeout") or 30),
    )


def build_fetch(cfg: dict[str, Any]) -> Any:
    """fetch(circular_id) -> record. A local archive dir, else gcn.nasa.gov."""
    if archive_dir := (cfg.get("archive") or {}).get("dir"):
        from circex.consume.sources import dir_fetch

        return dir_fetch(Path(archive_dir))

    import requests

    def fetch(circular_id: int) -> dict[str, Any] | None:
        try:
            resp = requests.get(f"https://gcn.nasa.gov/circulars/{circular_id}.json", timeout=30)
            return resp.json() if resp.status_code == 200 else None
        except Exception as exc:
            log.warning("fetch of circular %s failed: %s", circular_id, exc)
            return None

    return fetch


def record_result(result: pipeline.ProcessResult) -> None:
    RESULTS.append({**asdict(result), "at": datetime.now(UTC).isoformat()})
    del RESULTS[:-MAX_RESULTS]


def handle_record(record: dict[str, Any], ctx: dict[str, Any]) -> pipeline.ProcessResult:
    """Run one circular through the pipeline; park it if its event is unknown."""
    result = pipeline.process_circular(
        record,
        extractor=ctx["extractor"],
        client=ctx["client"],
        fetch=ctx["fetch"],
        cfg=ctx["cfg"],
        state=STATE,
    )
    if result.status == "unresolved-event":
        PENDING.append((datetime.now(UTC), record))
        log.info("circular %s parked: no GcnEvent for %s", result.circular_id, result.names)
    else:
        log.info(
            "circular %s %s obj=%s dateobs=%s via=%s +%d photometry (%d dup)",
            result.circular_id,
            result.status,
            result.obj_id,
            result.dateobs,
            result.matched_by,
            result.photometry_posted,
            result.photometry_skipped,
        )
    record_result(result)
    return result


async def retry_pending(ctx: dict[str, Any]) -> None:
    """Re-try parked circulars; the notice that creates the event often lands later."""
    rcfg = ctx["cfg"].get("resolver") or {}
    interval = float(rcfg.get("retry_interval_seconds") or 900)
    max_age = float(rcfg.get("retry_max_age_hours") or 72) * 3600
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(interval)
        if not PENDING:
            continue
        now = datetime.now(UTC)
        batch, PENDING[:] = list(PENDING), []
        for parked_at, record in batch:
            if (now - parked_at).total_seconds() > max_age:
                log.info("giving up on circular %s", record.get("circularId"))
                continue
            result = await loop.run_in_executor(
                _WORK_POOL, functools.partial(handle_record, record, ctx)
            )
            # handle_record re-parks it with a fresh timestamp; keep the original
            # so max_age measures age since first sighting, not since last retry.
            if result.status == "unresolved-event" and PENDING:
                PENDING[-1] = (parked_at, record)


async def run_consumer(ctx: dict[str, Any]) -> None:
    """Feed the pipeline from the GCN Kafka stream or a replay directory."""
    ccfg = ctx["cfg"].get("consumer") or {}
    loop = asyncio.get_running_loop()

    if replay_dir := ccfg.get("replay_dir"):
        from circex.consume.sources import replay_dir_records

        records = replay_dir_records(Path(replay_dir))
        log.info("replaying circulars from %s", replay_dir)
    else:
        from circex.consume.sources import gcn_kafka_records

        client_id, secret = ccfg.get("client_id"), ccfg.get("client_secret")
        if not (client_id and secret):
            log.warning("consumer enabled but GCN credentials are missing; not starting")
            return
        records = gcn_kafka_records(client_id, secret, topic=ccfg.get("topic") or "gcn.circulars")
        log.info("subscribed to %s", ccfg.get("topic") or "gcn.circulars")

    # The record iterators are blocking, so step them in the pool too.
    sentinel = object()
    while True:
        record = await loop.run_in_executor(_WORK_POOL, lambda: next(records, sentinel))
        if record is sentinel:
            log.info("consumer stream ended")
            return
        await loop.run_in_executor(_WORK_POOL, functools.partial(handle_record, record, ctx))


def _check_bearer(handler: tornado.web.RequestHandler, expected: str | None) -> bool:
    return not expected or handler.request.headers.get("Authorization") == f"Bearer {expected}"


class BaseAuthHandler(tornado.web.RequestHandler):
    def initialize(self, ctx: dict[str, Any]) -> None:
        self.ctx = ctx

    def prepare(self) -> None:
        expected = (self.ctx["cfg"].get("auth") or {}).get("incoming_bearer_token")
        if not _check_bearer(self, expected):
            self.set_status(401)
            self.finish({"error": "missing or wrong Authorization bearer token"})


class CircularHandler(BaseAuthHandler):
    """POST /circular/<id> — push one circular through by hand."""

    async def post(self, circular_id: str) -> None:
        record = await asyncio.get_running_loop().run_in_executor(
            _WORK_POOL, self.ctx["fetch"], int(circular_id)
        )
        if record is None:
            self.set_status(404)
            self.write({"error": f"circular {circular_id} not found"})
            return
        before = len(self.ctx["client"].plan)
        result = await asyncio.get_running_loop().run_in_executor(
            _WORK_POOL, functools.partial(handle_record, record, self.ctx)
        )
        self.write(
            {
                **asdict(result),
                "live": self.ctx["client"].enabled,
                "writes": self.ctx["client"].plan[before:],
            }
        )


class HealthHandler(BaseAuthHandler):
    def get(self) -> None:
        self.write(
            {
                "status": "ok",
                "live": self.ctx["client"].enabled,
                "extractor": getattr(self.ctx["extractor"], "extractor_id", "unknown"),
                "pending": len(PENDING),
                "processed": len(RESULTS),
            }
        )


class ResultsHandler(BaseAuthHandler):
    def get(self) -> None:
        self.write({"results": RESULTS[-100:], "pending": len(PENDING)})


def build_app(ctx: dict[str, Any]) -> tornado.web.Application:
    return tornado.web.Application(
        [
            (r"/circular/(\d+)", CircularHandler, {"ctx": ctx}),
            (r"/results", ResultsHandler, {"ctx": ctx}),
            (r"/health", HealthHandler, {"ctx": ctx}),
        ]
    )


def build_context(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "cfg": cfg,
        "client": build_client(cfg),
        "fetch": build_fetch(cfg),
        "extractor": pipeline.build_extractor(cfg),
    }


async def amain(cfg: dict[str, Any]) -> None:
    ctx = build_context(cfg)
    if not ctx["client"].enabled:
        log.warning("DRY RUN: writes are planned and logged, nothing is sent to SkyPortal")

    listener = cfg.get("listener") or {}
    app = build_app(ctx)
    server = app.listen(
        int(listener.get("port") or 7200), address=listener.get("host") or "0.0.0.0"
    )
    log.info("listening on %s:%s", listener.get("host"), listener.get("port"))

    tasks = [asyncio.create_task(retry_pending(ctx))]
    if (cfg.get("consumer") or {}).get("enabled", False):
        tasks.append(asyncio.create_task(run_consumer(ctx)))

    loop = asyncio.get_running_loop()

    async def shutdown() -> None:
        log.info("shutdown signal received")
        server.stop()
        for task in tasks:
            task.cancel()
        loop.stop()

    for signame in (signal.SIGTERM, signal.SIGINT):
        # add_signal_handler is Unix-only.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signame, lambda: asyncio.create_task(shutdown()))

    await asyncio.Event().wait()


def replay(cfg: dict[str, Any], directory: Path) -> int:
    """Run a directory of circulars through the pipeline and print the write plan."""
    from circex.consume.sources import dir_fetch, replay_dir_records

    cfg = {**cfg, "archive": {"dir": str(directory)}}
    ctx = build_context(cfg)
    ctx["fetch"] = dir_fetch(directory)
    for record in replay_dir_records(directory):
        handle_record(record, ctx)
    if not ctx["client"].enabled:
        print(render_plan(ctx["client"].plan))
    print(json.dumps({"processed": len(RESULTS), "pending": len(PENDING)}, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="YAML config (else SkyPortal's config stack)")
    parser.add_argument(
        "--replay", type=Path, help="Process a directory of {id}.json circulars and exit"
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.replay is not None:
        return replay(cfg, args.replay)
    asyncio.run(amain(cfg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
