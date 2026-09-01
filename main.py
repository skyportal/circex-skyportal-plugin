"""Circex -> SkyPortal GcnEvent service.

Runs inside SkyPortal: it loads the app's config, connects to the same database,
and writes through SkyPortal's own model functions. There is no HTTP surface and
no API token.

Circulars arrive from the live GCN stream (or a replay directory), and each is
extracted and attached to the GcnEvent it reports on. One whose event is not in
the database yet is parked and retried, since a circular routinely beats the
notice that creates the event.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

import pipeline
from skyportal_db import SkyPortalWriter, render_plan

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("circex_plugin")

PENDING: list[tuple[datetime, dict[str, Any]]] = []


def load_config(path: Path | None) -> dict[str, Any]:
    """Config from an explicit YAML file, else from SkyPortal's own stack."""
    if path is not None:
        node = yaml.safe_load(path.read_text())
        for key in ("services", "external", "circex", "params"):
            if not isinstance(node, dict) or key not in node:
                return node if isinstance(node, dict) else {}
            node = node[key]
        return node
    from baselayer.app.env import load_env

    _, app_cfg = load_env()
    return app_cfg["services.external.circex.params"]


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


async def handle_record(record: dict[str, Any], ctx: dict[str, Any]) -> pipeline.ProcessResult:
    """Run one circular through the pipeline in its own transaction."""
    from baselayer.app import models

    async with models.async_plain_session_factory() as session:
        result = await pipeline.process_circular(
            record,
            session=session,
            extractor=ctx["extractor"],
            writer=ctx["writer"],
            fetch=ctx["fetch"],
            cfg=ctx["cfg"],
        )
        if ctx["writer"].live:
            await session.commit()

    if result.status == "unresolved-event":
        PENDING.append((datetime.now(UTC), record))
        log.info("circular %s parked: no GcnEvent for %s", result.circular_id, result.names)
    else:
        log.info(
            "circular %s %s obj=%s dateobs=%s via=%s %d photometry",
            result.circular_id,
            result.status,
            result.obj_id,
            result.dateobs,
            result.matched_by,
            result.photometry,
        )
    return result


async def retry_pending(ctx: dict[str, Any]) -> None:
    """Re-try parked circulars; the notice that creates the event often lands later."""
    rcfg = ctx["cfg"].get("resolver") or {}
    interval = float(rcfg.get("retry_interval_seconds") or 900)
    max_age = float(rcfg.get("retry_max_age_hours") or 72) * 3600
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
            result = await handle_record(record, ctx)
            # Keep the original timestamp so max_age measures age since first
            # sighting, not since the last retry.
            if result.status == "unresolved-event" and PENDING:
                PENDING[-1] = (parked_at, record)


def _kafka_consumer(ccfg: dict[str, Any]) -> Any:
    """A stable consumer group, so a restart resumes where it stopped."""
    from gcn_kafka import Consumer

    consumer = Consumer(
        config={
            "group.id": ccfg["group_id"],
            "auto.offset.reset": ccfg.get("offset_reset") or "latest",
            "enable.auto.commit": False,
        },
        client_id=ccfg["client_id"],
        client_secret=ccfg["client_secret"],
        domain=ccfg.get("server") or "gcn.nasa.gov",
    )
    consumer.subscribe([ccfg.get("topic") or "gcn.circulars"])
    return consumer


async def run_consumer(ctx: dict[str, Any]) -> None:
    ccfg = ctx["cfg"].get("consumer") or {}
    loop = asyncio.get_running_loop()

    if replay_dir := ccfg.get("replay_dir"):
        from circex.consume.sources import replay_dir_records

        log.info("replaying circulars from %s", replay_dir)
        for record in replay_dir_records(Path(replay_dir)):
            await handle_record(record, ctx)
        return

    missing = [k for k in ("client_id", "client_secret", "group_id") if not ccfg.get(k)]
    if missing:
        log.warning("consumer enabled but %s not set; not starting", ", ".join(missing))
        return

    consumer = await loop.run_in_executor(None, _kafka_consumer, ccfg)
    log.info("subscribed as group %s", ccfg["group_id"])
    while True:
        messages = await loop.run_in_executor(None, consumer.consume, 10, 1.0)
        for message in messages:
            if message.error() or message.value() is None:
                continue
            try:
                record = json.loads(message.value())
            except json.JSONDecodeError as exc:
                log.warning("undecodable circular: %s", exc)
                consumer.commit(message)
                continue
            try:
                await handle_record(record, ctx)
            except Exception:
                # Leave the offset uncommitted so the circular is retried.
                log.exception("failed to handle circular %s", record.get("circularId"))
                continue
            consumer.commit(message)


def build_context(cfg: dict[str, Any]) -> dict[str, Any]:
    spcfg = cfg.get("skyportal") or {}
    return {
        "cfg": cfg,
        "writer": SkyPortalWriter(
            user_id=int(spcfg.get("user_id") or 1),
            live=bool((cfg.get("writes") or {}).get("live", False)),
        ),
        "fetch": build_fetch(cfg),
        "extractor": pipeline.build_extractor(cfg),
    }


async def amain(cfg: dict[str, Any]) -> None:
    from baselayer.app.env import load_env
    from baselayer.app.models import init_db

    _, app_cfg = load_env()
    init_db(**app_cfg["database"])

    ctx = build_context(cfg)
    if not ctx["writer"].live:
        log.warning("DRY RUN: writes are planned and logged, nothing is committed")

    tasks = [asyncio.create_task(retry_pending(ctx))]
    if (cfg.get("consumer") or {}).get("enabled", False):
        tasks.append(asyncio.create_task(run_consumer(ctx)))
    await asyncio.gather(*tasks)


async def replay(cfg: dict[str, Any], directory: Path) -> int:
    """Run a directory of circulars through the pipeline and print the plan."""
    from baselayer.app.env import load_env
    from baselayer.app.models import init_db
    from circex.consume.sources import dir_fetch, replay_dir_records

    _, app_cfg = load_env()
    init_db(**app_cfg["database"])

    cfg = {**cfg, "archive": {"dir": str(directory)}}
    ctx = build_context(cfg)
    ctx["fetch"] = dir_fetch(directory)
    for record in replay_dir_records(directory):
        await handle_record(record, ctx)
    if not ctx["writer"].live:
        print(render_plan(ctx["writer"].plan))
    print(json.dumps({"pending": len(PENDING)}, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="YAML config (else SkyPortal's stack)")
    parser.add_argument("--replay", type=Path, help="Process a directory of circulars and exit")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.replay is not None:
        return asyncio.run(replay(cfg, args.replay))
    asyncio.run(amain(cfg))
    return 0


if __name__ == "__main__":
    sys.exit(main())
