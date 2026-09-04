"""One circular in, SkyPortal writes out.

Extraction lives in `circex`; this module is the binding. For each circular it
reconstructs the whole event from the GCN cross-reference graph, aggregates it
into one counterpart, resolves which SkyPortal GcnEvent that belongs to, and
writes the source and the event-level attachments.

Photometry is not deduplicated here. SkyPortal has a unique index over
(obj_id, instrument_id, origin, mjd, fluxerr, flux) and `add_external_photometry`
resolves collisions against it, so re-seeing an event is idempotent by
construction and survives restarts.

Circex imports are deferred so this module loads without a model server or the
archive.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import gcn_event

log = logging.getLogger("circex_plugin.pipeline")

_MJD_EPOCH = datetime(1858, 11, 17, tzinfo=UTC)


def mjd_to_iso(mjd: float) -> str:
    return (_MJD_EPOCH + timedelta(days=mjd)).isoformat()


def build_extractor(cfg: dict[str, Any]) -> Any:
    """regex | llama | hybrid, per config."""
    ecfg = cfg.get("extractor") or {}
    kind = (ecfg.get("kind") or "regex").lower()

    from circex.extract.regex.extractor import RegexExtractor

    classifier = None
    if model_path := ecfg.get("sn_type_model"):
        from circex.classify.sn_type import SNTypeClassifier

        classifier = SNTypeClassifier.load(Path(model_path))

    regex = RegexExtractor(sn_classifier=classifier) if classifier else RegexExtractor()
    if kind == "regex":
        return regex

    cache = None
    if cache_path := ecfg.get("cache_path"):
        from circex.cache.llm import LLMCache

        Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
        cache = LLMCache(Path(cache_path))

    from circex.extract.llm.llama_server import LlamaServerExtractor

    llm = LlamaServerExtractor(
        base_url=ecfg.get("llama_url") or "http://localhost:8080",
        model_id=ecfg.get("llama_model") or "mistral-7b",
        cache=cache,
        timeout=float(ecfg.get("llama_timeout") or 300),
        require_fields=bool(ecfg.get("llama_require_fields", False)),
        api_key=ecfg.get("llama_api_key") or None,
    )
    if kind == "llama":
        return llm
    if kind == "hybrid":
        from circex.extract.hybrid import HybridExtractor

        # Keep the trained classifier as a fallback when the LLM abstains; the
        # default routing drops regex classification as noise.
        overrides = {"classification": ("llm", "regex")} if classifier else None
        return HybridExtractor(regex, llm, routing_overrides=overrides)
    raise ValueError(f"unknown extractor kind {kind!r} (expected regex, llama, or hybrid)")


def event_names(actions: Any) -> list[str]:
    """Candidate event names, designations first.

    A counterpart name (AT2017gfo) never resolves an event, so designations sort
    ahead of it; both are still written as aliases.
    """
    names: list[str] = []
    for extraction in getattr(actions, "extractions", ()):
        event = getattr(extraction, "event", None)
        raw = getattr(event, "event_name", None) if event is not None else None
        if raw is None:
            continue
        names.extend(n for n in (raw if isinstance(raw, list) else [raw]) if n)
    return sorted(dict.fromkeys(names), key=lambda n: gcn_event.parse_designation(n) is None)


def detection_window(points: list[Any], pad_days: float = 1.0) -> tuple[str, str] | None:
    """ISO bracket around the counterpart's detections, for sources_in_gcn."""
    mjds = [p.mjd for p in points if getattr(p, "mjd", None) is not None]
    if not mjds:
        return None
    return mjd_to_iso(min(mjds) - pad_days), mjd_to_iso(max(mjds) + pad_days)


# Identifies this event's Circex comment so later circulars update it in place.
COMMENT_MARKER = "Extracted by Circex"

GCN_CIRCULAR_URL = "https://gcn.nasa.gov/circulars/{circular_id}"

# Enough measurement lines to see what the circular said, not so many that a
# large table buries the rest of the comment.
_MAX_REPORTED_LINES = 12


def build_comment(actions: Any, records: list[dict[str, Any]]) -> str:
    """The event-level comment: what the circular reported, and what was written.

    Both halves matter to a reader — the measurements restate the circular, and
    the written/not-written sections say what actually reached the database, so a
    thin light curve is distinguishable from data that was dropped.
    """
    lines: list[str] = []
    for record in records:
        circular_id = record.get("circularId")
        subject = (record.get("subject") or "").strip()
        header = f"**GCN {circular_id}**" if circular_id else "**GCN circular**"
        if subject:
            header += f" — {subject}"
        lines.append(header)
        if circular_id:
            lines.append(GCN_CIRCULAR_URL.format(circular_id=circular_id))

    reported = _reported_lines(actions)
    if reported:
        lines.append("")
        lines.append("**Reported**")
        lines.extend(reported)

    written = _written_lines(actions)
    lines.append("")
    lines.append("**Written to SkyPortal**")
    lines.extend(written or ["- nothing postable from this circular"])

    if actions.skipped_reasons:
        counts = Counter(actions.skipped_reasons)
        lines.append("")
        lines.append("**Not written**")
        for reason, n in sorted(counts.items()):
            lines.append(f"- {n} photometry row(s): {reason}")

    notes = list(dict.fromkeys(n for e in actions.extractions for n in e.extraction_meta.notes))
    if notes:
        lines.append("")
        lines.append("**Notes**")
        lines.extend(f"- {note}" for note in notes)

    if actions.extractions:
        lines.append("")
        lines.append(f"_{COMMENT_MARKER} ({actions.extractions[0].extraction_meta.extractor})._")
    return "\n".join(lines)


def _reported_lines(actions: Any) -> list[str]:
    """One line per measurement, as the circular states it."""
    rows = [row for extraction in actions.extractions for row in extraction.photometry]
    out: list[str] = []
    for row in rows[:_MAX_REPORTED_LINES]:
        measurement = _radio_measurement(row) if row.frequency_ghz else _optical_measurement(row)
        if measurement is None:
            continue
        context = ", ".join(p for p in (row.telescope, _epoch(row)) if p)
        out.append(f"- {measurement}" + (f" ({context})" if context else ""))
    if len(rows) > _MAX_REPORTED_LINES:
        out.append(f"- ...and {len(rows) - _MAX_REPORTED_LINES} more row(s)")
    return out


def _radio_measurement(row: Any) -> str | None:
    unit = row.flux_density_unit or ""
    band = f"{row.frequency_ghz:g} GHz"
    if row.flux_density is not None:
        error = f" +/- {row.flux_density_error:g}" if row.flux_density_error is not None else ""
        return f"{band}: {row.flux_density:g}{error} {unit}".strip()
    if row.limiting_flux_density is not None:
        sigma = row.limiting_mag_sigma or 3.0
        return f"{band}: < {row.limiting_flux_density:g} {unit} ({sigma:g} sigma)".strip()
    return None


def _optical_measurement(row: Any) -> str | None:
    band = row.filter or row.bandpass or "unfiltered"
    if row.mag is not None:
        error = f" +/- {row.mag_error:g}" if row.mag_error is not None else ""
        return f"{band} = {row.mag:g}{error}"
    if row.limiting_mag is not None:
        sigma = row.limiting_mag_sigma or 3.0
        return f"{band} > {row.limiting_mag:g} ({sigma:g} sigma)"
    return None


def _epoch(row: Any) -> str:
    return (row.obs_time or "").replace("T", " ").replace("Z", " UT").strip()


def _written_lines(actions: Any) -> list[str]:
    lines: list[str] = []
    if actions.source is not None:
        lines.append(
            f"- Source `{actions.source.id}` at RA={actions.source.ra}, Dec={actions.source.dec}"
        )
    if actions.photometry:
        bands = sorted({p.filter for p in actions.photometry})
        lines.append(f"- {len(actions.photometry)} photometry point(s): {', '.join(bands)}")
    if actions.redshift is not None:
        z, z_err = actions.redshift
        lines.append(f"- Redshift z = {z}" + (f" +/- {z_err}" if z_err is not None else ""))
    for label in _classifications(actions):
        lines.append(f"- Classification: {label}")
    return lines


def _classifications(actions: Any) -> list[str]:
    """Class names from the extractions, in order, without repeats."""
    labels = []
    for extraction in actions.extractions:
        classification = getattr(extraction, "classification", None)
        name = getattr(classification, "classification", None) if classification else None
        if name:
            labels.append(str(name))
    return list(dict.fromkeys(labels))


@dataclass
class ProcessResult:
    circular_id: int
    obj_id: str | None = None
    dateobs: str | None = None
    matched_by: str = ""
    photometry: int = 0
    rejected: int = 0
    status: str = "nothing-postable"  # posted | unresolved-event | nothing-postable
    names: list[str] = field(default_factory=list)


def prepare_circular(
    record: dict[str, Any],
    *,
    extractor: Any,
    fetch: Any,
    cfg: dict[str, Any],
    trigger_time: Any = None,
) -> tuple[list[dict[str, Any]], Any]:
    """Fetch the cross-referenced circulars and extract them.

    Kept out of `process_circular` because both steps are slow — the extraction
    can take minutes against a language model — and a database session held open
    across them is closed under `idle_in_transaction_session_timeout`.
    """
    from circex.bot.aggregate import aggregate_event, gather_by_xref

    spcfg = cfg.get("skyportal") or {}
    records = gather_by_xref(int(record.get("circularId") or 0), fetch, max_hops=1)
    actions = aggregate_event(
        records,
        extractor,
        # Without this every "N hours after the trigger" epoch stays unresolved
        # and the row is dropped for having no observation time. The caller
        # passes the event's dateobs once it is known; the fallback is the first
        # circular's timestamp, which trails the burst by the time it took to
        # write about it.
        trigger_time=trigger_time or _trigger_time(records),
        instrument_map=spcfg.get("instrument_map") or {},
        bandpass_instrument_map=spcfg.get("bandpass_instrument_map") or {},
        default_instrument_id=spcfg.get("default_instrument_id"),
        group_ids=spcfg.get("group_ids") or [],
    )
    return records, actions


async def process_circular(
    record: dict[str, Any],
    *,
    session: Any,
    extractor: Any,
    writer: Any,
    fetch: Any,
    cfg: dict[str, Any],
    prepared: tuple[list[dict[str, Any]], Any] | None = None,
) -> ProcessResult:
    """Bind one circular's extraction to its SkyPortal GcnEvent and write it.

    Pass `prepared` to reuse work done outside the transaction; without it the
    fetch and extraction run here, holding the session open for their duration.
    """
    circular_id = int(record.get("circularId") or 0)
    spcfg = cfg.get("skyportal") or {}
    rcfg = cfg.get("resolver") or {}
    writes = cfg.get("writes") or {}
    group_ids = spcfg.get("group_ids") or []

    records, actions = (
        prepared
        if prepared is not None
        else prepare_circular(record, extractor=extractor, fetch=fetch, cfg=cfg)
    )
    result = ProcessResult(circular_id=circular_id)
    if actions.source is None:
        return result
    result.obj_id = actions.source.id

    names = event_names(actions)
    result.names = names
    match = await gcn_event.resolve_event(
        session,
        names=names,
        trigger_time=_trigger_time(records),
        order=rcfg.get("order"),
        window_hours=float(rcfg.get("window_hours") or 12),
        text=" ".join(str(r.get("body") or "") for r in records),
    )
    if match is None:
        # The event usually appears within minutes; the caller parks the
        # circular and retries rather than dropping the extraction.
        result.status = "unresolved-event"
        return result
    result.dateobs, result.matched_by = str(match.dateobs), match.matched_by

    # The epochs above were resolved against the first circular's timestamp,
    # which trails the burst. Now that the event is known, redo them against its
    # dateobs; the extraction cache makes the second pass cheap.
    if match.dateobs is not None and match.dateobs != _trigger_time(records):
        records, actions = prepare_circular(
            record,
            extractor=extractor,
            fetch=fetch,
            cfg=cfg,
            trigger_time=match.dateobs,
        )

    # A retraction cannot un-post what an earlier circular already wrote, so the
    # rows are marked unreliable instead — the reader sees the measurement and
    # the reason it is no longer trusted.
    withdrawn = [e.circular_id for e in actions.extractions if e.retraction]
    if withdrawn and actions.source is not None:
        result.rejected = await writer.reject_photometry(
            session,
            actions.source.id,
            "Counterpart retracted by " + ", ".join(f"GCN {c}" for c in withdrawn) + ".",
        )

    if writes.get("source", True):
        await writer.write_source(session, actions.source, group_ids)
        result.photometry = await writer.write_photometry(session, actions.photometry, group_ids)
        if actions.redshift is not None:
            z, z_err = actions.redshift
            await writer.set_redshift(session, actions.source.id, z, z_err)

    if writes.get("extraction", False):
        # Needs skyportal with the gcneventextractions table; a deployment
        # without it should leave this off rather than fail every circular.
        try:
            published = {r.get("circularId"): _circular_datetime(r) for r in records}
            subjects = {r.get("circularId"): (r.get("subject") or "").strip() for r in records}
            for extraction in actions.extractions:
                data = extraction.model_dump(mode="json")
                # The circular's title, alongside the extracted values: it is
                # what describes a circular that yielded no measurements, and
                # the schema has no field for it.
                if subject := subjects.get(extraction.circular_id):
                    data["subject"] = subject
                await writer.write_extraction(
                    session,
                    match.dateobs,
                    extraction.circular_id,
                    data,
                    published.get(extraction.circular_id),
                )
        except Exception as exc:
            log.warning("could not store the extraction: %s", exc)

    await gcn_event.write_event_bindings(
        session,
        writer,
        match,
        names=names,
        obj_id=actions.source.id,
        comment=build_comment(actions, records),
        detection_window=detection_window(actions.photometry),
        position=_position(actions),
        localization_cumprob=float(rcfg.get("localization_cumprob") or 0.95),
        writes=writes,
    )
    result.status = "posted"
    return result


def _position(actions: Any) -> tuple[float, float, Any] | None:
    """(ra, dec, ra_dec_error) for a synthesized localization.

    Read from the extraction rather than the source: an error region localizes
    the event but is deliberately not written as a source, and that is precisely
    the case a synthesized skymap exists for.
    """
    for extraction in actions.extractions:
        localization = getattr(extraction, "localization", None)
        if localization is None or localization.ra is None or localization.dec is None:
            continue
        return localization.ra, localization.dec, localization.ra_dec_error
    if actions.source is not None and actions.source.ra is not None:
        return actions.source.ra, actions.source.dec, None
    return None


def _circular_datetime(record: dict[str, Any]) -> datetime | None:
    """When GCN published this circular, from the feed's millisecond timestamp."""
    raw = record.get("createdOn")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw) / 1000, UTC).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def _trigger_time(records: list[dict[str, Any]]) -> datetime | None:
    """Earliest circular timestamp in the event, as a proxy for the trigger."""
    stamps = []
    for record in records:
        raw = record.get("createdOn") or record.get("trigger_time")
        if raw is None:
            continue
        try:
            when = (
                datetime.fromtimestamp(raw / 1000, tz=UTC)
                if isinstance(raw, int | float)
                else datetime.fromisoformat(str(raw))
            )
        except (ValueError, OSError, OverflowError):
            continue
        stamps.append(when if when.tzinfo else when.replace(tzinfo=UTC))
    return min(stamps) if stamps else None
