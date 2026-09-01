"""Probe the llama.cpp server the extractor talks to.

Three rungs, each a strictly harder ask than the last, so a failure localizes:

  1. /v1/models      — is anything listening (is the tunnel actually up)?
  2. a tiny fixed schema — does grammar-constrained decoding work at all? This is
     the reference call shape MSI was set up against.
  3. a real circular through Circex's LlamaServerExtractor — does the full
     CircularExtraction grammar decode in a workable time?

The servers run on the MSI compute node agc03 — 8080, and a second one on 8081.
From a laptop that means the UMN VPN plus a tunnel through the mangi login node:

    ./bin/tunnel.sh

Usage:
    uv run python bin/check_llm.py                      # probe 8080 and 8081
    uv run python bin/check_llm.py --url http://localhost:8081
    uv run python bin/check_llm.py --circular tests/fixtures/flurry/44834.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

# Deliberately tiny: rung 2 is testing the decoder, not the schema.
PROBE_SCHEMA = {
    "type": "object",
    "properties": {
        "source_name": {"type": "string"},
        "instrument": {"type": "string"},
        "detection_time_utc": {"type": "string"},
    },
    "required": ["source_name", "instrument", "detection_time_utc"],
}

PROBE_TEXT = """
Y.-H. I. Yin (HKU), R. Shi (PMO, CAS) on behalf of the Einstein Probe (EP) team:
The EP-WXT trigger 01709274666 at the time of 2026-07-10T18:07:01, is likely a
stellar flare associated with SS Cyg (Cataclysmic Binary).
"""


def rung_1_reachable(url: str, timeout: float) -> bool:
    try:
        resp = requests.get(f"{url}/v1/models", timeout=timeout)
    except requests.RequestException as exc:
        print(f"  FAIL  cannot reach {url}: {exc}")
        print("        Is the UMN VPN up and the tunnel running? See ./bin/tunnel.sh")
        return False
    if resp.status_code != 200:
        print(f"  FAIL  {url}/v1/models returned HTTP {resp.status_code}")
        return False
    models = [m.get("id") for m in resp.json().get("data", [])]
    print(f"  OK    reachable; models: {models or '(none advertised)'}")
    return True


def rung_2_constrained(url: str, timeout: float) -> bool:
    started = time.perf_counter()
    try:
        resp = requests.post(
            f"{url}/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "system",
                        "content": "You extract parameters from astronomy alert text. "
                        "Report values exactly as given; do not invent any.",
                    },
                    {"role": "user", "content": PROBE_TEXT},
                ],
                "temperature": 0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {"name": "probe", "schema": PROBE_SCHEMA},
                },
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = json.loads(resp.json()["choices"][0]["message"]["content"])
    except (requests.RequestException, KeyError, json.JSONDecodeError) as exc:
        print(f"  FAIL  constrained decode failed: {exc}")
        return False
    elapsed = time.perf_counter() - started
    missing = [k for k in PROBE_SCHEMA["required"] if k not in payload]
    if missing:
        print(f"  FAIL  response missing required keys {missing}: {payload}")
        return False
    print(f"  OK    constrained decode in {elapsed:.1f}s -> {json.dumps(payload)}")
    return True


def rung_3_extraction(url: str, model: str, circular: Path | None, timeout: float) -> bool:
    if circular is None:
        print("  SKIP  no --circular given")
        return True
    from circex.extract.llm.llama_server import LlamaServerExtractor
    from circex.extract.protocol import Circular

    record = json.loads(circular.read_text())
    started = time.perf_counter()
    try:
        extraction = LlamaServerExtractor(base_url=url, model_id=model, timeout=timeout).extract(
            Circular(
                circular_id=int(record.get("circularId") or 0),
                subject=str(record.get("subject") or ""),
                body=str(record.get("body") or ""),
            )
        )
    except Exception as exc:
        print(f"  FAIL  extraction raised: {exc}")
        return False
    elapsed = time.perf_counter() - started
    event = extraction.event.event_name if extraction.event else None
    summary = (
        f"circular {extraction.circular_id} in {elapsed:.1f}s: event={event} "
        f"photometry={len(extraction.photometry)} "
        f"redshift={extraction.redshift.redshift if extraction.redshift else None}"
    )
    # LlamaServerExtractor fail-softs: a bad response is logged and yields an
    # empty extraction rather than raising. An empty result on a circular this
    # dense means the server did not really answer, so treat it as a failure.
    if event is None and not extraction.photometry:
        print(f"  FAIL  extracted nothing from {summary}")
        print("        The server returned no usable JSON — see the warning above.")
        return False
    print(f"  OK    extracted {summary}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        action="append",
        help="server to probe; repeatable. Default: localhost 8080 and 8081.",
    )
    parser.add_argument("--model", default="mistral-7b")
    parser.add_argument("--circular", type=Path, help="a {id}.json circular for the full test")
    parser.add_argument("--timeout", type=float, default=300.0)
    args = parser.parse_args()
    urls = args.url or ["http://localhost:8080", "http://localhost:8081"]

    reachable = []
    for url in (u.rstrip("/") for u in urls):
        print(f"\n=== {url}")
        print("1. reachable")
        if not rung_1_reachable(url, min(args.timeout, 10.0)):
            continue
        print("2. constrained decode")
        if not rung_2_constrained(url, args.timeout):
            continue
        print("3. circex extraction")
        if not rung_3_extraction(url, args.model, args.circular, args.timeout):
            continue
        reachable.append(url)

    print(f"\n{len(reachable)}/{len(urls)} server(s) passed every rung")
    for url in reachable:
        print(f"  usable: {url}")
    # Probing every server is the point, so one bad server is not a hard failure
    # as long as something works.
    return 0 if reachable else 1


if __name__ == "__main__":
    sys.exit(main())
