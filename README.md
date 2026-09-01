# circex-skyportal-plugin

A SkyPortal service that turns incoming GCN circulars into structured data and
attaches it **to the GcnEvent it belongs to**. It wraps
[Circex](https://github.com/skyportal/Circex), which does the extraction, and is
responsible for the SkyPortal half: working out which event a circular is about,
and writing the result onto that event.

This is not an AnalysisService. Nothing here waits for a user to press "Run
analysis" — circulars stream in and events fill themselves out.

```
GCN circular  ──►  circex extract  ──►  resolve GcnEvent  ──►  SkyPortal writes
(Kafka / replay)   (regex + LLM)        (alias/designation/    source, photometry,
                                         trigger time)         alias, confirmation,
                                                               comment, tag
```

## What gets written

For each circular, once its event resolves:

| From the extraction | SkyPortal write |
|---|---|
| event name (`GRB 260604C`) | `POST /gcn_event/{dateobs}/alias` |
| counterpart name + position | `POST /sources` |
| photometry rows | `POST /photometry` (stamped with `gcn_dateobs`) |
| redshift | `PATCH /sources/{obj_id}` |
| the counterpart | `POST /sources_in_gcn/{dateobs}` — confirmed in the event |
| classification, notes, provenance | `POST /gcn_event/{dateobs}/comments` |
| an optical counterpart exists | `POST /gcn_event/{dateobs}/tags` |

Each channel is switched independently in `writes:`, and all of them are behind
the `writes.live` master switch.

## Resolving the event

The hard part. A circular says "GRB 260604C"; SkyPortal keys events by
`dateobs`. Three rungs are tried in order (`resolver.order`):

1. **alias** — `GET /gcn_event?partialdateobs=<name>`, which matches a `dateobs`
   prefix *or* a substring of the event's aliases. Both spellings are tried, so
   the circular's `GRB 260604C` finds a notice's `GRB260604C`, and `S260604a`
   finds `LVC#S260604a`.
2. **designation** — GRB/GW/EP/SVOM names encode their own UTC date, so
   `GRB 260604C` becomes 2026-06-04 and that day is searched.
3. **trigger** — the circular's own trigger time, ± `window_hours`.

Where a window holds several events, the one nearest the trigger time wins.

Rung 1 is what the alias write feeds: the first circular of an event usually
resolves by designation, and every later one resolves by alias.

A circular whose event resolves to nothing is **parked, not dropped** — circulars
routinely beat the notice that creates the event. The retry queue re-tries them
every `resolver.retry_interval_seconds` until `retry_max_age_hours`.

## Setup

```bash
uv sync
cp config.yaml.defaults config.yaml     # then edit, see below
uv run python main.py --config config.yaml
```

Set at least:

- `skyportal.base_url` / `skyportal.api_token` — the token needs GCN and source
  write ACLs.
- `skyportal.default_instrument_id` — SkyPortal **requires** an `instrument_id`
  on every photometry point. Rows that resolve to neither a mapped instrument
  nor this default cannot be posted and become a comment instead.
- `skyportal.group_ids` — where new counterpart sources are saved.

### Dry run

`writes.live: false` (the default) plans and logs every request and sends
nothing. Note that a dry run **still needs a working API token**: resolving an
event is a read, and without one nothing resolves and every circular parks.

Replay a directory of `{id}.json` circulars through the whole pipeline:

```bash
uv run python main.py --config config.yaml --replay tests/fixtures/flurry
```

### Live stream

```yaml
consumer:
  enabled: true
  client_id: ...        # https://gcn.nasa.gov/quickstart
  client_secret: ...
```

Needs the optional dependency: `uv sync --extra live`.

## The LLM backend

`extractor.kind` selects the engine:

- `regex` — Circex's regex baseline. No model server, works offline.
- `llama` — Mistral-7B on a llama.cpp server with grammar-constrained decoding.
- `hybrid` (default) — Circex's measured production routing: regex for event
  names and coordinates, the constrained LLM for photometry, redshift and
  classification.

The servers run on the MSI compute node `agc03`: one on **8080**, a second on
**8081**. `agc03` is only reachable through the `mangi` login node, so from a
laptop it takes the UMN VPN plus a two-hop tunnel:

```bash
./bin/tunnel.sh              # forwards 8080 and 8081, via mangi, from agc03
./bin/tunnel.sh 8081         # just one
```

Equivalently, by hand:

```bash
ssh -N -L 8080:localhost:8080 -L 8081:localhost:8081 \
    -J cough052@mangi.msi.umn.edu cough052@agc03
```

The `-J` (jump) form lands *on* agc03 and forwards from there, so it works
whether llama-server binds to `0.0.0.0` or only to `localhost`. Override the
account and node with `MSI_USER` / `MSI_NODE`.

Check the servers before running anything real:

```bash
uv run python bin/check_llm.py --circular tests/fixtures/flurry/44834.json
```

It probes each server on three rungs of increasing difficulty — is anything
listening, does grammar-constrained decoding work, does a full
`CircularExtraction` decode — so a failure tells you which layer broke.

When the service runs on MSI itself, no tunnel is needed at all.

## HTTP surface

Small and operational, not a webhook contract:

- `GET /health` — extractor id, live/dry, queue depths
- `GET /results` — the last 100 processed circulars
- `POST /circular/<id>` — push one circular through by hand; returns the result
  and, in dry-run, the exact writes it would have made

All require `Authorization: Bearer <auth.incoming_bearer_token>` when one is set.

## Development

```bash
uv sync
PYTHONPATH=. uv run pytest        # tests
uv run ruff check . && uv run ruff format --check .
```

### circex version

The `hybrid` and `llama` extractors, and `SkyPortalActions.extractions` (which
this plugin needs to read event names without extracting twice), landed after
circex 0.1.0 and are **not yet on PyPI**. The dependency therefore tracks the
repo:

```toml
"circex @ git+https://github.com/skyportal/Circex.git@main"
```

Pin that to a tag once circex cuts a release. To work against a local checkout
instead:

```bash
uv pip install -e ../Circex --no-deps
PYTHONPATH=. .venv/bin/pytest     # note: `uv run` re-syncs and undoes the above
```

Tests that need the newer circex skip themselves rather than fail, so the suite
stays green either way.
