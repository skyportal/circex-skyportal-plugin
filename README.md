# circex-skyportal-plugin

A SkyPortal service that turns incoming GCN circulars into structured data and
attaches it to the GcnEvent it belongs to. It wraps
[Circex](https://github.com/skyportal/Circex), which does the extraction, and is
responsible for the SkyPortal half: working out which event a circular is about,
and writing the result onto that event.

It runs inside SkyPortal and writes through SkyPortal's own model functions —
`post_source_async` and `add_external_photometry` — so there is no HTTP surface
and no API token. It exists as a plugin rather than a core service only because
the extraction stack is a heavy dependency most deployments don't want.

```
GCN circular  ──►  circex extract  ──►  resolve GcnEvent  ──►  database
(Kafka / replay)   (regex + LLM)        (alias/designation/    source, photometry,
                                         trigger time)         alias, confirmation,
                                                               comment
```

## What gets written

| From the extraction | Where it goes |
|---|---|
| event name (`GRB 260604C`) | `GcnEvent.aliases` |
| counterpart name + position | a source, via `post_source_async` |
| photometry rows | `add_external_photometry`, origin `circex` |
| redshift | `Obj.redshift` |
| the counterpart | `SourcesConfirmedInGCN` |
| classification, notes, provenance | a comment on the event |

Each channel is switched independently in `writes:`, all behind the
`writes.live` master switch.

Photometry is not deduplicated by this service. SkyPortal has a unique index
over `(obj_id, instrument_id, origin, mjd, fluxerr, flux)` and
`add_external_photometry` resolves collisions against it, so re-seeing an event
is idempotent and survives restarts.

## Resolving the event

The hard part. A circular says "GRB 260604C"; SkyPortal keys events by
`dateobs`. Three rungs are tried in order (`resolver.order`):

1. **alias** — substring match against `GcnEvent.aliases`, ignoring case and
   spaces, so the circular's `GRB 260604C` finds a notice's `GRB260604C`, and
   `S260604a` finds `LVC#S260604a`.
2. **designation** — GRB/GW/EP/SVOM/IC names encode their own UTC date, so
   `GRB 260604C` becomes 2026-06-04 and that day is searched.
3. **trigger** — the circular's own trigger time, ± `window_hours`.

Where a window holds several events, the one nearest the trigger time wins.

Rung 1 is what the alias write feeds: the first circular of an event usually
resolves by designation, and every later one by alias.

A circular whose event resolves to nothing is **parked, not dropped** —
circulars routinely beat the notice that creates the event. The retry queue
re-tries them until `retry_max_age_hours`.

## Setup

The service is configured under `services.external.circex.params`. Set at least:

- `skyportal.user_id` — writes are attributed to this user.
- `skyportal.default_instrument_id` — SkyPortal requires an `instrument_id` on
  every photometry point; rows resolving to neither a mapped instrument nor this
  default cannot be posted.
- `skyportal.group_ids` — where new counterpart sources are saved.

`writes.live: false` (the default) plans every write, logs it, and commits
nothing.

Replay a directory of `{id}.json` circulars through the whole pipeline:

```bash
python main.py --replay path/to/circulars/
```

For the live stream, set `consumer.enabled` with GCN credentials from
<https://gcn.nasa.gov/quickstart>, and install the optional dependency
(`pip install -e .[live]`). `consumer.group_id` must be stable across restarts
and distinct from any other consumer of the topic — that is what makes the
service resume where it stopped rather than skipping the backlog.

## The LLM backend

`extractor.kind` selects the engine:

- `regex` — Circex's regex baseline. No model server, works offline.
- `llama` — Mistral-7B on a llama.cpp server with grammar-constrained decoding.
- `hybrid` (default) — Circex's measured routing: regex for event names and
  coordinates, the constrained LLM for photometry, redshift and classification.

`llama_url` points at a llama.cpp OpenAI-compatible server; `llama_api_key` is
sent as a bearer token when the server is behind one. Set both per deployment.

`llama_require_fields` is **model-specific and must match `llama_model`**:

| model | `require_fields` | if mispaired |
|---|---|---|
| Mistral-7B | `false` | pads the photometry array with fabricated rows |
| Qwen3-27B | `true` | returns `{}` for every circular |

## Development

```bash
uv sync
PYTHONPATH=. uv run pytest
uv run ruff check . && uv run ruff format --check .
```

These tests cover the pure logic — designation decoding, alias matching, the
write plan. Anything touching the database is exercised by the in-container
integration tests in fritz, under
`extensions/skyportal/skyportal/tests/api/circex/`.

### circex version

`circex` is pinned from git until the next PyPI release:

```toml
"circex @ git+https://github.com/skyportal/Circex.git@main"
```
