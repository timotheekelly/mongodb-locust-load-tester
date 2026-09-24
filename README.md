# MongoDB Atlas Tier Benchmarking Suite

A [Locust](https://locust.io/)-based benchmarking suite for comparing MongoDB
Atlas cluster tiers (M0, Flex, M10, M30) on real test-bench metrics: latency,
throughput, and (when Atlas API credentials are configured) server-side
CPU/RAM/IOPS. Built on `locust` + `locust.contrib.mongodb.MongoDBUser` — no
other load-testing tools. Out of scope: cost/pricing modeling, qualitative
features (auto-scaling, backups, Performance Advisor).

There are two ways to use this repo:

1. **Just run a load test** — `benchmarks/locustfile.py` is a standalone
   Locust test. Point it at any MongoDB connection string and run it. No
   setup beyond that.
2. **Compare tiers systematically** — `benchmarks/orchestrator.py` seeds an
   identical target data volume into each tier, runs one or all three
   workload profiles against it, and prints a results summary at the end.
   This is what you want for "run all three workloads against all my
   tiers and show me the numbers."

Both use the same underlying document generator and Locust tasks, so the
numbers are directly comparable either way.

## Layout

```
config/
  base.yaml                    shared defaults (doc size, weights, tier limits, volume presets)
  profile_read_heavy.yaml      profile-specific overrides
  profile_balanced.yaml
  profile_cpu_intensive.yaml
tiers.yaml                     which tiers exist and which env vars hold their connection strings
.env.example                   template for the env vars tiers.yaml points to (copy to .env)
benchmarks/
  docgen.py                    shared nested-document generator (BSON-measured, size-padded)
  config.py                    base + profile + env var config loader
  locustfile.py                the actual Locust test (standalone, self-seeding)
  seed.py                      optional: pre-seed a collection to an exact target data volume
  orchestrator.py              optional: seed + run + summarize across profiles/tiers
  metrics_atlas.py             optional: pulls CPU/RAM/IOPS from the Atlas Admin API
  report.py                    optional: turns results/ into charts + a markdown report
results/<profile>/<volume>/<tier>/   Locust CSVs + atlas_metrics.json + run_metadata.json per run
reports/                             generated charts + markdown reports (optional, from report.py)
```

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

**Add your connection strings.** Copy `.env.example` to `.env` and fill in
real values — `.env` is gitignored, never commit it:

```bash
cp .env.example .env   # then edit .env with real values
```

`.env` holds the actual secrets (Mongo URIs, Atlas API credentials);
`tiers.yaml` only holds the *names* of the env vars to read them from, e.g.:

```yaml
tiers:
  - tier_label: M0
    mongo_uri_env: M0_MONGO_URI          # -> reads .env's M0_MONGO_URI
    cluster_name_env: M0_ATLAS_CLUSTER_NAME
```

To add a new tier or change a connection string: edit the matching line in
`.env` (or add a new `tier_label`/`mongo_uri_env` pair to `tiers.yaml` if
it's a genuinely new tier, e.g. a second M10 cluster). Nothing else needs
to change.

Nothing in this repo auto-loads `.env` — source it into your shell before
running anything, every session:

```bash
set -a; source .env; set +a
```

**macOS SSL certificate error**: if you see
`[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get
local issuer certificate` when connecting to Atlas, your Python install
can't find a CA bundle. Fix (one-time per shell session, or add to your
shell profile):

```bash
export SSL_CERT_FILE=$(.venv/bin/python -c "import certifi; print(certifi.where())")
```

`certifi` is already in `requirements.txt`, so this just points
Python/OpenSSL at the bundle it installed. Every command below assumes
both `.env` is sourced and `SSL_CERT_FILE` is set in the same shell.

## Option 1: just run a load test

```bash
BENCH_MONGO_URI="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority" \
    .venv/bin/python -m locust -f benchmarks/locustfile.py
```

Open [http://localhost:8089](http://localhost:8089) for Locust's web UI —
pick a user count and spawn rate, hit Start, watch requests/sec and latency
live. On first use it creates the collection/index if they don't exist and
builds its own working set as it inserts (reads/updates pick random keys
from whatever's been inserted so far) — nothing to seed first.

To use one of the three named workload profiles (weights tuned for a
specific bottleneck — see below) instead of the flat default weights, set
`BENCH_PROFILE`:

```bash
BENCH_PROFILE=read_heavy BENCH_MONGO_URI="mongodb+srv://..." \
    .venv/bin/python -m locust -f benchmarks/locustfile.py
```

Or run it headless (no web UI), same as any other Locust test:

```bash
BENCH_PROFILE=balanced BENCH_MONGO_URI="mongodb+srv://..." \
    .venv/bin/python -m locust -f benchmarks/locustfile.py --headless -u 10 -r 5 -t 5m
```

## Option 2: compare tiers with the orchestrator

This is the tool for "run [some/all] workloads against [some/all] of my
tiers and give me the results at the end." One command:

```bash
# everything: all 3 profiles x all tiers in tiers.yaml, sequentially
.venv/bin/python -m benchmarks.orchestrator --volume small
```

For each (profile, tier) pair, in turn, it: reseeds that tier's collection
to the target volume, runs the Locust workload headless, pulls Atlas
metrics for that exact time window (if configured), then moves to the
next combination. Sequentially, not concurrently — so tiers don't compete
for your machine's CPU as load generator; run this script from separate
load-generation hosts per tier if you want to parallelize.

At the end it prints a summary table across every run:

```
====================================================================================================
SUMMARY -- all runs
====================================================================================================
Profile         Tier     Requests    Fails     p50     p95     p99     req/s  Atlas metrics
----------------------------------------------------------------------------------------------------
read_heavy      M0          15747        0      79     220     350      52.5             no
balanced        M0          13048        0      64     340     580      43.5             no
cpu_intensive   M0           5531        0     100     280    4300      18.5             no
====================================================================================================
```

You can narrow it down in either dimension:

```bash
# one profile, all tiers
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy --volume small

# one profile, one tier
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy --volume small --tier M10

# a subset of profiles, one tier
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy,balanced --volume tiny --tier M0

# every profile, one tier
.venv/bin/python -m benchmarks.orchestrator --volume tiny --tier M0
```

Other flags: `--users`, `--spawn-rate`, `--run-time` override Locust's
defaults for every run in the batch; `--tiers-file` points at a different
tiers config if you keep more than one.

**M0's storage cap (~512MiB) means you must use `--volume tiny` for it** —
`small`/`medium`/`large` will be refused outright (with a clear message)
rather than failing partway through. This is enforced automatically per
tier via `tier_testing_limits` in `config/base.yaml`; `small` and above are
expected to work fine on M10/M30/Flex.

Every run's full Locust CSVs, `atlas_metrics.json` (or a clear
"unavailable" reason), and `run_metadata.json` land in
`results/<profile>/<volume>/<tier>/` — the summary table is just a quick
look, not the only record.

### Optional: turn results into charts + a report

```bash
.venv/bin/python -m benchmarks.report --profile read_heavy --volume small
.venv/bin/python -m benchmarks.report --cross-workload --volume small
```

The first produces `reports/read_heavy/small_report.md` (latency/
throughput/resource charts + a data table) for that one profile across
tiers. The second produces
`reports/cross_workload/small_cross_workload_report.md` comparing all
three profiles side by side, per tier.

## Workload profiles

Three fixed load shapes, each isolating a different bottleneck — not
persona/scenario storytelling, just weight changes on the same six Locust
tasks:

1. **read_heavy** — ~90%+ reads (point lookups + range queries), minimal
   writes. Likely where shared-CPU tiers (M0/Flex) hold up longest.
2. **balanced** — an even, OLTP-ish split between reads and writes.
3. **cpu_intensive** — heavily weighted toward the aggregation task
   (`$match` + `$group` + `$sort`), to isolate CPU/vCPU-share differences
   rather than I/O. Uses a smaller default document size than the other
   two, since this profile stresses the query engine, not payload size.

The six underlying tasks (all weighted, all configurable in
`config/base.yaml`): single-document insert, point lookup by key, sorted
range scan, single-document update, small batch bulk insert, and a
multi-stage aggregation pipeline.

## Configuration

- `config/base.yaml` — shared defaults: doc size (`doc_size_bytes`, BSON-
  measured and padded to hit the target exactly), nesting depth/width for
  the generated documents (capped by `max_nesting_fanout` so a bad value
  can't hang document generation), task weights, tier storage/connection
  limits (`tier_testing_limits`), and volume presets (`tiny`=256MiB,
  `small`=2GiB, `medium`=20GiB, `large`=500GiB).
- `config/profile_*.yaml` — one file per workload profile, overriding
  weights (and, for `cpu_intensive`, doc size).
- Any top-level key can be overridden via `BENCH_<UPPER_SNAKE_KEY>` env
  vars, e.g. `BENCH_DOC_SIZE_BYTES=5000`, `BENCH_MONGO_URI=...`.

Documents are nested (a few top-level scalar fields plus a recursive
subdocument structure), not flat — see `docgen.py`. The same generator
function is used everywhere (standalone runs, `seed.py`, and the
orchestrator) so documents are identical in shape no matter which path
inserted them.

## The optional pre-seeding workflow (`seed.py`)

The orchestrator calls this for you, but you can also run it directly if
you just want a specific tier seeded to a specific volume without running
a full Locust test yet:

```bash
.venv/bin/python -m benchmarks.seed --profile read_heavy --volume small \
    --mongo-uri "mongodb+srv://user:pass@m10-cluster.mongodb.net/..." \
    --tier-label M10
```

It drops and recreates the collection, then inserts documents until the
target volume is reached — document *count* is derived internally from
`target_volume / doc_size_bytes`, since a fixed count would produce very
different data volumes depending on doc size, whereas a fixed target
volume stays meaningful regardless. **500GB seeding is a real time/storage
commitment** — it prints an estimate and a warning before starting, and
refuses upfront if the target volume exceeds the given tier's storage cap
(via `--tier-label`) rather than failing confusingly partway through.

## Notes on things that weren't obvious while building this

- All six tasks in `locustfile.py` fire Locust's `request` event manually
  (none rely on `MongoDBUser`'s own built-in timing). This is deliberate:
  upstream's `MongoDBClient.__init__` does `self.db = self.client[db_name]`,
  where `self.client` is *attribute* access on a `MongoClient` — which
  PyMongo defines as shorthand for `self["client"]` (a database literally
  named `client`), not "myself". That silently sends all traffic through
  `execute_query`/`self.db[...]` into a bogus `client` database instead of
  the real one. `BenchUser` avoids this entirely by caching
  `self.client[self.db_name][self.collection_name]` (subscript access,
  which is unambiguous) in `on_start` and using that everywhere. If you
  ever see a stray `client` database on a cluster you've tested against,
  that's this bug from before the fix — drop it, it's not real data.
- `BenchUser`'s working set grows via a shared `seq` counter (`_doc_count`)
  rather than a fixed pre-seeded range, so a fresh/empty collection works
  out of the box. The one-time index-creation/count check at startup is
  guarded by a `threading.Lock` (cooperative under Locust's gevent
  runtime) — without it, multiple simulated users starting at once would
  each read a stale document count and reset the shared counter after
  others had already started incrementing it, causing duplicate-key
  errors on insert.
- Per-tier concurrency defaults come from
  `tier_testing_limits.<TIER>.default_users` in `config/base.yaml`
  (conservative for M0/Flex) and can be overridden with
  `--users`/`--spawn-rate`/`--run-time` on the orchestrator.
- `bulk_insert_batch_size` is deliberately small (20 docs/call at the
  default 10KiB doc size, ~200KiB/call). A larger batch size was enough to
  blow through M0's 512MiB storage cap within a few minutes of a
  write-heavy run on top of even a small seed.
- Atlas server-side metrics (CPU/RAM/IOPS) require an Atlas API key pair
  (`ATLAS_PUBLIC_KEY`/`ATLAS_PRIVATE_KEY` in `.env`) plus each tier's
  `atlas_project_id`/cluster name. Without these, metrics collection is
  skipped with a clear warning rather than failing the run, and the
  summary/report just show "unavailable" for that tier.
