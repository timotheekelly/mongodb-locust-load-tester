# MongoDB Atlas Tier Benchmarking Suite

Locust-based benchmarking suite comparing MongoDB Atlas cluster tiers (M0,
Flex, M10, M30) on real test-bench metrics: latency, throughput, CPU/RAM
saturation, and IOPS. Built on `locust` + `locust.contrib.mongodb.MongoDBUser`
(no other load-testing tools). Out of scope for this phase: cost/pricing
modeling, qualitative features (auto-scaling, backups, Performance Advisor).

## Layout

```
config/               base defaults + one yaml per workload profile
tiers.yaml            {tier_label, mongo_uri_env, cluster_name_env} list (env var names, not literal secrets)
benchmarks/
  docgen.py           shared nested-document generator (BSON-measured, size-padded)
  config.py           base + profile + env var config loader
  seed.py             pre-seeds a collection to a target data volume
  locustfile.py       MongoDBUser subclass, 6 weighted tasks
  metrics_atlas.py     Atlas Admin API CPU/RAM/IOPS/connections pull
  orchestrator.py      reseed + run + capture metrics, per tier or all tiers
  report.py            per-profile charts/report + cross-workload view
results/<profile>/<volume>/<tier>/   locust CSVs + atlas_metrics.json per run
reports/                             generated charts + markdown reports
```

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in real values (`.env` is gitignored,
never commit it). Nothing in this repo auto-loads `.env` — source it into
your shell before running anything:

```bash
cp .env.example .env   # then edit .env with real values
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

`certifi` is already a listed dependency, so `pip install -r requirements.txt`
covers it — this just points Python/OpenSSL at that bundle. Every command
below assumes both `.env` is sourced and `SSL_CERT_FILE` is set in the same
shell.

[tiers.yaml](tiers.yaml) doesn't hold secrets directly — each tier entry
names an env var to read the real connection string from (`mongo_uri_env`),
and optionally an env var for its Atlas cluster name (`cluster_name_env`).
`tier_label` must be one of `M0`, `FLEX`, `M10`, `M30` (matches
`tier_testing_limits` in [config/base.yaml](config/base.yaml), which drives
the storage-cap safety check and per-tier default concurrency). Set the env
vars it points to before running, e.g. for M0:

```bash
export M0_MONGO_URI="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
```

For server-side metrics (CPU/RAM/IOPS/connections), also set the Atlas
project id env var named by the top-level `atlas.project_id_env` in
`tiers.yaml`, each tier's cluster name env var, and Atlas Admin API
credentials:

```bash
export ATLAS_PROJECT_ID=...
export M0_ATLAS_CLUSTER_NAME=Cluster0
export ATLAS_PUBLIC_KEY=...
export ATLAS_PRIVATE_KEY=...
```

Without these, metrics collection is skipped with a warning and the report
notes "metrics unavailable" for that tier rather than failing the run.

## Configuration

- `config/base.yaml` — shared defaults: doc size (`doc_size_bytes`), nesting
  depth/width (capped by `max_nesting_fanout` so a bad value can't hang
  document generation), target working-set volume, task weights, tier
  storage/connection limits (`tier_testing_limits`), and volume presets
  (`tiny`=256MiB, `small`=2GB, `medium`=20GB, `large`=500GB).
- `config/profile_*.yaml` — one file per workload profile, overriding weights
  (and, for `cpu_intensive`, a smaller `doc_size_bytes` since that profile
  stresses the query engine, not payload size).
- Any top-level key can be overridden via `BENCH_<UPPER_SNAKE_KEY>` env vars,
  e.g. `BENCH_DOC_SIZE_BYTES=5000`, `BENCH_MONGO_URI=...`.

## Workload profiles

1. **read_heavy** — ~90%+ reads (point lookups + range queries).
2. **balanced** — even split between reads and writes.
3. **cpu_intensive** — heavily weighted toward the aggregation task
   (`$match` + `$group` + `$sort`), isolating CPU/vCPU-share differences.

## First-run walkthrough

**1. Seed one tier manually** (useful before committing to a full multi-tier
run):

```bash
.venv/bin/python -m benchmarks.seed --profile read_heavy --volume small \
    --mongo-uri "mongodb+srv://user:pass@m10-cluster.mongodb.net/..." \
    --tier-label M10
```

This drops and recreates the collection, then inserts documents until the
target volume is reached (document count is derived from
`target_volume / doc_size_bytes`, not configured directly). **500GB seeding
is a real time/storage commitment** — the script prints an estimate and a
warning before starting; it also refuses upfront if the target volume
exceeds a tier's storage cap (e.g. M0's ~512MB) rather than failing partway
through.

**2. Run the workload against that one tier**, either directly via Locust:

```bash
BENCH_PROFILE=read_heavy BENCH_MONGO_URI="mongodb+srv://...m10-cluster..." \
    .venv/bin/python -m locust -f benchmarks/locustfile.py --headless \
    -u 20 -r 5 -t 5m --csv results/read_heavy/small/M10/locust --csv-full-history
```

or through the orchestrator, which also reseeds and captures Atlas metrics
for you:

```bash
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy --volume small --tier M10
```

**3. Run all tiers** (sequentially — tiers never run concurrently from one
machine, so they don't compete for local CPU as load generator; to
parallelize, run this script from separate load-generation hosts per tier):

```bash
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy --volume small
```

Each tier's Locust CSVs, `atlas_metrics.json`, and `run_metadata.json` land
in `results/read_heavy/small/<TIER>/`.

**4. Generate reports**:

```bash
.venv/bin/python -m benchmarks.report --profile read_heavy --volume small
.venv/bin/python -m benchmarks.report --cross-workload --volume small
```

The first produces `reports/read_heavy/small_report.md` (latency/throughput/
resource charts + a data table) for that one profile across tiers. The
second produces `reports/cross_workload/small_cross_workload_report.md`
comparing all three profiles side by side, per tier.

## Notes

- `docgen.py` is the single source of document shape/size logic, shared by
  seeding and the live workload, so seeded and freshly-inserted documents
  are identical in shape.
- All six tasks in `locustfile.py` fire Locust's `request` event manually
  (none rely on `MongoDBUser`'s own built-in timing). This is deliberate:
  upstream's `MongoDBClient.__init__` does `self.db = self.client[db_name]`,
  where `self.client` is *attribute* access on a `MongoClient` — which
  PyMongo defines as shorthand for `self["client"]` (a database literally
  named `client`), not "myself". That silently sends all traffic through
  `execute_query`/`self.db[...]` into a bogus `client` database instead of
  the real one. `BenchUser` avoids this entirely by caching
  `self.client[self.db_name][self.collection_name]` (subscript access, which
  is unambiguous) in `on_start` and using that everywhere. If you ever see a
  stray `client` database on a cluster you've tested against, that's this
  bug from before the fix — drop it, it's not real data.
- Per-tier concurrency defaults come from
  `tier_testing_limits.<TIER>.default_users` in `config/base.yaml`
  (conservative for M0/Flex) and can be overridden with
  `--users`/`--spawn-rate`/`--run-time` on the orchestrator.

## Running a single tier (e.g. M0)

M0's storage cap is ~512MiB, so use the `tiny` (256MiB) preset rather than
`small`/`medium`/`large`:

```bash
export M0_MONGO_URI="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority"
.venv/bin/python -m benchmarks.orchestrator --profile read_heavy --volume tiny --tier M0
```

This reseeds just that tier's collection, runs the Locust workload against
it with M0's conservative default concurrency, and writes results to
`results/read_heavy/tiny/M0/` — the other tiers in `tiers.yaml` are left
untouched. Swap `--profile` for `balanced` or `cpu_intensive` to run the
other workload profiles the same way. Add `--run-time 30s` for a quick
connectivity check before committing to a full-length run.
