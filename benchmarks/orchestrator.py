"""Multi-tier, multi-profile orchestration: reseed + run the Locust workload
+ capture Atlas metrics, for one profile or all three, against one tier or
every tier in tiers.yaml, sequentially -- then print a consolidated summary
table of every run's results.

Tiers (and profiles) are run sequentially by design -- not concurrently --
so they don't compete for the local machine's CPU as load generator. To
parallelize across tiers, run this script from separate load-generation
hosts per tier instead.

Usage:
    # everything: all 3 profiles x all tiers in tiers.yaml, sequentially
    python -m benchmarks.orchestrator --volume small

    # one profile, all tiers
    python -m benchmarks.orchestrator --profile read_heavy --volume small

    # one profile, one tier
    python -m benchmarks.orchestrator --profile read_heavy --volume small --tier M10

    # a subset of profiles, one tier
    python -m benchmarks.orchestrator --profile read_heavy,balanced --volume tiny --tier M0

    # every tier (M0 -> FLEX -> M10 -> M30, in tiers.yaml order), every profile,
    # generating load from 4 local worker processes, then writing reports
    python -m benchmarks.orchestrator --volume small --workers 4 --report

    # push a tier to failure: ramp up 50 users every minute until it breaks
    python -m benchmarks.orchestrator --tier M30 --workers 8 --users 5000 \\
        --step-load --step-users 50 --step-time 1m --run-time 30m --report
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from locust.util.timespan import parse_timespan

from benchmarks import metrics_atlas, seed
from benchmarks.config import load_config, resolve_volume

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
ALL_PROFILES = ["read_heavy", "balanced", "cpu_intensive"]


def _resolve_env(var_name: str, what: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise SystemExit(f"Missing {what}: env var '{var_name}' is not set.")
    return value


def load_tiers_config(tiers_path: str) -> tuple[list[dict], dict]:
    with open(tiers_path) as f:
        data = yaml.safe_load(f)
    return data["tiers"], data.get("atlas", {})


def check_storage_cap(cfg: dict, tier_label: str, target_volume_bytes: int) -> None:
    limit = cfg.get("tier_testing_limits", {}).get(tier_label.upper(), {}).get("max_storage_bytes")
    if limit is not None and target_volume_bytes > limit:
        raise SystemExit(
            f"Refusing to run tier '{tier_label}': target volume "
            f"{target_volume_bytes / 1024**3:.2f}GB exceeds its storage cap of "
            f"{limit / 1024**3:.2f}GB. Choose a smaller --volume for this tier."
        )


def _run_locust(
    locustfile_path: str,
    load_flags: list[str],
    csv_prefix: str,
    proc_env: dict,
    workers: int,
) -> tuple[int, list[int]]:
    """Run Locust either as a single headless process, or (workers > 1) as a
    local master + N worker processes talking over Locust's own master-worker
    protocol on localhost -- the same distributed code path used on GKE, just
    with every process on this machine. Returns (master_exit_code,
    [worker_exit_codes])."""
    base_cmd = [sys.executable, "-m", "locust", "-f", locustfile_path]

    if workers <= 1:
        cmd = base_cmd + ["--headless", *load_flags, "--csv", csv_prefix, "--csv-full-history"]
        result = subprocess.run(cmd, env=proc_env)
        return result.returncode, []

    master_cmd = base_cmd + [
        "--headless",
        "--master",
        "--expect-workers",
        str(workers),
        *load_flags,
        "--csv",
        csv_prefix,
        "--csv-full-history",
    ]
    worker_cmd = base_cmd + ["--worker", "--master-host", "127.0.0.1"]

    master_proc = subprocess.Popen(master_cmd, env=proc_env)
    worker_procs = [subprocess.Popen(worker_cmd, env=proc_env) for _ in range(workers)]

    master_rc = master_proc.wait()
    worker_rcs = [p.wait() for p in worker_procs]
    return master_rc, worker_rcs


def run_tier(
    profile: str,
    volume_label: str,
    target_volume_bytes: int,
    tier: dict,
    atlas_cfg: dict,
    users: int | None,
    spawn_rate: int | None,
    run_time: str | None,
    workers: int = 1,
    step_load: bool = False,
    step_users: int | None = None,
    step_time: str | None = None,
) -> dict:
    """Reseed + run one (profile, tier) combination. Returns a small summary
    dict describing what happened, for the final consolidated printout."""
    tier_label = tier["tier_label"]
    mongo_uri = _resolve_env(tier["mongo_uri_env"], f"tier {tier_label} mongo URI")

    atlas_project_id = None
    if atlas_cfg.get("project_id_env"):
        atlas_project_id = os.environ.get(atlas_cfg["project_id_env"])
    atlas_cluster_name = None
    if tier.get("cluster_name_env"):
        atlas_cluster_name = os.environ.get(tier["cluster_name_env"])

    cfg = load_config(profile=profile, overrides={"mongo_uri": mongo_uri})
    check_storage_cap(cfg, tier_label, target_volume_bytes)

    tier_defaults = cfg.get("tier_testing_limits", {}).get(tier_label.upper(), {})
    effective_users = users or tier_defaults.get("default_users") or cfg["locust"]["users"]
    effective_spawn_rate = spawn_rate or cfg["locust"]["spawn_rate"]
    effective_run_time = run_time or cfg["locust"]["run_time"]

    out_dir = RESULTS_DIR / profile / volume_label / tier_label
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== [{profile} / {tier_label}] reseeding to {target_volume_bytes / 1024**3:.2f}GB ===")
    seed.seed(cfg, target_volume_bytes, tier_label=tier_label)

    load_desc = f"{effective_users} users" if not step_load else f"stepping to {effective_users} users, +{step_users}/{step_time}"
    print(f"=== [{profile} / {tier_label}] running Locust ({load_desc}, {workers} worker(s), {effective_run_time}) ===")
    run_start = datetime.now(timezone.utc)

    env_overrides = {"BENCH_PROFILE": profile, "BENCH_MONGO_URI": mongo_uri}
    if step_load:
        # Locust's CLI dropped --step-load; ramping is done via a LoadTestShape
        # class in locustfile.py that only activates when BENCH_STEP_LOAD is set.
        env_overrides.update(
            {
                "BENCH_STEP_LOAD": "1",
                "BENCH_STEP_USERS": str(step_users),
                "BENCH_STEP_SECONDS": str(parse_timespan(step_time)),
                "BENCH_STEP_SPAWN_RATE": str(effective_spawn_rate),
                "BENCH_MAX_USERS": str(effective_users),
                "BENCH_RUN_TIME_SECONDS": str(parse_timespan(effective_run_time)),
            }
        )
    proc_env = {**os.environ, **env_overrides}
    csv_prefix = str(out_dir / "locust")
    locustfile_path = str(Path(__file__).resolve().parent / "locustfile.py")

    if step_load:
        # Locust ignores -u/-r/-t once a LoadTestShape is defined -- the shape
        # class itself (via BENCH_RUN_TIME_SECONDS) enforces the duration cap.
        load_flags = []
    else:
        load_flags = ["-u", str(effective_users), "-r", str(effective_spawn_rate), "-t", effective_run_time]

    locust_result, worker_results = _run_locust(
        locustfile_path, load_flags, csv_prefix, proc_env, workers
    )
    if locust_result != 0 or any(r != 0 for r in worker_results):
        # Locust exits non-zero whenever any request failed during the run (e.g. a
        # tier hit a real limit like a storage quota or connection cap mid-run --
        # exactly the "push to failure" signal we want for M30). That's a
        # meaningful benchmark result, not a fatal error -- keep going so
        # atlas_metrics.json/run_metadata.json still get written for this run.
        print(
            f"  WARNING: Locust exited non-zero (master={locust_result}, workers={worker_results}) for "
            f"[{profile} / {tier_label}] (some requests failed -- see "
            f"{csv_prefix}_failures.csv). Continuing to capture metrics/metadata."
        )

    run_end = datetime.now(timezone.utc)

    print(f"=== [{profile} / {tier_label}] pulling Atlas metrics for the test window ===")
    metrics = metrics_atlas.fetch_process_metrics(
        atlas_project_id, atlas_cluster_name, run_start, run_end
    )
    if not metrics.get("available"):
        print(f"  WARNING: Atlas metrics unavailable for [{profile} / {tier_label}]: {metrics.get('reason')}")
    with open(out_dir / "atlas_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    run_metadata = {
        "tier_label": tier_label,
        "profile": profile,
        "volume_label": volume_label,
        "target_volume_bytes": target_volume_bytes,
        "users": effective_users,
        "spawn_rate": effective_spawn_rate,
        "run_time": effective_run_time,
        "run_start": run_start.isoformat(),
        "run_end": run_end.isoformat(),
        "locust_master_exit_code": locust_result,
        "locust_worker_exit_codes": worker_results,
        "workers": workers,
        "step_load": step_load,
    }
    with open(out_dir / "run_metadata.json", "w") as f:
        json.dump(run_metadata, f, indent=2)

    print(f"=== [{profile} / {tier_label}] done, results in {out_dir} ===")

    return {
        "profile": profile,
        "tier_label": tier_label,
        "out_dir": out_dir,
        "locust_exit_code": locust_result,
        "atlas_metrics_available": metrics.get("available", False),
    }


def _read_aggregated_stats(out_dir: Path) -> dict | None:
    stats_path = out_dir / "locust_stats.csv"
    if not stats_path.exists():
        return None
    with open(stats_path) as f:
        for row in csv.DictReader(f):
            if row.get("Name") == "Aggregated":
                return row
    return None


def print_summary(run_results: list[dict]) -> None:
    """Print a consolidated table of every (profile, tier) run's results --
    the orchestrator's own quick view, without needing benchmarks.report."""
    print("\n" + "=" * 100)
    print("SUMMARY -- all runs")
    print("=" * 100)
    header = f"{'Profile':<15} {'Tier':<8} {'Requests':>10} {'Fails':>8} {'p50':>7} {'p95':>7} {'p99':>7} {'req/s':>9} {'Atlas metrics':>14}"
    print(header)
    print("-" * len(header))
    for result in run_results:
        agg = _read_aggregated_stats(result["out_dir"])
        if agg is None:
            print(f"{result['profile']:<15} {result['tier_label']:<8} {'no data (run failed before producing CSVs)':>60}")
            continue
        req_count = agg.get("Request Count", "n/a")
        fail_count = agg.get("Failure Count", "n/a")
        p50 = agg.get("50%", "n/a")
        p95 = agg.get("95%", "n/a")
        p99 = agg.get("99%", "n/a")
        req_s = agg.get("Requests/s", "n/a")
        req_s_fmt = f"{float(req_s):.1f}" if req_s not in (None, "", "n/a") else "n/a"
        atlas_status = "yes" if result["atlas_metrics_available"] else "no"
        print(
            f"{result['profile']:<15} {result['tier_label']:<8} {req_count:>10} {fail_count:>8} "
            f"{p50:>7} {p95:>7} {p99:>7} {req_s_fmt:>9} {atlas_status:>14}"
        )
    print("=" * 100)
    print("Full CSVs, atlas_metrics.json, and run_metadata.json for each run are under results/<profile>/<volume>/<tier>/")
    print("Generate charts + a markdown report per profile with: python -m benchmarks.report --profile <profile> --volume <volume>")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--profile",
        default=None,
        help="read_heavy, balanced, cpu_intensive, a comma-separated subset, or omit for all three",
    )
    parser.add_argument("--volume", default="small", help="tiny/small/medium/large (see config/base.yaml) or a raw byte count")
    parser.add_argument("--tiers-file", default=str(Path(__file__).resolve().parent.parent / "tiers.yaml"))
    parser.add_argument(
        "--tier",
        default=None,
        help="A tier_label (e.g. M10), a comma-separated subset (e.g. FLEX,M10,M30), or omit for every tier in tiers.yaml",
    )
    parser.add_argument("--users", type=int, default=None, help="Override Locust user count for every run (with --step-load, this is the ceiling it steps up to)")
    parser.add_argument("--spawn-rate", type=int, default=None)
    parser.add_argument("--run-time", default=None, help='e.g. "5m", "1h"')
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Run Locust as a local master + N worker processes (distributed mode on localhost) instead of one process. Use >1 to generate enough load to stress M10/M30.",
    )
    parser.add_argument(
        "--step-load",
        action="store_true",
        help="Ramp users up in steps (via Locust's --step-load) instead of spawning straight to --users, to find the point where a tier starts failing.",
    )
    parser.add_argument("--step-users", type=int, default=None, help="Users to add per step (required with --step-load)")
    parser.add_argument("--step-time", default=None, help='Time per step, e.g. "1m" (required with --step-load)')
    parser.add_argument(
        "--report",
        action="store_true",
        help="After all runs finish, generate per-profile + cross-workload reports (same as running benchmarks.report).",
    )
    args = parser.parse_args()

    if args.step_load and (args.step_users is None or args.step_time is None):
        raise SystemExit("--step-load requires both --step-users and --step-time")

    if args.profile:
        profiles = [p.strip() for p in args.profile.split(",") if p.strip()]
        unknown = [p for p in profiles if p not in ALL_PROFILES]
        if unknown:
            raise SystemExit(f"Unknown profile(s) {unknown}; choose from {ALL_PROFILES}")
    else:
        profiles = ALL_PROFILES

    volume_label = args.volume if isinstance(args.volume, str) and not args.volume.isdigit() else str(args.volume)
    target_volume_bytes = resolve_volume(args.volume)

    tiers, atlas_cfg = load_tiers_config(args.tiers_file)
    if args.tier:
        wanted = {t.strip().upper() for t in args.tier.split(",") if t.strip()}
        tiers = [t for t in tiers if t["tier_label"].upper() in wanted]
        found = {t["tier_label"].upper() for t in tiers}
        missing = wanted - found
        if missing:
            raise SystemExit(f"No tier(s) {sorted(missing)} found in {args.tiers_file}")

    print(f"Running profiles {profiles} against tiers {[t['tier_label'] for t in tiers]} at volume '{volume_label}'")

    run_results = []
    for tier in tiers:
        for profile in profiles:
            result = run_tier(
                profile,
                volume_label,
                target_volume_bytes,
                tier,
                atlas_cfg,
                args.users,
                args.spawn_rate,
                args.run_time,
                workers=args.workers,
                step_load=args.step_load,
                step_users=args.step_users,
                step_time=args.step_time,
            )
            run_results.append(result)

    print_summary(run_results)

    if args.report:
        tier_labels = [t["tier_label"] for t in tiers]
        print("\n=== generating reports ===")
        for profile in profiles:
            subprocess.run(
                [sys.executable, "-m", "benchmarks.report", "--profile", profile, "--volume", volume_label],
                check=False,
            )
        subprocess.run(
            [sys.executable, "-m", "benchmarks.report", "--cross-workload", "--volume", volume_label, "--tiers", *tier_labels],
            check=False,
        )


if __name__ == "__main__":
    main()
