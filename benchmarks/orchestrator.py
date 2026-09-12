"""Multi-tier orchestration: reseed + run the Locust workload + capture Atlas
metrics, for one tier or for every tier in tiers.yaml, sequentially.

Tiers are run sequentially by design -- not concurrently -- so they don't
compete for the local machine's CPU as load generator. To parallelize,
run this script from separate load-generation hosts per tier instead.

Usage:
    # all tiers in tiers.yaml, sequentially
    python -m benchmarks.orchestrator --profile read_heavy --volume small

    # a single tier only
    python -m benchmarks.orchestrator --profile read_heavy --volume small --tier M10
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from benchmarks import metrics_atlas, seed
from benchmarks.config import load_config, resolve_volume

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


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


def run_tier(
    profile: str,
    volume_label: str,
    target_volume_bytes: int,
    tier: dict,
    atlas_cfg: dict,
    users: int | None,
    spawn_rate: int | None,
    run_time: str | None,
) -> None:
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

    print(f"\n=== Tier {tier_label}: reseeding to {target_volume_bytes / 1024**3:.2f}GB ===")
    seed.seed(cfg, target_volume_bytes, tier_label=tier_label)

    print(f"=== Tier {tier_label}: running Locust ({effective_users} users, {effective_run_time}) ===")
    run_start = datetime.now(timezone.utc)

    env_overrides = {"BENCH_PROFILE": profile, "BENCH_MONGO_URI": mongo_uri}
    proc_env = {**os.environ, **env_overrides}
    csv_prefix = str(out_dir / "locust")
    cmd = [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(Path(__file__).resolve().parent / "locustfile.py"),
        "--headless",
        "-u",
        str(effective_users),
        "-r",
        str(effective_spawn_rate),
        "-t",
        effective_run_time,
        "--csv",
        csv_prefix,
        "--csv-full-history",
    ]
    locust_result = subprocess.run(cmd, env=proc_env)
    if locust_result.returncode != 0:
        # Locust exits non-zero whenever any request failed during the run (e.g. a
        # tier hit a real limit like a storage quota or connection cap mid-run).
        # That's a meaningful benchmark result, not a fatal error -- keep going so
        # atlas_metrics.json/run_metadata.json still get written for this tier.
        print(
            f"  WARNING: Locust exited with code {locust_result.returncode} for tier "
            f"{tier_label} (some requests failed -- see {csv_prefix}_failures.csv). "
            "Continuing to capture metrics/metadata for this run."
        )

    run_end = datetime.now(timezone.utc)

    print(f"=== Tier {tier_label}: pulling Atlas metrics for the test window ===")
    metrics = metrics_atlas.fetch_process_metrics(
        atlas_project_id, atlas_cluster_name, run_start, run_end
    )
    if not metrics.get("available"):
        print(f"  WARNING: Atlas metrics unavailable for tier {tier_label}: {metrics.get('reason')}")
    with open(out_dir / "atlas_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    with open(out_dir / "run_metadata.json", "w") as f:
        json.dump(
            {
                "tier_label": tier_label,
                "profile": profile,
                "volume_label": volume_label,
                "target_volume_bytes": target_volume_bytes,
                "users": effective_users,
                "spawn_rate": effective_spawn_rate,
                "run_time": effective_run_time,
                "run_start": run_start.isoformat(),
                "run_end": run_end.isoformat(),
                "locust_exit_code": locust_result.returncode,
            },
            f,
            indent=2,
        )

    print(f"=== Tier {tier_label}: done, results in {out_dir} ===")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="read_heavy/balanced/cpu_intensive")
    parser.add_argument("--volume", default="small", help="small/medium/large or a raw byte count")
    parser.add_argument("--tiers-file", default=str(Path(__file__).resolve().parent.parent / "tiers.yaml"))
    parser.add_argument("--tier", default=None, help="Run only this tier_label (e.g. M10) instead of all tiers")
    parser.add_argument("--users", type=int, default=None, help="Override Locust user count for the run(s)")
    parser.add_argument("--spawn-rate", type=int, default=None)
    parser.add_argument("--run-time", default=None, help='e.g. "5m", "1h"')
    args = parser.parse_args()

    volume_label = args.volume if isinstance(args.volume, str) and not args.volume.isdigit() else str(args.volume)
    target_volume_bytes = resolve_volume(args.volume)

    tiers, atlas_cfg = load_tiers_config(args.tiers_file)
    if args.tier:
        tiers = [t for t in tiers if t["tier_label"].upper() == args.tier.upper()]
        if not tiers:
            raise SystemExit(f"No tier '{args.tier}' found in {args.tiers_file}")

    for tier in tiers:
        run_tier(
            args.profile,
            volume_label,
            target_volume_bytes,
            tier,
            atlas_cfg,
            args.users,
            args.spawn_rate,
            args.run_time,
        )


if __name__ == "__main__":
    main()
