"""Same multi-tier, multi-profile run matrix as benchmarks.orchestrator, but
generating load from a real GKE cluster (k8s/) instead of local processes --
use this once you actually need more load-generation capacity than one
machine can drive (chiefly: pushing M10/M30 to their breaking point).

Seeding and Atlas metrics collection still happen from wherever you run this
script (same as orchestrator.py) -- only the Locust master/worker run itself
moves into the cluster, as one-shot Jobs (k8s/locust-master-job.yaml,
k8s/locust-worker-job.yaml) that are applied, waited on, and torn down for
each (tier, profile) combination.

Prerequisites (see README's "Distributed load testing on GKE" section):
  - kubectl pointed at your GKE cluster (`kubectl config current-context`)
  - k8s/locust-master-job.yaml / locust-worker-job.yaml's `image:` pushed to
    a registry your cluster can pull from
  - the same tier env vars orchestrator.py already reads locally (e.g.
    M0_MONGO_URI) -- this script creates/updates the in-cluster Secret from
    them directly, so nothing extra needs to live in k8s/secret.yaml

Usage:
    # one profile, one tier, 4 in-cluster workers
    python -m benchmarks.orchestrator_k8s --profile read_heavy --volume small --tier M0 --workers 4

    # push M30 to failure: ramp up over the cluster, 8 workers
    python -m benchmarks.orchestrator_k8s --tier M30 --volume small --workers 8 \\
        --step-load --step-users 100 --step-time 1m --users 20000 --run-time 60m --report
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from locust.util.timespan import parse_timespan

from benchmarks import metrics_atlas, seed
from benchmarks.config import load_config, resolve_volume
from benchmarks.orchestrator import (
    ALL_PROFILES,
    _read_aggregated_stats,
    _resolve_env,
    check_storage_cap,
    load_tiers_config,
)

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
K8S_DIR = Path(__file__).resolve().parent.parent / "k8s"
MASTER_JOB_NAME = "locust-master"
WORKER_JOB_NAME = "locust-worker"


def _kubectl(*args: str, namespace: str, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["kubectl", "--namespace", namespace, *args]
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def _kubectl_apply_yaml(doc: dict, namespace: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(doc, f)
        path = f.name
    try:
        subprocess.run(["kubectl", "--namespace", namespace, "apply", "-f", path], check=True)
    finally:
        os.unlink(path)


def _delete_jobs(namespace: str) -> None:
    """Clear out any leftover master/worker Jobs from a prior run before
    starting a new one -- Jobs (unlike Deployments) don't get replaced in
    place by re-applying, since their pod template is immutable once created."""
    for name in (MASTER_JOB_NAME, WORKER_JOB_NAME):
        _kubectl("delete", "job", name, "--ignore-not-found", "--wait=true", namespace=namespace, check=False)


def _apply_secret(mongo_uri: str, namespace: str) -> None:
    """Create/update the in-cluster Secret straight from the same env var
    orchestrator.py already resolved -- avoids ever writing real credentials
    to a k8s/secret.yaml on disk."""
    result = subprocess.run(
        [
            "kubectl", "--namespace", namespace,
            "create", "secret", "generic", "locust-secrets",
            f"--from-literal=BENCH_MONGO_URI={mongo_uri}",
            "--dry-run=client", "-o", "yaml",
        ],
        check=True, capture_output=True, text=True,
    )
    subprocess.run(["kubectl", "--namespace", namespace, "apply", "-f", "-"], input=result.stdout, text=True, check=True)


def _apply_configmap(
    profile: str,
    image: str,
    workers: int,
    effective_users: int,
    effective_spawn_rate: int,
    effective_run_time: str,
    step_load: bool,
    step_users: int | None,
    step_time: str | None,
    namespace: str,
) -> None:
    data = {
        "BENCH_PROFILE": profile,
        "BENCH_DATABASE": "bench",
        "BENCH_COLLECTION": "docs",
        "LOCUST_HEADLESS": "true",
        "LOCUST_EXPECT_WORKERS": str(workers),
    }
    if step_load:
        data.update(
            {
                "BENCH_STEP_LOAD": "true",
                "BENCH_STEP_USERS": str(step_users),
                "BENCH_STEP_SECONDS": str(parse_timespan(step_time)),
                "BENCH_STEP_SPAWN_RATE": str(effective_spawn_rate),
                "BENCH_MAX_USERS": str(effective_users),
                "BENCH_RUN_TIME_SECONDS": str(parse_timespan(effective_run_time)),
                # LOCUST_USERS/RUN_TIME are ignored by Locust once a
                # LoadTestShape is defined, but set harmlessly for clarity.
                "LOCUST_USERS": str(effective_users),
                "LOCUST_SPAWN_RATE": str(effective_spawn_rate),
                "LOCUST_RUN_TIME": effective_run_time,
            }
        )
    else:
        data.update(
            {
                "BENCH_STEP_LOAD": "false",
                "LOCUST_USERS": str(effective_users),
                "LOCUST_SPAWN_RATE": str(effective_spawn_rate),
                "LOCUST_RUN_TIME": effective_run_time,
            }
        )

    _kubectl_apply_yaml(
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "locust-config"}, "data": data},
        namespace,
    )


def _apply_jobs(workers: int, image: str, namespace: str) -> None:
    master = yaml.safe_load((K8S_DIR / "locust-master-job.yaml").read_text())
    master["spec"]["template"]["spec"]["containers"][0]["image"] = image
    _kubectl_apply_yaml(master, namespace)

    worker = yaml.safe_load((K8S_DIR / "locust-worker-job.yaml").read_text())
    worker["spec"]["parallelism"] = workers
    worker["spec"]["completions"] = workers
    worker["spec"]["template"]["spec"]["containers"][0]["image"] = image
    _kubectl_apply_yaml(worker, namespace)


def _master_pod_name(namespace: str) -> str | None:
    result = _kubectl(
        "get", "pods", "-l", f"job-name={MASTER_JOB_NAME}",
        "-o", "jsonpath={.items[0].metadata.name}",
        namespace=namespace, check=False,
    )
    return result.stdout.strip() or None


def _wait_for_master(namespace: str, timeout_seconds: int) -> str:
    """Block until the master container writes /results/.exit_code (see the
    `command` override in locust-master-job.yaml), returning "complete" or
    "failed". Deliberately doesn't watch Job status: the container sleeps
    after Locust exits (so the pod stays Running and `kubectl cp` can still
    reach it), which means the Job itself won't report success/failure until
    the sleep ends -- far too late to be useful here."""
    deadline = time.monotonic() + timeout_seconds
    pod_name = None
    while time.monotonic() < deadline:
        if pod_name is None:
            pod_name = _master_pod_name(namespace)
        if pod_name:
            result = _kubectl(
                "exec", pod_name, "--", "cat", "/results/.exit_code",
                namespace=namespace, check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                return "complete" if result.stdout.strip() == "0" else "failed"
        time.sleep(5)
    return "timeout"


def _copy_results(namespace: str, out_dir: Path) -> None:
    pod_name = _master_pod_name(namespace)
    if pod_name is None:
        print("  WARNING: couldn't find the master pod to copy results from")
        return
    subprocess.run(
        ["kubectl", "--namespace", namespace, "cp", f"{pod_name}:/results/.", str(out_dir)],
        check=False,
    )


def run_tier_k8s(
    profile: str,
    volume_label: str,
    target_volume_bytes: int,
    tier: dict,
    atlas_cfg: dict,
    users: int | None,
    spawn_rate: int | None,
    run_time: str | None,
    workers: int,
    step_load: bool,
    step_users: int | None,
    step_time: str | None,
    image: str,
    namespace: str,
    master_wait_timeout: int,
    skip_seed: bool = False,
) -> dict:
    tier_label = tier["tier_label"]
    mongo_uri = _resolve_env(tier["mongo_uri_env"], f"tier {tier_label} mongo URI")

    atlas_project_id = os.environ.get(atlas_cfg["project_id_env"]) if atlas_cfg.get("project_id_env") else None
    atlas_cluster_name = os.environ.get(tier["cluster_name_env"]) if tier.get("cluster_name_env") else None

    cfg = load_config(profile=profile, overrides={"mongo_uri": mongo_uri})
    check_storage_cap(cfg, tier_label, target_volume_bytes)

    tier_defaults = cfg.get("tier_testing_limits", {}).get(tier_label.upper(), {})
    effective_users = users or tier_defaults.get("default_users") or cfg["locust"]["users"]
    effective_spawn_rate = spawn_rate or cfg["locust"]["spawn_rate"]
    effective_run_time = run_time or cfg["locust"]["run_time"]

    out_dir = RESULTS_DIR / profile / volume_label / tier_label
    out_dir.mkdir(parents=True, exist_ok=True)

    if skip_seed:
        print(f"\n=== [{profile} / {tier_label}] --skip-seed set, not reseeding ===")
    else:
        print(f"\n=== [{profile} / {tier_label}] reseeding to {target_volume_bytes / 1024**3:.2f}GB ===")
        seed.seed(cfg, target_volume_bytes, tier_label=tier_label)

    print(f"=== [{profile} / {tier_label}] (k8s) clearing any leftover Jobs ===")
    _delete_jobs(namespace)

    print(f"=== [{profile} / {tier_label}] (k8s) applying secret/config/jobs ({workers} worker pods) ===")
    _apply_secret(mongo_uri, namespace)
    _apply_configmap(
        profile, image, workers, effective_users, effective_spawn_rate, effective_run_time,
        step_load, step_users, step_time, namespace,
    )
    run_start = datetime.now(timezone.utc)
    _apply_jobs(workers, image, namespace)

    print(f"=== [{profile} / {tier_label}] (k8s) waiting for master Job to finish (timeout {master_wait_timeout}s) ===")
    outcome = _wait_for_master(namespace, master_wait_timeout)
    if outcome != "complete":
        print(f"  WARNING: master Job ended with '{outcome}' for [{profile} / {tier_label}] -- see `kubectl logs job/{MASTER_JOB_NAME}`")
    run_end = datetime.now(timezone.utc)

    print(f"=== [{profile} / {tier_label}] (k8s) copying results out of the master pod ===")
    _copy_results(namespace, out_dir)
    _delete_jobs(namespace)

    print(f"=== [{profile} / {tier_label}] pulling Atlas metrics for the test window ===")
    metrics = metrics_atlas.fetch_process_metrics(atlas_project_id, atlas_cluster_name, run_start, run_end)
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
        "workers": workers,
        "step_load": step_load,
        "backend": "gke",
        "master_job_outcome": outcome,
    }
    with open(out_dir / "run_metadata.json", "w") as f:
        json.dump(run_metadata, f, indent=2)

    print(f"=== [{profile} / {tier_label}] done, results in {out_dir} ===")

    return {
        "profile": profile,
        "tier_label": tier_label,
        "out_dir": out_dir,
        "locust_exit_code": 0 if outcome == "complete" else 1,
        "atlas_metrics_available": metrics.get("available", False),
    }


def print_summary(run_results: list[dict]) -> None:
    print("\n" + "=" * 100)
    print("SUMMARY -- all runs (GKE-backed)")
    print("=" * 100)
    header = f"{'Profile':<15} {'Tier':<8} {'Requests':>10} {'Fails':>8} {'p50':>7} {'p95':>7} {'p99':>7} {'req/s':>9} {'Atlas metrics':>14}"
    print(header)
    print("-" * len(header))
    for result in run_results:
        agg = _read_aggregated_stats(result["out_dir"])
        if agg is None:
            print(f"{result['profile']:<15} {result['tier_label']:<8} {'no data (results copy may have failed)':>60}")
            continue
        req_count = agg.get("Request Count", "n/a")
        fail_count = agg.get("Failure Count", "n/a")
        p50, p95, p99 = agg.get("50%", "n/a"), agg.get("95%", "n/a"), agg.get("99%", "n/a")
        req_s = agg.get("Requests/s", "n/a")
        req_s_fmt = f"{float(req_s):.1f}" if req_s not in (None, "", "n/a") else "n/a"
        atlas_status = "yes" if result["atlas_metrics_available"] else "no"
        print(
            f"{result['profile']:<15} {result['tier_label']:<8} {req_count:>10} {fail_count:>8} "
            f"{p50:>7} {p95:>7} {p99:>7} {req_s_fmt:>9} {atlas_status:>14}"
        )
    print("=" * 100)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--volume", default="small")
    parser.add_argument("--tiers-file", default=str(Path(__file__).resolve().parent.parent / "tiers.yaml"))
    parser.add_argument("--tier", default=None)
    parser.add_argument("--users", type=int, default=None)
    parser.add_argument("--spawn-rate", type=int, default=None)
    parser.add_argument("--run-time", default=None)
    parser.add_argument("--workers", type=int, default=4, help="Number of in-cluster worker pods (Job parallelism)")
    parser.add_argument("--step-load", action="store_true")
    parser.add_argument("--step-users", type=int, default=None)
    parser.add_argument("--step-time", default=None)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--namespace", default="default")
    parser.add_argument(
        "--image",
        required=True,
        help="Image reference your GKE cluster can pull, e.g. gcr.io/<project>/locust-mongo-bench:latest",
    )
    parser.add_argument("--master-wait-timeout", type=int, default=3600, help="Seconds to wait for the master Job to finish")
    parser.add_argument("--skip-seed", action="store_true", help="Don't reseed the collection before running (e.g. it's already seeded from a prior run)")
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

    context = subprocess.run(["kubectl", "config", "current-context"], capture_output=True, text=True)
    print(f"kubectl context: {context.stdout.strip() or '(none configured)'}")
    print(f"Running profiles {profiles} against tiers {[t['tier_label'] for t in tiers]} at volume '{volume_label}' via GKE ({args.workers} workers)")

    run_results = []
    for tier in tiers:
        for profile in profiles:
            result = run_tier_k8s(
                profile, volume_label, target_volume_bytes, tier, atlas_cfg,
                args.users, args.spawn_rate, args.run_time,
                args.workers, args.step_load, args.step_users, args.step_time,
                args.image, args.namespace, args.master_wait_timeout,
                skip_seed=args.skip_seed,
            )
            run_results.append(result)

    print_summary(run_results)

    if args.report:
        tier_labels = [t["tier_label"] for t in tiers]
        print("\n=== generating reports ===")
        for profile in profiles:
            subprocess.run([sys.executable, "-m", "benchmarks.report", "--profile", profile, "--volume", volume_label], check=False)
        subprocess.run(
            [sys.executable, "-m", "benchmarks.report", "--cross-workload", "--volume", volume_label, "--tiers", *tier_labels],
            check=False,
        )


if __name__ == "__main__":
    main()
