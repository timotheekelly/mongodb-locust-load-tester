"""Turn an --offered-load-stages run into the table that actually matters for
a capacity benchmark: offered load -> achieved throughput -> p95 latency ->
error rate -> what Atlas metric correlates with the drop-off. Answers "at
what offered load does this tier stop scaling, and why" instead of just
"average latency at N users".

Usage:
    python -m benchmarks.analyze_saturation --profile read_heavy --volume small --tier M30

Reads results/<profile>/<volume>/<tier>/{run_metadata.json, locust_stats_history.csv,
atlas_metrics.json} (written by `benchmarks.orchestrator --offered-load-stages ...`
or `benchmarks.orchestrator_k8s` with the same flags) and writes
reports/saturation/<tier>_<profile>_<volume>_saturation.md + a chart.
"""

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"

# A stage's first half is ramp-up/transition from the previous stage's load
# level; only the second half reflects that stage's steady-state behavior.
STAGE_WARMUP_FRACTION = 0.5
# Below this achieved/offered ratio, call the stage "not keeping up".
SATURATION_RATIO_THRESHOLD = 0.9
# Above this multiple of the previous stage's p95, call it a latency cliff.
LATENCY_CLIFF_MULTIPLE = 2.0
# A low achieved/offered ratio or a latency jump can both happen transiently
# at low offered loads (e.g. a stage-1 connection-pool warm-up blip) with
# zero actual failures -- real saturation means requests are actually
# failing, not just a noisy throughput dip that recovers next stage. This is
# the minimum error rate required before either signal above counts.
MIN_ERROR_PCT_FOR_SATURATION = 2.0


def _load_run(out_dir: Path) -> tuple[dict, list[dict], dict]:
    metadata_path = out_dir / "run_metadata.json"
    if not metadata_path.exists():
        raise SystemExit(f"No run_metadata.json in {out_dir} -- did you run with --offered-load-stages?")
    metadata = json.loads(metadata_path.read_text())
    if not metadata.get("offered_load_stages"):
        raise SystemExit(
            f"{metadata_path} has no offered_load_stages -- this run wasn't an offered-load sweep "
            "(use `benchmarks.orchestrator --offered-load-stages ...` to produce one)."
        )

    history_path = out_dir / "locust_stats_history.csv"
    if not history_path.exists():
        raise SystemExit(f"No locust_stats_history.csv in {out_dir}")
    with open(history_path) as f:
        history_rows = list(csv.DictReader(f))
    # Deliberately NOT filtering to Locust's own "Aggregated" row: that row
    # combines every Type/Name that fired an event, including the "META"/
    # "STARTED" markers used to measure started/sec separately from
    # completed/sec (see locustfile.py's _fire_started). Mixing those in
    # would dilute latency percentiles with artificial 0ms entries and
    # double-count throughput. Instead, callers filter by Type themselves:
    # "MONGODB" rows are real completed operations, "META" rows are start
    # markers.

    atlas_path = out_dir / "atlas_metrics.json"
    atlas = json.loads(atlas_path.read_text()) if atlas_path.exists() else {"available": False}

    return metadata, history_rows, atlas


def _avg_atlas_metric(
    atlas: dict, measurement_name: str, window_start: datetime, window_end: datetime, primary_only: bool = False
) -> float | None:
    """primary_only=True restricts to the replica set's primary node --
    this driver (no custom read preference) sends every read and write to
    the primary only, so averaging a metric like CPU across all replica
    members, including idle secondaries, dilutes the one node actually
    doing the work. Falls back to all processes if primary_host wasn't
    resolved (e.g. a shared tier with a different topology)."""
    if not atlas.get("available"):
        return None
    processes = atlas.get("processes", {})
    primary_host = atlas.get("primary_host")
    if primary_only and primary_host and primary_host in processes:
        processes = {primary_host: processes[primary_host]}
    values = []
    for measurements in processes.values():
        for m in measurements:
            if m.get("name") != measurement_name:
                continue
            for point in m.get("dataPoints", []):
                if point.get("value") is None:
                    continue
                ts = datetime.fromisoformat(point["timestamp"].replace("Z", "+00:00"))
                if window_start <= ts < window_end:
                    values.append(point["value"])
    return sum(values) / len(values) if values else None


def analyze(profile: str, volume_label: str, tier_label: str) -> Path:
    out_dir = RESULTS_DIR / profile / volume_label / tier_label
    metadata, history_rows, atlas = _load_run(out_dir)

    stages_rps = metadata["offered_load_stages"]
    stage_seconds = metadata["offered_load_stage_seconds"]

    # Stage boundaries derived from the actual "User Count" column, not from
    # run_start + i*stage_seconds. The latter assumes the Locust shape
    # starts ticking the instant the orchestrator applies the Jobs, but the
    # real delay (pods scheduling, image pull, workers connecting one by
    # one -- sometimes 30-60+ seconds) pushes the real stage transitions
    # later than that assumption predicts. On a real run this was enough to
    # shift every computed window by a full stage, making "achieved at
    # offered=X" actually measure the previous stage's rate instead. Each
    # simulated user targets ~1 req/sec, so a stage's target user count is
    # directly observable in the data regardless of how long startup took.
    ts_to_users: dict[int, int] = {}
    for r in history_rows:
        ts = int(r["Timestamp"])
        if ts not in ts_to_users:
            try:
                ts_to_users[ts] = int(float(r["User Count"]))
            except (ValueError, KeyError):
                pass

    stage_results = []
    prev_p95 = None
    prev_achieved = None
    saturation_stage = None

    for i, offered in enumerate(stages_rps):
        matching_ts = sorted(ts for ts, users in ts_to_users.items() if users == offered)
        if not matching_ts:
            stage_results.append(
                {"offered": offered, "achieved": None, "p95": None, "error_pct": None, "note": "no data (run may have ended early)"}
            )
            continue
        stage_start = datetime.fromtimestamp(matching_ts[0], tz=timezone.utc)
        stage_end = datetime.fromtimestamp(matching_ts[-1], tz=timezone.utc) + timedelta(seconds=1)
        steady_start = stage_start + timedelta(seconds=(stage_end - stage_start).total_seconds() * STAGE_WARMUP_FRACTION)

        rows = [
            r for r in history_rows
            if steady_start <= datetime.fromtimestamp(int(r["Timestamp"]), tz=timezone.utc) < stage_end
        ]
        mongo_rows = [r for r in rows if r.get("Type") == "MONGODB"]
        started_rows = [r for r in rows if r.get("Type") == "META" and r.get("Name") == "STARTED"]

        if not mongo_rows:
            stage_results.append(
                {"offered": offered, "achieved": None, "p95": None, "error_pct": None, "note": "no data (run may have ended early)"}
            )
            continue

        # Sum across the (mutually exclusive) MONGODB operation names at
        # each timestamp, then average across timestamps in the window --
        # equivalent to what Locust's own "Aggregated" row would show,
        # minus the META/STARTED contamination.
        by_timestamp: dict[str, list[dict]] = {}
        for r in mongo_rows:
            by_timestamp.setdefault(r["Timestamp"], []).append(r)
        per_ts_achieved = [sum(float(r["Requests/s"]) for r in rs) for rs in by_timestamp.values()]
        per_ts_failures = [sum(float(r["Failures/s"]) for r in rs) for rs in by_timestamp.values()]
        achieved = sum(per_ts_achieved) / len(per_ts_achieved)
        failures = sum(per_ts_failures) / len(per_ts_failures)
        error_pct = (failures / achieved * 100) if achieved > 0 else 0.0

        # Weight each row's own p95 by its share of that timestamp's
        # requests, so a heavier operation type doesn't get diluted by a
        # lighter one's percentile.
        weighted_p95_sum = 0.0
        weight_total = 0.0
        for r in mongo_rows:
            p95_val = r.get("95%")
            req_s = float(r.get("Requests/s") or 0)
            if p95_val in (None, "", "N/A") or req_s <= 0:
                continue
            weighted_p95_sum += float(p95_val) * req_s
            weight_total += req_s
        p95 = (weighted_p95_sum / weight_total) if weight_total > 0 else None

        started = (
            sum(float(r["Requests/s"]) for r in started_rows) / len(started_rows)
            if started_rows else None
        )

        # primary_only=True throughout: every read and write from this
        # benchmark goes to the primary (no custom read preference), so
        # averaging across idle secondaries would understate all four of
        # these for the node actually doing the work.
        #
        # PROCESS_NORMALIZED_CPU_USER (0-100% of this tier's total compute
        # allocation) preferred over the raw PROCESS_CPU_USER (percent of
        # ONE core, ambiguous on a multi-vCPU tier) -- falls back to raw if
        # the normalized metric isn't exposed for this tier.
        cpu = _avg_atlas_metric(atlas, "PROCESS_NORMALIZED_CPU_USER", steady_start, stage_end, primary_only=True)
        cpu_is_normalized = cpu is not None
        if cpu is None:
            cpu = _avg_atlas_metric(atlas, "PROCESS_CPU_USER", steady_start, stage_end, primary_only=True)
        iops_r = _avg_atlas_metric(atlas, "DISK_PARTITION_IOPS_READ", steady_start, stage_end, primary_only=True)
        iops_w = _avg_atlas_metric(atlas, "DISK_PARTITION_IOPS_WRITE", steady_start, stage_end, primary_only=True)
        connections = _avg_atlas_metric(atlas, "CONNECTIONS", steady_start, stage_end, primary_only=True)

        has_real_errors = error_pct >= MIN_ERROR_PCT_FOR_SATURATION
        is_saturated = has_real_errors and ((achieved / offered) < SATURATION_RATIO_THRESHOLD if offered > 0 else False)
        is_latency_cliff = (
            has_real_errors and prev_p95 is not None and p95 is not None and p95 > prev_p95 * LATENCY_CLIFF_MULTIPLE
        )
        # A real drop in *absolute* achieved throughput as offered load
        # increases is unambiguous evidence of hitting a wall, regardless
        # of error rate -- a healthy system's achieved throughput should
        # never regress when you ask it for more, even if it can't fully
        # keep up. Not gated on has_real_errors: this caught a real 9x
        # latency cliff (449ms -> 4241ms) that the error-rate gate alone
        # missed because only 0.1% of requests technically errored out.
        is_throughput_regression = (
            prev_achieved is not None and achieved is not None and achieved < prev_achieved * SATURATION_RATIO_THRESHOLD
        )
        if saturation_stage is None and (is_saturated or is_latency_cliff or is_throughput_regression):
            saturation_stage = i

        stage_results.append(
            {
                "offered": offered,
                "started": started,
                "achieved": achieved,
                "p95": p95,
                "error_pct": error_pct,
                "cpu_pct": cpu,
                "cpu_is_normalized": cpu_is_normalized,
                "iops_read": iops_r,
                "iops_write": iops_w,
                "connections": connections,
                "note": None,
            }
        )
        prev_p95 = p95 if p95 is not None else prev_p95
        prev_achieved = achieved if achieved is not None else prev_achieved

    _write_report(profile, volume_label, tier_label, stage_results, saturation_stage, atlas)
    return REPORTS_DIR / "saturation" / f"{tier_label}_{profile}_{volume_label}_saturation.md"


def _fmt(value, spec="{:.1f}"):
    return spec.format(value) if value is not None else "n/a"


def _write_report(
    profile: str,
    volume_label: str,
    tier_label: str,
    stage_results: list[dict],
    saturation_stage: int | None,
    atlas: dict,
) -> None:
    out_dir = REPORTS_DIR / "saturation"
    out_dir.mkdir(parents=True, exist_ok=True)

    offered = [s["offered"] for s in stage_results]
    started = [s["started"] for s in stage_results]
    achieved = [s["achieved"] or 0 for s in stage_results]
    p95 = [s["p95"] or 0 for s in stage_results]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    if any(v is not None for v in started):
        ax1.plot(offered, [v or 0 for v in started], marker="^", color="tab:orange", alpha=0.7, label="Started/sec (task invocations)")
    ax1.plot(offered, achieved, marker="o", color="tab:blue", label="Achieved (completed)/sec")
    ax1.plot(offered, offered, linestyle="--", color="gray", alpha=0.5, label="Offered = achieved (ideal)")
    ax1.set_xlabel("Offered load (req/s)")
    ax1.set_ylabel("Achieved throughput (req/s)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.plot(offered, p95, marker="s", color="tab:red", label="p95 latency (ms)")
    ax2.set_ylabel("p95 latency (ms)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    if saturation_stage is not None:
        ax1.axvline(offered[saturation_stage], color="black", linestyle=":", alpha=0.6)

    ax1.set_title(f"Saturation sweep -- {tier_label} / {profile} ({volume_label})")
    fig.tight_layout()
    chart_path = out_dir / f"{tier_label}_{profile}_{volume_label}_saturation.png"
    fig.savefig(chart_path)
    plt.close(fig)

    lines = [f"# Saturation sweep -- {tier_label} / {profile} ({volume_label})\n"]
    lines.append(f"![saturation chart]({chart_path.name})\n")
    cpu_is_normalized = any(s.get("cpu_is_normalized") for s in stage_results if not s.get("note"))
    cpu_label = "primary CPU% (normalized)" if cpu_is_normalized else "primary CPU% (raw, 1 core)"
    lines.append(f"| Offered (req/s) | Started (req/s) | Achieved (req/s) | p95 (ms) | Errors | {cpu_label} | avg IOPS (r/w) | connections |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for i, s in enumerate(stage_results):
        marker = " **<- first stage with a real error rate**" if i == saturation_stage else ""
        if s["note"]:
            lines.append(f"| {s['offered']} | {s['note']} |  |  |  |  |  |  |")
            continue
        iops = f"{_fmt(s['iops_read'], '{:.0f}')}/{_fmt(s['iops_write'], '{:.0f}')}" if s["iops_read"] is not None else "n/a"
        lines.append(
            f"| {s['offered']} | {_fmt(s['started'])} | {_fmt(s['achieved'])}{marker} | {_fmt(s['p95'])} | {_fmt(s['error_pct'])}% | "
            f"{_fmt(s['cpu_pct'])} | {iops} | {_fmt(s['connections'], '{:.0f}')} |"
        )
    lines.append(
        f"\n*CPU is the **primary** node's own value only (not averaged across the replica set) -- this driver "
        "sends every read and write to the primary by default, so including idle secondaries would understate "
        "the node that's actually doing the work.*"
    )

    lines.append("\n## Interpretation\n")
    stages_matching_offered = [
        s for s in stage_results
        if not s["note"] and s["started"] is not None and s["offered"] > 0
        and abs(s["started"] - s["offered"]) / s["offered"] < 0.1
    ]
    if stages_matching_offered:
        last_clean = stages_matching_offered[-1]
        lines.append(
            f"Offered, started, and achieved throughput tracked closely (within 10%) through **{last_clean['offered']} req/s** "
            f"(p95 {_fmt(last_clean['p95'])}ms) -- that's the highest rate this test actually confirmed the system sustains "
            "cleanly, not an upper bound on its capacity."
        )
    if saturation_stage is None:
        lines.append(
            f"\nNo stage in this sweep reached a real error rate (>= {MIN_ERROR_PCT_FOR_SATURATION}%) -- "
            f"this run doesn't establish a capacity ceiling, only that one wasn't reached by {offered[-1]} req/s offered. "
            "Try higher offered-load stages to find it."
        )
    else:
        s = stage_results[saturation_stage]
        lines.append(
            f"\nAt **{s['offered']} req/s offered**, started/achieved throughput had fallen to "
            f"**{_fmt(s['started'])}/{_fmt(s['achieved'])} req/s** with **{_fmt(s['error_pct'])}% errors** and p95 "
            f"latency of **{_fmt(s['p95'])}ms** -- the first stage with a real error rate, not necessarily the exact "
            "capacity limit (the true ceiling could be anywhere between the last clean stage and this one)."
        )
        if not atlas.get("available"):
            lines.append(
                "\nAtlas metrics weren't available for this run (see atlas_metrics.json's `reason`), so the "
                "resource bottleneck (CPU vs IOPS vs connections vs cache) can't be identified from this report alone."
            )
        else:
            cpu_before = stage_results[max(saturation_stage - 1, 0)].get("cpu_pct")
            cpu_at = s.get("cpu_pct")
            if cpu_at is not None and cpu_before is not None:
                lines.append(
                    f"\nPrimary CPU moved from {_fmt(cpu_before)}% to {_fmt(cpu_at)}% across this point. "
                    + (
                        "That's high enough to be a plausible contributing factor."
                        if cpu_at > 80
                        else "CPU stayed low, which rules out CPU saturation as the cause here -- but low CPU alone "
                        "doesn't identify what the real limiting factor is. Operation queues, disk read latency, "
                        "cache eviction pressure, connection-pool waits, and the load generator itself are all still "
                        "on the table and weren't ruled out by this report."
                    )
                )

    lines.append(
        "\n*Note:* this table reports Locust-level requests/sec, which for `bulk_insert` is one Locust "
        "request per batched `insert_many` call (not one per document) -- see `bulk_insert_batch_size` in "
        "config/base.yaml if you need true per-document MongoDB ops/sec instead."
    )
    lines.append(
        "\n*On \"Started\" vs \"Offered\" vs \"Achieved\":* each simulated user targets 1 op/sec "
        "(`constant_throughput(1)`), but Locust's own documented behavior is that a user stops waiting "
        "between tasks once a single task takes longer than that interval -- so if **Started** tracks well "
        "below **Offered**, the load generator itself stopped pacing to the target rate (this happens "
        "once p95 latency exceeds ~1s) and \"offered\" no longer reflects a real external arrival rate at "
        "that stage. If **Started** tracks close to **Offered** but **Achieved** falls short, that's a "
        "stronger signal that the shortfall is downstream -- MongoDB or the driver, not the generator's "
        "own pacing."
    )

    report_path = out_dir / f"{tier_label}_{profile}_{volume_label}_saturation.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")

    print("\n" + "=" * 100)
    print(f"SATURATION SWEEP -- {tier_label} / {profile} ({volume_label})")
    print("=" * 100)
    print(f"{'Offered':>10} {'Started':>10} {'Achieved':>10} {'p95 (ms)':>10} {'Errors':>8} {'avg CPU%':>10}")
    for i, s in enumerate(stage_results):
        marker = "  <-- first real error rate" if i == saturation_stage else ""
        if s["note"]:
            print(f"{s['offered']:>10} {s['note']:>50}")
            continue
        print(
            f"{s['offered']:>10} {_fmt(s['started']):>10} {_fmt(s['achieved']):>10} {_fmt(s['p95']):>10} "
            f"{_fmt(s['error_pct'])+'%':>8} {_fmt(s['cpu_pct']):>10}{marker}"
        )
    print("=" * 100)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--volume", required=True)
    parser.add_argument("--tier", required=True)
    args = parser.parse_args()
    analyze(args.profile, args.volume, args.tier)


if __name__ == "__main__":
    main()
