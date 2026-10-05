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
    history_rows = [r for r in history_rows if r.get("Name") == "Aggregated"]

    atlas_path = out_dir / "atlas_metrics.json"
    atlas = json.loads(atlas_path.read_text()) if atlas_path.exists() else {"available": False}

    return metadata, history_rows, atlas


def _avg_atlas_metric(atlas: dict, measurement_name: str, window_start: datetime, window_end: datetime) -> float | None:
    if not atlas.get("available"):
        return None
    values = []
    for measurements in atlas.get("processes", {}).values():
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
    run_start = datetime.fromisoformat(metadata["run_start"])

    stage_results = []
    prev_p95 = None
    saturation_stage = None

    for i, offered in enumerate(stages_rps):
        stage_start = run_start + timedelta(seconds=i * stage_seconds)
        stage_end = run_start + timedelta(seconds=(i + 1) * stage_seconds)
        steady_start = stage_start + timedelta(seconds=stage_seconds * STAGE_WARMUP_FRACTION)

        rows = [
            r for r in history_rows
            if steady_start <= datetime.fromtimestamp(int(r["Timestamp"]), tz=timezone.utc) < stage_end
        ]

        if not rows:
            stage_results.append(
                {"offered": offered, "achieved": None, "p95": None, "error_pct": None, "note": "no data (run may have ended early)"}
            )
            continue

        achieved = sum(float(r["Requests/s"]) for r in rows) / len(rows)
        failures = sum(float(r["Failures/s"]) for r in rows) / len(rows)
        error_pct = (failures / achieved * 100) if achieved > 0 else 0.0
        p95_values = [float(r["95%"]) for r in rows if r.get("95%") not in (None, "", "N/A")]
        p95 = sum(p95_values) / len(p95_values) if p95_values else None

        cpu = _avg_atlas_metric(atlas, "PROCESS_CPU_USER", steady_start, stage_end)
        iops_r = _avg_atlas_metric(atlas, "DISK_PARTITION_IOPS_READ", steady_start, stage_end)
        iops_w = _avg_atlas_metric(atlas, "DISK_PARTITION_IOPS_WRITE", steady_start, stage_end)
        connections = _avg_atlas_metric(atlas, "CONNECTIONS", steady_start, stage_end)

        is_saturated = (achieved / offered) < SATURATION_RATIO_THRESHOLD if offered > 0 else False
        is_latency_cliff = prev_p95 is not None and p95 is not None and p95 > prev_p95 * LATENCY_CLIFF_MULTIPLE
        if saturation_stage is None and (is_saturated or is_latency_cliff):
            saturation_stage = i

        stage_results.append(
            {
                "offered": offered,
                "achieved": achieved,
                "p95": p95,
                "error_pct": error_pct,
                "cpu_pct": cpu,
                "iops_read": iops_r,
                "iops_write": iops_w,
                "connections": connections,
                "note": None,
            }
        )
        prev_p95 = p95 if p95 is not None else prev_p95

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
    achieved = [s["achieved"] or 0 for s in stage_results]
    p95 = [s["p95"] or 0 for s in stage_results]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(offered, achieved, marker="o", color="tab:blue", label="Achieved throughput (req/s)")
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
    lines.append("| Offered (req/s) | Achieved (req/s) | p95 (ms) | Errors | avg CPU% | avg IOPS (r/w) | connections |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|")
    for i, s in enumerate(stage_results):
        marker = " **<- saturation knee**" if i == saturation_stage else ""
        if s["note"]:
            lines.append(f"| {s['offered']} | {s['note']} |  |  |  |  |  |")
            continue
        iops = f"{_fmt(s['iops_read'], '{:.0f}')}/{_fmt(s['iops_write'], '{:.0f}')}" if s["iops_read"] is not None else "n/a"
        lines.append(
            f"| {s['offered']} | {_fmt(s['achieved'])}{marker} | {_fmt(s['p95'])} | {_fmt(s['error_pct'])}% | "
            f"{_fmt(s['cpu_pct'])} | {iops} | {_fmt(s['connections'], '{:.0f}')} |"
        )

    lines.append("\n## Interpretation\n")
    if saturation_stage is None:
        lines.append(
            "No saturation knee detected within the tested range -- throughput tracked offered load "
            f"and p95 stayed stable through {offered[-1]} req/s. Try higher offered-load stages to find the ceiling."
        )
    else:
        s = stage_results[saturation_stage]
        prior = stage_results[saturation_stage - 1] if saturation_stage > 0 else None
        prior_desc = f"{prior['offered']} req/s (p95 {_fmt(prior['p95'])}ms)" if prior else "the first tested stage"
        lines.append(
            f"Throughput scaled with offered load up to **{prior_desc}**. "
            f"At **{s['offered']} req/s offered**, the system achieved **{_fmt(s['achieved'])} req/s** "
            f"({_fmt(s['error_pct'])}% errors), with p95 latency of **{_fmt(s['p95'])}ms**."
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
                    f"\navg CPU% moved from {_fmt(cpu_before)}% to {_fmt(cpu_at)}% across the knee -- "
                    + ("CPU looks like a contributing bottleneck." if cpu_at > 80 else "CPU doesn't look saturated; check IOPS/connections/cache instead.")
                )

    lines.append(
        "\n*Note:* this table reports Locust-level requests/sec, which for `bulk_insert` is one Locust "
        "request per batched `insert_many` call (not one per document) -- see `bulk_insert_batch_size` in "
        "config/base.yaml if you need true per-document MongoDB ops/sec instead."
    )

    report_path = out_dir / f"{tier_label}_{profile}_{volume_label}_saturation.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")

    print("\n" + "=" * 100)
    print(f"SATURATION SWEEP -- {tier_label} / {profile} ({volume_label})")
    print("=" * 100)
    print(f"{'Offered':>10} {'Achieved':>10} {'p95 (ms)':>10} {'Errors':>8} {'avg CPU%':>10}")
    for i, s in enumerate(stage_results):
        marker = "  <-- saturation knee" if i == saturation_stage else ""
        if s["note"]:
            print(f"{s['offered']:>10} {s['note']:>50}")
            continue
        print(
            f"{s['offered']:>10} {_fmt(s['achieved']):>10} {_fmt(s['p95']):>10} "
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
