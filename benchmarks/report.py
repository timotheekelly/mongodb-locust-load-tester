"""Generate per-workload-profile reports (charts + markdown) from results/,
plus a combined cross-workload view per tier.

Usage:
    python -m benchmarks.report --profile read_heavy --volume small
    python -m benchmarks.report --cross-workload --volume small --tiers M0 FLEX M10 M30
"""

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"


def _read_locust_stats(tier_dir: Path) -> dict | None:
    stats_path = tier_dir / "locust_stats.csv"
    if not stats_path.exists():
        return None
    rows = []
    with open(stats_path) as f:
        rows = list(csv.DictReader(f))
    agg = next((r for r in rows if r["Name"] == "Aggregated"), None)
    return {"aggregated": agg, "per_op": [r for r in rows if r["Name"] != "Aggregated"]}


def _read_atlas_metrics(tier_dir: Path) -> dict:
    metrics_path = tier_dir / "atlas_metrics.json"
    if not metrics_path.exists():
        return {"available": False, "reason": "atlas_metrics.json not found"}
    with open(metrics_path) as f:
        return json.load(f)


def _avg_metric(atlas_metrics: dict, measurement_name: str) -> float | None:
    if not atlas_metrics.get("available"):
        return None
    values = []
    for measurements in atlas_metrics.get("processes", {}).values():
        for m in measurements:
            if m.get("name") == measurement_name:
                for point in m.get("dataPoints", []):
                    if point.get("value") is not None:
                        values.append(point["value"])
    return sum(values) / len(values) if values else None


def collect_profile_data(profile: str, volume_label: str) -> dict[str, dict]:
    profile_dir = RESULTS_DIR / profile / volume_label
    if not profile_dir.exists():
        raise SystemExit(f"No results found at {profile_dir}")

    data = {}
    for tier_dir in sorted(profile_dir.iterdir()):
        if not tier_dir.is_dir():
            continue
        stats = _read_locust_stats(tier_dir)
        atlas = _read_atlas_metrics(tier_dir)
        data[tier_dir.name] = {"stats": stats, "atlas": atlas}
    return data


def _chart_latency(data: dict[str, dict], out_path: Path) -> None:
    tiers = list(data.keys())
    p50, p95, p99 = [], [], []
    for tier in tiers:
        agg = (data[tier]["stats"] or {}).get("aggregated") or {}
        p50.append(float(agg.get("50%", 0) or 0))
        p95.append(float(agg.get("95%", 0) or 0))
        p99.append(float(agg.get("99%", 0) or 0))

    x = range(len(tiers))
    width = 0.25
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar([i - width for i in x], p50, width, label="p50")
    ax.bar(list(x), p95, width, label="p95")
    ax.bar([i + width for i in x], p99, width, label="p99")
    ax.set_xticks(list(x))
    ax.set_xticklabels(tiers)
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency percentiles by tier")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def _chart_throughput(data: dict[str, dict], out_path: Path) -> None:
    tiers = list(data.keys())
    throughput = []
    for tier in tiers:
        agg = (data[tier]["stats"] or {}).get("aggregated") or {}
        throughput.append(float(agg.get("Requests/s", 0) or 0))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(tiers, throughput)
    ax.set_ylabel("Requests/sec")
    ax.set_title("Throughput by tier")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def _chart_resources(data: dict[str, dict], out_path: Path) -> None:
    tiers = list(data.keys())
    cpu, mem, iops_r, iops_w = [], [], [], []
    for tier in tiers:
        atlas = data[tier]["atlas"]
        cpu.append(_avg_metric(atlas, "PROCESS_CPU_USER") or 0)
        mem.append(_avg_metric(atlas, "SYSTEM_MEMORY_USED") or 0)
        iops_r.append(_avg_metric(atlas, "DISK_PARTITION_IOPS_READ") or 0)
        iops_w.append(_avg_metric(atlas, "DISK_PARTITION_IOPS_WRITE") or 0)

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    axes[0].bar(tiers, cpu)
    axes[0].set_title("Avg CPU % (user)")
    axes[1].bar(tiers, mem)
    axes[1].set_title("Avg memory used")
    axes[2].bar(tiers, iops_r, label="read")
    axes[2].bar(tiers, iops_w, bottom=iops_r, label="write")
    axes[2].set_title("Avg IOPS")
    axes[2].legend()
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def generate_profile_report(profile: str, volume_label: str) -> Path:
    data = collect_profile_data(profile, volume_label)
    out_dir = REPORTS_DIR / profile
    out_dir.mkdir(parents=True, exist_ok=True)

    _chart_latency(data, out_dir / f"{volume_label}_latency.png")
    _chart_throughput(data, out_dir / f"{volume_label}_throughput.png")
    _chart_resources(data, out_dir / f"{volume_label}_resources.png")

    lines = [f"# {profile} workload -- {volume_label} working set\n"]
    lines.append("## Latency & throughput\n")
    lines.append(f"![latency]({volume_label}_latency.png)\n")
    lines.append(f"![throughput]({volume_label}_throughput.png)\n")
    lines.append("## Server-side resources\n")
    lines.append(f"![resources]({volume_label}_resources.png)\n")

    lines.append("## Data table\n")
    lines.append("| Tier | p50 (ms) | p95 (ms) | p99 (ms) | req/s | avg CPU% | avg IOPS (r/w) | metrics available |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for tier, d in data.items():
        agg = (d["stats"] or {}).get("aggregated") or {}
        atlas = d["atlas"]
        cpu = _avg_metric(atlas, "PROCESS_CPU_USER")
        iops_r = _avg_metric(atlas, "DISK_PARTITION_IOPS_READ")
        iops_w = _avg_metric(atlas, "DISK_PARTITION_IOPS_WRITE")
        req_s = agg.get("Requests/s")
        lines.append(
            f"| {tier} | {agg.get('50%', 'n/a')} | {agg.get('95%', 'n/a')} | {agg.get('99%', 'n/a')} | "
            f"{f'{float(req_s):.1f}' if req_s not in (None, '') else 'n/a'} | "
            f"{f'{cpu:.1f}' if cpu is not None else 'n/a'} | "
            f"{f'{iops_r:.0f}/{iops_w:.0f}' if iops_r is not None else 'n/a'} | "
            f"{'yes' if atlas.get('available') else 'NO (' + str(atlas.get('reason')) + ')'} |"
        )

    report_path = out_dir / f"{volume_label}_report.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    return report_path


def generate_cross_workload_report(volume_label: str, tiers: list[str]) -> Path:
    profiles = ["read_heavy", "balanced", "cpu_intensive"]
    out_dir = REPORTS_DIR / "cross_workload"
    out_dir.mkdir(parents=True, exist_ok=True)

    per_tier: dict[str, dict[str, dict]] = {tier: {} for tier in tiers}
    for profile in profiles:
        try:
            data = collect_profile_data(profile, volume_label)
        except SystemExit:
            continue
        for tier in tiers:
            if tier in data:
                per_tier[tier][profile] = data[tier]

    fig, ax = plt.subplots(figsize=(10, 6))
    width = 0.25
    x = range(len(tiers))
    for i, profile in enumerate(profiles):
        p99s = []
        for tier in tiers:
            agg = (per_tier[tier].get(profile, {}).get("stats") or {}).get("aggregated") or {}
            p99s.append(float(agg.get("99%", 0) or 0))
        ax.bar([j + (i - 1) * width for j in x], p99s, width, label=profile)
    ax.set_xticks(list(x))
    ax.set_xticklabels(tiers)
    ax.set_ylabel("p99 latency (ms)")
    ax.set_title(f"Cross-workload p99 latency by tier ({volume_label} working set)")
    ax.legend()
    fig.tight_layout()
    chart_path = out_dir / f"{volume_label}_cross_workload_p99.png"
    fig.savefig(chart_path)
    plt.close(fig)

    lines = [f"# Cross-workload comparison -- {volume_label} working set\n"]
    lines.append(f"![p99 comparison]({chart_path.name})\n")
    lines.append("| Tier | Profile | p50 | p95 | p99 | req/s |")
    lines.append("|---|---|---|---|---|---|")
    for tier in tiers:
        for profile in profiles:
            agg = (per_tier[tier].get(profile, {}).get("stats") or {}).get("aggregated") or {}
            if not agg:
                continue
            lines.append(
                f"| {tier} | {profile} | {agg.get('50%','n/a')} | {agg.get('95%','n/a')} | "
                f"{agg.get('99%','n/a')} | {agg.get('Requests/s','n/a')} |"
            )

    report_path = out_dir / f"{volume_label}_cross_workload_report.md"
    report_path.write_text("\n".join(lines))
    print(f"Wrote {report_path}")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=None, help="read_heavy/balanced/cpu_intensive")
    parser.add_argument("--volume", required=True, help="Volume label matching the results dir, e.g. small")
    parser.add_argument("--cross-workload", action="store_true", help="Generate the cross-workload-per-tier view instead")
    parser.add_argument("--tiers", nargs="+", default=["M0", "FLEX", "M10", "M30"])
    args = parser.parse_args()

    if args.cross_workload:
        generate_cross_workload_report(args.volume, args.tiers)
    else:
        if not args.profile:
            raise SystemExit("--profile is required unless --cross-workload is set")
        generate_profile_report(args.profile, args.volume)


if __name__ == "__main__":
    main()
