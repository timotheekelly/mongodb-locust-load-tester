"""Derive a plain-language threshold framework from actual per-tier,
per-workload results -- e.g. "on the read-heavy workload, p99 latency held
steady on M0 up to N req/s but degraded sharply beyond that".

Thresholds are NOT hardcoded ahead of time: this reads the time-series CSV
Locust wrote during each run (locust_stats_history.csv, from --csv-full-history)
and looks for the point where p99 latency jumps sharply relative to its
running baseline, correlating against that point's throughput and (when
available) CPU%.

Usage:
    python -m benchmarks.decision_matrix --volume small
"""

import argparse
import csv
from pathlib import Path


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"

PROFILES = ["read_heavy", "balanced", "cpu_intensive"]
TIERS = ["M0", "FLEX", "M10", "M30"]

# A p99 jump of this multiple over the running baseline marks "degradation."
DEGRADATION_MULTIPLIER = 2.0
MIN_SAMPLES_FOR_BASELINE = 3


def _read_history(tier_dir: Path) -> list[dict]:
    history_path = tier_dir / "locust_stats_history.csv"
    if not history_path.exists():
        return []
    with open(history_path) as f:
        rows = list(csv.DictReader(f))
    return [r for r in rows if r.get("Name") == "Aggregated"]


def find_degradation_point(history: list[dict]) -> dict | None:
    """Walk the time series; once a running baseline of p99 is established,
    flag the first point where p99 exceeds DEGRADATION_MULTIPLIER x baseline.
    Returns that point's row (with 99%, Requests/s, Total Request Count) or
    None if no degradation was observed in the run.
    """
    if len(history) < MIN_SAMPLES_FOR_BASELINE + 1:
        return None

    baseline_samples = [_to_float(r.get("99%")) for r in history[:MIN_SAMPLES_FOR_BASELINE]]
    baseline = sum(baseline_samples) / len(baseline_samples)
    if baseline <= 0:
        return None

    for row in history[MIN_SAMPLES_FOR_BASELINE:]:
        p99 = _to_float(row.get("99%"))
        if p99 >= baseline * DEGRADATION_MULTIPLIER:
            return row
    return None


def analyze_tier_profile(profile: str, volume_label: str, tier: str) -> dict:
    tier_dir = RESULTS_DIR / profile / volume_label / tier
    if not tier_dir.exists():
        return {"available": False}

    history = _read_history(tier_dir)
    if not history:
        return {"available": False, "reason": "no locust_stats_history.csv (run with --csv-full-history)"}

    final = history[-1]
    degradation = find_degradation_point(history)

    return {
        "available": True,
        "final_p99": _to_float(final.get("99%")),
        "final_throughput": _to_float(final.get("Requests/s")),
        "degradation_point": degradation,
    }


def render_statement(profile: str, tier: str, analysis: dict) -> str:
    if not analysis.get("available"):
        return f"- **{tier} / {profile}**: no data ({analysis.get('reason', 'missing results')})."

    degradation = analysis.get("degradation_point")
    if degradation:
        throughput = _to_float(degradation.get("Requests/s"))
        p99 = _to_float(degradation.get("99%"))
        return (
            f"- **{tier} / {profile}**: p99 latency held steady, then degraded sharply "
            f"(jumped to {p99:.0f}ms) once throughput reached ~{throughput:.1f} req/s."
        )
    return (
        f"- **{tier} / {profile}**: no sharp degradation observed within this run's load range "
        f"(ended at {analysis['final_throughput']:.1f} req/s, p99={analysis['final_p99']:.0f}ms). "
        "Sustained load may need to go higher to find this tier's ceiling."
    )


def generate(volume_label: str) -> Path:
    lines = [f"# Decision matrix -- {volume_label} working set\n"]
    lines.append(
        "Derived from actual run results below. Read each line as: this tier, on this "
        "workload, showed its latency 'knee' at roughly this throughput -- useful for the "
        "\"aha, I'm hitting the same bottleneck\" framing.\n"
    )

    for profile in PROFILES:
        lines.append(f"## {profile}\n")
        for tier in TIERS:
            analysis = analyze_tier_profile(profile, volume_label, tier)
            lines.append(render_statement(profile, tier, analysis))
        lines.append("")

    out_dir = REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{volume_label}_decision_matrix.md"
    out_path.write_text("\n".join(lines))
    print(f"Wrote {out_path}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume", required=True, help="Volume label matching the results dir, e.g. small")
    args = parser.parse_args()
    generate(args.volume)


if __name__ == "__main__":
    main()
