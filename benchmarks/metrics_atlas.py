"""Pull server-side CPU/RAM/IOPS/connection metrics from the Atlas Admin API,
scoped to a test run's time window.

Degrades gracefully: if ATLAS_PUBLIC_KEY/ATLAS_PRIVATE_KEY env vars aren't
set, or a project_id/cluster_name isn't supplied for a tier, this returns a
dict noting metrics are unavailable rather than raising -- callers should
still write that dict alongside the Locust CSVs so the report generator can
show "metrics missing for this tier" explicitly instead of silently
omitting it.

Uses HTTP Digest auth against the Atlas Admin API v2
(cloud.mongodb.com/api/atlas/v2), per
https://www.mongodb.com/docs/atlas/reference/api-resources-spec/v2/#tag/Monitoring-and-Logs
"""

import os
from datetime import datetime, timezone
from typing import Any

import requests
from requests.auth import HTTPDigestAuth

ATLAS_API_BASE = "https://cloud.mongodb.com/api/atlas/v2"
ATLAS_API_VERSION_HEADER = "application/vnd.atlas.2023-11-15+json"

# Metrics measured -- see Atlas Admin API "measurements" for the full list.
DEFAULT_MEASUREMENTS = [
    "PROCESS_CPU_USER",
    "PROCESS_CPU_KERNEL",
    "SYSTEM_MEMORY_USED",
    "SYSTEM_MEMORY_AVAILABLE",
    "DISK_PARTITION_IOPS_READ",
    "DISK_PARTITION_IOPS_WRITE",
    "CONNECTIONS",
]


def _atlas_credentials() -> tuple[str, str] | None:
    public_key = os.environ.get("ATLAS_PUBLIC_KEY")
    private_key = os.environ.get("ATLAS_PRIVATE_KEY")
    if not public_key or not private_key:
        return None
    return public_key, private_key


def fetch_process_metrics(
    project_id: str | None,
    cluster_name: str | None,
    start: datetime,
    end: datetime,
    granularity: str = "PT1M",
) -> dict[str, Any]:
    """Fetch CPU/RAM/IOPS/connection measurements for a cluster's processes
    over [start, end]. Returns a dict; on any missing config/credential or
    API failure, returns {"available": False, "reason": "..."} instead of
    raising, so a run's report can show the gap explicitly.
    """
    creds = _atlas_credentials()
    if creds is None:
        return {"available": False, "reason": "ATLAS_PUBLIC_KEY/ATLAS_PRIVATE_KEY not set in environment"}
    if not project_id or not cluster_name:
        return {"available": False, "reason": "atlas_project_id/atlas_cluster_name not configured for this tier"}

    try:
        processes = _list_processes(project_id, cluster_name, creds)
        if not processes:
            return {"available": False, "reason": f"no processes found for cluster '{cluster_name}'"}

        per_process: dict[str, Any] = {}
        for hostname_port in processes:
            measurements = _get_measurements(
                project_id, hostname_port, DEFAULT_MEASUREMENTS, start, end, granularity, creds
            )
            per_process[hostname_port] = measurements

        return {
            "available": True,
            "project_id": project_id,
            "cluster_name": cluster_name,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "processes": per_process,
        }
    except requests.RequestException as e:
        return {"available": False, "reason": f"Atlas API request failed: {e}"}


def _list_processes(project_id: str, cluster_name: str, creds: tuple[str, str]) -> list[str]:
    url = f"{ATLAS_API_BASE}/groups/{project_id}/processes"
    resp = requests.get(
        url,
        auth=HTTPDigestAuth(*creds),
        headers={"Accept": ATLAS_API_VERSION_HEADER},
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return [p["id"] for p in results if cluster_name.lower() in p.get("userAlias", "").lower() or cluster_name.lower() in p.get("id", "").lower()]


def _get_measurements(
    project_id: str,
    hostname_port: str,
    measurement_names: list[str],
    start: datetime,
    end: datetime,
    granularity: str,
    creds: tuple[str, str],
) -> dict[str, Any]:
    url = f"{ATLAS_API_BASE}/groups/{project_id}/processes/{hostname_port}/measurements"
    params = {
        "granularity": granularity,
        "start": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "m": measurement_names,
    }
    resp = requests.get(
        url,
        auth=HTTPDigestAuth(*creds),
        headers={"Accept": ATLAS_API_VERSION_HEADER},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("measurements", [])
