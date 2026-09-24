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
# Flex-tier clusters live under a separate /flexClusters resource, not
# /clusters, and only exist in a newer API version than the rest of this
# module uses -- see _get_cluster_hostnames.
ATLAS_FLEX_API_VERSION_HEADER = "application/vnd.atlas.2024-11-13+json"

# Metrics measured -- see Atlas Admin API "measurements" for the full list.
# CPU/memory/connections are process-level; IOPS is only exposed per disk
# *partition* (a sub-resource of a process), not at the process level -- a
# process-level request for DISK_PARTITION_IOPS_* returns a 404
# INVALID_METRIC_NAME, so it's fetched separately via _get_disk_measurements.
PROCESS_MEASUREMENTS = [
    "PROCESS_CPU_USER",
    "PROCESS_CPU_KERNEL",
    "SYSTEM_MEMORY_USED",
    "SYSTEM_MEMORY_AVAILABLE",
    "CONNECTIONS",
]
DISK_MEASUREMENTS = [
    "DISK_PARTITION_IOPS_READ",
    "DISK_PARTITION_IOPS_WRITE",
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
            return {
                "available": False,
                "reason": (
                    f"no processes found for cluster '{cluster_name}' -- if this is a Flex-tier "
                    "cluster, this is expected: Flex doesn't expose process-level monitoring via "
                    "the Atlas API at all (similar to, but more limited than, M0's partial support)"
                ),
            }

        per_process: dict[str, Any] = {}
        for hostname_port in processes:
            measurements = _get_measurements(
                project_id, hostname_port, PROCESS_MEASUREMENTS, start, end, granularity, creds
            )
            measurements += _get_disk_measurements(
                project_id, hostname_port, DISK_MEASUREMENTS, start, end, granularity, creds
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


def _get_cluster_hostnames(project_id: str, cluster_name: str, creds: tuple[str, str]) -> set[str]:
    """Return the real hostnames (no port) backing a cluster, from its
    connection string. Needed because matching by cluster *name* against
    process records is unreliable -- see _list_processes.

    Tries the regular dedicated/shared-tier /clusters resource first, then
    falls back to /flexClusters (Flex-tier clusters -- e.g. the successor to
    M2/M5 -- aren't visible under /clusters at all, and only exist in a
    newer API version than the rest of this module targets).
    """
    url = f"{ATLAS_API_BASE}/groups/{project_id}/clusters/{cluster_name}"
    resp = requests.get(
        url,
        auth=HTTPDigestAuth(*creds),
        headers={"Accept": ATLAS_API_VERSION_HEADER},
        timeout=30,
    )
    if resp.status_code in (400, 404):
        flex_url = f"{ATLAS_API_BASE}/groups/{project_id}/flexClusters/{cluster_name}"
        resp = requests.get(
            flex_url,
            auth=HTTPDigestAuth(*creds),
            headers={"Accept": ATLAS_FLEX_API_VERSION_HEADER},
            timeout=30,
        )
    resp.raise_for_status()
    standard = resp.json().get("connectionStrings", {}).get("standard", "")
    hosts_part = standard.split("://", 1)[-1].split("/", 1)[0]
    return {h.split(":")[0] for h in hosts_part.split(",") if h}


def _list_processes(project_id: str, cluster_name: str, creds: tuple[str, str]) -> list[str]:
    # Match by real hostname, not by cluster name string matching. Process
    # records' "id" field is an internal replica-set node name that doesn't
    # always contain the cluster's friendly name (this is normal for shared
    # tiers like M0 -- id is e.g. "atlas-tjeq4l-shard-00-00...", with no
    # "benchmark-m0" in it anywhere, even though it *is* that cluster).
    # "userAlias", however, always matches the cluster's real connection
    # string hostname, so we resolve hostnames from the cluster's own
    # connection string first and match against that instead.
    hostnames = _get_cluster_hostnames(project_id, cluster_name, creds)

    url = f"{ATLAS_API_BASE}/groups/{project_id}/processes"
    resp = requests.get(
        url,
        auth=HTTPDigestAuth(*creds),
        headers={"Accept": ATLAS_API_VERSION_HEADER},
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return [p["id"] for p in results if p.get("userAlias", "").split(":")[0] in hostnames]


def _get_measurements_with_fallback(
    url: str,
    measurement_names: list[str],
    start: datetime,
    end: datetime,
    granularity: str,
    creds: tuple[str, str],
) -> list[dict]:
    """GET a measurements endpoint, dropping any metric name the server
    rejects as invalid and retrying, rather than failing the whole request.

    Different tiers support different metric sets -- e.g. M0 (shared/free)
    doesn't expose SYSTEM_MEMORY_USED/AVAILABLE at all, where dedicated
    tiers do. A single unsupported name in the list otherwise makes the API
    reject the entire batch with 404 INVALID_METRIC_NAME.
    """
    remaining = list(measurement_names)
    while remaining:
        params = {
            "granularity": granularity,
            "start": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "m": remaining,
        }
        resp = requests.get(
            url,
            auth=HTTPDigestAuth(*creds),
            headers={"Accept": ATLAS_API_VERSION_HEADER},
            params=params,
            timeout=30,
        )
        if resp.status_code == 404:
            body = resp.json()
            if body.get("errorCode") == "INVALID_METRIC_NAME":
                invalid = body.get("parameters", [None])[0]
                if invalid in remaining:
                    remaining.remove(invalid)
                    continue
        resp.raise_for_status()
        return resp.json().get("measurements", [])
    return []


def _get_measurements(
    project_id: str,
    hostname_port: str,
    measurement_names: list[str],
    start: datetime,
    end: datetime,
    granularity: str,
    creds: tuple[str, str],
) -> list[dict]:
    url = f"{ATLAS_API_BASE}/groups/{project_id}/processes/{hostname_port}/measurements"
    return _get_measurements_with_fallback(url, measurement_names, start, end, granularity, creds)


def _list_disk_partitions(project_id: str, hostname_port: str, creds: tuple[str, str]) -> list[str]:
    url = f"{ATLAS_API_BASE}/groups/{project_id}/processes/{hostname_port}/disks"
    resp = requests.get(
        url,
        auth=HTTPDigestAuth(*creds),
        headers={"Accept": ATLAS_API_VERSION_HEADER},
        timeout=30,
    )
    resp.raise_for_status()
    return [d["partitionName"] for d in resp.json().get("results", [])]


def _get_disk_measurements(
    project_id: str,
    hostname_port: str,
    measurement_names: list[str],
    start: datetime,
    end: datetime,
    granularity: str,
    creds: tuple[str, str],
) -> list[dict]:
    """IOPS is exposed per disk partition (usually just "data"), not at the
    process level -- see the module-level comment on DISK_MEASUREMENTS."""
    partitions = _list_disk_partitions(project_id, hostname_port, creds)
    all_measurements: list[dict] = []
    for partition in partitions:
        url = f"{ATLAS_API_BASE}/groups/{project_id}/processes/{hostname_port}/disks/{partition}/measurements"
        measurements = _get_measurements_with_fallback(url, measurement_names, start, end, granularity, creds)
        for measurement in measurements:
            measurement["partitionName"] = partition
            all_measurements.append(measurement)
    return all_measurements
