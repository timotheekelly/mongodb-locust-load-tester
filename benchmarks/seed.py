"""Seeding script: pre-populate a collection to a target data volume.

Usage:
    python -m benchmarks.seed --profile read_heavy --volume small \
        --mongo-uri "mongodb+srv://..." --tier-label M10

Computes the target document count internally from target_volume / doc_size,
then generates and inserts documents (via the shared docgen module) until
that count is reached. Drops and recreates the collection first so every run
starts from identical, freshly-seeded data.
"""

import argparse
import sys
import time

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from benchmarks import docgen
from benchmarks.config import load_config, resolve_volume

# Rough throughput assumption used only to print a time estimate up front,
# not a guarantee -- actual seeding speed depends on network/tier.
ESTIMATED_INSERTS_PER_SEC = 500


def check_tier_storage_limit(cfg: dict, tier_label: str | None, target_volume_bytes: int) -> None:
    if not tier_label:
        return
    tier_testing_limits = cfg.get("tier_testing_limits", {})
    limit = tier_testing_limits.get(tier_label.upper(), {}).get("max_storage_bytes")
    if limit is not None and target_volume_bytes > limit:
        raise SystemExit(
            f"Refusing to seed: target volume {target_volume_bytes / 1024**3:.2f}GB exceeds "
            f"tier '{tier_label}'s storage cap of {limit / 1024**3:.2f}GB. "
            "Pick a smaller --volume or a larger tier."
        )


def seed(cfg: dict, target_volume_bytes: int, tier_label: str | None = None) -> int:
    check_tier_storage_limit(cfg, tier_label, target_volume_bytes)

    doc_size = cfg["doc_size_bytes"]
    doc_count = max(1, target_volume_bytes // doc_size)
    batch_size = cfg.get("seed_batch_size", 100)

    est_seconds = doc_count / ESTIMATED_INSERTS_PER_SEC
    print(
        f"Seeding target: {target_volume_bytes / 1024**3:.2f}GB / {doc_size} bytes per doc "
        f"= {doc_count} documents. Rough estimate at ~{ESTIMATED_INSERTS_PER_SEC} inserts/sec: "
        f"~{est_seconds / 60:.1f} minutes (varies a lot with tier and network)."
    )
    if target_volume_bytes >= 500 * 1024**3:
        print(
            "WARNING: 500GB seeding is a real storage/time commitment -- this can take "
            "hours and will consume the full 500GB on the target cluster's disk. "
            "Make sure the tier's storage capacity and your patience both allow for it."
        )

    client = MongoClient(cfg["mongo_uri"])
    db = client[cfg["database"]]
    coll = db[cfg["collection"]]

    print(f"Dropping existing collection {cfg['database']}.{cfg['collection']} (if any)...")
    coll.drop()
    coll.create_index("seq", unique=True)

    nesting_depth = cfg.get("nesting_depth", 3)
    nesting_width = cfg.get("nesting_width", 3)
    max_fanout = cfg.get("max_nesting_fanout", docgen.MAX_NESTING_FANOUT)

    start = time.time()
    inserted = 0
    seq = 0
    while inserted < doc_count:
        n = min(batch_size, doc_count - inserted)
        batch = [
            docgen.generate_document(seq + i, doc_size, nesting_depth, nesting_width, max_fanout)
            for i in range(n)
        ]
        try:
            coll.insert_many(batch, ordered=False)
        except BulkWriteError as e:
            write_errors = e.details.get("writeErrors", [])
            duplicates = [err for err in write_errors if err.get("code") == 11000]
            other = [err for err in write_errors if err.get("code") != 11000]
            if other:
                # Something other than a duplicate key -- summarize (never dump
                # the full failed document, which includes the large filler
                # field) and stop, since this isn't a case we know how to
                # recover from safely.
                codes = sorted({err.get("code") for err in other})
                raise RuntimeError(
                    f"insert_many failed with {len(other)} non-duplicate-key error(s) "
                    f"(codes: {codes}); first message: {other[0].get('errmsg')}"
                ) from None
            if duplicates:
                # Another writer already has these seq values (e.g. a stray
                # process from an earlier run against this same collection).
                # With ordered=False, every non-conflicting doc in this batch
                # still got inserted -- treat this as a warning, not fatal.
                dup_seqs = [err["keyValue"].get("seq") for err in duplicates]
                print(
                    f"  WARNING: {len(duplicates)} duplicate-key error(s) in this batch "
                    f"(seq {min(dup_seqs)}-{max(dup_seqs)} already existed -- likely a "
                    "stray writer against this collection). Continuing."
                )
        seq += n
        inserted += n
        if inserted % (batch_size * 20) == 0 or inserted == doc_count:
            elapsed = time.time() - start
            rate = inserted / elapsed if elapsed > 0 else 0
            print(f"  {inserted}/{doc_count} docs inserted ({rate:.0f}/sec)")

    elapsed = time.time() - start
    print(f"Done: {inserted} documents seeded in {elapsed:.1f}s.")
    client.close()
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=None, help="Workload profile name (read_heavy/balanced/cpu_intensive)")
    parser.add_argument("--volume", default="small", help="Target volume: small/medium/large or a raw byte count")
    parser.add_argument("--mongo-uri", default=None, help="Overrides config mongo_uri")
    parser.add_argument("--database", default=None)
    parser.add_argument("--collection", default=None)
    parser.add_argument("--tier-label", default=None, help="M0/FLEX/M10/M30 -- enables storage cap safety check")
    args = parser.parse_args()

    overrides = {}
    if args.mongo_uri:
        overrides["mongo_uri"] = args.mongo_uri
    if args.database:
        overrides["database"] = args.database
    if args.collection:
        overrides["collection"] = args.collection

    cfg = load_config(profile=args.profile, overrides=overrides)
    target_volume_bytes = resolve_volume(args.volume)

    try:
        seed(cfg, target_volume_bytes, tier_label=args.tier_label)
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
