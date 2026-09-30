import os
import random
import threading
import time

from bson import BSON
from locust import LoadTestShape, events, task
from locust.contrib.mongodb import MongoDBUser
from pymongo import DESCENDING

from benchmarks import docgen
from benchmarks.config import load_config


_PROFILE = os.environ.get("BENCH_PROFILE")
_CFG = load_config(profile=_PROFILE)

_WEIGHTS = _CFG.get("weights", {})
_DOC_SIZE = _CFG["doc_size_bytes"]
_NESTING_DEPTH = _CFG.get("nesting_depth", 3)
_NESTING_WIDTH = _CFG.get("nesting_width", 3)
_MAX_FANOUT = _CFG.get("max_nesting_fanout", docgen.MAX_NESTING_FANOUT)
_RANGE_LIMIT = _CFG.get("range_query_limit", 50)
_BULK_BATCH = _CFG.get("bulk_insert_batch_size", 100)


# Locust uses gevent, so this lock becomes cooperative after monkey-patching.
# It protects one-time collection setup across simulated users.
_init_lock = threading.Lock()


def _bson_size(document: dict | None) -> int:
    """Return the encoded BSON size of a single document."""
    if document is None:
        return 0
    return len(BSON.encode(document))


def _bson_size_many(documents: list[dict]) -> int:
    """Return the total encoded BSON size of a list of documents."""
    return sum(_bson_size(document) for document in documents)


def _fire(
    request_type: str,
    name: str,
    start: float,
    response_length: int = 0,
    exception=None,
) -> None:
    """Record a MongoDB operation in Locust."""
    response_time = (time.perf_counter() - start) * 1000

    events.request.fire(
        request_type=request_type,
        name=name,
        response_time=response_time,
        response_length=response_length,
        exception=exception,
    )


if os.environ.get("BENCH_STEP_LOAD", "").lower() in ("1", "true"):
    # Locust's core CLI dropped --step-load in favor of LoadTestShape classes;
    # this is only defined (and only takes effect) when explicitly requested
    # via BENCH_STEP_LOAD, so a normal -u/-r run isn't silently overridden.
    class StepLoadShape(LoadTestShape):
        step_users = int(os.environ["BENCH_STEP_USERS"])
        step_seconds = int(os.environ["BENCH_STEP_SECONDS"])
        spawn_rate = int(os.environ.get("BENCH_STEP_SPAWN_RATE", 10))
        max_users = int(os.environ["BENCH_MAX_USERS"])
        # A LoadTestShape makes Locust ignore --run-time entirely, so the
        # overall duration cap has to be enforced here instead.
        total_seconds = int(os.environ["BENCH_RUN_TIME_SECONDS"])

        def tick(self):
            run_time = self.get_run_time()
            if run_time >= self.total_seconds:
                return None
            current_step = run_time // self.step_seconds
            users = min(self.max_users, int((current_step + 1) * self.step_users))
            return (users, self.spawn_rate)


class BenchUser(MongoDBUser):
    abstract = False

    conn_string = _CFG["mongo_uri"]
    db_name = _CFG["database"]
    collection_name = _CFG["collection"]

    # Shared across all simulated users *within this process*. In
    # distributed mode (Locust master + worker pods on GKE), each worker is
    # a separate OS process with its own copy of this class -- there's no
    # cross-process coordination here, and none is needed:
    #
    # - Reads/updates only ever target the range that existed when THIS
    #   process started (`_max_readable_sequence`, fixed once at startup).
    #   Every worker sees the same pre-seeded data, so this needs no
    #   coordination even though each worker computes it independently.
    # - Inserts allocate from a per-process random namespace
    #   (`_insert_seq_base`) instead of a shared counter starting at the
    #   seeded max. Two workers independently reading "highest existing
    #   seq" and then both incrementing their own local counter from that
    #   same base would allocate the same seq values and collide on the
    #   unique index -- this is exactly the failure mode a naive shared
    #   counter would hit at scale. Randomizing each worker's insert range
    #   makes collisions between workers (and with the seeded range)
    #   astronomically unlikely without needing any cross-process locking.
    _initialized = False
    _insert_seq_base = 0
    _next_sequence = 0
    _max_readable_sequence = -1  # fixed at startup: the pre-seeded range every process shares
    _own_inserted_count = 0      # this process's own inserts, tracked separately -- see _random_seq

    def on_start(self) -> None:
        self._coll = self.client[self.db_name][self.collection_name]

        if not BenchUser._initialized:
            with _init_lock:
                if not BenchUser._initialized:
                    self._coll.create_index("seq", unique=True)

                    # Only used to size the readable range -- gaps from
                    # failed writes don't matter for that, so
                    # count_documents() (cheaper than a sorted find) is fine
                    # here, unlike when computing a shared insert base.
                    latest = self._coll.find_one(
                        {"seq": {"$exists": True}},
                        projection={"seq": 1},
                        sort=[("seq", DESCENDING)],
                    )
                    BenchUser._max_readable_sequence = latest["seq"] if latest is not None else -1

                    # A random 48-bit-plus offset, unique per process with
                    # overwhelming probability, well clear of any realistic
                    # seeded seq range.
                    BenchUser._insert_seq_base = random.SystemRandom().randrange(2**48, 2**62)
                    BenchUser._next_sequence = BenchUser._insert_seq_base

                    BenchUser._initialized = True

    @classmethod
    def _allocate_seq(cls) -> int:
        """Allocate a unique sequence number from this process's insert range.

        Locust's gevent execution model means this small in-memory operation
        runs without an I/O yield point between the read and increment.
        """
        seq = cls._next_sequence
        cls._next_sequence += 1
        return seq

    @classmethod
    def _mark_readable(cls, seq: int) -> None:
        """Let this process read back its own inserts, in addition to the
        pre-seeded range every process starts with.

        Inserted seqs live in a separate, far-away random range (see
        on_start), so counting them (rather than folding them into
        _max_readable_sequence) is what lets _random_seq pick one without
        landing in the empty gap between the two ranges.
        """
        if seq >= cls._insert_seq_base:
            cls._own_inserted_count += 1

    @classmethod
    def _random_seq(cls) -> int | None:
        """Choose a sequence to read/update: either from the pre-seeded
        range every process starts with, or (occasionally) from one of this
        process's own successful inserts.

        Sequence numbers may contain gaps if writes have failed, so a point
        lookup can legitimately return no document even for a chosen seq
        that should exist.
        """
        seeded_size = cls._max_readable_sequence + 1
        total = seeded_size + cls._own_inserted_count
        if total <= 0:
            return None

        pick = random.randint(0, total - 1)
        if pick < seeded_size:
            return pick
        # One of this process's own inserts. Gaps from failed inserts mean
        # this isn't guaranteed to hit an existing doc, same as the seeded
        # range -- that's fine, see the docstring above.
        return cls._insert_seq_base + (pick - seeded_size)

    @task(_WEIGHTS.get("insert", 1))
    def insert_one(self):
        seq = self._allocate_seq()

        doc = docgen.generate_document(
            seq,
            _DOC_SIZE,
            _NESTING_DEPTH,
            _NESTING_WIDTH,
            _MAX_FANOUT,
        )

        start = time.perf_counter()

        try:
            self._coll.insert_one(doc)
            self._mark_readable(seq)

            _fire(
                "MONGODB",
                "INSERT_ONE",
                start,
                response_length=_bson_size(doc),
            )

        except Exception as exc:
            _fire(
                "MONGODB",
                "INSERT_ONE",
                start,
                exception=exc,
            )

    @task(_WEIGHTS.get("point_lookup", 1))
    def point_lookup(self):
        seq = self._random_seq()

        if seq is None:
            return

        start = time.perf_counter()

        try:
            doc = self._coll.find_one({"seq": seq})

            _fire(
                "MONGODB",
                "POINT_LOOKUP",
                start,
                response_length=_bson_size(doc),
            )

        except Exception as exc:
            _fire(
                "MONGODB",
                "POINT_LOOKUP",
                start,
                exception=exc,
            )

    @task(_WEIGHTS.get("range_query", 1))
    def range_query(self):
        start_seq = self._random_seq()

        if start_seq is None:
            return

        start = time.perf_counter()

        try:
            cursor = (
                self._coll
                .find({"seq": {"$gte": start_seq}})
                .sort("seq", 1)
                .limit(_RANGE_LIMIT)
            )

            results = list(cursor)

            _fire(
                "MONGODB",
                "RANGE_QUERY",
                start,
                response_length=_bson_size_many(results),
            )

        except Exception as exc:
            _fire(
                "MONGODB",
                "RANGE_QUERY",
                start,
                exception=exc,
            )

    @task(_WEIGHTS.get("update", 1))
    def update_one(self):
        seq = self._random_seq()

        if seq is None:
            return

        start = time.perf_counter()

        try:
            self._coll.update_one(
                {"seq": seq},
                {
                    "$set": {
                        "status": random.choice(
                            ["active", "inactive", "pending"]
                        ),
                        "score": random.random() * 100,
                    }
                },
            )

            _fire(
                "MONGODB",
                "UPDATE_ONE",
                start,
                response_length=0,
            )

        except Exception as exc:
            _fire(
                "MONGODB",
                "UPDATE_ONE",
                start,
                exception=exc,
            )

    @task(_WEIGHTS.get("bulk_insert", 1))
    def bulk_insert(self):
        sequences = [
            self._allocate_seq()
            for _ in range(_BULK_BATCH)
        ]

        docs = [
            docgen.generate_document(
                seq,
                _DOC_SIZE,
                _NESTING_DEPTH,
                _NESTING_WIDTH,
                _MAX_FANOUT,
            )
            for seq in sequences
        ]

        start = time.perf_counter()

        try:
            self._coll.insert_many(
                docs,
                ordered=False,
            )

            self._mark_readable(max(sequences))

            _fire(
                "MONGODB",
                "BULK_INSERT",
                start,
                response_length=_bson_size_many(docs),
            )

        except Exception as exc:
            # With ordered=False MongoDB may have successfully inserted some
            # documents before reporting an error. Sequence gaps are allowed,
            # so advancing the readable range remains safe.
            self._mark_readable(max(sequences))

            _fire(
                "MONGODB",
                "BULK_INSERT",
                start,
                exception=exc,
            )

    @task(_WEIGHTS.get("aggregate", 1))
    def aggregate(self):
        start = time.perf_counter()

        try:
            pipeline = [
                {
                    "$match": {
                        "category": random.choice(
                            ["a", "b", "c", "d", "e"]
                        )
                    }
                },
                {
                    "$group": {
                        "_id": "$status",
                        "avg_score": {"$avg": "$score"},
                        "max_score": {"$max": "$score"},
                        "count": {"$sum": 1},
                    }
                },
                {
                    "$sort": {
                        "avg_score": -1
                    }
                },
            ]

            results = list(
                self._coll.aggregate(pipeline)
            )

            _fire(
                "MONGODB",
                "AGGREGATE",
                start,
                response_length=_bson_size_many(results),
            )

        except Exception as exc:
            _fire(
                "MONGODB",
                "AGGREGATE",
                start,
                exception=exc,
            )