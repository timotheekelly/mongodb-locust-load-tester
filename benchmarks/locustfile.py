import os
import random
import threading
import time

from bson import BSON
from locust import events, task
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


class BenchUser(MongoDBUser):
    abstract = False

    conn_string = _CFG["mongo_uri"]
    db_name = _CFG["database"]
    collection_name = _CFG["collection"]

    # Shared across all simulated users.
    _initialized = False

    # Sequence numbers are identifiers, not document counts. Gaps caused by
    # failed writes are therefore harmless.
    _next_sequence = 0

    # Highest sequence known to exist when the benchmark starts. Reads can
    # also target newer sequences allocated during the run; misses are valid.
    _max_readable_sequence = -1

    def on_start(self) -> None:
        self._coll = self.client[self.db_name][self.collection_name]

        if not BenchUser._initialized:
            with _init_lock:
                if not BenchUser._initialized:
                    self._coll.create_index("seq", unique=True)

                    # count_documents() cannot determine the next sequence
                    # because seq values may contain gaps. Find the actual
                    # highest existing sequence instead.
                    latest = self._coll.find_one(
                        {"seq": {"$exists": True}},
                        projection={"seq": 1},
                        sort=[("seq", DESCENDING)],
                    )

                    if latest is not None:
                        highest_seq = latest["seq"]
                        BenchUser._next_sequence = highest_seq + 1
                        BenchUser._max_readable_sequence = highest_seq
                    else:
                        BenchUser._next_sequence = 0
                        BenchUser._max_readable_sequence = -1

                    BenchUser._initialized = True

    @classmethod
    def _allocate_seq(cls) -> int:
        """Allocate a unique sequence number.

        Locust's gevent execution model means this small in-memory operation
        runs without an I/O yield point between the read and increment.
        """
        seq = cls._next_sequence
        cls._next_sequence += 1
        return seq

    @classmethod
    def _mark_readable(cls, seq: int) -> None:
        """Expand the readable sequence range after a successful write."""
        if seq > cls._max_readable_sequence:
            cls._max_readable_sequence = seq

    @classmethod
    def _random_seq(cls) -> int | None:
        """Choose a sequence from the current readable range.

        Sequence numbers may contain gaps if writes have failed, so a point
        lookup can legitimately return no document.
        """
        if cls._max_readable_sequence < 0:
            return None

        return random.randint(0, cls._max_readable_sequence)

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