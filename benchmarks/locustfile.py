"""Standalone Locust workload simulator for MongoDB -- run it directly, no
separate seed step required:

    locust -f benchmarks/locustfile.py

then open http://localhost:8089 for the web UI, or run headless:

    BENCH_PROFILE=read_heavy BENCH_MONGO_URI="mongodb+srv://..." \
        locust -f benchmarks/locustfile.py --headless -u 10 -r 5 -t 5m

On first use it creates the collection/index if they don't exist and builds
up its own working set as it runs (inserts grow the readable key space
live, like a real app would). If you point it at a collection already
pre-seeded by benchmarks/seed.py (e.g. via the orchestrator, for a fixed
target data volume across tiers), it picks up from that existing data
instead of starting empty -- both are valid ways to use this file.

Built on locust.contrib.mongodb.MongoDBUser, which wraps PyMongo. Note this
file does NOT use MongoDBUser's own `self.client.db` attribute or its
`execute_query` helper: upstream's `MongoDBClient.__init__` does
`self.db = self.client[db_name]`, and `self.client` there is *attribute*
access on a MongoClient -- which pymongo defines as shorthand for
`self["client"]` (a database literally named "client"), not "myself". So
upstream's `self.db` actually ends up as a Collection named `db_name` living
inside a database called `client`, and `execute_query`/`self.db[coll_name]`
then nests a *second* level in, producing a dotted collection name like
`client.<db_name>.<collection_name>` -- silently bypassing the real target
database entirely. All access here goes through `self.client[db_name]`
(subscript access on the MongoClient), which is unambiguous and correct.

Every operation is timed by firing Locust's `request` event manually, since
none of them go through the base class's (broken) built-in timing.
"""

import os
import random
import threading
import time

from locust import events, task
from locust.contrib.mongodb import MongoDBUser

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

# Locust runs under gevent, which patches threading.Lock into a cooperative
# (non-OS) lock -- this makes the one-time init below safe even though
# multiple simulated users can call on_start() concurrently.
_init_lock = threading.Lock()


def _fire(request_type: str, name: str, start: float, response_length: int = 0, exception=None) -> None:
    response_time = (time.time() - start) * 1000
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

    _initialized = False  # guards one-time index/count setup, shared across simulated users
    _doc_count = 0        # next seq to hand out; also the current size of the readable working set

    def on_start(self) -> None:
        # Subscript access (self.client[name]) -- NOT attribute access
        # (self.client.name) -- is the only reliable way to get a Database
        # from a MongoClient; see module docstring.
        self._coll = self.client[self.db_name][self.collection_name]

        if not BenchUser._initialized:
            # Double-checked locking: many users can reach this concurrently
            # (create_index/count_documents are network calls, so they yield
            # the greenlet) -- without the lock, several could each read a
            # stale count and clobber _doc_count after others had already
            # started incrementing it, producing duplicate `seq` values.
            with _init_lock:
                if not BenchUser._initialized:
                    self._coll.create_index("seq", unique=True)
                    BenchUser._doc_count = self._coll.count_documents({})
                    BenchUser._initialized = True

    def _next_seq(self) -> int:
        """Hand out the next seq and grow the working set by one.

        Shared as a plain class attribute rather than a lock: Locust's
        gevent-based concurrency model runs one greenlet at a time between
        I/O yield points, so this increment can't be preempted mid-statement.
        """
        seq = BenchUser._doc_count
        BenchUser._doc_count += 1
        return seq

    def _random_seq(self) -> int | None:
        """A random seq from the current working set, or None if it's still empty
        (e.g. the very start of a run against a fresh, unseeded collection)."""
        if BenchUser._doc_count == 0:
            return None
        return random.randint(0, BenchUser._doc_count - 1)

    @task(_WEIGHTS.get("insert", 1))
    def insert_one(self):
        doc = docgen.generate_document(self._next_seq(), _DOC_SIZE, _NESTING_DEPTH, _NESTING_WIDTH, _MAX_FANOUT)
        start = time.time()
        try:
            self._coll.insert_one(doc)
            _fire("MONGODB", "INSERT_ONE", start, response_length=_DOC_SIZE)
        except Exception as e:
            _fire("MONGODB", "INSERT_ONE", start, exception=e)

    @task(_WEIGHTS.get("point_lookup", 1))
    def point_lookup(self):
        seq = self._random_seq()
        if seq is None:
            return
        start = time.time()
        try:
            doc = self._coll.find_one({"seq": seq})
            _fire("MONGODB", "QUERY", start, response_length=1 if doc else 0)
        except Exception as e:
            _fire("MONGODB", "QUERY", start, exception=e)

    @task(_WEIGHTS.get("range_query", 1))
    def range_query(self):
        start_seq = self._random_seq()
        if start_seq is None:
            return
        start = time.time()
        try:
            cursor = self._coll.find({"seq": {"$gte": start_seq}}).sort("seq", 1).limit(_RANGE_LIMIT)
            results = list(cursor)
            _fire("MONGODB", "RANGE_QUERY", start, response_length=len(results))
        except Exception as e:
            _fire("MONGODB", "RANGE_QUERY", start, exception=e)

    @task(_WEIGHTS.get("update", 1))
    def update_one(self):
        seq = self._random_seq()
        if seq is None:
            return
        start = time.time()
        try:
            self._coll.update_one(
                {"seq": seq},
                {"$set": {"status": random.choice(["active", "inactive", "pending"]), "score": random.random() * 100}},
            )
            _fire("MONGODB", "UPDATE_ONE", start)
        except Exception as e:
            _fire("MONGODB", "UPDATE_ONE", start, exception=e)

    @task(_WEIGHTS.get("bulk_insert", 1))
    def bulk_insert(self):
        docs = [
            docgen.generate_document(self._next_seq(), _DOC_SIZE, _NESTING_DEPTH, _NESTING_WIDTH, _MAX_FANOUT)
            for _ in range(_BULK_BATCH)
        ]
        start = time.time()
        try:
            self._coll.insert_many(docs, ordered=False)
            _fire("MONGODB", "BULK_INSERT", start, response_length=_DOC_SIZE * len(docs))
        except Exception as e:
            _fire("MONGODB", "BULK_INSERT", start, exception=e)

    @task(_WEIGHTS.get("aggregate", 1))
    def aggregate(self):
        start = time.time()
        try:
            pipeline = [
                {"$match": {"category": random.choice(["a", "b", "c", "d", "e"])}},
                {
                    "$group": {
                        "_id": "$status",
                        "avg_score": {"$avg": "$score"},
                        "max_score": {"$max": "$score"},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"avg_score": -1}},
            ]
            results = list(self._coll.aggregate(pipeline))
            _fire("MONGODB", "AGGREGATE", start, response_length=len(results))
        except Exception as e:
            _fire("MONGODB", "AGGREGATE", start, exception=e)
