"""Shared document generator used by both seed.py and the Locust tasks.

Produces nested, realistic-shaped documents:
  - a handful of top-level scalar fields (queryable/updatable directly)
  - a genuinely nested substructure (recursive subdocuments with arrays of
    children), depth/width configurable
  - a filler field padding the document to an exact target BSON size

Document size is measured via real BSON encoding (bson.BSON.encode), not a
string-length estimate, so the padding calculation is accurate regardless of
driver/encoding overhead.
"""

import random
import string
import time
from typing import Any

from bson import BSON

# Safety cap: width ** depth is the total leaf-node fan-out of the nested
# structure. A bad config (e.g. depth=10, width=10) must not be able to make
# generation hang or blow up memory -- refuse anything past this.
MAX_NESTING_FANOUT = 200


def _random_string(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def validate_nesting(depth: int, width: int, max_fanout: int = MAX_NESTING_FANOUT) -> None:
    fanout = width**depth if depth > 0 else 1
    if fanout > max_fanout:
        raise ValueError(
            f"nesting_depth={depth} nesting_width={width} implies fan-out of "
            f"{fanout} nodes, exceeding the safety cap of {max_fanout}. "
            "Reduce depth/width or raise max_nesting_fanout explicitly."
        )


def _build_nested(depth: int, width: int, node_id: str) -> dict[str, Any]:
    node: dict[str, Any] = {
        "node_id": node_id,
        "label": _random_string(8),
        "value": random.random() * 1000,
    }
    if depth > 0:
        node["children"] = [_build_nested(depth - 1, width, f"{node_id}.{i}") for i in range(width)]
    else:
        node["children"] = []
    return node


def generate_document(
    seq: int,
    doc_size_bytes: int = 524288,
    nesting_depth: int = 3,
    nesting_width: int = 3,
    max_nesting_fanout: int = MAX_NESTING_FANOUT,
) -> dict[str, Any]:
    """Build one document of the shared shape, padded to exactly doc_size_bytes.

    `seq` is a sequential integer key, indexed at seed time, used for random
    point lookups/range scans/updates against the pre-seeded working set.
    """
    validate_nesting(nesting_depth, nesting_width, max_nesting_fanout)

    doc: dict[str, Any] = {
        "seq": seq,
        "category": random.choice(["a", "b", "c", "d", "e"]),
        "status": random.choice(["active", "inactive", "pending"]),
        "score": random.random() * 100,
        "created_at": time.time(),
        "tags": [_random_string(6) for _ in range(3)],
        "nested": _build_nested(nesting_depth, nesting_width, "root"),
        "filler": "",
    }

    current_size = len(BSON.encode(doc))
    pad_needed = doc_size_bytes - current_size
    if pad_needed > 0:
        # BSON string overhead is ~5 bytes (length prefix + null terminator);
        # oversize slightly then trim to hit the target exactly.
        doc["filler"] = _random_string(pad_needed)
        actual_size = len(BSON.encode(doc))
        overshoot = actual_size - doc_size_bytes
        if overshoot > 0:
            doc["filler"] = doc["filler"][:-overshoot]
    # If pad_needed <= 0, the nested structure alone already meets or exceeds
    # the target -- leave filler empty rather than truncating real fields.

    return doc


def measure_size(doc: dict[str, Any]) -> int:
    return len(BSON.encode(doc))
