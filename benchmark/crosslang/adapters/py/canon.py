"""Canonical result serialization — byte-matching benchmark/crosslang/oracle.ts canonVal/canonRow.

Shared by the sdk (hand-SQL) + ir (interpreter) python cells. Hand-rolled to the SAME rules the
rust/go seams use (int bare, string JSON-quoted, bool→0/1, null) so every cell's stdout equals the
one dialect-independent sqlite oracle string. The per-op projected field order + the read/rel/tx
result shapes mirror oracle.ts (FIELDS / REL_FIELDS / stateSnapshot) exactly.
"""

from __future__ import annotations

import datetime
import json
from typing import Any, List, Mapping, Sequence


def canon_val(v: Any) -> str:
    """One value → the oracle's canonical token (mirror oracle.ts canonVal)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        # Integral floats print bare (the bench has no fractional projected value); mirrors JS Number.
        return str(int(v)) if v.is_integer() else repr(v)
    if isinstance(v, datetime.datetime):
        return json.dumps(v.strftime("%Y-%m-%d %H:%M:%S"), ensure_ascii=False)
    if isinstance(v, datetime.date):
        return json.dumps(v.strftime("%Y-%m-%d"), ensure_ascii=False)
    return json.dumps(str(v), ensure_ascii=False)


def canon_row(row: Mapping[str, Any], fields: Sequence[str]) -> str:
    return "{" + ",".join(f"{json.dumps(f)}:{canon_val(row.get(f))}" for f in fields) + "}"


def canon_rows(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str:
    return "[" + ",".join(canon_row(r, fields) for r in rows) + "]"


# The projected field order per op (== oracle.ts FIELDS == the native row struct field order).
FIELDS = {
    "findAll": ["id", "email", "name"],
    "findFirst": ["id", "email", "name"],
    "findUnique": ["id", "email", "name"],
    "filterPaginateSort": ["id", "title", "content", "published", "author_id", "created_at"],
    "upsert": ["id"],
}

# read+rel 2-LEVEL: parent fields + child (relation) fields + the relation key name (== oracle.ts REL_FIELDS).
REL_FIELDS = {
    "nestedFindAll": {"parent": ["id", "email", "name"], "child": ["id", "title", "author_id"], "rel": "posts"},
    "nestedFindFirst": {"parent": ["id", "email", "name"], "child": ["id", "title", "author_id"], "rel": "posts"},
    "nestedFindUnique": {"parent": ["id", "email", "name"], "child": ["id", "title", "author_id"], "rel": "posts"},
}

# read+rel FULL 3-LEVEL chain (#119): parent + level-2 (posts) + level-3 (comments) field orders (== oracle.ts REL3_FIELDS).
REL3_FIELDS = {
    "nestedRelations": {"parent": ["id", "email", "name"], "posts": ["id", "title", "author_id"], "comments": ["id", "body", "post_id"]},
    "compositeRelations": {"parent": ["tenant_id", "user_id", "name"], "posts": ["tenant_id", "post_id", "user_id", "title"], "comments": ["tenant_id", "comment_id", "post_id", "body"]},
}


def rel_json(rel: str, parents: List[str], child_lists: List[str]) -> str:
    """{"rows":[parent…],"<rel>":[[child…]…]} — the native T2 {rows, <rel>} shape (oracle.ts)."""
    return '{"rows":[' + ",".join(parents) + '],' + json.dumps(rel) + ":[" + ",".join(child_lists) + "]}"


def rel3_json(parents: List[str], posts_lists: List[str], comments_lists: List[str]) -> str:
    """{"rows":[parent…],"posts":[[post…]…],"comments":[[comment…]…]} — the native 3-level shape (oracle.ts)."""
    return (
        '{"rows":[' + ",".join(parents) + '],"posts":[' + ",".join(posts_lists) + '],"comments":[' + ",".join(comments_lists) + "]}"
    )


def tx_json(committed: bool, users: Sequence[Mapping[str, Any]], posts: Sequence[Mapping[str, Any]]) -> str:
    """{"committed":<b>,"state":{"users":[…],"posts":[…]}} — the write/tx affected-tables snapshot."""
    state = '{"users":' + canon_rows(users, ["id", "email", "name"]) + ',"posts":' + canon_rows(posts, ["id", "title", "author_id"]) + "}"
    return '{"committed":' + ("true" if committed else "false") + ',"state":' + state + "}"
