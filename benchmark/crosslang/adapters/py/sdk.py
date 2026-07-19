"""The python SDK BASELINE — raw driver + hand-SQL for benchmark_* (the fair 1.0x denominator).

The python twin of the go SDK cell (adapters/go/sdk.go): reads rows DIRECTLY by column name (NOT via
the litedbmodel runtime — that is the ir cell). Dialect-aware placeholders (`?` normalized to `%s` for
pg/psycopg + mysql/pymysql by db._bind; pg arrays bind a list to `= ANY(?)`). mysql has no RETURNING,
so upsert + tx-chain inserts do an explicit strip+re-select (mirror the seam behavior, not the code).
v1-faithful returning (upsert→[{id}], no-returning writes→null, tx→{committed,state}).
"""

from __future__ import annotations

from typing import Any, Dict, List

from canon import FIELDS, REL_FIELDS, canon_rows, rel_json, tx_json
from db import RawDB

BATCH_EMAILS = [f"many{i}@bench.com" for i in range(10)]
BATCH_NAMES = [f"Many {i}" for i in range(10)]
UPSERT_EMAILS = ["user1@example.com", "user2@example.com"] + BATCH_EMAILS[:8]


# ── reads ──
def _find_all(db: RawDB) -> str:
    return canon_rows(db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100"), FIELDS["findAll"])


def _find_first(db: RawDB) -> str:
    return canon_rows(db.query("SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", ["User%"]), FIELDS["findFirst"])


def _find_unique(db: RawDB) -> str:
    return canon_rows(db.query("SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", ["user500@example.com"]), FIELDS["findUnique"])


def _filter_paginate_sort(db: RawDB) -> str:
    # `published` is BOOLEAN on pg (psycopg rejects int=bool), int on sqlite/mysql — bind per dialect.
    published = True if db.dialect == "postgres" else 1
    rows = db.query("SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10", [published])
    return canon_rows(rows, FIELDS["filterPaginateSort"])


# ── single writes (v1: no-returning → null; upsert → [{id}]) ──
def _create(db: RawDB) -> str:
    db.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", ["new@bench.com", "New"])
    return "null"


def _update(db: RawDB) -> str:
    db.execute("UPDATE benchmark_users SET name = ? WHERE id = ?", ["Updated 100", 100])
    return "null"


def _upsert(db: RawDB) -> str:
    if db.dialect == "mysql":
        db.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)", ["user1@example.com", "Upserted One"])
        rows = db.query("SELECT id FROM benchmark_users WHERE email = ? ORDER BY id", ["user1@example.com"])
    else:
        rows = db.query("INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id", ["user1@example.com", "Upserted One"])
    return canon_rows(rows, FIELDS["upsert"])


# ── batch writes (v1: no-returning → null) ──
def _insert_many_values(db: RawDB, emails: List[str], names: List[str], tail: str) -> str:
    tuples = ", ".join("(?, ?)" for _ in emails)
    params: List[Any] = []
    for e, n in zip(emails, names):
        params += [e, n]
    db.execute(f"INSERT INTO benchmark_users (email, name) VALUES {tuples}{tail}", params)
    return "null"


def _create_many(db: RawDB) -> str:
    return _insert_many_values(db, BATCH_EMAILS, BATCH_NAMES, "")


def _upsert_many(db: RawDB) -> str:
    tail = (
        " ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)"
        if db.dialect == "mysql"
        else " ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name"
    )
    return _insert_many_values(db, UPSERT_EMAILS, BATCH_NAMES, tail)


def _update_many(db: RawDB) -> str:
    # hand-OPTIMIZED single CASE update (not a per-row loop); NO returning (v1) → null.
    cases = " ".join(f"WHEN {i + 1} THEN ?" for i in range(10))
    db.execute(f"UPDATE benchmark_users SET name = CASE id {cases} END WHERE id IN (1,2,3,4,5,6,7,8,9,10)", list(BATCH_NAMES))
    return "null"


# ── read + relation (parent + ONE batched IN child, N+1 avoided) ──
def _in_clause(db: RawDB, keys: List[Any]):
    if db.dialect == "postgres":
        return "= ANY(?)", [list(keys)]
    marks = ", ".join("?" for _ in keys)
    return f"IN ({marks})", list(keys)


def _rel_single(db: RawDB, parent_sql, parent_params, parent_key, parent_fields, child_sql_tmpl, child_key, child_fields, rel) -> str:
    parents = db.query(parent_sql, parent_params)
    keys = [r[parent_key] for r in parents]
    in_clause, child_params = _in_clause(db, keys)
    children = db.query(child_sql_tmpl.replace("{IN}", in_clause), child_params)
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    for c in children:
        groups.setdefault(c[child_key], []).append(c)
    from canon import canon_row  # local: per-row projection

    ps = [canon_row(r, parent_fields) for r in parents]
    cs = ["[" + ",".join(canon_row(c, child_fields) for c in groups.get(r[parent_key], [])) + "]" for r in parents]
    return rel_json(rel, ps, cs)


def _rel(op: str):
    m = REL_FIELDS[op]
    return m["parent"], m["child"], m["rel"]


def _nested_find_all(db: RawDB) -> str:
    p, c, rel = _rel("nestedFindAll")
    return _rel_single(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", [], "id", p,
                       "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", c, rel)


def _nested_find_first(db: RawDB) -> str:
    p, c, rel = _rel("nestedFindFirst")
    return _rel_single(db, "SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", ["User%"], "id", p,
                       "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", c, rel)


def _nested_find_unique(db: RawDB) -> str:
    p, c, rel = _rel("nestedFindUnique")
    return _rel_single(db, "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", ["user1@example.com"], "id", p,
                       "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", c, rel)


# FULL 3-level SDK (#119): THREE batched queries (parents; level-2 by parent key; level-3 by ALL
# level-2 ids) + client stitch into {rows,posts,comments} (comments flattened per parent, in post order).
def _stitch3(db: RawDB, parents, parent_key, posts_sql_tmpl, posts_key, posts_id, comments_sql_tmpl, comments_key) -> str:
    from canon import REL3_FIELDS, canon_row, canon_rows, rel3_json

    op = "nestedRelations" if parent_key == "id" else "compositeRelations"
    m = REL3_FIELDS[op]
    pkeys = [r[parent_key] for r in parents]
    p_in, p_params = _in_clause(db, pkeys)
    posts = db.query(posts_sql_tmpl.replace("{IN}", p_in), p_params) if pkeys else []
    pids = [r[posts_id] for r in posts]
    c_in, c_params = _in_clause(db, pids)
    comments = db.query(comments_sql_tmpl.replace("{IN}", c_in), c_params) if pids else []
    posts_by_parent: Dict[Any, List[Dict[str, Any]]] = {}
    for p in posts:
        posts_by_parent.setdefault(p[posts_key], []).append(p)
    comments_by_post: Dict[Any, List[Dict[str, Any]]] = {}
    for c in comments:
        comments_by_post.setdefault(c[comments_key], []).append(c)
    rows_s, posts_s, comments_s = [], [], []
    for r in parents:
        ps = posts_by_parent.get(r[parent_key], [])
        rows_s.append(canon_row(r, m["parent"]))
        posts_s.append(canon_rows(ps, m["posts"]))
        flat_comments = [c for p in ps for c in comments_by_post.get(p[posts_id], [])]
        comments_s.append(canon_rows(flat_comments, m["comments"]))
    return rel3_json(rows_s, posts_s, comments_s)


def _nested_relations(db: RawDB) -> str:
    users = db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
    return _stitch3(db, users, "id",
                    "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", "id",
                    "SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC", "post_id")


def _composite_relations(db: RawDB) -> str:
    # tenant_id fixed = 1: level-2 posts + level-3 comments both filtered by tenant, stitched by sub-key.
    tusers = db.query("SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC", [1])
    tposts = db.query("SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? ORDER BY post_id ASC", [1])
    tcomments = db.query("SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments WHERE tenant_id = ? ORDER BY comment_id ASC", [1])
    from canon import REL3_FIELDS, canon_row, canon_rows, rel3_json

    m = REL3_FIELDS["compositeRelations"]
    posts_by_user: Dict[Any, List[Dict[str, Any]]] = {}
    for p in tposts:
        posts_by_user.setdefault(p["user_id"], []).append(p)
    comments_by_post: Dict[Any, List[Dict[str, Any]]] = {}
    for c in tcomments:
        comments_by_post.setdefault(c["post_id"], []).append(c)
    rows_s, posts_s, comments_s = [], [], []
    for u in tusers:
        ps = posts_by_user.get(u["user_id"], [])
        rows_s.append(canon_row(u, m["parent"]))
        posts_s.append(canon_rows(ps, m["posts"]))
        comments_s.append(canon_rows([c for p in ps for c in comments_by_post.get(p["post_id"], [])], m["comments"]))
    return rel3_json(rows_s, posts_s, comments_s)


# ── transactions (BEGIN … COMMIT/ROLLBACK, then {committed,state}) ──
def _insert_user_id(db: RawDB, email: str, name: str) -> int:
    if db.dialect == "mysql":
        _, last = db.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", [email, name])
        return last
    rows = db.query("INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id", [email, name])
    return rows[0]["id"]


def _upsert_user_id(db: RawDB, email: str, name: str) -> int:
    if db.dialect == "mysql":
        db.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)", [email, name])
        return db.query("SELECT id FROM benchmark_users WHERE email = ? ORDER BY id", [email])[0]["id"]
    rows = db.query("INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id", [email, name])
    return rows[0]["id"]


def _state_snapshot(db: RawDB):
    users = db.query("SELECT id, email, name FROM benchmark_users ORDER BY id")
    posts = db.query("SELECT id, title, author_id FROM benchmark_posts ORDER BY id")
    return users, posts


def _run_tx(db: RawDB, body) -> str:
    committed = False
    db.begin()
    try:
        body()
        db.commit()
        committed = True
    except Exception:
        db.rollback()
        committed = False
    users, posts = _state_snapshot(db)
    return tx_json(committed, users, posts)


def _delete(db: RawDB) -> str:
    def body():
        uid = _insert_user_id(db, "del0@bench.com", "Del")
        db.execute("DELETE FROM benchmark_users WHERE id = ?", [uid])

    return _run_tx(db, body)


def _nested_create(db: RawDB) -> str:
    def body():
        uid = _insert_user_id(db, "nc@bench.com", "NC")
        db.execute("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", [uid, "NC Post"])

    return _run_tx(db, body)


def _nested_update(db: RawDB) -> str:
    def body():
        db.execute("UPDATE benchmark_users SET name = ? WHERE id = ?", ["NU", 7])
        db.execute("UPDATE benchmark_posts SET title = ? WHERE author_id = ?", ["NU Post", 7])

    return _run_tx(db, body)


def _nested_upsert(db: RawDB) -> str:
    def body():
        uid = _upsert_user_id(db, "user1@example.com", "NUp")
        db.execute("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", [uid, "NUp Post"])

    return _run_tx(db, body)


_DISPATCH = {
    "findAll": _find_all, "filterPaginateSort": _filter_paginate_sort, "findFirst": _find_first, "findUnique": _find_unique,
    "create": _create, "update": _update, "upsert": _upsert,
    "createMany": _create_many, "upsertMany": _upsert_many, "updateMany": _update_many,
    "nestedFindAll": _nested_find_all, "nestedFindFirst": _nested_find_first, "nestedFindUnique": _nested_find_unique,
    "nestedRelations": _nested_relations, "compositeRelations": _composite_relations,
    "delete": _delete, "nestedCreate": _nested_create, "nestedUpdate": _nested_update, "nestedUpsert": _nested_upsert,
}


def sdk_cell(op: str, db: RawDB) -> str:
    fn = _DISPATCH.get(op)
    if fn is None:
        raise ValueError(f"sdk: unknown op {op}")
    return fn(db)
