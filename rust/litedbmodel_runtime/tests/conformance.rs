//! Integration tests for the litedbmodel SCP Rust runtime (WS7e, #34; static-makeSQL flip #43/#45).
//!
//! These exercise the public surface against small hand-written static-makeSQL fixtures and the
//! in-proc `rusqlite` driver — REAL SQL, not stubs — covering the render axis (SKIP-fragment drop /
//! WHERE-AND connector / IN-list single-JSON param / LIMIT deferral / dialect `?`→`$N`), a read
//! bundle end-to-end, and a gate-first write transaction (commit + short-circuit). The frozen 36-
//! vector corpus is asserted by `rust/vectors_runner`; these are the crate-local `cargo test` gate.

use behavior_contracts::{deep_equals, Value};
use litedbmodel_runtime::{
    dialect_for, execute_bundle, execute_transaction_bundle, render_placeholders,
    render_read_primary, render_statements, Driver, Node, SqliteDriver,
};

/// Build a native `Node` fixture from a JSON string literal — the runtime's OWN native JSON parser
/// (the runtime + these tests carry NO external JSON crate). `nj(r#"{…}"#)` replaces the old `json!(…)`.
fn nj(s: &str) -> Node {
    Node::parse(s).expect("test fixture JSON parses")
}

fn scope(pairs: &[(&str, Value)]) -> Vec<(String, Value)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// Assert an optional field value equals an expected bc Value (bc::Value has no PartialEq).
fn assert_val(got: Option<&Value>, want: &Value) {
    match got {
        Some(v) => assert!(deep_equals(v, want), "value mismatch: {v:?} != {want:?}"),
        None => panic!("expected a value, got None"),
    }
}

/// The canonical Feed read node's static statement templates (SELECT + WHERE + LIMIT) — native Nodes.
fn feed_statements() -> Vec<Node> {
    match nj(r#"[
        {"sql": "SELECT id, author_id, title, status FROM posts", "params": []},
        {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": true},
        {"sql": "status = ?", "params": [{"ref": ["status"]}], "whereFragment": true,
         "skip": {"not": [{"ne": [{"refOpt": ["status"]}, null]}]}},
        {"sql": " ORDER BY id ASC", "params": []},
        {"sql": " LIMIT ?", "params": [{"coalesce": [{"refOpt": ["limit"]}, 20]}]}
    ]"#)
    {
        Node::Array(a) => a,
        _ => unreachable!(),
    }
}

/// The Feed statements JSON (as a raw string) — for embedding inline in a read-graph fixture.
const FEED_STATEMENTS_JSON: &str = r#"[
    {"sql": "SELECT id, author_id, title, status FROM posts", "params": []},
    {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": true},
    {"sql": "status = ?", "params": [{"ref": ["status"]}], "whereFragment": true,
     "skip": {"not": [{"ne": [{"refOpt": ["status"]}, null]}]}},
    {"sql": " ORDER BY id ASC", "params": []},
    {"sql": " LIMIT ?", "params": [{"coalesce": [{"refOpt": ["limit"]}, 20]}]}
]"#;

#[test]
fn render_all_fragments_present() {
    let s = scope(&[
        ("author_id", Value::Int(7)),
        ("status", Value::Str("live".into())),
        ("limit", Value::Int(5)),
    ]);
    let r = render_statements(&feed_statements(), "sqlite", &s).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id, author_id, title, status FROM posts WHERE author_id = ? AND status = ? ORDER BY id ASC LIMIT ?"
    );
    assert_eq!(r.params.len(), 3);
}

#[test]
fn render_skip_drops_status_and_defaults_limit() {
    // status absent (present-as-null) → skip drops the fragment; coalesce defaults the limit.
    let s = scope(&[
        ("author_id", Value::Int(7)),
        ("status", Value::Null),
        ("limit", Value::Null),
    ]);
    let r = render_statements(&feed_statements(), "sqlite", &s).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?"
    );
    assert_eq!(r.params.len(), 2);
    assert!(deep_equals(&r.params[1], &Value::Int(20)));
}

#[test]
fn render_postgres_dollar_placeholders() {
    let s = scope(&[
        ("author_id", Value::Int(7)),
        ("status", Value::Str("live".into())),
        ("limit", Value::Int(5)),
    ]);
    let r = render_statements(&feed_statements(), "postgres", &s).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id, author_id, title, status FROM posts WHERE author_id = $1 AND status = $2 ORDER BY id ASC LIMIT $3"
    );
}

#[test]
fn render_in_list_single_json_param_sqlite() {
    let stmts = match nj(r#"[
        {"sql": "SELECT id FROM posts", "params": []},
        {"sql": "id IN (SELECT value FROM json_each(?))",
         "params": [{"__jsonArray": {"ref": ["ids"]}, "dialect": "sqlite"}], "whereFragment": true}
    ]"#)
    {
        Node::Array(a) => a,
        _ => unreachable!(),
    };
    let s = scope(&[(
        "ids",
        Value::Arr(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
    )]);
    let r = render_statements(&stmts, "sqlite", &s).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id FROM posts WHERE id IN (SELECT value FROM json_each(?))"
    );
    assert_eq!(r.params.len(), 1);
    assert!(deep_equals(&r.params[0], &Value::Str("[1,2,3]".into()))); // single JSON param
}

#[test]
fn render_placeholder_rewrite_quote_aware() {
    // A `?` inside a string literal is NOT a placeholder (mirrors TS renderPlaceholders).
    assert_eq!(
        render_placeholders("SELECT '?' AS q WHERE a = ?", "postgres"),
        "SELECT '?' AS q WHERE a = $1"
    );
    assert_eq!(
        render_placeholders("a = ? AND b = ?", "sqlite"),
        "a = ? AND b = ?"
    );
}

#[test]
fn render_read_primary_picks_first_body_node() {
    let graph = nj(&format!(
        r#"{{"dialect": "sqlite", "name": "Feed",
             "statementsById": {{"n0": {stmts}}},
             "optionalHeads": ["status", "limit"],
             "ir": {{"irVersion": 1, "exprVersion": 2, "components": [{{"name": "Feed", "body": [{{"id": "n0"}}]}}]}}}}"#,
        stmts = FEED_STATEMENTS_JSON,
    ));
    // status + limit omitted → normalized present-as-null → skip drop + coalesce default.
    let r = render_read_primary(&graph, &scope(&[("author_id", Value::Int(7))])).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?"
    );
    assert_eq!(r.params.len(), 2);
}

#[test]
fn order_by_nulls_dialects() {
    assert_eq!(
        dialect_for("sqlite")
            .unwrap()
            .order_by_nulls("c", "ASC", "FIRST"),
        "c ASC NULLS FIRST"
    );
    assert_eq!(
        dialect_for("mysql")
            .unwrap()
            .order_by_nulls("c", "ASC", "FIRST"),
        "c IS NULL DESC, c ASC"
    );
    assert_eq!(
        dialect_for("mysql")
            .unwrap()
            .order_by_nulls("c", "DESC", "LAST"),
        "c IS NULL ASC, c DESC"
    );
}

#[test]
fn dialect_for_unknown_fails_closed() {
    assert!(dialect_for("oracle").is_err());
}

#[test]
fn execute_read_bundle_end_to_end() {
    let schema = vec![
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, status TEXT)"
            .to_string(),
        "INSERT INTO posts (id, author_id, title, status) VALUES (1, 7, 'Hello', 'live')"
            .to_string(),
        "INSERT INTO posts (id, author_id, title, status) VALUES (2, 7, 'World', 'live')"
            .to_string(),
    ];
    let driver = SqliteDriver::in_memory(&schema).unwrap();
    // A single-node static-makeSQL read bundle: bc drives the surrogate `__makeSqlNode` node, the
    // makeSQL handler renders its statements + executes REAL SQL, Φ maps the rows to `posts`.
    let bundle = nj(r#"{
        "dialect": "sqlite",
        "name": "Feed",
        "readGraph": {
            "dialect": "sqlite",
            "name": "Feed",
            "ir": {
                "irVersion": 1,
                "exprVersion": 2,
                "components": [{
                    "name": "Feed",
                    "inputPorts": {"author_id": {"required": true, "type": "unknown"}},
                    "body": [{
                        "component": "__makeSqlNode",
                        "id": "n0",
                        "ports": {"__scope": {"obj": {"author_id": {"ref": ["author_id"]}}}}
                    }],
                    "output": {"obj": {"posts": {"ref": ["n0"]}}},
                    "plan": {"concurrency": 1, "groups": [[0]]}
                }]
            },
            "statementsById": {
                "n0": [
                    {"sql": "SELECT id, author_id, title FROM posts", "params": []},
                    {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": true},
                    {"sql": " ORDER BY id ASC", "params": []}
                ]
            },
            "optionalHeads": []
        },
        "optionalHeads": [],
        "relations": {}
    }"#);
    let out = execute_bundle(&bundle, &nj(r#"{"author_id": 7}"#), &driver).unwrap();
    let posts = out.obj_get("posts").expect("posts key");
    match posts {
        Value::Arr(rows) => assert_eq!(rows.len(), 2),
        other => panic!("expected array, got {other:?}"),
    }
}

fn write_bundle() -> Node {
    nj(r#"{
        "dialect": "sqlite",
        "name": "Create",
        "optionalHeads": [],
        "relations": {},
        "transaction": {
            "phase": "create",
            "entityFrom": "tx_body_1",
            "statements": [
                {
                    "id": "tx_requires_0", "role": "gate:requires", "gate": "existsElseRollback",
                    "op": {"sql": "SELECT 1 FROM users WHERE id = ?", "params": [{"ref": ["author_id"]}]}
                },
                {
                    "id": "tx_body_1", "role": "body",
                    "op": {"sql": "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title", "params": [{"ref": ["author_id"]}, {"ref": ["title"]}]}
                }
            ]
        }
    }"#)
}

fn tx_schema() -> Vec<String> {
    vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY)".to_string(),
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)".to_string(),
        "INSERT INTO users (id) VALUES (7)".to_string(),
    ]
}

#[test]
fn transaction_commits_on_gate_pass() {
    let driver = SqliteDriver::in_memory(&tx_schema()).unwrap();
    let out = execute_transaction_bundle(
        &write_bundle(),
        &nj(r#"{"author_id": 7, "title": "New Post"}"#),
        &driver,
    )
    .unwrap();
    assert_val(out.obj_get("committed"), &Value::Bool(true));
    let entity = out.obj_get("entity").unwrap();
    assert_val(entity.obj_get("author_id"), &Value::Int(7));
}

#[test]
fn transaction_short_circuits_on_missing_requires() {
    let driver = SqliteDriver::in_memory(&tx_schema()).unwrap();
    let out = execute_transaction_bundle(
        &write_bundle(),
        &nj(r#"{"author_id": 999, "title": "Orphan"}"#),
        &driver,
    )
    .unwrap();
    assert_val(out.obj_get("committed"), &Value::Bool(false));
    let sc = out.obj_get("shortCircuit").unwrap();
    assert_val(sc.obj_get("reason"), &Value::Str("requires_absent".into()));
    // the body write never ran → posts is empty
    let mut stmt = driver.prepare("SELECT COUNT(*) AS n FROM posts");
    let rows = stmt.all(&[]).unwrap();
    assert_val(rows[0].obj_get("n"), &Value::Int(0));
}

// M4 (re-audit): an UNKNOWN / forward-incompatible gate rule FAILS CLOSED (aligned with TS +
// Python + Go + PHP): the tx aborts (Err) and does NOT commit — a corrupt gate must never be
// silently skipped into a COMMIT.
#[test]
fn transaction_unknown_gate_fails_closed() {
    let driver = SqliteDriver::in_memory(&tx_schema()).unwrap();
    // The write bundle with the requires gate tagged with a bogus rule the runtime does not recognize
    // (built natively — the same tx as write_bundle() but `gate` = an unknown rule).
    let bundle = nj(r#"{
        "dialect": "sqlite", "name": "Create", "optionalHeads": [], "relations": {},
        "transaction": {
            "phase": "create", "entityFrom": "tx_body_1",
            "statements": [
                {"id": "tx_requires_0", "role": "gate:requires", "gate": "someFutureGateRuleThatDoesNotExist",
                 "op": {"sql": "SELECT 1 FROM users WHERE id = ?", "params": [{"ref": ["author_id"]}]}},
                {"id": "tx_body_1", "role": "body",
                 "op": {"sql": "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title", "params": [{"ref": ["author_id"]}, {"ref": ["title"]}]}}
            ]
        }
    }"#);
    let res =
        execute_transaction_bundle(&bundle, &nj(r#"{"author_id": 7, "title": "X"}"#), &driver);
    assert!(
        res.is_err(),
        "an unknown gate rule must fail closed (Err), not commit"
    );
    let msg = format!("{}", res.err().unwrap());
    assert!(
        msg.contains("unknown gate rule"),
        "error should name the unknown gate rule, got: {msg}"
    );
    // FAIL-CLOSED: nothing committed.
    let mut stmt = driver.prepare("SELECT COUNT(*) AS n FROM posts");
    let rows = stmt.all(&[]).unwrap();
    assert_val(rows[0].obj_get("n"), &Value::Int(0));
}
