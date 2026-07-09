//! Integration tests for the litedbmodel SCP Rust runtime (WS7e, #34).
//!
//! These exercise the public surface against small hand-written §8 fixtures and the in-proc
//! `rusqlite` driver — REAL SQL, not stubs — covering render (SKIP / IN-list / empty-WHERE /
//! dialect `?`→`$N`), a read bundle end-to-end, and a gate-first write transaction (commit +
//! short-circuit). The frozen 47-vector corpus is asserted by `rust/vectors_runner`; these are the
//! crate-local `cargo test` gate on top.

use behavior_contracts::{deep_equals, Value};
use litedbmodel_runtime::{
    dialect_for, execute_bundle, execute_transaction_bundle, render_operation, Dialect, Driver,
    SqliteDriver,
};
use serde_json::json;

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

#[test]
fn render_eq_and_in_list_expands() {
    let op = json!({
        "component": "Select",
        "sql": "SELECT id FROM posts{where} ORDER BY id ASC",
        "where": {
            "connector": "AND",
            "fragments": [
                {"always": true, "sql": "author_id = ?", "params": [{"ref": ["authorId"]}]},
                {"always": true, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0}
            ]
        },
        "params": []
    });
    let s = scope(&[
        ("authorId", Value::Int(7)),
        (
            "ids",
            Value::Arr(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        ),
    ]);
    let r = render_operation(&op, &s, Dialect::Sqlite).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id FROM posts WHERE author_id = ? AND id IN (?, ?, ?) ORDER BY id ASC"
    );
    assert_eq!(r.params.len(), 4);
}

#[test]
fn render_empty_in_list_degenerates() {
    let op = json!({
        "component": "Select",
        "sql": "SELECT id FROM posts{where}",
        "where": {"connector": "AND", "fragments": [
            {"always": true, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0}
        ]},
        "params": []
    });
    let s = scope(&[("ids", Value::Arr(vec![]))]);
    let r = render_operation(&op, &s, Dialect::Sqlite).unwrap();
    assert_eq!(r.sql, "SELECT id FROM posts WHERE 1 = 0");
    assert!(r.params.is_empty());
}

#[test]
fn render_skip_null_collapses_whole_where() {
    let op = json!({
        "component": "Select",
        "sql": "SELECT id FROM posts{where}",
        "where": {"connector": "AND", "fragments": [
            {"when": {"ne": [{"refOpt": ["status"]}, null]}, "sql": "status = ?", "params": [{"ref": ["status"]}]}
        ]},
        "params": []
    });
    let s = scope(&[("status", Value::Null)]);
    let r = render_operation(&op, &s, Dialect::Sqlite).unwrap();
    assert_eq!(r.sql, "SELECT id FROM posts");
    assert!(r.params.is_empty());
}

#[test]
fn render_postgres_dollar_placeholders() {
    let op = json!({
        "component": "Select",
        "sql": "SELECT id FROM posts WHERE author_id = ? AND title = ?",
        "where": null,
        "params": [{"ref": ["authorId"]}, {"ref": ["title"]}]
    });
    let s = scope(&[
        ("authorId", Value::Int(7)),
        ("title", Value::Str("x".into())),
    ]);
    let r = render_operation(&op, &s, Dialect::Postgres).unwrap();
    assert_eq!(
        r.sql,
        "SELECT id FROM posts WHERE author_id = $1 AND title = $2"
    );
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
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)".to_string(),
        "INSERT INTO posts (id, author_id, title) VALUES (1, 7, 'Hello')".to_string(),
        "INSERT INTO posts (id, author_id, title) VALUES (2, 7, 'World')".to_string(),
    ];
    let driver = SqliteDriver::in_memory(&schema).unwrap();
    let bundle = json!({
        "irVersion": 1,
        "exprVersion": 2,
        "dialect": "sqlite",
        "component": {
            "name": "Feed",
            "inputPorts": {"author_id": {"required": true, "type": "unknown"}},
            "body": [
                {"component": "Select", "id": "n0", "ports": {"__scope": {"obj": {"author_id": {"ref": ["author_id"]}}}}}
            ],
            "output": {"obj": {"posts": {"ref": ["n0"]}}},
            "plan": {"concurrency": 1, "groups": [[0]]}
        },
        "operations": {
            "n0": {
                "component": "Select",
                "sql": "SELECT id, author_id, title FROM posts{where} ORDER BY id ASC",
                "where": {"connector": "AND", "fragments": [
                    {"always": true, "sql": "author_id = ?", "params": [{"ref": ["author_id"]}]}
                ]},
                "params": []
            }
        },
        "optionalHeads": []
    });
    let out = execute_bundle(&bundle, &json!({"author_id": 7}), &driver).unwrap();
    let posts = out.obj_get("posts").expect("posts key");
    match posts {
        Value::Arr(rows) => assert_eq!(rows.len(), 2),
        other => panic!("expected array, got {other:?}"),
    }
}

fn write_bundle() -> serde_json::Value {
    json!({
        "irVersion": 1,
        "exprVersion": 2,
        "dialect": "sqlite",
        "component": {
            "name": "Create",
            "inputPorts": {"author_id": {"required": true}, "title": {"required": true}},
            "body": [{"component": "Insert", "id": "n0", "ports": {"__scope": {"obj": {}}}}],
            "output": {"ref": ["n0"]},
            "plan": {"concurrency": 1, "groups": [[0]]}
        },
        "operations": {},
        "optionalHeads": [],
        "transaction": {
            "phase": "create",
            "entityFrom": "tx_body_1",
            "statements": [
                {
                    "id": "tx_requires_0", "role": "gate:requires", "gate": "existsElseRollback",
                    "op": {"component": "Select", "sql": "SELECT 1 FROM users WHERE id = ?", "where": null, "params": [{"ref": ["author_id"]}]}
                },
                {
                    "id": "tx_body_1", "role": "body",
                    "op": {"component": "Insert", "sql": "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title", "where": null, "params": [{"ref": ["author_id"]}, {"ref": ["title"]}]}
                }
            ]
        }
    })
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
        &json!({"author_id": 7, "title": "New Post"}),
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
        &json!({"author_id": 999, "title": "Orphan"}),
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
