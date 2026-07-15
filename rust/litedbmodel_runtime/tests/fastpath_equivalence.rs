//! Behavior-equality proof for the single-node read fast-path (perf epic).
//!
//! `execute_read_graph` short-circuits the corpus read-graph shape — a SINGLE `componentRef` body
//! node with Φ output `{ref:[n0]}` — evaluating the `__scope` ports, rendering + executing the
//! node's static statements, and returning its rows DIRECTLY, skipping bc `run_behavior`'s plan
//! machinery. The general bc path is the correctness oracle: this test drives the SAME graph BOTH
//! ways — the fast-path (`execute_read_graph`) and forced-through-bc
//! (`execute_read_graph_orchestrator_for_test`) — over a real in-memory SQLite driver, and asserts the two
//! results are byte-identical for every corpus-relevant read shape (WHERE present, SKIP-dropped
//! fragment + coalesced LIMIT, and a single-JSON IN-list param).

use behavior_contracts::{deep_equals, Value};
use litedbmodel_runtime::{
    encode_value, execute_read_graph, execute_read_graph_orchestrator_for_test, Node, SqliteDriver,
};
/// Build a native `Node` fixture from a JSON string literal — the runtime's OWN native JSON parser
/// (the runtime + these tests carry NO external JSON crate).
fn nj(s: &str) -> Node {
    Node::parse(s).expect("test fixture JSON parses")
}

fn schema() -> Vec<String> {
    vec![
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, status TEXT)"
            .to_string(),
        "INSERT INTO posts VALUES (1, 7, 'A', 'live')".to_string(),
        "INSERT INTO posts VALUES (2, 7, 'B', 'draft')".to_string(),
        "INSERT INTO posts VALUES (3, 9, 'C', 'live')".to_string(),
    ]
}

/// The corpus `find` read-graph shape: a single `__makeSqlNode` componentRef, `output={ref:[n0]}`.
/// Statements = SELECT + optional WHERE author_id + SKIP-guarded WHERE status + ORDER BY + LIMIT,
/// exercising the fragment-connector, skip-drop, and coalesce-default render axes.
fn find_graph() -> Node {
    nj(r#"{
        "dialect": "sqlite",
        "name": "Find",
        "ir": {
            "irVersion": 1,
            "exprVersion": 2,
            "components": [{
                "name": "Find",
                "inputPorts": {"author_id": {"required": true, "type": "unknown"}},
                "body": [{
                    "component": "__makeSqlNode",
                    "id": "n0",
                    "ports": {"__scope": {"obj": {
                        "author_id": {"ref": ["author_id"]},
                        "status": {"refOpt": ["status"]},
                        "limit": {"refOpt": ["limit"]}
                    }}}
                }],
                "output": {"ref": ["n0"]},
                "plan": {"concurrency": 1, "groups": [[0]]}
            }]
        },
        "statementsById": {
            "n0": [
                {"sql": "SELECT id, author_id, title, status FROM posts", "params": []},
                {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": true},
                {
                    "sql": "status = ?",
                    "params": [{"ref": ["status"]}],
                    "whereFragment": true,
                    "skip": {"not": [{"ne": [{"refOpt": ["status"]}, null]}]}
                },
                {"sql": " ORDER BY id ASC", "params": []},
                {"sql": " LIMIT ?", "params": [{"coalesce": [{"refOpt": ["limit"]}, 20]}]}
            ]
        },
        "optionalHeads": ["status", "limit"]
    }"#)
}

/// The corpus `ByIds` shape: a single-JSON IN-list param, still `output={ref:[n0]}`.
fn by_ids_graph() -> Node {
    nj(r#"{
        "dialect": "sqlite",
        "name": "ByIds",
        "ir": {
            "irVersion": 1,
            "exprVersion": 2,
            "components": [{
                "name": "ByIds",
                "inputPorts": {"ids": {"required": true, "type": "unknown"}},
                "body": [{
                    "component": "__makeSqlNode",
                    "id": "n0",
                    "ports": {"__scope": {"obj": {"ids": {"ref": ["ids"]}}}}
                }],
                "output": {"ref": ["n0"]},
                "plan": {"concurrency": 1, "groups": [[0]]}
            }]
        },
        "statementsById": {
            "n0": [
                {"sql": "SELECT id, author_id, title FROM posts", "params": []},
                {
                    "sql": "id IN (SELECT value FROM json_each(?))",
                    "params": [{"__jsonArray": {"ref": ["ids"]}, "dialect": "sqlite"}],
                    "whereFragment": true
                },
                {"sql": " ORDER BY id ASC", "params": []}
            ]
        },
        "optionalHeads": []
    }"#)
}

/// Run one graph+input BOTH ways and assert byte-identical results. Returns the (shared) result so
/// the caller can additionally sanity-check the row count.
fn assert_fastpath_equals_bc(graph: &Node, input: &Node, label: &str) -> Value {
    let input_scope = litedbmodel_runtime::decode_scope(input).unwrap();
    let driver_fast = SqliteDriver::in_memory(&schema()).unwrap();
    let driver_bc = SqliteDriver::in_memory(&schema()).unwrap();

    let via_fast = execute_read_graph(graph, &input_scope, &driver_fast)
        .unwrap_or_else(|e| panic!("[{label}] fast-path failed: {e:?}"));
    let via_bc = execute_read_graph_orchestrator_for_test(graph, &input_scope, &driver_bc)
        .unwrap_or_else(|e| panic!("[{label}] bc path failed: {e:?}"));

    assert!(
        deep_equals(&via_fast, &via_bc),
        "[{label}] fast-path result diverged from the bc oracle:\n  fast = {:?}\n  bc   = {:?}",
        encode_value(&via_fast),
        encode_value(&via_bc)
    );
    // And the serialized JSON is byte-identical (the strongest form of the equality bar).
    assert_eq!(
        encode_value(&via_fast).to_string(),
        encode_value(&via_bc).to_string(),
        "[{label}] serialized fast-path result is not byte-identical to bc"
    );
    via_fast
}

#[test]
fn fastpath_matches_bc_where_present() {
    // author_id + status + limit all present → both WHERE fragments render, explicit LIMIT.
    let out = assert_fastpath_equals_bc(
        &find_graph(),
        &nj(r#"{"author_id": 7, "status": "live", "limit": 5}"#),
        "where_present",
    );
    // Sanity: only post 1 (author 7, live).
    match out {
        Value::Arr(rows) => assert_eq!(rows.len(), 1, "expected 1 live post by author 7"),
        other => panic!("expected array rows, got {other:?}"),
    }
}

#[test]
fn fastpath_matches_bc_skip_and_default_limit() {
    // status omitted → SKIP drops that fragment (no dangling AND); limit omitted → coalesce → 20.
    let out = assert_fastpath_equals_bc(
        &find_graph(),
        &nj(r#"{"author_id": 7}"#),
        "skip_and_default_limit",
    );
    match out {
        Value::Arr(rows) => assert_eq!(rows.len(), 2, "expected 2 posts by author 7"),
        other => panic!("expected array rows, got {other:?}"),
    }
}

#[test]
fn fastpath_matches_bc_in_list() {
    // Single-JSON IN-list param path, still the fast-cased single-ref shape.
    let out = assert_fastpath_equals_bc(&by_ids_graph(), &nj(r#"{"ids": [1, 3]}"#), "in_list");
    match out {
        Value::Arr(rows) => assert_eq!(rows.len(), 2, "expected posts 1 and 3"),
        other => panic!("expected array rows, got {other:?}"),
    }
}
