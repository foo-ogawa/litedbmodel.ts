//! #40 (gap fix) — the PRODUCTION pooled read path fans out independent sibling relations (Rust).
//!
//! `parallel_read.rs` proves the STANDALONE primitive `dispatch_read_nodes_parallel`. This test
//! drives the PRODUCTION entry point `execute_read_graph_pooled` (the live PG/MySQL read path the
//! livedb_runner uses) through a multi-sibling read GRAPH, so the fan-out is proven where it ships:
//!
//!   1. FAN-OUT: a read graph with N independent sibling nodes in ONE plan stage (concurrency 16),
//!      driven through the production entry, overlaps (wall ≈ ONE op's latency) with peak in-flight
//!      = N (= sibling count, under the cap 16), and its Φ output equals the serial path.
//!   2. SERIAL PERTURBATION (negative check): the SAME graph with plan `concurrency: 1` → peak
//!      in-flight = 1 and wall ≈ N × latency (proves the parallelism is real, not incidental).
//!   3. DETERMINISM: under a SHUFFLED-latency mock where LATER-dispatched siblings finish FIRST,
//!      the Φ-merged result is BYTE-IDENTICAL to the serial `execute_read_graph`.
//!   4. SINGLE-SIBLING IDENTITY: a one-relation graph is byte-identical pooled vs serial.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use behavior_contracts::Value;
use litedbmodel_runtime::{
    encode_value, execute_read_graph, execute_read_graph_pooled, Driver, PreparedStatement,
    RunInfo, Scope,
};
use serde_json::{json, Value as J};

/// Convert a serde_json test fixture to the runtime's native `Node` (the runtime is serde_json-free).
fn to_node(v: &J) -> litedbmodel_runtime::Node {
    use litedbmodel_runtime::Node;
    match v {
        J::Null => Node::Null,
        J::Bool(b) => Node::Bool(*b),
        J::Number(n) => n
            .as_i64()
            .map(Node::Int)
            .unwrap_or_else(|| Node::Float(n.as_f64().unwrap_or(0.0))),
        J::String(s) => Node::Str(s.clone()),
        J::Array(a) => Node::Array(a.iter().map(to_node).collect()),
        J::Object(o) => Node::Object(o.iter().map(|(k, val)| (k.clone(), to_node(val))).collect()),
    }
}

/// A Sync driver: per-query it can sleep a per-SQL latency, and tracks concurrent in-flight count.
/// `delays` maps a SQL string to its sleep; a missing key uses `default_latency`.
struct LatencyDriver {
    default_latency: Duration,
    delays: HashMap<String, Duration>,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    total_calls: AtomicUsize,
    start_order: Mutex<Vec<String>>,
}

impl LatencyDriver {
    fn new(default_latency: Duration, delays: HashMap<String, Duration>) -> Self {
        LatencyDriver {
            default_latency,
            delays,
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            total_calls: AtomicUsize::new(0),
            start_order: Mutex::new(Vec::new()),
        }
    }
}

impl Driver for LatencyDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(LatencyStmt {
            driver: self,
            sql: sql.to_string(),
        })
    }
}

struct LatencyStmt<'a> {
    driver: &'a LatencyDriver,
    sql: String,
}

impl PreparedStatement for LatencyStmt<'_> {
    fn all(&mut self, _params: &[Value]) -> Result<Vec<Value>, litedbmodel_runtime::SqlFailure> {
        let d = self.driver;
        d.total_calls.fetch_add(1, Ordering::SeqCst);
        d.start_order.lock().unwrap().push(self.sql.clone());
        let now = d.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        d.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        let latency = d
            .delays
            .get(&self.sql)
            .copied()
            .unwrap_or(d.default_latency);
        std::thread::sleep(latency);
        d.in_flight.fetch_sub(1, Ordering::SeqCst);
        // Echo the SQL so the caller can assert per-node result identity + ordering.
        Ok(vec![Value::Obj(vec![(
            "sql".to_string(),
            Value::Str(self.sql.clone()),
        )])])
    }

    fn run(&mut self, _params: &[Value]) -> Result<RunInfo, litedbmodel_runtime::SqlFailure> {
        unreachable!("read path only")
    }
}

/// A full read GRAPH (surrogate bc IR + per-node statements + plan) of N independent sibling
/// `__makeSqlNode`s in ONE plan stage, each a trivial `SELECT <i>`, Φ-merged under `rel<i>`.
fn sibling_graph(n: usize, concurrency: i64) -> J {
    let mut statements = serde_json::Map::new();
    let mut body = Vec::new();
    let mut output = serde_json::Map::new();
    for i in 0..n {
        let id = format!("rel{i}");
        statements.insert(
            id.clone(),
            json!([{ "sql": format!("SELECT {i}"), "params": [] }]),
        );
        body.push(json!({
            "id": id,
            "component": "__makeSqlNode",
            "ports": { "__scope": { "obj": {} } }
        }));
        output.insert(id.clone(), json!({ "ref": [id] }));
    }
    json!({
        "dialect": "sqlite",
        "name": "Siblings",
        "statementsById": statements,
        "optionalHeads": [],
        "ir": {
            "irVersion": 1,
            "exprVersion": 2,
            "components": [{
                "name": "Siblings",
                "inputPorts": {},
                "body": body,
                "output": { "obj": output },
                "plan": { "concurrency": concurrency, "groups": [(0..n).collect::<Vec<_>>()] }
            }]
        }
    })
}

/// A non-latency, single-connection serial driver echoing the SQL (the serial-path oracle).
struct EchoDriver;
impl Driver for EchoDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(EchoStmt(sql.to_string()))
    }
}
struct EchoStmt(String);
impl PreparedStatement for EchoStmt {
    fn all(&mut self, _p: &[Value]) -> Result<Vec<Value>, litedbmodel_runtime::SqlFailure> {
        Ok(vec![Value::Obj(vec![(
            "sql".to_string(),
            Value::Str(self.0.clone()),
        )])])
    }
    fn run(&mut self, _p: &[Value]) -> Result<RunInfo, litedbmodel_runtime::SqlFailure> {
        unreachable!()
    }
}

#[test]
fn production_pooled_fans_out_siblings() {
    const N: usize = 8;
    const LATENCY_MS: u64 = 60;
    let driver = LatencyDriver::new(Duration::from_millis(LATENCY_MS), HashMap::new());
    let graph = sibling_graph(N, 16);
    let input: Scope = Vec::new();

    let t0 = Instant::now();
    let result = execute_read_graph_pooled(&to_node(&graph), &input, &driver).expect("pooled ok");
    let elapsed = t0.elapsed();

    // 1. Overlap: N=8 × 60ms serial = 480ms; concurrent ≈ 60ms.
    let serial_wall = Duration::from_millis(LATENCY_MS * N as u64);
    assert!(
        elapsed < serial_wall / 2,
        "expected concurrent (<{:?}), took {elapsed:?}",
        serial_wall / 2
    );
    assert_eq!(driver.total_calls.load(Ordering::SeqCst), N);
    let peak = driver.peak_in_flight.load(Ordering::SeqCst);
    assert_eq!(peak, N, "expected all {N} siblings in flight, peak={peak}");

    // 3. Determinism (basic): Φ output equals the SERIAL path byte-for-byte.
    let serial = execute_read_graph(&to_node(&graph), &input, &EchoDriver).expect("serial ok");
    assert_eq!(encode_value(&result), encode_value(&serial));

    eprintln!(
        "RUST PRODUCTION PARALLEL PROOF: {N} sibling queries @ {LATENCY_MS}ms via \
         execute_read_graph_pooled → wall {elapsed:?} (serial would be {serial_wall:?}), \
         peak in-flight = {peak}"
    );
}

#[test]
fn production_pooled_concurrency_one_stays_serial() {
    // SERIAL PERTURBATION: concurrency=1 → peak in-flight = 1, wall ≈ N × latency.
    const N: usize = 4;
    const LATENCY_MS: u64 = 30;
    let driver = LatencyDriver::new(Duration::from_millis(LATENCY_MS), HashMap::new());
    let graph = sibling_graph(N, 1);
    let input: Scope = Vec::new();

    let t0 = Instant::now();
    execute_read_graph_pooled(&to_node(&graph), &input, &driver).expect("pooled ok");
    let elapsed = t0.elapsed();

    assert_eq!(
        driver.peak_in_flight.load(Ordering::SeqCst),
        1,
        "concurrency=1 must be serial"
    );
    // Wall must be ≈ N × latency (serial), well over the concurrent bound.
    assert!(
        elapsed >= Duration::from_millis(LATENCY_MS * (N as u64 - 1)),
        "serial wall {elapsed:?} too short"
    );
}

#[test]
fn production_pooled_deterministic_under_shuffled_completion() {
    // DETERMINISM: rel0 sleeps LONGEST, rel{N-1} shortest → later-dispatched siblings finish first.
    const N: usize = 6;
    let mut delays = HashMap::new();
    for i in 0..N {
        // rel0 → 60ms, rel5 → 10ms.
        delays.insert(
            format!("SELECT {i}"),
            Duration::from_millis((N - i) as u64 * 10),
        );
    }
    let driver = LatencyDriver::new(Duration::from_millis(1), delays);
    let graph = sibling_graph(N, 16);
    let input: Scope = Vec::new();

    let result = execute_read_graph_pooled(&to_node(&graph), &input, &driver).expect("pooled ok");
    let serial = execute_read_graph(&to_node(&graph), &input, &EchoDriver).expect("serial ok");

    // Byte-identical to the serial path despite reverse completion order.
    assert_eq!(
        encode_value(&result),
        encode_value(&serial),
        "shuffled-completion result diverged from serial"
    );
}

#[test]
fn production_pooled_single_sibling_identity() {
    // A one-relation graph: pooled falls back to serial (no multi-member stage) → identical.
    let graph = sibling_graph(1, 16);
    let input: Scope = Vec::new();
    let pooled =
        execute_read_graph_pooled(&to_node(&graph), &input, &EchoDriver).expect("pooled ok");
    let serial = execute_read_graph(&to_node(&graph), &input, &EchoDriver).expect("serial ok");
    assert_eq!(encode_value(&pooled), encode_value(&serial));
}
