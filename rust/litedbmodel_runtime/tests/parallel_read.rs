//! #40 — proof that independent sibling-relation read nodes are dispatched CONCURRENTLY.
//!
//! bc's `run_behavior` owns the read-graph orchestration and calls the makeSQL handler serially
//! (sync `FnMut` seam); the published behavior-contracts crate exposes no async/parallel
//! `run_behavior`. So #40 puts the sibling fan-out in the EXECUTOR layer:
//! `dispatch_read_nodes_parallel` renders + executes independent stage nodes via bc's
//! `run_plan_parallel` on scoped worker threads, each hitting its OWN pooled connection.
//!
//! This test proves the concurrency two ways with a LATENCY-INJECTING, connection-counting mock
//! `Driver` (the sanctioned substitute when live PG/MySQL isn't up):
//!   1. Wall-clock: N sibling queries each sleeping `D` finish in ≈`D`, not ≈`N·D` (they overlap).
//!   2. Instrumentation: the peak count of SIMULTANEOUSLY in-flight queries reaches N (all the
//!      sibling connections were checked out at once), and the assembled result order is
//!      byte-identical to declaration order (determinism preserved).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use behavior_contracts::Value;
use litedbmodel_runtime::Node;
use litedbmodel_runtime::{dispatch_read_nodes_parallel, Driver, PreparedStatement, RunInfo};

/// Build a native `Node` fixture from a JSON string literal — the runtime's OWN native JSON parser
/// (the runtime + these tests carry NO external JSON crate).
fn nj(s: &str) -> Node {
    Node::parse(s).expect("test fixture JSON parses")
}

/// A Sync driver that sleeps `latency` on each query and tracks concurrent in-flight count.
struct LatencyDriver {
    latency: Duration,
    in_flight: AtomicUsize,
    peak_in_flight: AtomicUsize,
    total_calls: AtomicUsize,
    /// The order queries STARTED — proves they interleave rather than run head-to-tail.
    start_order: Mutex<Vec<String>>,
}

impl LatencyDriver {
    fn new(latency: Duration) -> Self {
        LatencyDriver {
            latency,
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
    fn begin_tx(
        &self,
    ) -> Result<Box<dyn litedbmodel_runtime::TxConnection + '_>, litedbmodel_runtime::SqlFailure>
    {
        litedbmodel_runtime::forwarding_tx(self)
    }
    fn acquire_tx(
        &self,
    ) -> Result<Box<dyn litedbmodel_runtime::TxConnection + '_>, litedbmodel_runtime::SqlFailure>
    {
        litedbmodel_runtime::forwarding_tx_no_begin(self)
    }
}

struct LatencyStmt<'a> {
    driver: &'a LatencyDriver,
    sql: String,
}

impl PreparedStatement for LatencyStmt<'_> {
    fn all(&mut self, _params: &[Value]) -> Result<Vec<Value>, litedbmodel_runtime::SqlFailure> {
        self.driver.total_calls.fetch_add(1, Ordering::SeqCst);
        self.driver
            .start_order
            .lock()
            .unwrap()
            .push(self.sql.clone());
        let now = self.driver.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        // Track the peak simultaneous in-flight count.
        self.driver.peak_in_flight.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(self.driver.latency);
        self.driver.in_flight.fetch_sub(1, Ordering::SeqCst);
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

/// A synthetic read graph of N sibling nodes, each a trivial static `SELECT <i>` statement (native).
fn sibling_graph(n: usize) -> Node {
    let mut stmts = String::new();
    for i in 0..n {
        if i > 0 {
            stmts.push(',');
        }
        stmts.push_str(&format!(
            r#""rel{i}": [{{"sql": "SELECT {i}", "params": []}}]"#
        ));
    }
    nj(&format!(
        r#"{{"dialect": "sqlite", "statementsById": {{{stmts}}}}}"#
    ))
}

#[test]
fn sibling_relations_dispatch_concurrently() {
    const N: usize = 8;
    const LATENCY_MS: u64 = 60;
    let driver = LatencyDriver::new(Duration::from_millis(LATENCY_MS));
    let graph = sibling_graph(N);

    // N independent sibling nodes, each with an (empty) scope — no intra-stage data edge.
    let nodes: Vec<(String, Vec<(String, Value)>)> =
        (0..N).map(|i| (format!("rel{i}"), Vec::new())).collect();

    let t0 = Instant::now();
    let results =
        dispatch_read_nodes_parallel(&driver, &graph, "sqlite", &nodes, 16).expect("dispatch ok");
    let elapsed = t0.elapsed();

    // 1. Wall-clock overlap: N=8 × 60ms serial = 480ms; concurrent ≈ 60ms. Allow generous slack
    //    for thread startup — anything well under the serial time proves overlap.
    let serial = Duration::from_millis(LATENCY_MS * N as u64);
    assert!(
        elapsed < serial / 2,
        "expected concurrent (<{:?}), took {elapsed:?} (serial would be {serial:?})",
        serial / 2
    );

    // 2. All N queries ran, and the peak simultaneous in-flight count reached N (real parallel).
    assert_eq!(driver.total_calls.load(Ordering::SeqCst), N);
    let peak = driver.peak_in_flight.load(Ordering::SeqCst);
    assert_eq!(
        peak, N,
        "expected all {N} siblings in flight at once, peak={peak}"
    );

    // 3. Determinism: assembled results are in declaration order regardless of finish order.
    assert_eq!(results.len(), N);
    for (i, r) in results.iter().enumerate() {
        let want = nj(&format!(r#"[{{"sql": "SELECT {i}"}}]"#));
        let got = litedbmodel_runtime::encode_value(r);
        assert_eq!(got, want, "node rel{i} out of order or wrong");
    }

    eprintln!(
        "PARALLEL PROOF: {N} sibling queries @ {LATENCY_MS}ms each → wall {elapsed:?} \
         (serial would be {serial:?}), peak in-flight = {peak}"
    );
}

#[test]
fn concurrency_one_stays_serial() {
    // concurrency=1 must fall back to serial: peak in-flight = 1, wall ≈ N·latency.
    const N: usize = 4;
    const LATENCY_MS: u64 = 30;
    let driver = LatencyDriver::new(Duration::from_millis(LATENCY_MS));
    let graph = sibling_graph(N);
    let nodes: Vec<(String, Vec<(String, Value)>)> =
        (0..N).map(|i| (format!("rel{i}"), Vec::new())).collect();

    dispatch_read_nodes_parallel(&driver, &graph, "sqlite", &nodes, 1).expect("dispatch ok");
    assert_eq!(
        driver.peak_in_flight.load(Ordering::SeqCst),
        1,
        "concurrency=1 must be serial"
    );
    // Start order under serial is exactly declaration order.
    let order = driver.start_order.lock().unwrap().clone();
    let want: Vec<String> = (0..N).map(|i| format!("SELECT {i}")).collect();
    assert_eq!(order, want);
}
