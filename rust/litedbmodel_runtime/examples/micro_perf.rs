//! Micro re-measure for the runtime perf epic: `find` (plain read, `execute_bundle`) and `hasMany`
//! (relation read, `read_bundle_pooled`) driven against the REAL in-memory SQLite driver over the
//! frozen crosslang corpus (`benchmark/crosslang/generated/bundles.json`), reporting per-op wall-
//! clock µs AND heap allocations/op via a counting global allocator.
//!
//! This is a mock-driver micro loop (in-proc rusqlite, no external DB) — the sanctioned substitute
//! when live PG/MySQL isn't up. Run from the crate dir:
//!   cargo run --release --example micro_perf -- <path-to-bundles.json>

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use behavior_contracts::Value;
use litedbmodel_runtime::{
    execute_bundle, read_bundle_pooled, Driver, PreparedStatement, SqliteDriver,
};
use serde_json::Value as J;

// ── counting allocator ────────────────────────────────────────────────────────
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static COUNTING: AtomicU64 = AtomicU64::new(0); // 0 = off, 1 = on

struct CountingAlloc;
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) == 1 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ── Sync view over &SqliteDriver (single-threaded bench; relations have one sibling) ──
struct SyncDriverRef<'a>(&'a SqliteDriver);
impl Driver for SyncDriverRef<'_> {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        self.0.prepare(sql)
    }
}
unsafe impl Sync for SyncDriverRef<'_> {}

fn cases(block: &J) -> HashMap<String, J> {
    let mut m = HashMap::new();
    for c in block["cases"].as_array().unwrap() {
        m.insert(c["case"].as_str().unwrap().to_string(), c.clone());
    }
    m
}

fn seed(driver: &SqliteDriver, seed: &[String]) {
    for s in seed {
        driver.prepare(s).run(&[]).expect("seed");
    }
}

/// Time + count allocations for `iters` runs of `f`, after `warm` warmup iterations.
fn measure(warm: usize, iters: usize, mut f: impl FnMut()) -> (f64, f64) {
    for _ in 0..warm {
        f();
    }
    // Allocation count over ONE representative op (steady state).
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(1, Ordering::Relaxed);
    f();
    COUNTING.store(0, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed) as f64;

    // Wall-clock over `iters`.
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let us = t0.elapsed().as_secs_f64() * 1e6 / iters as f64;
    (us, allocs)
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "benchmark/crosslang/generated/bundles.json".to_string());
    let raw = std::fs::read_to_string(&path).expect("read bundles.json");
    let j: J = serde_json::from_str(&raw).expect("parse bundles.json");

    let schema: Vec<String> = j["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    let seed_stmts: Vec<String> = j["seed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    let sqlite_cases = cases(&j["dialects"]["sqlite"]);

    let driver = SqliteDriver::in_memory(&schema).expect("schema");
    seed(&driver, &seed_stmts);

    const WARM: usize = 2_000;
    const ITERS: usize = 50_000;

    // find — a plain read (execute_bundle). This is the single-componentRef fast-path shape.
    let find = &sqlite_cases["find"];
    let (fus, fal) = measure(WARM, ITERS, || {
        let out = execute_bundle(&find["bundle"], &find["input"], &driver).unwrap();
        std::hint::black_box(&out);
    });

    // hasMany — a relation read (read_bundle_pooled): primary read + batched relation load.
    let hm = &sqlite_cases["hasMany"];
    let hm_with = hm["withRelation"].as_str().unwrap().to_string();
    let sync = SyncDriverRef(&driver);
    let conns: HashMap<String, &(dyn Driver + Sync)> = HashMap::new();
    let (hus, hal) = measure(WARM, ITERS, || {
        let out: Value = read_bundle_pooled(
            &hm["bundle"],
            &hm["input"],
            &sync,
            std::slice::from_ref(&hm_with),
            &conns,
        )
        .unwrap();
        std::hint::black_box(&out);
    });

    println!("=== litedbmodel_runtime micro re-measure (mock in-proc SQLite driver) ===");
    println!("iters/op = {ITERS}, warmup = {WARM}");
    println!("find    : {fus:7.2} µs/op   {fal:6.0} allocs/op");
    println!("hasMany : {hus:7.2} µs/op   {hal:6.0} allocs/op");
}
