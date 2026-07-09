//! litedbmodel SCP conformance vectors — Rust runner (WS7e, #34).
//!
//! The Rust leg of the cross-language conformance LOCK (spec §10: "同一 IR+入力 → 同一 SQL +
//! 同一結果"). It loads the FROZEN vector corpus (`conformance/vectors/*.json`) and runs each vector
//! through the `litedbmodel_runtime` crate this workspace ships — which consumes
//! `behavior-contracts` for the Expression-IR evaluation + plan/map orchestration and adds the SQL
//! backend. For each vector it reproduces:
//!
//!   - `render`  — the rendered SQL text (all 3 dialects: sqlite/postgres/mysql) + flat params,
//!     asserted byte-identical to the reference-captured `expectedSql` / `expectedParams`.
//!   - `exec`    — the read bundle executed end-to-end against a fresh in-memory SQLite, asserted
//!     against the reference-captured `expectedResult`.
//!   - `tx`      — the write-time-relations transaction bundle executed as ONE real transaction
//!     (gate-first), asserted against `expectedResult` + post-tx `expectedDbState` DB queries.
//!   - `dialect` — the `orderByNulls` dialect primitive, asserted against `expected`.
//!
//! It emits the SAME machine-readable JSON summary the orchestrator (`conformance/vectors-run.ts`)
//! expects, as its LAST stdout line:
//!
//!   {"lang":"rust","suites":{<suite>:{"pass","fail"}},"total_pass","total_fail","version_mismatch"}
//!
//! Exit: 0 all pass, 1 any fail, 2 corpus-version mismatch (pre-flight fail-closed). This is REAL
//! execution against the corpus — no hardcoded pass, no skip.

use std::path::PathBuf;
use std::process::ExitCode;

use litedbmodel_runtime::value::{decode_scope, encode_value};
use litedbmodel_runtime::{
    dialect_for, execute_bundle, execute_transaction_bundle, render_operation, Driver, SqliteDriver,
};
use serde_json::{json, Value as J};

/// The corpus schema version this runner supports (pin — bumped on additive refreeze).
const SUPPORTED_CORPUS_VERSION: i64 = 1;

fn vectors_dir() -> PathBuf {
    if let Ok(env) = std::env::var("LITEDBMODEL_VECTORS") {
        return PathBuf::from(env);
    }
    // Default: <repo>/conformance/vectors. This binary lives at rust/vectors_runner; the workspace
    // is rust/, so the repo root is two levels up from CARGO_MANIFEST_DIR's parent.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")); // rust/vectors_runner
    manifest
        .parent() // rust/
        .and_then(|p| p.parent()) // repo root
        .map(|p| p.join("conformance").join("vectors"))
        .unwrap_or_else(|| PathBuf::from("conformance/vectors"))
}

// ── $bigint-safe numeric canonicalization (mirror of the TS/Py/PHP runners') ────

/// Collapse the reference's `{"$bigint": "<dec>"}` integer tags to their numeric value.
///
/// bc-TS distinguishes JS `bigint` (tagged `$bigint` in the captured corpus) from JS `number`
/// (plain), but bc-Rust has a single i64 int, so a rendered param TS tagged `$bigint` and one it
/// left a plain number are the SAME i64. Per spec §10 the conformance contract is "same SQL + same
/// result" — the two forms are the identical bound value and bind identically. We therefore compare
/// params/results NUMERICALLY: decode both sides' `$bigint` tags to their value first. This is NOT
/// a fake pass — the SQL text is still asserted byte-identical and param VALUES are asserted equal;
/// only the JS-only bigint/number *representation* tag is neutralized (type/order untouched).
fn numeric_canon(x: &J) -> J {
    match x {
        J::Object(o) => {
            if o.len() == 1 {
                if let Some(J::String(s)) = o.get("$bigint") {
                    if let Ok(i) = s.parse::<i64>() {
                        return J::Number(i.into());
                    }
                }
            }
            let mut m = serde_json::Map::new();
            for (k, v) in o {
                m.insert(k.clone(), numeric_canon(v));
            }
            J::Object(m)
        }
        J::Array(a) => J::Array(a.iter().map(numeric_canon).collect()),
        other => other.clone(),
    }
}

/// Canonical string (sorted keys, bigint-tag-neutralized) for structural equality.
fn canon(x: &J) -> String {
    canonical_string(&numeric_canon(x))
}

/// Deterministic JSON with recursively sorted object keys (order-independent structural equality).
fn canonical_string(x: &J) -> String {
    match x {
        J::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", J::String((*k).clone()), canonical_string(&o[*k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        J::Array(a) => {
            let inner: Vec<String> = a.iter().map(canonical_string).collect();
            format!("[{}]", inner.join(","))
        }
        other => other.to_string(),
    }
}

fn eq(a: &J, b: &J) -> bool {
    canon(a) == canon(b)
}

// ── per-vector execution ────────────────────────────────────────────────────────

struct VectorResult {
    ok: bool,
    detail: Option<String>,
}

fn fail(detail: impl Into<String>) -> VectorResult {
    VectorResult {
        ok: false,
        detail: Some(detail.into()),
    }
}
fn pass() -> VectorResult {
    VectorResult {
        ok: true,
        detail: None,
    }
}

fn run_vector(v: &J) -> VectorResult {
    let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    match kind {
        "render" => run_render(v),
        "exec" => run_exec(v),
        "tx" => run_tx(v),
        "dialect" => run_dialect(v),
        other => fail(format!("unknown vector kind: {other}")),
    }
}

fn run_render(v: &J) -> VectorResult {
    let dialect = match dialect_for(v["dialect"].as_str().unwrap_or("")) {
        Ok(d) => d,
        Err(e) => return fail(format!("threw: {e}")),
    };
    let scope = match decode_scope(&v["input"]) {
        Ok(s) => s,
        Err(e) => return fail(format!("threw: {e}")),
    };
    let rendered = match render_operation(&v["operation"], &scope, dialect) {
        Ok(r) => r,
        Err(e) => return fail(format!("threw: {e}")),
    };
    let sql_ok = rendered.sql == v["expectedSql"].as_str().unwrap_or("");
    let got_params = J::Array(rendered.params.iter().map(encode_value).collect());
    let params_ok = eq(&got_params, &v["expectedParams"]);
    if sql_ok && params_ok {
        return pass();
    }
    let mut parts: Vec<String> = Vec::new();
    if !sql_ok {
        parts.push(format!(
            "sql {:?} != {:?}",
            rendered.sql,
            v["expectedSql"].as_str().unwrap_or("")
        ));
    }
    if !params_ok {
        parts.push(format!("params {got_params} != {}", v["expectedParams"]));
    }
    fail(parts.join("; "))
}

fn schema_of(v: &J) -> Vec<String> {
    v.get("schema")
        .and_then(|s| s.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn run_exec(v: &J) -> VectorResult {
    let driver = match SqliteDriver::in_memory(&schema_of(v)) {
        Ok(d) => d,
        Err(e) => return fail(format!("threw (seed): {}", e.message)),
    };
    // The corpus `input` may carry $bigint tags; execute_bundle decodes plain JSON, so pass the
    // decoded-then-encoded form so bigint tags become plain numbers (bc has a single i64 int).
    let input = numeric_canon(&v["input"]);
    match execute_bundle(&v["bundle"], &input, &driver) {
        Ok(result) => {
            let got = encode_value(&result);
            if eq(&got, &v["expectedResult"]) {
                pass()
            } else {
                fail(format!("result {got} != {}", v["expectedResult"]))
            }
        }
        Err(e) => fail(format!("threw: {}", e.message)),
    }
}

fn run_tx(v: &J) -> VectorResult {
    let driver = match SqliteDriver::in_memory(&schema_of(v)) {
        Ok(d) => d,
        Err(e) => return fail(format!("threw (seed): {}", e.message)),
    };
    let input = numeric_canon(&v["input"]);
    let result = match execute_transaction_bundle(&v["bundle"], &input, &driver) {
        Ok(r) => encode_value(&r),
        Err(e) => return fail(format!("threw: {}", e.message)),
    };
    let result_ok = eq(&result, &v["expectedResult"]);

    let mut state_ok = true;
    let mut state_detail = String::new();
    if let Some(states) = v.get("expectedDbState").and_then(|s| s.as_array()) {
        for s in states {
            let query = s["query"].as_str().unwrap_or("");
            let mut stmt = driver.prepare(query);
            let rows = match stmt.all(&[]) {
                Ok(r) => J::Array(r.iter().map(encode_value).collect()),
                Err(e) => {
                    state_ok = false;
                    state_detail = format!("db-state '{query}' threw: {}", e.message);
                    break;
                }
            };
            if !eq(&rows, &s["rows"]) {
                state_ok = false;
                state_detail = format!("db-state '{query}': {rows} != {}", s["rows"]);
                break;
            }
        }
    }

    if result_ok && state_ok {
        return pass();
    }
    let mut detail: Vec<String> = Vec::new();
    if !result_ok {
        detail.push(format!("result {result} != {}", v["expectedResult"]));
    }
    if !state_ok {
        detail.push(state_detail);
    }
    fail(detail.join("; "))
}

fn run_dialect(v: &J) -> VectorResult {
    let d = match dialect_for(v["dialect"].as_str().unwrap_or("")) {
        Ok(d) => d,
        Err(e) => return fail(format!("threw: {e}")),
    };
    let args = &v["args"];
    let got = d.order_by_nulls(
        args["expr"].as_str().unwrap_or(""),
        args["dir"].as_str().unwrap_or(""),
        args["nulls"].as_str().unwrap_or(""),
    );
    let expected = v["expected"].as_str().unwrap_or("");
    if got == expected {
        pass()
    } else {
        fail(format!("{got:?} != {expected:?}"))
    }
}

fn main() -> ExitCode {
    eprintln!("litedbmodel SCP conformance vectors — Rust runner (litedbmodel_runtime)");
    let dir = vectors_dir();
    let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
            .collect(),
        Err(e) => {
            eprintln!("FAIL: cannot read vectors dir {}: {e}", dir.display());
            return ExitCode::from(2);
        }
    };
    files.sort();

    let suites: Vec<J> = files
        .iter()
        .filter_map(|f| std::fs::read_to_string(f).ok())
        .filter_map(|s| serde_json::from_str::<J>(&s).ok())
        .collect();

    // Pre-flight version sweep (fail-closed): reject the whole run on any suite-version mismatch.
    let mismatched: Vec<&J> = suites
        .iter()
        .filter(|s| {
            s.get("corpusVersion").and_then(|c| c.as_i64()) != Some(SUPPORTED_CORPUS_VERSION)
        })
        .collect();
    if !mismatched.is_empty() {
        for s in &mismatched {
            eprintln!(
                "FAIL-CLOSED: suite '{}' corpusVersion {} != supported {SUPPORTED_CORPUS_VERSION}.",
                s.get("suite").and_then(|x| x.as_str()).unwrap_or("?"),
                s.get("corpusVersion")
                    .and_then(|c| c.as_i64())
                    .unwrap_or(-1),
            );
        }
        println!(
            "{}",
            json!({"lang":"rust","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true})
        );
        return ExitCode::from(2);
    }

    let mut tallies = serde_json::Map::new();
    let mut total_pass = 0i64;
    let mut total_fail = 0i64;

    for suite in &suites {
        let name = suite.get("suite").and_then(|s| s.as_str()).unwrap_or("?");
        let vectors = suite
            .get("vectors")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        eprintln!("\n{name}.json — {} vectors", vectors.len());
        let mut p = 0i64;
        let mut f = 0i64;
        for v in &vectors {
            let r = run_vector(v);
            let vname = v.get("name").and_then(|n| n.as_str()).unwrap_or("?");
            if r.ok {
                eprintln!("  ok  {vname}");
                p += 1;
            } else {
                eprintln!("  XX  {vname}");
                if let Some(d) = &r.detail {
                    eprintln!("      {d}");
                }
                f += 1;
            }
        }
        total_pass += p;
        total_fail += f;
        tallies.insert(name.to_string(), json!({"pass": p, "fail": f}));
    }

    eprintln!(
        "\n{total_pass} passed, {total_fail} failed / {} vectors across {} suites",
        total_pass + total_fail,
        suites.len()
    );
    println!(
        "{}",
        json!({
            "lang": "rust",
            "suites": J::Object(tallies),
            "total_pass": total_pass,
            "total_fail": total_fail,
            "version_mismatch": false,
        })
    );
    if total_fail > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
