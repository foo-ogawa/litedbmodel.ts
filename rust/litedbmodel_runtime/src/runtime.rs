//! litedbmodel SCP Rust runtime — the static makeSQL bundle interpreter (WS7e #34; makeSQL flip,
//! epic #43/#45).
//!
//! The thin Rust leg of the multi-language SCP runtime. It consumes the language-neutral, portable
//! static-makeSQL artifacts the corpus ships (pure JSON: a read `readGraph` = a bc surrogate
//! `ComponentGraphIR` + per-node STATIC statement templates, or a write `transaction` plan of
//! gate-first makeSQL statements) and executes them against a SQL driver — SEMANTICS-IDENTICAL to
//! the TS reference (`src/scp/runtime.ts` + `src/scp/makesql/*`) and the audited Python/Go/PHP
//! sibling ports.
//!
//! It re-implements NO generic execution/expression evaluation: bc's [`run_behavior`] owns the
//! plan/map/wire/output orchestration and its `evaluate_expression` owns the CLOSED Expression-IR
//! (ref/refOpt/coalesce/eq/…, the SKIP guards + deferred value-specs). The static makeSQL
//! assemble/render/execute lives in [`crate::static_bundle`]; this module is the thin FACADE that
//! dispatches a bundle to the read graph executor ([`execute_bundle`]) or the gate-first write
//! transaction ([`execute_transaction_bundle`]). The reduced fragment-tree render path (`render.rs`)
//! is RETIRED for the SQL path — makeSQL is the sole read/render path.

use std::cell::RefCell;

use behavior_contracts::Value;

use crate::dialect::dialect_for;
use crate::driver::Driver;
use crate::errors::{re_error_to_sql_failure, SqlFailure};
use crate::node::{encode_value, Node as J};
use crate::static_bundle::{
    execute_read_graph, execute_read_graph_pooled, render_read_primary, render_tx_op,
    to_driver_param,
};
use crate::value::{decode_scope, Scope};

/// The reserved binding the body write's RETURNING row is exposed under (mirrors TS ENTITY_ROOT).
pub const ENTITY_ROOT: &str = "__entity";

/// A SELECT (leading verb) or a write with RETURNING yields rows (mirrors TS `hasReturn`).
fn is_return_stmt(sql: &str) -> bool {
    let head: String = sql.chars().take(8).collect::<String>().to_ascii_lowercase();
    if head
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|tok| tok == "select")
    {
        return true;
    }
    let lower = sql.to_ascii_lowercase();
    lower
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|tok| tok == "returning")
}

// ── Public runtime entrypoints ────────────────────────────────────────────────

/// Render a §8 read bundle's `readGraph` primary node → `{"sql", "params"}` for the render axis.
pub fn render_read_primary_bundle(read_graph: &J, input: &J) -> Result<J, String> {
    let scope = decode_scope(input)?;
    let rendered = render_read_primary(read_graph, &scope)?;
    Ok(J::Object(vec![
        ("sql".to_string(), J::Str(rendered.sql)),
        (
            "params".to_string(),
            J::Array(rendered.params.iter().map(encode_value).collect()),
        ),
    ]))
}

/// Execute a §8 read/exec SqlBundle end-to-end (bc `run_behavior` + the makeSQL handler).
///
/// The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
/// runtime-core, never re-running litedbmodel's Backend-Compile. A read bundle carries a
/// `readGraph` (the surrogate IR + static statements): bc drives map/Φ/wiring and the makeSQL
/// handler renders + executes each node. Returns the component's Φ output — byte-identical to the
/// TS `executeBundle`.
pub fn execute_bundle(bundle: &J, input: &J, driver: &dyn Driver) -> Result<Value, SqlFailure> {
    let read_graph = bundle.get("readGraph").filter(|g| !g.is_null()).ok_or_else(|| {
        let name = bundle.get("name").and_then(|n| n.as_str()).unwrap_or("");
        plain_failure(&format!(
            "scp runtime: bundle '{name}' carries no read graph (single-statement writes ride the write path)"
        ))
    })?;
    let input_scope = decode_scope(input).map_err(|e| plain_failure(&e))?;
    execute_read_graph(read_graph, &input_scope, driver)
}

/// Execute a §8 read SqlBundle on a `Sync` (pooled) driver — the PRODUCTION live PG/MySQL read
/// path (#40). Identical to [`execute_bundle`] except the read graph runs through
/// [`execute_read_graph_pooled`], which dispatches INDEPENDENT sibling read nodes of a plan stage
/// CONCURRENTLY (each on its own pooled connection), capped at the plan `concurrency` (default 16).
///
/// The Φ output is byte-identical to the serial [`execute_bundle`] — parallelism changes only the
/// wall-clock. A single-relation / zero-sibling read graph (or `concurrency <= 1`) transparently
/// runs serially, exactly as before. Writes are UNCHANGED (`execute_transaction_bundle` stays
/// serial on one pinned connection — this path is READ-only).
pub fn execute_bundle_pooled(
    bundle: &J,
    input: &J,
    driver: &(dyn Driver + Sync),
) -> Result<Value, SqlFailure> {
    let read_graph = bundle.get("readGraph").filter(|g| !g.is_null()).ok_or_else(|| {
        let name = bundle.get("name").and_then(|n| n.as_str()).unwrap_or("");
        plain_failure(&format!(
            "scp runtime: execute_bundle_pooled requires a read bundle ('{name}' carries no read graph; \
             the write-tx path stays serial via execute_transaction_bundle)"
        ))
    })?;
    let input_scope = decode_scope(input).map_err(|e| plain_failure(&e))?;
    execute_read_graph_pooled(read_graph, &input_scope, driver)
}

// ── Write-time relations: gate-first transaction (spec §6 — port of tx.ts) ──────

/// Evaluate a gate rule on a statement result → short-circuit reason, or None to continue.
fn gate_short_circuit(
    gate: &str,
    rows: usize,
    changes: i64,
) -> Result<Option<&'static str>, String> {
    match gate {
        "existsElseRollback" => Ok(if rows == 0 {
            Some("requires_absent")
        } else {
            None
        }),
        "insertedElseRollback" => Ok(if changes == 0 {
            Some("unique_collision")
        } else {
            None
        }),
        "insertedElseNoop" => Ok(if changes == 0 {
            Some("idempotent_duplicate")
        } else {
            None
        }),
        other => Err(format!("scp write: unknown gate rule '{other}'")),
    }
}

/// Render + execute one tx statement's makeSQL op; return (rows, changes). Mirrors TS `execStatement`.
fn exec_statement(
    driver: &dyn Driver,
    op: &J,
    scope: &Scope,
    dialect_name: &str,
) -> Result<(Vec<Value>, i64), SqlFailure> {
    let rendered = render_tx_op(op, scope, dialect_name).map_err(|e| plain_failure(&e))?;
    let params: Vec<Value> = rendered.params.iter().map(to_driver_param).collect();
    let mut stmt = driver.prepare(&rendered.sql);
    if is_return_stmt(&rendered.sql) {
        let rows = stmt.all(&params)?;
        let n = rows.len() as i64;
        Ok((rows, n))
    } else {
        let info = stmt.run(&params)?;
        Ok((Vec::new(), info.changes))
    }
}

/// Execute a §8 SqlBundle's derived transaction plan as ONE real transaction (gate-first).
///
/// Byte-for-byte port of the TS `executeTransactionBundle` → `executeTransaction` (spec §6):
/// statements run in the plan's fixed order (requires → idempotency → unique → body → derive →
/// edges → emits, topo-ordered for composite DAGs); a failing gate ROLLBACKs and the tail never
/// executes. On success COMMITs and returns the `$.entity` RETURNING row. A short-circuit returns
/// `committed:false` (a legitimate gate outcome), NOT a raised failure; a driver failure ROLLBACKs
/// then raises a mapped SqlFailure.
pub fn execute_transaction_bundle(
    bundle: &J,
    input: &J,
    driver: &dyn Driver,
) -> Result<Value, SqlFailure> {
    let plan = bundle.get("transaction").filter(|p| !p.is_null()).ok_or_else(|| {
        plain_failure(
            "scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)",
        )
    })?;
    let dialect_name = str_field(bundle, "dialect")?.to_string();
    // Fail closed on an unknown dialect (mirrors the sibling ports).
    dialect_for(&dialect_name).map_err(|e| plain_failure(&e))?;

    let input_scope = decode_scope(input).map_err(|e| plain_failure(&e))?;
    let statements = plan
        .get("statements")
        .and_then(|s| s.as_array())
        .ok_or_else(|| plain_failure("scp write: transaction plan missing 'statements'"))?;
    let entity_from = plan.get("entityFrom").and_then(|e| e.as_str());
    // Batch mode (createMany/updateMany/deleteMany): gate-free, ref-free plan (entityFrom null, every
    // statement a plain body) — accumulate each body statement's RETURNING rows in order.
    let is_batch = entity_from.is_none()
        && statements.iter().all(|s| {
            s.get("gate").and_then(|g| g.as_str()).is_none()
                && s.get("binds").and_then(|b| b.as_str()).is_none()
                && s.get("role").and_then(|r| r.as_str()) == Some("body")
        });

    driver.prepare("BEGIN").run(&[])?;
    let scope = RefCell::new(input_scope);
    let mut executed: Vec<Value> = Vec::new();
    let mut entity: Value = Value::Null;
    let mut returned_rows: Vec<Value> = Vec::new();

    let mut run = || -> Result<Value, SqlFailure> {
        for stmt in statements {
            let op = stmt
                .get("op")
                .ok_or_else(|| plain_failure("scp write: statement missing 'op'"))?;
            let id = stmt
                .get("id")
                .and_then(|i| i.as_str())
                .ok_or_else(|| plain_failure("scp write: statement missing 'id'"))?;
            let (rows, changes) = exec_statement(driver, op, &scope.borrow(), &dialect_name)?;
            executed.push(Value::Str(id.to_string()));

            if let Some(gate) = stmt.get("gate").and_then(|g| g.as_str()) {
                let reason =
                    gate_short_circuit(gate, rows.len(), changes).map_err(|e| plain_failure(&e))?;
                if let Some(reason) = reason {
                    driver.prepare("ROLLBACK").run(&[])?;
                    return Ok(Value::Obj(vec![
                        ("committed".into(), Value::Bool(false)),
                        (
                            "shortCircuit".into(),
                            Value::Obj(vec![
                                ("statementId".into(), Value::Str(id.to_string())),
                                ("reason".into(), Value::Str(reason.to_string())),
                            ]),
                        ),
                        ("entity".into(), Value::Null),
                        ("executed".into(), Value::Arr(executed.clone())),
                    ]));
                }
            }

            // Batch mode: capture ALL of this body statement's RETURNING rows (in order) before we
            // consume `rows` into `first_row` below.
            if is_batch && !rows.is_empty() {
                returned_rows.push(Value::Arr(rows.clone()));
            }
            let first_row = rows.into_iter().next();

            // Capture the SOLE body RETURNING row as `$.entity` (WS5 single-write back-compat).
            if entity_from == Some(id) {
                entity = first_row.clone().unwrap_or(Value::Null);
                if !matches!(entity, Value::Null) {
                    scope
                        .borrow_mut()
                        .push((ENTITY_ROOT.to_string(), entity.clone()));
                }
            }

            // WS8a composite: bind THIS statement's RETURNING row under its `binds` name so a later
            // `$.ref.<binds>.<field>` resolves against it (the tx-DAG data-dependency edge). Self-
            // describing — the runtime binds the row the plan told it to; no re-derivation.
            if let Some(binds) = stmt.get("binds").and_then(|b| b.as_str()) {
                if let Some(row) = first_row {
                    if !matches!(row, Value::Null) {
                        scope.borrow_mut().push((binds.to_string(), row));
                    }
                }
            }
        }

        driver.prepare("COMMIT").run(&[])?;
        let mut out = vec![
            ("committed".to_string(), Value::Bool(true)),
            ("entity".to_string(), entity.clone()),
            ("executed".to_string(), Value::Arr(executed.clone())),
        ];
        if !returned_rows.is_empty() {
            out.push((
                "returnedRows".to_string(),
                Value::Arr(returned_rows.clone()),
            ));
        }
        Ok(Value::Obj(out))
    };

    match run() {
        Ok(v) => Ok(v),
        Err(e) => {
            // best-effort rollback; the original failure is surfaced.
            let _ = driver.prepare("ROLLBACK").run(&[]);
            Err(e)
        }
    }
}

// ── Dialect primitive (render axis + dialect suite) ────────────────────────────

/// The dialect NULLS-ordering primitive (native for PG/SQLite, IS NULL for MySQL).
pub fn order_by_nulls(
    expr: &str,
    direction: &str,
    nulls: &str,
    dialect_name: &str,
) -> Result<String, String> {
    Ok(dialect_for(dialect_name)?.order_by_nulls(expr, direction, nulls))
}

// ── small helpers ──────────────────────────────────────────────────────────────

fn plain_failure(message: &str) -> SqlFailure {
    if message.contains("SQLITE_") {
        re_error_to_sql_failure(message)
    } else {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: message.to_string(),
        }
    }
}

fn str_field<'a>(v: &'a J, key: &str) -> Result<&'a str, SqlFailure> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| plain_failure(&format!("scp runtime: bundle missing string '{key}'")))
}
