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
use crate::errors::{re_error_to_sql_failure, RuntimeError, SqlFailure};
use crate::exec_context::{self, ExecutionContext, StatementIntent, TxDecision};
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

/// Execute a §8 read/exec SqlBundle end-to-end (native read-graph walker + per-statement render).
///
/// The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
/// runtime-core, never re-running litedbmodel's Backend-Compile. A read bundle carries a
/// `readGraph` (the REAL Select-node IR + static statements): a CLOSED-SET native walker drives
/// map/Φ/wiring (never bc `run_behavior`) and renders + executes each node's statements. Returns
/// the component's Φ output — byte-identical to the TS `executeBundle`.
pub fn execute_bundle(bundle: &J, input: &J, driver: &dyn Driver) -> Result<Value, RuntimeError> {
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
) -> Result<Value, RuntimeError> {
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
///
/// Every statement runs through the CENTRAL SEAM on the tx-scoped ctx (§2/§3): the seam resolves the
/// tx's OWNED connection (per-execution ownership) and runs there — a SELECT/RETURNING via
/// [`exec_context::execute`], a non-returning write via [`exec_context::run`]. No direct
/// `driver.prepare(...)` — the tx-owned connection is the ONLY driver contact.
fn exec_statement(
    ctx: &ExecutionContext,
    op: &J,
    scope: &Scope,
    dialect_name: &str,
) -> Result<(Vec<Value>, i64), SqlFailure> {
    let rendered = render_tx_op(op, scope, dialect_name).map_err(|e| plain_failure(&e))?;
    let params: Vec<Value> = rendered.params.iter().map(to_driver_param).collect();
    if is_return_stmt(&rendered.sql) {
        let rows = exec_context::execute(ctx, &rendered.sql, &params, &StatementIntent::write())?;
        let n = rows.len() as i64;
        Ok((rows, n))
    } else {
        let info = exec_context::run(ctx, &rendered.sql, &params, &StatementIntent::write())?;
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
    // Backward-compat wrapper (§6): wrap the raw driver in a base ctx and drive the write plan as its
    // OWN auto-tx with the write=tx guard OFF (the internal per-execution-ownership plane the Phase A
    // ownership proofs + the conformance/livedb runners use — they run a plan directly, NOT inside a
    // user `transaction()`). BYTE-IDENTICAL to the pre-#82 behavior. A user-facing write instead rides
    // [`execute_transaction_bundle_ctx`] with the guard ON.
    let base_ctx = exec_context::for_driver(driver);
    execute_transaction_bundle_ctx(bundle, input, &base_ctx, false)
}

/// Execute a §8 SqlBundle's derived transaction plan on an explicit [`ExecutionContext`] (Phase B /
/// #82) — the ctx-threaded write entry that JOINS an ambient user `transaction()` or (outside one)
/// opens its own guarded auto-tx.
///
/// ## Ambient-tx JOIN vs. its own envelope (the #86 core; rust = explicit ctx)
///
/// It drives [`exec_context::transaction_decided`], which decides the envelope from the passed ctx:
///   - **inside a user `transaction()`** (`ctx.in_transaction()` is true — a connection is pinned) →
///     the plan JOINS: its statements run on the outer's owned connection with NO new BEGIN/COMMIT,
///     so N writes in one boundary are ONE physical transaction (one BEGIN, one COMMIT, one conn);
///   - **outside any transaction** (a base ctx) → it opens its OWN BEGIN…COMMIT on a freshly-acquired
///     owned connection (the per-execution auto-tx; concurrent calls each own a DISTINCT connection ⇒
///     isolated). No isolation/retry here — those ride the user `transaction()` options; a bare
///     auto-tx uses the defaults (a bare `BEGIN`).
///
/// ## write=tx guard (#86)
///
/// With `guard` true (the DEFAULT for a user-facing write), a write with NO ambient user tx is
/// REJECTED via [`crate::tx_options::check_write_allowed`] BEFORE any SQL:
/// [`crate::tx_options::write_outside_transaction`] (no active tx) /
/// [`crate::tx_options::write_in_read_only`] (read-only scope). Inside a `transaction()` the ctx is
/// tx-scoped ⇒ the guard passes and the write joins. `guard` is INTERNAL-only (never exposed on a
/// user-facing surface — per the #86 audit note): the conformance / livedb / ownership-proof paths
/// pass `false` to run a plan as its own auto-tx.
pub fn execute_transaction_bundle_ctx(
    bundle: &J,
    input: &J,
    ctx: &ExecutionContext,
    guard: bool,
) -> Result<Value, SqlFailure> {
    let plan = bundle.get("transaction").filter(|p| !p.is_null()).ok_or_else(|| {
        plain_failure(
            "scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)",
        )
    })?;
    let dialect_name = str_field(bundle, "dialect")?.to_string();
    // Fail closed on an unknown dialect (mirrors the sibling ports).
    dialect_for(&dialect_name).map_err(|e| plain_failure(&e))?;
    let tx_dialect = crate::tx_options::Dialect::parse(&dialect_name)?;

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

    // write=tx guard (#86), enforced at ENTRY so it sees the CALLER's ctx: a write inside a user
    // `transaction()` has a tx-scoped ctx (⇒ passes + JOINS the outer); a bare write outside any
    // boundary has a base ctx (⇒ write_outside_transaction); a write in a read-only scope ⇒
    // write_in_read_only (checked first). Tx-control statements the runtime itself issues (BEGIN/
    // COMMIT/SET) never pass through here — only data-write plans do.
    if guard {
        crate::tx_options::check_write_allowed(
            "WRITE",
            statements
                .first()
                .and_then(|s| s.get("id"))
                .and_then(|i| i.as_str()),
            ctx.in_transaction(),
            ctx.read_only(),
        )?;
    }

    // The write-tx auto-tx defaults (no isolation / no retry / no rollback_only) when this plan opens
    // its OWN envelope (outside a user `transaction()`). Inside one, `transaction_decided` JOINS and
    // ignores these — the outer's options own the envelope. Retry is OFF here: a bare plan auto-tx
    // matches the Phase A byte-identical behavior; the user `transaction()` boundary owns retry.
    let auto_opts = crate::tx_options::TransactionOptions {
        retry_on_error: false,
        ..crate::tx_options::TransactionOptions::default()
    };

    exec_context::transaction_decided(ctx, tx_dialect, &auto_opts, |tx_ctx| {
        let scope = RefCell::new(input_scope.clone());
        let mut executed: Vec<Value> = Vec::new();
        let mut entity: Value = Value::Null;
        let mut returned_rows: Vec<Value> = Vec::new();

        for stmt in statements {
            let op = stmt
                .get("op")
                .ok_or_else(|| plain_failure("scp write: statement missing 'op'"))?;
            let id = stmt
                .get("id")
                .and_then(|i| i.as_str())
                .ok_or_else(|| plain_failure("scp write: statement missing 'id'"))?;
            let (rows, changes) = exec_statement(tx_ctx, op, &scope.borrow(), &dialect_name)?;
            executed.push(Value::Str(id.to_string()));

            if let Some(gate) = stmt.get("gate").and_then(|g| g.as_str()) {
                let reason =
                    gate_short_circuit(gate, rows.len(), changes).map_err(|e| plain_failure(&e))?;
                if let Some(reason) = reason {
                    // A failed gate ROLLBACKs (via the combinator) and returns `committed:false` — a
                    // legitimate outcome, NOT an error.
                    return Ok(TxDecision::Rollback(Value::Obj(vec![
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
                    ])));
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
        Ok(TxDecision::Commit(Value::Obj(out)))
    })
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
