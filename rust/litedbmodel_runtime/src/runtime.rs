//! litedbmodel SCP Rust runtime — the §8 bundle interpreter (WS7e, #34).
//!
//! The thin Rust leg of the multi-language SCP runtime. It consumes the language-neutral §8
//! published `SqlBundle` (pure JSON: `sql`, a fragment tree, closed-set Expression-IR param slots,
//! and an optional transaction plan, dialect-tagged) and executes it against a SQL driver,
//! semantics-identical to the TS reference (`src/scp/runtime.ts` + `write-runtime.ts`) and the
//! audited Python/PHP sibling ports.
//!
//! It re-implements NO generic execution/expression evaluation: bc's [`run_behavior`] owns the
//! plan/map/wire/output orchestration and its `evaluate_expression` owns the CLOSED Expression-IR
//! (ref/refOpt/coalesce/eq/…, the SKIP guards + param slots). This module adds ONLY the
//! SQL-backend concerns (spec §11): render the pre-compiled op per dialect (`render_operation`) →
//! bind params → execute REAL SQL → row→result assembly; and, for a Command bundle, the gate-first
//! write transaction ([`execute_transaction_bundle`]). This mirrors the TS runtime's division of
//! labor exactly (bc-core + a SQL handler). The old standalone `litedbmodel.rs` SQL generation is
//! RETIRED — the SQL now comes wholly from the published bundle.

use std::cell::RefCell;

use behavior_contracts::{run_behavior, ComponentExec, ExecOutcome, Value};
use serde_json::{json, Value as J};

use crate::dialect::{dialect_for, Dialect};
use crate::driver::Driver;
use crate::errors::{re_error_to_sql_failure, SqlFailure};
use crate::render::render_operation;
use crate::value::{decode_scope, encode_value, value_obj_to_scope, Scope};

/// The synthetic port that carries a SQL node's render scope (mirrors TS SCOPE_PORT).
pub const SCOPE_PORT: &str = "__scope";
/// The reserved binding the body write's RETURNING row is exposed under (mirrors TS ENTITY_ROOT).
pub const ENTITY_ROOT: &str = "__entity";

// ── Value normalization at the SQL boundary ───────────────────────────────────

/// Convert a rendered param to a driver-bindable value (mirrors TS `toDriverParam`).
///
/// bc evaluates integers to a plain i64, so no bigint narrowing is needed; a bool is left as-is
/// (bound as 0/1). An object [`Value::Obj`] is an emit payload (`{obj:…}`) serialized to compact
/// JSON for a text column — byte-identical to the TS `JSON.stringify` (no spaces). Arrays are
/// IN-list elements handled during expansion, never reaching here as a single write param.
fn to_driver_param(v: &Value) -> Value {
    match v {
        Value::Obj(_) => Value::Str(compact_json(&encode_value(v))),
        other => other.clone(),
    }
}

/// Serialize JSON with no inter-token spaces (matches JS `JSON.stringify`).
fn compact_json(v: &J) -> String {
    // serde_json's default Formatter already emits compact `{"a":1,"b":2}` output.
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
}

/// A SELECT or a write with RETURNING yields rows (mirrors TS `hasReturn`).
fn is_return_stmt(component: Option<&str>, sql: &str) -> bool {
    if component == Some("Select") {
        return true;
    }
    // case-insensitive whole-word `returning`.
    let lower = sql.to_ascii_lowercase();
    lower
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|tok| tok == "returning")
}

// ── Handler: render → execute → assembly (bc ComponentExec seam) ───────────────

/// The SQL handler registry backing every Catalog leaf (Select/Insert/Update/Delete).
///
/// Mirrors the Python/PHP `_build_handlers`: `run_behavior` dispatches every leaf here via
/// `exec_ctx`, carrying the node id (→ the compiled operation) and the evaluated `__scope` port.
/// The handler renders + executes and returns a bc [`ExecOutcome`] (`Ok(rows)` / `Error(msg)`), so
/// bc folds SQL failures into its Skip/Policy propagation exactly like the TS runtime.
struct SqlHandlers<'a> {
    driver: &'a dyn Driver,
    operations: &'a J,
    dialect: Dialect,
}

impl SqlHandlers<'_> {
    fn execute_rendered(&self, op: &J, scope: &Scope) -> ExecOutcome {
        let rendered = match render_operation(op, scope, self.dialect) {
            Ok(r) => r,
            Err(e) => return ExecOutcome::Error(e),
        };
        let params: Vec<Value> = rendered.params.iter().map(to_driver_param).collect();
        let component = op.get("component").and_then(|c| c.as_str());
        let mut stmt = self.driver.prepare(&rendered.sql);
        if is_return_stmt(component, &rendered.sql) {
            match stmt.all(&params) {
                Ok(rows) => ExecOutcome::Ok(Value::Arr(rows)),
                Err(e) => ExecOutcome::Error(e.message),
            }
        } else {
            match stmt.run(&params) {
                Ok(info) => ExecOutcome::Ok(Value::Arr(vec![Value::Obj(vec![
                    ("changes".into(), Value::Int(info.changes)),
                    ("lastInsertRowid".into(), Value::Int(info.last_insert_rowid)),
                ])])),
                Err(e) => ExecOutcome::Error(e.message),
            }
        }
    }
}

impl ComponentExec for SqlHandlers<'_> {
    fn exec(
        &mut self,
        _component: &str,
        _ports: &[(String, Value)],
        _bound: Option<&Value>,
    ) -> Option<ExecOutcome> {
        // run_behavior always dispatches through exec_ctx (we need the node id → operation).
        Some(ExecOutcome::Error(
            "scp runtime: exec_ctx is required (node id needed to resolve the operation)".into(),
        ))
    }

    fn exec_ctx(
        &mut self,
        node_id: &str,
        component: &str,
        ports: &[(String, Value)],
        _bound: Option<&Value>,
    ) -> Option<ExecOutcome> {
        let op = match self.operations.get(node_id) {
            Some(op) => op,
            None => {
                return Some(ExecOutcome::Error(format!(
                    "scp runtime: no compiled operation for node '{node_id}' ({component})"
                )))
            }
        };
        let scope_val = ports.iter().find(|(k, _)| k == SCOPE_PORT).map(|(_, v)| v);
        let scope = match scope_val.and_then(value_obj_to_scope) {
            Some(s) => s,
            None => {
                return Some(ExecOutcome::Error(format!(
                    "scp runtime: node '{node_id}' surrogate scope did not evaluate to an object"
                )))
            }
        };
        Some(self.execute_rendered(op, &scope))
    }
}

// ── Input normalization (schema-driven — SSoT, no ad-hoc code default) ─────────

/// Normalize omitted OPTIONAL bindings to present-as-null (mirrors TS `normalizeInput`).
///
/// "Optional" comes from the SSoT: EITHER the component's Input Port schema marks the port
/// `required != true`, OR the head is listed in the bundle's `optionalHeads` (a SKIP-guarded /
/// refOpt head the TS compile derived). A REQUIRED, non-optional missing head is left absent so a
/// real wiring bug surfaces loudly as bc's UNKNOWN_BINDING — never silently defaulted.
fn normalize_input(component: &J, optional_heads: &[String], input_scope: &Scope) -> Scope {
    let mut out = input_scope.clone();
    let present = |scope: &Scope, k: &str| scope.iter().any(|(sk, _)| sk == k);
    if let Some(ports) = component.get("inputPorts").and_then(|p| p.as_object()) {
        for (port, schema) in ports {
            let required = schema.get("required") == Some(&J::Bool(true));
            if !required && !present(&out, port) {
                out.push((port.clone(), Value::Null));
            }
        }
    }
    for head in optional_heads {
        if !present(&out, head) {
            out.push((head.clone(), Value::Null));
        }
    }
    out
}

// ── Public runtime entrypoints ────────────────────────────────────────────────

/// Render a §8 CompiledOperation → `{"sql", "params"}` for a dialect name (render axis API).
pub fn render_operation_bundle(
    operation: &J,
    scope: &Scope,
    dialect_name: &str,
) -> Result<J, String> {
    let rendered = render_operation(operation, scope, dialect_for(dialect_name)?)?;
    Ok(json!({
        "sql": rendered.sql,
        "params": rendered.params.iter().map(encode_value).collect::<Vec<_>>(),
    }))
}

/// Execute a §8 read/exec SqlBundle end-to-end (bc `run_behavior` + SQL handlers).
///
/// The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
/// runtime-core, never re-running litedbmodel's Backend-Compile. Returns the component's `output`
/// (Φ merge) with each SQL node's slot filled by its executed row list — byte-identical to the TS
/// `executeBundle`.
pub fn execute_bundle(bundle: &J, input: &J, driver: &dyn Driver) -> Result<Value, SqlFailure> {
    let surrogate = bundle
        .get("component")
        .ok_or_else(|| plain_failure("scp runtime: bundle missing 'component'"))?;
    let dialect = dialect_for(str_field(bundle, "dialect")?).map_err(|e| plain_failure(&e))?;
    let ir = json!({
        "irVersion": bundle.get("irVersion"),
        "exprVersion": bundle.get("exprVersion"),
        "components": [surrogate],
    });
    let operations = bundle
        .get("operations")
        .ok_or_else(|| plain_failure("scp runtime: bundle missing 'operations'"))?;

    let input_scope = decode_scope(input).map_err(|e| plain_failure(&e))?;
    let optional_heads = string_array(bundle.get("optionalHeads"));
    let normalized = normalize_input(surrogate, &optional_heads, &input_scope);
    let entry = surrogate.get("name").and_then(|n| n.as_str());

    let mut handlers = SqlHandlers {
        driver,
        operations,
        dialect,
    };
    run_behavior(&ir, &mut handlers, &normalized, entry)
        .map_err(|e| re_error_to_sql_failure(&e.to_string()))
}

// ── Write-time relations: gate-first transaction (spec §6 — port of write-runtime.ts) ──

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

/// Render + execute one tx statement; return (rows, changes). Mirrors TS `execStatement`.
fn exec_statement(
    driver: &dyn Driver,
    op: &J,
    scope: &Scope,
    dialect: Dialect,
) -> Result<(Vec<Value>, i64), SqlFailure> {
    let rendered = render_operation(op, scope, dialect).map_err(|e| plain_failure(&e))?;
    let params: Vec<Value> = rendered.params.iter().map(to_driver_param).collect();
    let component = op.get("component").and_then(|c| c.as_str());
    let mut stmt = driver.prepare(&rendered.sql);
    if is_return_stmt(component, &rendered.sql) {
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
/// edges → emits); a failing gate ROLLBACKs and the tail never executes. On success COMMITs and
/// returns the `$.entity` RETURNING row. A short-circuit returns `committed:false` (a legitimate
/// gate outcome), NOT a raised failure; a driver failure ROLLBACKs then raises a mapped SqlFailure.
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
    let dialect = dialect_for(str_field(bundle, "dialect")?).map_err(|e| plain_failure(&e))?;

    let input_scope = decode_scope(input).map_err(|e| plain_failure(&e))?;
    let statements = plan
        .get("statements")
        .and_then(|s| s.as_array())
        .ok_or_else(|| plain_failure("scp write: transaction plan missing 'statements'"))?;
    let entity_from = plan.get("entityFrom").and_then(|e| e.as_str());

    driver.prepare("BEGIN").run(&[])?;
    let scope = RefCell::new(input_scope);
    let mut executed: Vec<Value> = Vec::new();
    let mut entity: Value = Value::Null;

    let mut run = || -> Result<Value, SqlFailure> {
        for stmt in statements {
            let op = stmt
                .get("op")
                .ok_or_else(|| plain_failure("scp write: statement missing 'op'"))?;
            let id = stmt
                .get("id")
                .and_then(|i| i.as_str())
                .ok_or_else(|| plain_failure("scp write: statement missing 'id'"))?;
            let (rows, changes) = exec_statement(driver, op, &scope.borrow(), dialect)?;
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

            if entity_from == Some(id) {
                entity = rows.into_iter().next().unwrap_or(Value::Null);
                if !matches!(entity, Value::Null) {
                    scope
                        .borrow_mut()
                        .push((ENTITY_ROOT.to_string(), entity.clone()));
                }
            }
        }

        driver.prepare("COMMIT").run(&[])?;
        Ok(Value::Obj(vec![
            ("committed".into(), Value::Bool(true)),
            ("entity".into(), entity.clone()),
            ("executed".into(), Value::Arr(executed.clone())),
        ]))
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
    // A structural error carrying a SQLITE_ tag is re-surfaced with the mapped kind; otherwise a
    // plain driver_error. This keeps handler-surfaced failures (embedding a code) structured.
    map_or_plain(message)
}

fn map_or_plain(message: &str) -> SqlFailure {
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

fn string_array(v: Option<&J>) -> Vec<String> {
    v.and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|e| e.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}
