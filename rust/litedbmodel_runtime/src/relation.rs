//! litedbmodel v2 SCP — read-relation batch EXECUTION (Rust port of src/scp/relation.ts, #43).
//!
//! Byte-for-byte port of the TS reference relation runtime: the STATIC pre-compiled batch op
//! (`bundle["relations"][name]` — pure JSON) is EXECUTED, never regenerated. A relation op carries
//! the batched child SELECT text with ONE `?` for the deduped-key array param; the runtime dedupes
//! the parent keys, resolves the deferred PG array cast from the REAL keys, renders `?`→`$N`, short-
//! circuits an empty key set (NO query), runs the batch, groups the child rows by target key, and
//! distributes them onto the parents per cardinality (hasMany → list, belongsTo/hasOne → single or
//! null). The SAME `run_relation_op` / `distribute_to_parent` / `dedupe_keys` the TS eager path uses.
//!
//! PG binds the deduped key set as ONE array param (`Value::Arr` → `PgParam::Array`, `= ANY($1::T[])`);
//! MySQL/SQLite bind the JSON-encoded array string (server-side `json_each`/`JSON_TABLE`). The batch is
//! grouped-then-distributed by key, so the hydrated result is deterministic regardless of query-
//! completion order (#40 parallel-safe). The primary read rides `execute_bundle_pooled` (the #40
//! executor-layer sibling fan-out); the declaratively-selected relations are then batch-loaded in
//! declaration order — mirroring the TS `buildResultSet` sequential declarative-select loop exactly.

use std::collections::HashMap;

use behavior_contracts::Value;

use crate::driver::Driver;
use crate::errors::SqlFailure;
use crate::exec_context::{self, StatementIntent};
use crate::node::{write_json_string, Node as J};
use crate::runtime::execute_bundle_pooled;
use crate::static_bundle::{render_placeholders, resolve_pg_array_cast};

/// One relation batch op read out of `bundle["relations"][name]` (pure JSON). Single-key relations
/// carry `parent_key`/`target_key`; composite (#47 item 1) carry `parent_keys`/`target_keys`.
struct RelationOp {
    name: String,
    kind: String,
    parent_key: String,
    target_key: String,
    parent_keys: Option<Vec<String>>,
    target_keys: Option<Vec<String>>,
    dialect: String,
    /// CROSS-DB (V0 R1): the target model's connection tag (None for a same-DB relation).
    connection: Option<String>,
    sql: String,
}

fn op_from_json(o: &J) -> RelationOp {
    let s = |k: &str| o.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let str_arr = |k: &str| -> Option<Vec<String>> {
        o.get(k).and_then(|v| v.as_array()).map(|a| {
            a.iter()
                .map(|e| e.as_str().unwrap_or("").to_string())
                .collect()
        })
    };
    RelationOp {
        name: s("name"),
        kind: s("kind"),
        parent_key: s("parentKey"),
        target_key: s("targetKey"),
        parent_keys: str_arr("parentKeys"),
        target_keys: str_arr("targetKeys"),
        dialect: s("dialect"),
        connection: o
            .get("connection")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        sql: s("sql"),
    }
}

impl RelationOp {
    /// The ordered PARENT / CHILD key columns (single-key → 1-element; composite → the tuple).
    fn parent_key_cols(&self) -> Vec<String> {
        self.parent_keys
            .clone()
            .unwrap_or_else(|| vec![self.parent_key.clone()])
    }
    fn target_key_cols(&self) -> Vec<String> {
        self.target_keys
            .clone()
            .unwrap_or_else(|| vec![self.target_key.clone()])
    }
}

/// The stringified key identity for dedupe/grouping (tuple → space-joined scalars, mirror of TS).
fn key_identity(values: &[Value]) -> String {
    values
        .iter()
        .map(stringify_key)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Mirror TS `String(v)` for the key-identity used by dedupe + grouping: a whole float prints as an
/// integer (a scanned int column may arrive as Float or Int), bool → "true"/"false".
fn stringify_key(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                (*f as i64).to_string()
            } else {
                f.to_string()
            }
        }
        Value::Str(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Look up a field of an object [`Value`] by key (linear scan — bc objects are ordered pair lists).
fn obj_get<'a>(o: &'a Value, key: &str) -> Option<&'a Value> {
    match o {
        Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}

/// The deduped, non-null parent-key TUPLES (insertion order preserved). Drop a tuple if ANY key
/// column is null; dedupe on the stringified tuple identity. Port of the TS `dedupeKeys`.
fn dedupe_keys(parents: &[Value], key_cols: &[String]) -> Vec<Vec<Value>> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<Vec<Value>> = Vec::new();
    for p in parents {
        let mut tuple: Vec<Value> = Vec::with_capacity(key_cols.len());
        let mut any_null = false;
        for c in key_cols {
            match obj_get(p, c) {
                None | Some(Value::Null) => {
                    any_null = true;
                    break;
                }
                Some(v) => tuple.push(v.clone()),
            }
        }
        if any_null {
            continue;
        }
        if seen.insert(key_identity(&tuple)) {
            out.push(tuple);
        }
    }
    out
}

/// Bind the deduped keys to the op's params per dialect + arity (mirrors TS `bindKeys`). Single-key:
/// PG → ONE scalar array (`Value::Arr`); MySQL/SQLite → ONE JSON scalar-array string. Composite:
/// PG → ONE array param PER key column (transposed tuples); MySQL/SQLite → ONE JSON array-of-tuples
/// string. Returns the positional param list.
fn bind_keys(op: &RelationOp, tuples: &[Vec<Value>]) -> Vec<Value> {
    let composite = op.parent_keys.is_some();
    if op.dialect == "postgres" {
        let n_cols = if composite {
            op.parent_key_cols().len()
        } else {
            1
        };
        return (0..n_cols)
            .map(|col| Value::Arr(tuples.iter().map(|t| t[col].clone()).collect()))
            .collect();
    }
    // MySQL/SQLite: ONE JSON param — a scalar array (single-key) or an array-of-tuples (composite).
    let json = if composite {
        json_tuples(tuples)
    } else {
        json_array(&tuples.iter().map(|t| t[0].clone()).collect::<Vec<_>>())
    };
    vec![Value::Str(json)]
}

/// Compact JSON serialization of one scalar key (mirror of TS `JSON.stringify` element form).
fn json_scalar(k: &Value) -> String {
    match k {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                (*f as i64).to_string()
            } else {
                f.to_string()
            }
        }
        Value::Str(s) => {
            let mut out = String::new();
            write_json_string(s, &mut out);
            out
        }
        other => format!("{other:?}"),
    }
}

/// Compact JSON array serialization matching TS `JSON.stringify` for scalar keys (int/str/bool/null).
fn json_array(keys: &[Value]) -> String {
    let parts: Vec<String> = keys.iter().map(json_scalar).collect();
    format!("[{}]", parts.join(","))
}

/// Compact JSON array-of-tuples (`[[k1,k2],…]`) — the composite MySQL/SQLite JSON param.
fn json_tuples(tuples: &[Vec<Value>]) -> String {
    let parts: Vec<String> = tuples.iter().map(|t| json_array(t)).collect();
    format!("[{}]", parts.join(","))
}

/// The child rows grouped for a batch: stringified target-key identity → child rows.
type RelationBatch = HashMap<String, Vec<Value>>;

/// Run ONE relation batch op for a set of parent rows (port of TS `runRelationOp`). Dedup the parent-
/// key tuples, resolve the deferred PG array cast(s) from the REAL keys (one per key column for
/// composite) BEFORE the `?`→`$N` render; on a NON-empty key set execute binding the keys (single
/// array / per-column arrays / JSON tuples) and group the child rows by target-key identity. An
/// EMPTY key set issues NO query, matching TS.
fn run_relation_op(
    op: &RelationOp,
    parents: &[Value],
    driver: &dyn Driver,
) -> Result<RelationBatch, SqlFailure> {
    let p_cols = op.parent_key_cols();
    let keys = dedupe_keys(parents, &p_cols);
    let mut batch: RelationBatch = HashMap::new();
    let mut sql = op.sql.clone();
    if op.dialect == "postgres" {
        for col in 0..p_cols.len() {
            let col_vals: Vec<Value> = keys.iter().map(|t| t[col].clone()).collect();
            sql = resolve_pg_array_cast(&sql, &col_vals);
        }
    }
    let sql = render_placeholders(&sql, &op.dialect);
    if keys.is_empty() {
        return Ok(batch);
    }
    let t_cols = op.target_key_cols();
    let bound = bind_keys(op, &keys);
    // The relation batch read funnels through the CENTRAL SEAM (§2) on a backward-compat ctx over the
    // relation's driver (no tx — relation batches are read-only; empty middleware) — one pooled
    // connection per batch, byte-identical to the pre-seam `driver.prepare().all()`.
    let ctx = exec_context::for_driver(driver);
    let rows = exec_context::execute(&ctx, &sql, &bound, &StatementIntent::read())?;
    for row in rows {
        let tuple: Vec<Value> = t_cols
            .iter()
            .map(|c| obj_get(&row, c).cloned().unwrap_or(Value::Null))
            .collect();
        let k = key_identity(&tuple);
        batch.entry(k).or_default().push(row);
    }
    Ok(batch)
}

/// Distribute a resolved batch onto ONE parent per cardinality (port of TS `distributeToParent`):
/// hasMany → the child list (`[]` when none); belongsTo/hasOne → the single child (or null). Keyed
/// by the parent's key-tuple identity.
fn distribute_to_parent(op: &RelationOp, parent: &Value, batch: &RelationBatch) -> Value {
    let p_cols = op.parent_key_cols();
    let mut tuple: Vec<Value> = Vec::with_capacity(p_cols.len());
    let mut any_null = false;
    for c in &p_cols {
        match obj_get(parent, c) {
            None | Some(Value::Null) => {
                any_null = true;
                break;
            }
            Some(v) => tuple.push(v.clone()),
        }
    }
    let rows = if any_null {
        None
    } else {
        batch.get(&key_identity(&tuple))
    };
    if op.kind == "hasMany" {
        return Value::Arr(rows.cloned().unwrap_or_default());
    }
    match rows {
        Some(r) if !r.is_empty() => r[0].clone(),
        _ => Value::Null,
    }
}

/// Batch-load + hydrate ONE declared relation onto an ALREADY-FETCHED parent row list, using the
/// SAME `run_relation_op` / `distribute_to_parent` the runtime's own read path uses (NO reimplemented
/// grouping — the semantics stay single-sourced here). `op_json` is the relation op as it appears
/// under `bundle["relations"][name]` (pure JSON). The public seam the codegen bench cell uses: it
/// runs the GENERATED de-interpreted module for the primary read (its own distinct code entry — NOT
/// `execute_bundle`), then hydrates the companion relation through this shared runtime stitch so the
/// hydrated result is byte-identical to `read_bundle_pooled`'s.
pub fn stitch_relation(
    op_json: &J,
    mut parents: Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<Value>, SqlFailure> {
    let op = op_from_json(op_json);
    let batch = run_relation_op(&op, &parents, driver)?;
    for row in parents.iter_mut() {
        let child = distribute_to_parent(&op, row, &batch);
        if let Value::Obj(pairs) = row {
            pairs.push((op.name.clone(), child));
        }
    }
    Ok(parents)
}

/// Run a READ bundle's primary row list, then batch-load + hydrate the selected relations onto each
/// parent (port of the TS `readBundle` typed-object surface, declarative-select path). The primary
/// read rides [`execute_bundle_pooled`] (the #40 executor-layer sibling fan-out); each named relation
/// in `with_names` is then batch-prefetched ONCE over the whole page (staged, no N+1) via the SAME
/// `run_relation_op` and attached onto each parent as an own field — in declaration order, mirroring
/// the TS `buildResultSet` sequential declarative-select loop.
/// The driver a relation runs against: its tagged cross-DB connection, else the primary `driver`.
/// CROSS-DB (V0 R1): a relation whose op carries a `connection` tag (its target model lives in a
/// DIFFERENT DB — v1 `LazyRelation.ts:236`) routes to `connections[tag]`. Loud failure when the tag
/// has no registered driver (a real wiring bug — never a silent same-DB fallback that would run the
/// target's query on the wrong DB). Untagged relations use the primary `driver`.
fn driver_for_op<'a>(
    op: &RelationOp,
    driver: &'a (dyn Driver + Sync),
    connections: &'a std::collections::HashMap<String, &'a (dyn Driver + Sync)>,
) -> Result<&'a (dyn Driver + Sync), SqlFailure> {
    match &op.connection {
        None => Ok(driver),
        Some(tag) => connections
            .get(tag.as_str())
            .copied()
            .ok_or_else(|| SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: format!(
                    "cross-DB relation '{}': no driver registered for connection '{}' \
                 (pass it in read_bundle_pooled connections)",
                    op.name, tag
                ),
            }),
    }
}

pub fn read_bundle_pooled(
    bundle: &J,
    input: &J,
    driver: &(dyn Driver + Sync),
    with_names: &[String],
    connections: &std::collections::HashMap<String, &(dyn Driver + Sync)>,
) -> Result<Value, SqlFailure> {
    let out = execute_bundle_pooled(bundle, input, driver)?;
    let mut rows = match out {
        Value::Arr(rows) => rows,
        _ => {
            return Err(SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: "scp read: the read behavior output is not a row list; the typed-object \
                          read surface expects a Select-shaped output"
                    .into(),
            })
        }
    };
    let relations = bundle.get("relations");
    for name in with_names {
        let op_json = relations
            .and_then(|r| r.get(name))
            .ok_or_else(|| SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: format!(
                    "declarative select: relation '{name}' is not declared on this model"
                ),
            })?;
        let op = op_from_json(op_json);
        let rel_driver = driver_for_op(&op, driver, connections)?;
        let batch = run_relation_op(&op, &rows, rel_driver)?;
        for row in rows.iter_mut() {
            let child = distribute_to_parent(&op, row, &batch);
            if let Value::Obj(pairs) = row {
                pairs.push((name.clone(), child));
            }
        }
    }
    Ok(Value::Arr(rows))
}
