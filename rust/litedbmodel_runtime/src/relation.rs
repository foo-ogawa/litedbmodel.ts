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
use crate::runtime::execute_bundle_pooled;
use crate::static_bundle::{render_placeholders, resolve_pg_array_cast};

/// One relation batch op read out of `bundle["relations"][name]` (pure JSON).
struct RelationOp {
    kind: String,
    parent_key: String,
    target_key: String,
    dialect: String,
    sql: String,
}

fn op_from_json(o: &serde_json::Value) -> RelationOp {
    let s = |k: &str| o.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
    RelationOp {
        kind: s("kind"),
        parent_key: s("parentKey"),
        target_key: s("targetKey"),
        dialect: s("dialect"),
        sql: s("sql"),
    }
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

/// The deduped, non-null parent-key values (insertion order preserved — deterministic). A byte-for-
/// byte port of the TS `dedupeKeys` (skip null, dedupe on String(v), keep first-seen order).
fn dedupe_keys(parents: &[Value], parent_key: &str) -> Vec<Value> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<Value> = Vec::new();
    for p in parents {
        match obj_get(p, parent_key) {
            None | Some(Value::Null) => continue,
            Some(v) => {
                let s = stringify_key(v);
                if seen.insert(s) {
                    out.push(v.clone());
                }
            }
        }
    }
    out
}

/// Bind the deduped key set to the op's single array param per dialect (mirrors TS `bindKeys`):
/// PG → the array verbatim (`Value::Arr` → `PgParam::Array`); MySQL/SQLite → the JSON-encoded array
/// string (server-side expansion). Compact JSON matches the TS `JSON.stringify` byte form.
fn bind_keys(op: &RelationOp, keys: &[Value]) -> Value {
    if op.dialect == "postgres" {
        Value::Arr(keys.to_vec())
    } else {
        Value::Str(json_array(keys))
    }
}

/// Compact JSON array serialization matching TS `JSON.stringify` for scalar keys (int/str/bool/null).
fn json_array(keys: &[Value]) -> String {
    let parts: Vec<String> = keys
        .iter()
        .map(|k| match k {
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
            Value::Str(s) => serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()),
            other => format!("{other:?}"),
        })
        .collect();
    format!("[{}]", parts.join(","))
}

/// The child rows grouped for a batch: stringified target-key → child rows.
type RelationBatch = HashMap<String, Vec<Value>>;

/// Run ONE relation batch op for a set of parent rows (port of TS `runRelationOp`). Dedup the parent
/// keys, resolve the deferred PG array cast from the REAL keys BEFORE the `?`→`$N` render (PG only),
/// render placeholders; on a NON-empty key set execute the batch binding the keys as the SINGLE array
/// param and group the child rows by target key. An EMPTY key set issues NO query (the correct empty-
/// set behaviour), matching TS.
fn run_relation_op(
    op: &RelationOp,
    parents: &[Value],
    driver: &dyn Driver,
) -> Result<RelationBatch, SqlFailure> {
    let keys = dedupe_keys(parents, &op.parent_key);
    let mut batch: RelationBatch = HashMap::new();
    let mut sql = op.sql.clone();
    if op.dialect == "postgres" {
        sql = resolve_pg_array_cast(&sql, &keys);
    }
    let sql = render_placeholders(&sql, &op.dialect);
    if keys.is_empty() {
        return Ok(batch);
    }
    let bound = bind_keys(op, &keys);
    let rows = driver.prepare(&sql).all(std::slice::from_ref(&bound))?;
    for row in rows {
        let k = obj_get(&row, &op.target_key)
            .map(stringify_key)
            .unwrap_or_else(|| "null".to_string());
        batch.entry(k).or_default().push(row);
    }
    Ok(batch)
}

/// Distribute a resolved batch onto ONE parent per cardinality (port of TS `distributeToParent`):
/// hasMany → the child list (`[]` when none); belongsTo/hasOne → the single child (or null). Keyed
/// by String(parent[parentKey]).
fn distribute_to_parent(op: &RelationOp, parent: &Value, batch: &RelationBatch) -> Value {
    let rows = match obj_get(parent, &op.parent_key) {
        None | Some(Value::Null) => None,
        Some(k) => batch.get(&stringify_key(k)),
    };
    if op.kind == "hasMany" {
        return Value::Arr(rows.cloned().unwrap_or_default());
    }
    match rows {
        Some(r) if !r.is_empty() => r[0].clone(),
        _ => Value::Null,
    }
}

/// Run a READ bundle's primary row list, then batch-load + hydrate the selected relations onto each
/// parent (port of the TS `readBundle` typed-object surface, declarative-select path). The primary
/// read rides [`execute_bundle_pooled`] (the #40 executor-layer sibling fan-out); each named relation
/// in `with_names` is then batch-prefetched ONCE over the whole page (staged, no N+1) via the SAME
/// `run_relation_op` and attached onto each parent as an own field — in declaration order, mirroring
/// the TS `buildResultSet` sequential declarative-select loop.
pub fn read_bundle_pooled(
    bundle: &serde_json::Value,
    input: &serde_json::Value,
    driver: &(dyn Driver + Sync),
    with_names: &[String],
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
        let batch = run_relation_op(&op, &rows, driver)?;
        for row in rows.iter_mut() {
            let child = distribute_to_parent(&op, row, &batch);
            if let Value::Obj(pairs) = row {
                pairs.push((name.clone(), child));
            }
        }
    }
    Ok(Value::Arr(rows))
}
