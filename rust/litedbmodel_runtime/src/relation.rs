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

use crate::codegen_exec::{ArrayParamShape, Wire};
use crate::driver::Driver;
use crate::errors::{LimitExceededError, RuntimeError, SqlFailure, LIMIT_CONTEXT_RELATION};
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
    /// The COMPOSITE-key param-shape DESCRIPTOR, resolved from the dialect at SQL generation
    /// (`compileRelationOp`) and carried on the op — so `bind_keys` binds a composite key set without
    /// naming a DB (PerColumn `UNNEST` arrays vs SingleJson array-of-tuples). Single-key relations ignore
    /// it (they bind ONE `Value::Arr` the Driver resolves). The array-bind SSoT, shared native/mode-2.
    key_shape: ArrayParamShape,
    /// CROSS-DB (V0 R1): the target model's connection tag (None for a same-DB relation).
    connection: Option<String>,
    sql: String,
    /// The child (target) table name — carried for the [`LimitExceededError`] `model` field (the
    /// relation guard reports the relation's TARGET TABLE as `model`, the relation NAME as `relation`).
    target_table: Option<String>,
    /// Hard-limit runaway cap (Phase E-2, epic #74) — the effective per-batch row cap RESOLVED at
    /// compile and baked as a plain number. Absent ⇒ no check (disabled / intrinsic-limit window).
    hard_limit: Option<i64>,
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
        // `keyShape` is baked by `compileRelationOp` (per dialect): `per_column` → UNNEST arrays, else the
        // array-of-tuples JSON. Only composite relations read it (single-key binds a Driver-resolved array).
        // A bundle PREDATING the descriptor derives the SAME value the gen stage bakes (a backward-compat
        // shim, NOT a second live decision — a fresh corpus always carries `keyShape`); `bind_keys` then
        // stays dialect-blind (it reads `key_shape` only).
        key_shape: match o.get("keyShape").and_then(|v| v.as_str()) {
            Some("per_column") => ArrayParamShape::PerColumn,
            Some(_) => ArrayParamShape::SingleJson,
            None if s("dialect") == "postgres" => ArrayParamShape::PerColumn,
            None => ArrayParamShape::SingleJson,
        },
        connection: o
            .get("connection")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        sql: s("sql"),
        target_table: o
            .get("targetTable")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        hard_limit: o.get("hardLimit").and_then(|v| v.as_i64()),
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

/// The per-parent key TUPLE (one entry per parent, order preserved) read from Value parent rows — the
/// KEY-EXTRACTION half of the loader for the Value parent path. A missing/null key column yields a
/// `Value::Null` slot (dropped by [`dedupe_tuples`] before the query, and treated as "no match" by
/// [`distribute_by_tuple`]). The typed loader ([`hydrate_relation`]) supplies the SAME shape via its
/// `key_of` accessor instead of `obj_get`, so both paths feed the ONE batch/group core.
fn parent_key_tuples(parents: &[Value], key_cols: &[String]) -> Vec<Vec<Value>> {
    parents
        .iter()
        .map(|p| {
            key_cols
                .iter()
                .map(|c| obj_get(p, c).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

/// The deduped, non-null key TUPLES for the batched child query (insertion order preserved). Drop a
/// tuple if ANY key column is null; dedupe on the stringified tuple identity. Port of the TS `dedupeKeys`
/// (its per-parent extraction is now [`parent_key_tuples`] / the typed `key_of`, so this dedupe is shared
/// by BOTH parent paths — one batch/group core, no per-path re-implementation).
fn dedupe_tuples(tuples: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<Vec<Value>> = Vec::new();
    for t in tuples {
        if t.iter().any(|v| matches!(v, Value::Null)) {
            continue;
        }
        if seen.insert(key_identity(t)) {
            out.push(t.clone());
        }
    }
    out
}

/// Bind the deduped keys to the op's params per dialect + arity (mirrors TS `bindKeys`). Single-key:
/// PG → ONE scalar array (`Value::Arr`); MySQL/SQLite → ONE JSON scalar-array string. Composite:
/// PG → ONE array param PER key column (transposed tuples); MySQL/SQLite → ONE JSON array-of-tuples
/// string. Returns the positional param list.
fn bind_keys(op: &RelationOp, tuples: &[Vec<Value>]) -> Vec<Value> {
    if op.parent_keys.is_none() {
        // Single-key: ONE scalar-array param. The Driver's param-binder binds it native (Postgres) or as
        // the `json_each(?)` JSON string (MySQL/SQLite) — the array-bind SSoT; NO dialect branch here
        // (invariant #3: DB differences resolve in the Driver, not the executor/render layer).
        return vec![Value::Arr(tuples.iter().map(|t| t[0].clone()).collect())];
    }
    // COMPOSITE key — the param ARITY is baked into the per-dialect SQL, so the FORM is chosen by the
    // generation-stage `key_shape` DESCRIPTOR (invariant #5), NOT by a dialect branch here (invariant #3:
    // the executor is dialect-blind): PerColumn → ONE array param PER key column (N params, transposed
    // tuples, `UNNEST($1::T[], $2::T[], …)`); SingleJson → ONE array-of-tuples JSON (1 param, `JSON_TABLE(?)`).
    match op.key_shape {
        ArrayParamShape::PerColumn => {
            let n_cols = op.parent_key_cols().len();
            (0..n_cols)
                .map(|col| Value::Arr(tuples.iter().map(|t| t[col].clone()).collect()))
                .collect()
        }
        ArrayParamShape::SingleJson => vec![Value::Str(json_tuples(tuples))],
    }
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

/// The child rows grouped for a batch: stringified target-key identity → child rows (generic over the
/// child ROW representation `C` — the Value path supplies `Value` children, the #140 native path supplies
/// bc-de-boxed TYPED children — ONE group/distribute core over both, no per-path re-implementation).
type RelationBatch<C> = HashMap<String, Vec<C>>;

/// The batched child driver rows for a relation op (the EXEC authority — the ONE place that dedupes the
/// parent-key tuples, resolves the deferred PG array cast(s) from the REAL keys before the `?`→`$N`
/// render, binds the keys per dialect+arity, runs the batch, and enforces the hard-limit). An EMPTY key
/// set issues NO query (returns no rows), matching TS. SHARED by BOTH consumers: the Value grouping
/// ([`run_relation_op`], mode-2 / conformance) and the typed de-box ([`hydrate_relation_typed`], native) —
/// dedupe/cast/render/bind/exec/hard-limit are single-sourced HERE (never duplicated per path).
fn fetch_child_rows(
    op: &RelationOp,
    per_parent_tuples: &[Vec<Value>],
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let p_cols = op.parent_key_cols();
    let keys = dedupe_tuples(per_parent_tuples);
    let mut sql = op.sql.clone();
    if op.dialect == "postgres" {
        for col in 0..p_cols.len() {
            let col_vals: Vec<Value> = keys.iter().map(|t| t[col].clone()).collect();
            sql = resolve_pg_array_cast(&sql, &col_vals);
        }
    }
    let sql = render_placeholders(&sql, &op.dialect);
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let bound = bind_keys(op, &keys);
    // The relation batch read funnels through the CENTRAL SEAM (§2) on a backward-compat ctx over the
    // relation's driver (no tx — relation batches are read-only; empty middleware) — one pooled
    // connection per batch, byte-identical to the pre-seam `driver.prepare().all()`.
    let ctx = exec_context::for_driver(driver);
    let rows = exec_context::execute(&ctx, &sql, &bound, &StatementIntent::read())?;
    // Hard-limit runaway guard (Phase E-2, epic #74; v1 `_selectForRelation`): POST-fetch, BEFORE
    // grouping/de-box — if the batch TOTAL exceeds the baked cap, throw with the EXACT count (the batch is
    // fetched in full, no N+1). Absent `hard_limit` ⇒ disabled / intrinsic-limit relation ⇒ no
    // check. ⚠ Field mapping mirrors the TS reference: `model` = the relation's TARGET TABLE,
    // `relation` = the relation NAME. A LimitExceededError propagates as its OWN error (not a
    // SqlFailure). Throws BEFORE grouping/hydration so an over-cap read never assembles an unbounded set.
    if let Some(cap) = op.hard_limit {
        // The SHARED runaway check (SSoT) — the SAME `LimitExceededError::check` the find-context guard
        // (mode-2 + native codegen read) calls, here with the relation context (EXACT count, the batch
        // is fetched in full). One `count > cap ⇒ throw` primitive, no per-context re-implementation.
        LimitExceededError::check(
            cap,
            rows.len() as i64,
            LIMIT_CONTEXT_RELATION,
            op.target_table.clone(),
            Some(op.name.clone()),
        )?;
    }
    Ok(rows)
}

/// Group child ROWS by their target-key identity (port of TS `runRelationOp`'s grouping half) — GENERIC
/// over the child row representation `C`: the Value path passes `Value` rows with an `obj_get`-based key
/// reader, the native path passes bc-de-boxed TYPED structs with a field-access key reader. ONE grouping
/// (insertion order preserved within a key), no Value/typed duplication.
fn group_children<C>(
    rows: Vec<C>,
    target_key_of: impl Fn(&C) -> Vec<Value>,
) -> RelationBatch<C> {
    let mut batch: RelationBatch<C> = HashMap::new();
    for row in rows {
        let tuple = target_key_of(&row);
        batch.entry(key_identity(&tuple)).or_default().push(row);
    }
    batch
}

/// The child ROWS matched to ONE parent key TUPLE (the group/distribute lookup half) — GENERIC over `C`.
/// A null anywhere in the tuple ⇒ no match (empty). Keyed by the tuple's stringified identity. Shared by
/// the Value distributor ([`distribute_by_tuple`]) and the typed loader ([`hydrate_relation_typed`]).
fn matched_children<C: Clone>(tuple: &[Value], batch: &RelationBatch<C>) -> Vec<C> {
    if tuple.iter().any(|v| matches!(v, Value::Null)) {
        return Vec::new();
    }
    batch.get(&key_identity(tuple)).cloned().unwrap_or_default()
}

/// Group a batch of VALUE child rows by target key (the Value-path adapter over the generic
/// [`group_children`]: the key reader is `obj_get` over the ordered pair list). Byte-identical grouping to
/// the pre-#140 `run_relation_op`.
fn group_value_children(op: &RelationOp, rows: Vec<Value>) -> RelationBatch<Value> {
    let t_cols = op.target_key_cols();
    group_children(rows, |row| {
        t_cols
            .iter()
            .map(|c| obj_get(row, c).cloned().unwrap_or(Value::Null))
            .collect()
    })
}

/// Distribute a resolved VALUE batch onto ONE parent's key TUPLE per cardinality (port of TS
/// `distributeToParent`): hasMany → the child list (`[]` when none); belongsTo/hasOne → the single child
/// (or null). Reuses the GENERIC [`matched_children`] lookup so the Value path (tuple via `obj_get`) and
/// the typed path (tuple via `key_of`) share ONE distribution core.
fn distribute_by_tuple(op: &RelationOp, tuple: &[Value], batch: &RelationBatch<Value>) -> Value {
    let rows = matched_children(tuple, batch);
    if op.kind == "hasMany" {
        return Value::Arr(rows);
    }
    match rows.into_iter().next() {
        Some(r) => r,
        None => Value::Null,
    }
}

/// Parse a relation-ops JSON blob ONCE and pick the top-level relation `name` — the meta-load entry a
/// consumer calls in SETUP (once), so no per-iteration JSON parse ever reaches the hot path (the loader
/// takes the parsed [`J`] node). Loud on a missing relation (a wiring bug, never a silent empty op).
pub fn relation_op(relations_json: &str, name: &str) -> Result<J, RuntimeError> {
    let ops = J::parse(relations_json).map_err(|e| {
        RuntimeError::Sql(SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!("relation_op: invalid relation-ops JSON: {e}"),
        })
    })?;
    ops.get(name).cloned().ok_or_else(|| {
        RuntimeError::Sql(SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!("relation_op: relation '{name}' is not declared on this model"),
        })
    })
}

/// Recurse the declared `childRelations` over the just-hydrated child ROWS (Value — the driver's native
/// rows; relations are a Value-path RUNTIME concern per #131, the child SQL lives in the relation
/// metadata, NOT a bc-generated typed native module). Shared by the Value [`stitch_relation_tree`] and the
/// typed [`hydrate_relation`] so the level traversal is single-sourced (no per-path copy).
fn recurse_child_relations(
    op_json: &J,
    child_rows: Vec<Value>,
    driver: &dyn Driver,
) -> Result<(), RuntimeError> {
    if let Some(children) = op_json.get("childRelations").and_then(|c| c.as_array()) {
        for child in children {
            stitch_relation_tree(child, child_rows.clone(), driver)?;
        }
    }
    Ok(())
}

/// Batch-load + hydrate ONE declared relation onto an ALREADY-FETCHED VALUE parent row list, using the
/// SAME `run_relation_op` / `distribute_by_tuple` core the typed path and the runtime's own read path use
/// (NO reimplemented grouping — the semantics stay single-sourced here). `op_json` is the relation op as
/// it appears under `bundle["relations"][name]` (pure JSON). Byte-identical to `read_bundle_pooled`'s
/// hydration for the same parents.
pub fn stitch_relation(
    op_json: &J,
    mut parents: Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let op = op_from_json(op_json);
    let tuples = parent_key_tuples(&parents, &op.parent_key_cols());
    let batch = group_value_children(&op, fetch_child_rows(&op, &tuples, driver)?);
    for (row, tuple) in parents.iter_mut().zip(tuples.iter()) {
        let child = distribute_by_tuple(&op, tuple, &batch);
        if let Value::Obj(pairs) = row {
            pairs.push((op.name.clone(), child));
        }
    }
    Ok(parents)
}

/// Hydrate ONE relation op AND its declared `childRelations` recursively onto VALUE `parents` — the
/// MULTI-LEVEL relation-loading SSoT for the Value path. Each level reuses [`stitch_relation`]; the rows
/// hydrated at one level become the parents whose OWN `childRelations` load next (posts → comments), so a
/// whole relation TREE is loaded with one batched query per level (never N+1). Returns the top-level
/// parents with the relation attached.
pub fn stitch_relation_tree(
    op_json: &J,
    parents: Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let name = op_json
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    let stitched = stitch_relation(op_json, parents, driver)?;
    // The rows hydrated at THIS level (`stitched[i][name]`) are the parents for their child relations.
    let mut level: Vec<Value> = Vec::new();
    for p in &stitched {
        if let Some(Value::Arr(arr)) = obj_get(p, &name) {
            level.extend(arr.iter().cloned());
        }
    }
    recurse_child_relations(op_json, level, driver)?;
    Ok(stitched)
}

/// A typed parent key → the loader's scalar key TUPLE, reusing the [`crate::codegen_exec::ToWireParam`]
/// scalar→`Value` SSoT. A single-column key is any scalar (`i64` / `String` / …); a composite key is a
/// tuple `(A, B)`. Lets the typed [`hydrate_relation`] caller pass a NATURAL accessor (`|r| r.id`,
/// `|r| (r.tenant_id, r.user_id)`) with NO `Value` plumbing in the consumer.
pub trait IntoKeyTuple {
    fn into_key_tuple(self) -> Vec<Value>;
}
impl<T: crate::codegen_exec::ToWireParam> IntoKeyTuple for T {
    fn into_key_tuple(self) -> Vec<Value> {
        vec![crate::codegen_exec::wp(&self)]
    }
}
impl<A: crate::codegen_exec::ToWireParam, B: crate::codegen_exec::ToWireParam> IntoKeyTuple
    for (A, B)
{
    fn into_key_tuple(self) -> Vec<Value> {
        vec![
            crate::codegen_exec::wp(&self.0),
            crate::codegen_exec::wp(&self.1),
        ]
    }
}

/// The TYPED relation loader (#138 parent-typing → #140 child-typing): hydrate ONE relation LEVEL onto a
/// TYPED native-read parent list, returning each parent paired with its TYPED child structs — NO
/// `Value::Obj` retained past the de-box on the native path. The parent key is read via the caller's
/// NATURAL accessor (`|r| r.id`, `|r| (r.tenant_id, r.user_id)`), lowered to the loader's key tuple by
/// [`IntoKeyTuple`]. The batched child driver rows come from the SHARED [`fetch_child_rows`] (the ONE
/// dedupe/cast/render/bind/exec/hard-limit authority the Value path also uses), then a bc-generated CHILD
/// `decode` de-boxes the driver rows into TYPED structs `Vec<C>` (the SAME bc de-box a primary read uses,
/// via the module's wire seam), and the SHARED [`group_children`] / [`matched_children`] group + distribute
/// by key — ONE core, no Value/typed duplication. `child_key_of` reads the TYPED child's target-key tuple
/// by field access (no `obj_get`). Multi-level (`childRelations`) is driven by the litedbmodel-GENERATED
/// companion (each level is a concrete `C` — Rust cannot recurse generically over the heterogeneous child
/// types), which re-invokes THIS per-level loader over the just-hydrated typed children. Byte-equal to the
/// Value path for the same keys.
pub fn hydrate_relation_typed<P, K: IntoKeyTuple, C: Clone>(
    op_json: &J,
    parents: Vec<P>,
    key_of: impl Fn(&P) -> K,
    decode: impl Fn(Wire) -> Result<Vec<C>, RuntimeError>,
    child_key_of: impl Fn(&C) -> Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<(P, Vec<C>)>, RuntimeError> {
    let op = op_from_json(op_json);
    let tuples: Vec<Vec<Value>> = parents.iter().map(|p| key_of(p).into_key_tuple()).collect();
    let rows = fetch_child_rows(&op, &tuples, driver)?;
    let children = decode(Wire::from_rows(rows))?;
    let batch = group_children(children, child_key_of);
    Ok(parents
        .into_iter()
        .zip(tuples.iter())
        .map(|(p, tuple)| (p, matched_children(tuple, &batch)))
        .collect())
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
) -> Result<Value, RuntimeError> {
    let out = execute_bundle_pooled(bundle, input, driver)?;
    let mut rows = match out {
        Value::Arr(rows) => rows,
        _ => {
            return Err(RuntimeError::Sql(SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: "scp read: the read behavior output is not a row list; the typed-object \
                          read surface expects a Select-shaped output"
                    .into(),
            }))
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
        let tuples = parent_key_tuples(&rows, &op.parent_key_cols());
        let batch = group_value_children(&op, fetch_child_rows(&op, &tuples, rel_driver)?);
        for (row, tuple) in rows.iter_mut().zip(tuples.iter()) {
            let child = distribute_by_tuple(&op, tuple, &batch);
            if let Value::Obj(pairs) = row {
                pairs.push((name.clone(), child));
            }
        }
    }
    Ok(Value::Arr(rows))
}
