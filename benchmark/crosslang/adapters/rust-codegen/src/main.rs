//! litedbmodel cross-language adapter — Rust CODEGEN cell (dedicated binary; epic #44 perf;
//! #60 milestone 1 — typed-NATIVE READ codegen).
//!
//! OWNER ORDERS (absolute): the codegen path carries NO IR data and parses NO JSON at execution
//! time — no JSON crate anywhere in this crate (the manifest carries none; the only
//! JSON-ish text this binary ever produces is the line-protocol RESPONSE, hand-written via
//! `format!`, and the single-string IN-list/relation params the SQL dialects define, built by the
//! native `json_array_text` writers below — both are protocol/SQL surface, not IR interpretation).
//!
//! Execution data comes from the GENERATED NATIVE COMPANION (`generated/codegen/rust/companion.rs`
//! — pre-decoded statement plans / transaction plans / relation batch ops / bench inputs / schema
//! + seed, emitted by benchmark/crosslang/generate.ts through a CLOSED-SET fail-closed decoder).
//!
//! READS execute THROUGH the bc-GENERATED typed-NATIVE modules (`generated/codegen/rust/<case>.rs`,
//! bc#77/#90 `rust-typed-native` — RUNTIME-FREE: zero boxed `Value`/`RawValue` on the module's own
//! surface, no bc-runtime import at all in the generated module). This crate implements each
//! module's `HandlerNR<Comp>` trait: `node_*` builds the render scope from the CONCRETE native
//! ports struct (a handful of scalars — cheap, not a per-row cost), renders + executes the SAME
//! native statement-render engine the prior raw-ABI adapter used (`render_read`, unchanged), and
//! materializes each row DIRECTLY into the module's own concrete `T0` row struct via a per-case
//! `rusqlite::Row` field read (NO `Value::Obj`/`RawValue::Row` on the row-materialization plane —
//! genuinely zero-boxing on the read hot path). Only 4 of the 6 read cases are typed-native-covered
//! (bc#86 gap: `complexWhere`/`inList`'s IN-list has an array-typed head bc's typed-native emitter
//! has no native port type for) — those 2 cases are NOT wired here (reported, not silently
//! defaulted to the retired `-typed-raw` path); the harness marks them as an expected gap.
//!
//! WRITES (`batchInsert`/`writeTxGate`) are NOT codegen-module cases anymore (#60 m1) — they stay
//! on this crate's hand-written NATIVE gate-first transaction execution (`exec_tx`), called
//! DIRECTLY (no bc-generated module wrapper at all now — there is no write codegen module to wrap).
//!
//! Fail-closed: an unknown case / dialect / spec shape / driver value PANICS loudly — never a
//! silent degrade (the companion generation already fail-closed on out-of-set shapes).

use behavior_contracts::Value;
use std::cell::Cell;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::time::Instant;

#[path = "../../../generated/codegen/rust/companion.rs"]
mod companion;

// ── bc-GENERATED typed-NATIVE modules (bc#77/#90 rust-typed-native; each its own compile unit) ──
// Only the 4 typed-native-COVERED read cases (#60 m1; complexWhere/inList are a bc#86 gap — NOT
// generated, see the module doc above). Writes have NO codegen module anymore (exec_tx, native).
#[path = "../../../generated/codegen/rust/belongsTo.rs"]
mod cg_belongs_to;
#[path = "../../../generated/codegen/rust/find.rs"]
mod cg_find;
#[path = "../../../generated/codegen/rust/hasMany.rs"]
mod cg_has_many;
#[path = "../../../generated/codegen/rust/hasManyLimit.rs"]
mod cg_has_many_limit;

use companion::{
    CasePlan, Dialect, Gate, InVal, Lit, ReadPlan, RelKind, Relation, Skip, Spec, TxPlan,
};

type Scope = Vec<(String, Value)>;

// ═══════════════════════════════════════════════════════════════════════════════
// SQL driver seam (native; mirrors litedbmodel_runtime's Driver semantics)
// ═══════════════════════════════════════════════════════════════════════════════

/// A single fetched row's cells, by column name — the typed-native read hot path's ONLY row
/// abstraction. NO `Value`/`RawValue` boxing: a cell is read directly as its native scalar type
/// (mirrors `rusqlite::Row::get::<&str, T>`). Implemented once for a REAL sqlite row and once for
/// the mock driver's in-memory fixture row (both equally "zero-boxing" — the mock never touches
/// `Value` either).
trait RowCells {
    fn get_i64(&self, col: &str) -> i64;
    fn get_str(&self, col: &str) -> String;
}

trait Driver {
    /// SELECT/RETURNING: the row list (each row an ordered `Value::Obj`). Used by the WRITE tx
    /// path + relation stitch (which consume boxed `Value` — those planes are NOT the typed-native
    /// read hot path this crate migrated off boxing; #60 m1 scoped the zero-boxing claim to reads).
    fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, String>;
    /// SELECT for the typed-native READ hot path (#60 m1): visit each fetched row as `&dyn
    /// RowCells` — the caller (a `HandlerNR::node_*` impl) reads named columns DIRECTLY into the
    /// module's own concrete `T0` struct fields. NO `Value::Obj`/`RawValue::Row` intermediate is
    /// ever built on this path — genuinely zero-boxing row materialization.
    fn query_rows(&self, sql: &str, params: &[Value], visit: &mut dyn FnMut(&dyn RowCells));
    /// Non-returning statement: the affected-row count.
    fn execute(&self, sql: &str, params: &[Value]) -> Result<i64, String>;
}

/// Convert a rendered param to a rusqlite bindable value. Only the closed scalar set reaches the
/// driver (IN-lists are single JSON-text params on sqlite/mysql; a PG array param never reaches
/// sqlite). Anything else is a bug — panic loudly (fail-closed), never fabricate.
fn to_sql_value(v: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as S;
    match v {
        Value::Null => S::Null,
        Value::Bool(b) => S::Integer(if *b { 1 } else { 0 }),
        Value::Int(i) => S::Integer(*i),
        Value::Float(f) => S::Real(*f),
        Value::Str(s) => S::Text(s.clone()),
        other => panic!(
            "codegen native driver: a {} reached the sqlite param binder (expected a scalar) — fail-closed",
            type_name(other)
        ),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "string",
        Value::Arr(_) => "array",
        Value::Obj(_) => "object",
    }
}

struct SqliteDriver {
    conn: rusqlite::Connection,
}

impl SqliteDriver {
    fn in_memory(schema: &[&str]) -> Self {
        let conn = rusqlite::Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .expect("pragma");
        for stmt in schema {
            conn.execute_batch(stmt).expect("schema");
        }
        SqliteDriver { conn }
    }
}

/// `rusqlite::Row` IS a `RowCells` directly — a typed-native `node_*` handler reads its module's
/// concrete `T0` fields straight off the live sqlite row (no `Value`/`RawValue` intermediate).
impl RowCells for rusqlite::Row<'_> {
    fn get_i64(&self, col: &str) -> i64 {
        self.get::<&str, i64>(col)
            .unwrap_or_else(|e| panic!("codegen native: row column '{col}' is not an i64: {e}"))
    }
    fn get_str(&self, col: &str) -> String {
        self.get::<&str, String>(col)
            .unwrap_or_else(|e| panic!("codegen native: row column '{col}' is not a string: {e}"))
    }
}

fn from_sql_ref(r: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef as R;
    match r {
        R::Null => Value::Null,
        R::Integer(i) => Value::Int(i),
        R::Real(f) => Value::Float(f),
        R::Text(b) => Value::Str(String::from_utf8_lossy(b).into_owned()),
        R::Blob(b) => Value::Str(String::from_utf8_lossy(b).into_owned()),
    }
}

impl Driver for SqliteDriver {
    fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, String> {
        let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sql_value).collect();
        let mut stmt = self.conn.prepare(sql).map_err(|e| e.to_string())?;
        let col_names: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let refs: Vec<&dyn rusqlite::ToSql> =
            bound.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut rows = stmt.query(refs.as_slice()).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        loop {
            match rows.next() {
                Ok(Some(row)) => {
                    let mut obj: Vec<(String, Value)> = Vec::with_capacity(col_names.len());
                    for (i, name) in col_names.iter().enumerate() {
                        let cell = row.get_ref(i).map_err(|e| e.to_string())?;
                        obj.push((name.clone(), from_sql_ref(cell)));
                    }
                    out.push(Value::Obj(obj));
                }
                Ok(None) => break,
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(out)
    }

    fn query_rows(&self, sql: &str, params: &[Value], visit: &mut dyn FnMut(&dyn RowCells)) {
        let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sql_value).collect();
        let mut stmt = self
            .conn
            .prepare(sql)
            .unwrap_or_else(|e| panic!("codegen native: prepare '{sql}' failed: {e}"));
        let refs: Vec<&dyn rusqlite::ToSql> =
            bound.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut rows = stmt
            .query(refs.as_slice())
            .unwrap_or_else(|e| panic!("codegen native: query '{sql}' failed: {e}"));
        loop {
            match rows.next() {
                // The row IS a RowCells — visited DIRECTLY, no Value::Obj/RawValue::Row ever built.
                Ok(Some(row)) => visit(row),
                Ok(None) => break,
                Err(e) => panic!("codegen native: row fetch '{sql}' failed: {e}"),
            }
        }
    }

    fn execute(&self, sql: &str, params: &[Value]) -> Result<i64, String> {
        let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sql_value).collect();
        let refs: Vec<&dyn rusqlite::ToSql> =
            bound.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut stmt = self.conn.prepare(sql).map_err(|e| e.to_string())?;
        stmt.execute(refs.as_slice())
            .map(|n| n as i64)
            .map_err(|e| e.to_string())
    }
}

// ── mock driver (I/O-excluded micro axis; fixture-identical to the other adapters) ──
struct MockDriver;

fn obj(pairs: &[(&str, Value)]) -> Value {
    Value::Obj(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn fixture(sql: &str) -> Vec<Value> {
    let s = sql.to_lowercase();
    let st = s.trim_start();
    if st.starts_with("select") {
        if s.contains("from comments") {
            return (1..=25)
                .map(|i| {
                    obj(&[
                        ("id", Value::Int(i)),
                        ("post_id", Value::Int(((i - 1) % 5) + 1)),
                        ("body", Value::Str(format!("comment-{i}"))),
                    ])
                })
                .collect();
        }
        if s.contains("from users") {
            return vec![obj(&[
                ("id", Value::Int(1)),
                ("name", Value::Str("user-1".into())),
            ])];
        }
        if s.contains("from posts") || s.contains("from ") {
            return (1..=5)
                .map(|i| {
                    obj(&[
                        ("id", Value::Int(i)),
                        ("author_id", Value::Int(1)),
                        ("title", Value::Str(format!("post-{i}"))),
                        ("status", Value::Str("live".into())),
                        ("views", Value::Int(i * 10)),
                        ("created_at", Value::Str("2026-02-01".into())),
                    ])
                })
                .collect();
        }
        return vec![obj(&[("1", Value::Int(1))])];
    }
    if s.contains("returning") {
        return vec![obj(&[
            ("id", Value::Int(41)),
            ("author_id", Value::Int(1)),
            ("title", Value::Str("txn-post".into())),
        ])];
    }
    Vec::new()
}

/// A fixture cell — the mock driver's native (non-`Value`) row representation for the typed-native
/// read hot path. Only the two scalar kinds the bench fixture data needs.
enum FixtureCell {
    Int(i64),
    Str(String),
}

/// A fixture row (named cells) — a `RowCells` impl with NO `Value`/`RawValue` anywhere, matching
/// the REAL sqlite row's zero-boxing property.
struct FixtureRow(Vec<(&'static str, FixtureCell)>);

impl RowCells for FixtureRow {
    fn get_i64(&self, col: &str) -> i64 {
        match self.0.iter().find(|(k, _)| *k == col) {
            Some((_, FixtureCell::Int(i))) => *i,
            other => panic!("codegen native: fixture column '{col}' missing/wrong type: {other:?}"),
        }
    }
    fn get_str(&self, col: &str) -> String {
        match self.0.iter().find(|(k, _)| *k == col) {
            Some((_, FixtureCell::Str(s))) => s.clone(),
            other => panic!("codegen native: fixture column '{col}' missing/wrong type: {other:?}"),
        }
    }
}
impl std::fmt::Debug for FixtureCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FixtureCell::Int(i) => write!(f, "Int({i})"),
            FixtureCell::Str(s) => write!(f, "Str({s:?})"),
        }
    }
}

fn frow(pairs: Vec<(&'static str, FixtureCell)>) -> FixtureRow {
    FixtureRow(pairs)
}

/// The MICRO-bench read fixtures as native fixture rows (mirror of `fixture` — SAME data, zero
/// boxing). The micro signal measures the codegen client path, so the mock feeds rows directly (no
/// `Value`/`RawValue` materialization at all).
fn fixture_rows(sql: &str) -> Vec<FixtureRow> {
    let s = sql.to_lowercase();
    let st = s.trim_start();
    if st.starts_with("select") {
        if s.contains("from comments") {
            return (1..=25)
                .map(|i| {
                    frow(vec![
                        ("id", FixtureCell::Int(i)),
                        ("post_id", FixtureCell::Int(((i - 1) % 5) + 1)),
                        ("body", FixtureCell::Str(format!("comment-{i}"))),
                    ])
                })
                .collect();
        }
        if s.contains("from users") {
            return vec![frow(vec![
                ("id", FixtureCell::Int(1)),
                ("name", FixtureCell::Str("user-1".into())),
            ])];
        }
        if s.contains("from posts") || s.contains("from ") {
            return (1..=5)
                .map(|i| {
                    frow(vec![
                        ("id", FixtureCell::Int(i)),
                        ("author_id", FixtureCell::Int(1)),
                        ("title", FixtureCell::Str(format!("post-{i}"))),
                        ("status", FixtureCell::Str("live".into())),
                        ("views", FixtureCell::Int(i * 10)),
                        ("created_at", FixtureCell::Str("2026-02-01".into())),
                    ])
                })
                .collect();
        }
        return vec![frow(vec![("1", FixtureCell::Int(1))])];
    }
    if s.contains("returning") {
        return vec![frow(vec![
            ("id", FixtureCell::Int(41)),
            ("author_id", FixtureCell::Int(1)),
            ("title", FixtureCell::Str("txn-post".into())),
        ])];
    }
    Vec::new()
}

impl Driver for MockDriver {
    fn query(&self, sql: &str, _params: &[Value]) -> Result<Vec<Value>, String> {
        Ok(fixture(sql))
    }
    fn query_rows(&self, sql: &str, _params: &[Value], visit: &mut dyn FnMut(&dyn RowCells)) {
        for row in fixture_rows(sql) {
            visit(&row);
        }
    }
    fn execute(&self, _sql: &str, _params: &[Value]) -> Result<i64, String> {
        Ok(1)
    }
}

// ── counting driver (fairness cost probe: DML statements + rows; tx-control excluded) ──
struct CountingDriver<'a> {
    inner: &'a dyn Driver,
    queries: Cell<u64>,
    rows: Cell<u64>,
}

fn is_tx_control(sql: &str) -> bool {
    let up = sql.trim_start().to_uppercase();
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "PRAGMA",
    ]
    .iter()
    .any(|k| up.starts_with(k))
}

impl Driver for CountingDriver<'_> {
    fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, String> {
        let dml = !is_tx_control(sql);
        if dml {
            self.queries.set(self.queries.get() + 1);
        }
        let rows = self.inner.query(sql, params)?;
        if dml {
            self.rows.set(self.rows.get() + rows.len() as u64);
        }
        Ok(rows)
    }
    fn query_rows(&self, sql: &str, params: &[Value], visit: &mut dyn FnMut(&dyn RowCells)) {
        let dml = !is_tx_control(sql);
        if dml {
            self.queries.set(self.queries.get() + 1);
        }
        let mut n: u64 = 0;
        self.inner.query_rows(sql, params, &mut |row| {
            n += 1;
            visit(row);
        });
        if dml {
            self.rows.set(self.rows.get() + n);
        }
    }
    fn execute(&self, sql: &str, params: &[Value]) -> Result<i64, String> {
        if !is_tx_control(sql) {
            self.queries.set(self.queries.get() + 1);
        }
        self.inner.execute(sql, params)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE value-spec / statement render engine (mirrors the runtime's
// render_statements semantics for the CLOSED companion set — byte-identical
// SQL text + params for the shapes in scope; no JSON, no IR)
// ═══════════════════════════════════════════════════════════════════════════════

fn scope_get<'a>(scope: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    scope.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Native scope ref walk (bc `{ref:[..]}` semantics). Fail-closed: a missing head/segment PANICS
/// (mirrors bc UNKNOWN_BINDING — never a silent null).
fn ref_path(scope: &[(String, Value)], path: &[&str]) -> Value {
    let mut cur = scope_get(scope, path[0])
        .unwrap_or_else(|| {
            panic!(
                "codegen native: unknown binding '{}' (fail-closed)",
                path[0]
            )
        })
        .clone();
    for seg in &path[1..] {
        cur = match cur {
            Value::Obj(pairs) => pairs
                .iter()
                .find(|(k, _)| k == seg)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| {
                    panic!("codegen native: unknown property '.{seg}' (fail-closed)")
                }),
            other => panic!(
                "codegen native: ref path '.{seg}' into a {} (fail-closed)",
                type_name(&other)
            ),
        };
    }
    cur
}

/// JSON string escape (native writer; JS `JSON.stringify` form: unicode left raw, control chars
/// escaped, `"`/`\` escaped).
fn json_escape_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// One IN-list element in the single-JSON-text param (mirrors the runtime `eval_spec` encode:
/// mysql booleans as 1/0; closed scalar set — anything else is fail-closed).
fn json_in_list_element(v: &Value, mysql: bool, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => {
            if mysql {
                out.push_str(if *b { "1" } else { "0" });
            } else {
                out.push_str(if *b { "true" } else { "false" });
            }
        }
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => out.push_str(&format!("{f:?}")),
        Value::Str(s) => json_escape_into(s, out),
        other => panic!(
            "codegen native: IN-list element is a {} (outside the closed scalar set — fail-closed)",
            type_name(other)
        ),
    }
}

fn json_in_list_text(arr: &[Value], mysql: bool) -> String {
    let mut out = String::with_capacity(2 + arr.len() * 4);
    out.push('[');
    for (i, e) in arr.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json_in_list_element(e, mysql, &mut out);
    }
    out.push(']');
    out
}

/// Evaluate one companion value-spec against the scope (native mirror of the runtime `eval_spec`).
fn eval_spec(spec: &Spec, scope: &[(String, Value)]) -> Value {
    match spec {
        Spec::Ref(path) => ref_path(scope, path),
        Spec::Str(s) => Value::Str((*s).to_string()),
        Spec::Int(i) => Value::Int(*i),
        Spec::ArrLit(elems) => Value::Arr(
            elems
                .iter()
                .map(|l| match l {
                    Lit::Null => Value::Null,
                    Lit::Bool(b) => Value::Bool(*b),
                    Lit::Int(i) => Value::Int(*i),
                    Lit::Str(s) => Value::Str((*s).to_string()),
                })
                .collect(),
        ),
        Spec::JsonArray { head, dialect } => {
            let arr = match ref_path(scope, head) {
                Value::Arr(a) => a,
                other => panic!(
                    "codegen native: IN-list value-spec evaluated to a {} (fail-closed)",
                    type_name(&other)
                ),
            };
            if *dialect == Dialect::Postgres {
                Value::Arr(arr) // bound as ONE array param
            } else {
                Value::Str(json_in_list_text(&arr, *dialect == Dialect::Mysql))
            }
        }
    }
}

// ── Dialect placeholder render (`?` → `$N` for PG; quote-aware — byte-port) ────
fn render_placeholders(sql: &str, dialect: Dialect) -> String {
    if dialect != Dialect::Postgres {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    let mut in_string = false;
    for ch in sql.chars() {
        if in_string {
            out.push(ch);
            if ch == '\'' {
                in_string = false;
            }
        } else if ch == '\'' {
            out.push(ch);
            in_string = true;
        } else if ch == '?' {
            index += 1;
            out.push('$');
            out.push_str(&index.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

// ── Deferred PG array-cast resolution (byte-port of the runtime's #46 render step) ──
const PG_ARRAY_CAST_TOKEN: &str = "@@PG_ARRAY_CAST@@";

fn infer_pg_array_type(values: &[Value]) -> &'static str {
    match values.first() {
        None => "text[]",
        Some(Value::Bool(_)) => "boolean[]",
        Some(Value::Int(_)) => "int[]",
        Some(Value::Float(_)) => {
            let all_int = values.iter().all(|v| match v {
                Value::Float(f) => f.fract() == 0.0,
                _ => false,
            });
            if all_int {
                "int[]"
            } else {
                "numeric[]"
            }
        }
        _ => "text[]",
    }
}

fn resolve_pg_array_cast(sql: &str, values: &[Value]) -> String {
    match sql.find(PG_ARRAY_CAST_TOKEN) {
        None => sql.to_string(),
        Some(at) => format!(
            "{}{}{}",
            &sql[..at],
            infer_pg_array_type(values),
            &sql[at + PG_ARRAY_CAST_TOKEN.len()..]
        ),
    }
}

/// Native mirror of the runtime `render_statements` for the companion's closed set: SKIP-if-null
/// statement drop (absent == null), ` WHERE `/` AND ` connector for whereFragment, per-statement
/// PG deferred array-cast resolve, `?`→`$N` render. The `?`/param arity was asserted at
/// generation time (the sql + specs are static data).
fn render_read(plan: &ReadPlan, scope: &[(String, Value)]) -> (String, Vec<Value>) {
    let mut sql = String::new();
    let mut params: Vec<Value> = Vec::new();
    let mut where_seen = false;
    for stmt in plan.stmts {
        if let Some(Skip::IfNull(head)) = &stmt.skip {
            // refOpt(head) == null → skip (an absent head reads as null — the SKIP contract).
            if matches!(scope_get(scope, head), None | Some(Value::Null)) {
                continue;
            }
        }
        let stmt_start = params.len();
        for spec in stmt.params {
            params.push(eval_spec(spec, scope));
        }
        if stmt.where_fragment {
            sql.push_str(if where_seen { " AND " } else { " WHERE " });
            where_seen = true;
        }
        if plan.dialect == Dialect::Postgres && stmt.sql.contains(PG_ARRAY_CAST_TOKEN) {
            let mut resolved = stmt.sql.to_string();
            for p in &params[stmt_start..] {
                if let Value::Arr(arr) = p {
                    if !resolved.contains(PG_ARRAY_CAST_TOKEN) {
                        break;
                    }
                    resolved = resolve_pg_array_cast(&resolved, arr);
                }
            }
            sql.push_str(&resolved);
        } else {
            sql.push_str(stmt.sql);
        }
    }
    (render_placeholders(&sql, plan.dialect), params)
}

// ═══════════════════════════════════════════════════════════════════════════════
// #60 milestone 1 — typed-NATIVE READ handlers (bc#77/#90 HandlerNR<Comp> seam).
//
// Each covered case's bc-generated module (find/belongsTo/hasMany/hasManyLimit) declares its OWN
// `HandlerNR<Comp>` trait + `PortsNR<Comp><node>`/`RawRowNR<Comp><node>`/`InNR<Comp>`/`T0` types (a
// DISTINCT Rust type per module, even where structurally identical — e.g. belongsTo/hasMany/
// hasManyLimit all wrap the SAME `Posts` entry). `node_n0` receives the CONCRETE native ports
// struct directly (already typed scalars — no Value/RawValue on the port), builds the tiny render
// scope (a handful of fields, not a per-row cost) for the EXISTING native statement-render engine
// (`render_read`, unchanged), executes via `Driver::query_rows` (the zero-boxing row visitor — NO
// Value::Obj/RawValue::Row is ever built), and decodes each visited row DIRECTLY into the module's
// own `T0` struct fields. A macro instantiates one impl per module (they cannot share a body — each
// `T0`/`PortsNR*`/trait is a distinct generated type), keeping the decode explicit and auditable.
// ═══════════════════════════════════════════════════════════════════════════════

/// Render `plan` against a scope built from the covered node's typed ports (cheap — a handful of
/// scalar clones, not a per-row cost) and fetch rows via the zero-boxing `query_rows` visitor.
fn render_and_fetch(
    plan: &ReadPlan,
    scope: Scope,
    driver: &dyn Driver,
    mut decode_row: impl FnMut(&dyn RowCells),
) {
    let (sql, params) = render_read(plan, &scope);
    driver.query_rows(&sql, &params, &mut decode_row);
}

/// `find`: HandlerNRFind — one WHERE-bound scalar port per head (author_id/status/since).
impl HandlerNRFind for (&'static ReadPlan, &dyn Driver) {
    fn node_n0(
        &self,
        ports: &cg_find::PortsNRFindN0,
        _bound: Option<String>,
    ) -> Option<cg_find::RawRowNRFindN0> {
        let scope: Scope = vec![
            ("author_id".to_string(), Value::Int(ports.f_author_id)),
            ("status".to_string(), Value::Str(ports.f_status.clone())),
            ("since".to_string(), Value::Str(ports.f_since.clone())),
        ];
        let mut val = Vec::new();
        render_and_fetch(self.0, scope, self.1, |row| {
            val.push(cg_find::T0 {
                id: row.get_i64("id"),
                author_id: row.get_i64("author_id"),
                title: row.get_str("title"),
                status: row.get_str("status"),
                views: row.get_i64("views"),
                created_at: row.get_str("created_at"),
            });
        });
        Some(cg_find::RawRowNRFindN0 {
            is_error: false,
            err: String::new(),
            val,
        })
    }
}
use cg_find::HandlerNRFind;

/// The `belongsTo`/`hasMany`/`hasManyLimit` cases all wrap the SAME `Posts` entry (a single scalar
/// `author_id` WHERE port) — each module's types are STRUCTURALLY identical but DISTINCT Rust
/// types, so one macro arm instantiates the (otherwise-identical) impl per module.
macro_rules! impl_posts_handler {
    ($module:ident) => {
        impl $module::HandlerNRPosts for (&'static ReadPlan, &dyn Driver) {
            fn node_n0(
                &self,
                ports: &$module::PortsNRPostsN0,
                _bound: Option<String>,
            ) -> Option<$module::RawRowNRPostsN0> {
                let scope: Scope = vec![("author_id".to_string(), Value::Int(ports.f_author_id))];
                let mut val = Vec::new();
                render_and_fetch(self.0, scope, self.1, |row| {
                    val.push($module::T0 {
                        id: row.get_i64("id"),
                        author_id: row.get_i64("author_id"),
                        title: row.get_str("title"),
                    });
                });
                Some($module::RawRowNRPostsN0 {
                    is_error: false,
                    err: String::new(),
                    val,
                })
            }
        }
    };
}
impl_posts_handler!(cg_belongs_to);
impl_posts_handler!(cg_has_many);
impl_posts_handler!(cg_has_many_limit);

/// WRITE handler: run the NATIVE gate-first transaction plan and return the TransactionResult
/// (committed/executed/shortCircuit/entity always present — present-as-null for an absent optional
/// — plus returnedRows only when a batch RETURNING produced rows) as a plain `Value::Obj`. #60 m1:
/// writes have NO bc-generated module boundary anymore — `exec_tx` is called DIRECTLY (no
/// RawComponentExec/bind_raw wrapper; that ABI existed only to satisfy the retired typed-raw
/// generated write module).
fn run_write(
    plan: &'static TxPlan,
    dialect: Dialect,
    input: &[(String, Value)],
    driver: &dyn Driver,
) -> Value {
    exec_tx(plan, dialect, input, driver)
        .unwrap_or_else(|e| panic!("codegen: write tx failed: {e}"))
}

/// NATIVE gate-first transaction execution (mirror of the runtime's executeTransactionBundle for
/// the companion's closed set): BEGIN → per-statement native param eval + render + execute → gate
/// short-circuit (ROLLBACK + committed:false result) → entityFrom/binds RETURNING-row scope binds
/// → batch returnedRows accumulation → COMMIT. A driver failure ROLLBACKs (best-effort) and errors.
/// #60 m1: returns a plain `Value::Obj` (no bc-generated write module de-boxes this anymore — the
/// write path has no codegen-module boundary at all, so there is no ABI shape to match beyond what
/// the verify/canon leg + the ir-path's `TransactionResult` compare against).
fn exec_tx(
    plan: &TxPlan,
    dialect: Dialect,
    input: &[(String, Value)],
    driver: &dyn Driver,
) -> Result<Value, String> {
    driver.execute("BEGIN", &[])?;
    let mut scope: Scope = input.to_vec();
    let mut executed: Vec<Value> = Vec::new();
    let mut entity: Value = Value::Null;
    let mut returned_rows: Vec<Value> = Vec::new();

    let mut body = || -> Result<Option<Value>, String> {
        for stmt in plan.statements {
            let params: Vec<Value> = stmt.params.iter().map(|s| eval_spec(s, &scope)).collect();
            let sql = render_placeholders(stmt.sql, dialect);
            let (rows, changes) = if stmt.is_return {
                let rows = driver.query(&sql, &params)?;
                let n = rows.len() as i64;
                (rows, n)
            } else {
                (Vec::new(), driver.execute(&sql, &params)?)
            };
            executed.push(Value::Str(stmt.id.to_string()));

            if let Some(gate) = &stmt.gate {
                let reason = match gate {
                    Gate::ExistsElseRollback => (rows.is_empty()).then_some("requires_absent"),
                    Gate::InsertedElseRollback => (changes == 0).then_some("unique_collision"),
                    Gate::InsertedElseNoop => (changes == 0).then_some("idempotent_duplicate"),
                };
                if let Some(reason) = reason {
                    driver.execute("ROLLBACK", &[])?;
                    let sc = Value::Obj(vec![
                        ("statementId".to_string(), Value::Str(stmt.id.to_string())),
                        ("reason".to_string(), Value::Str(reason.to_string())),
                    ]);
                    let row = Value::Obj(vec![
                        ("committed".to_string(), Value::Bool(false)),
                        (
                            "executed".to_string(),
                            Value::Arr(std::mem::take(&mut executed)),
                        ),
                        ("shortCircuit".to_string(), sc),
                        ("entity".to_string(), Value::Null),
                    ]);
                    return Ok(Some(row));
                }
            }

            if plan.is_batch && !rows.is_empty() {
                returned_rows.push(Value::Arr(rows.clone()));
            }
            let first_row = rows.into_iter().next();

            if plan.entity_from == Some(stmt.id) {
                entity = first_row.clone().unwrap_or(Value::Null);
                if !matches!(entity, Value::Null) {
                    scope.push(("__entity".to_string(), entity.clone()));
                }
            }
            if let Some(binds) = stmt.binds {
                if let Some(row) = first_row {
                    if !matches!(row, Value::Null) {
                        scope.push((binds.to_string(), row));
                    }
                }
            }
        }
        Ok(None)
    };

    match body() {
        Ok(Some(short_circuit)) => Ok(short_circuit),
        Ok(None) => {
            driver.execute("COMMIT", &[])?;
            let mut fields = vec![
                ("committed".to_string(), Value::Bool(true)),
                ("executed".to_string(), Value::Arr(executed)),
                ("shortCircuit".to_string(), Value::Null),
                ("entity".to_string(), entity),
            ];
            if !returned_rows.is_empty() {
                fields.push(("returnedRows".to_string(), Value::Arr(returned_rows)));
            }
            Ok(Value::Obj(fields))
        }
        Err(e) => {
            let _ = driver.execute("ROLLBACK", &[]);
            Err(e)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE relation batch stitch (mirror of the runtime's stitch_relation for the
// companion's single-key closed set)
// ═══════════════════════════════════════════════════════════════════════════════

fn obj_get<'a>(o: &'a Value, key: &str) -> Option<&'a Value> {
    match o {
        Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}

/// Key identity string (mirror of the runtime `stringify_key`).
fn stringify_key(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                (*f as i64).to_string()
            } else {
                f.to_string()
            }
        }
        Value::Str(s) => s.clone(),
        other => panic!(
            "codegen native: relation key is a {} (outside the closed scalar set — fail-closed)",
            type_name(other)
        ),
    }
}

/// One relation-batch JSON key element (mirror of the runtime relation `json_scalar`).
fn relation_json_key(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                out.push_str(&(*f as i64).to_string());
            } else {
                out.push_str(&f.to_string());
            }
        }
        Value::Str(s) => json_escape_into(s, out),
        other => panic!(
            "codegen native: relation key is a {} (outside the closed scalar set — fail-closed)",
            type_name(other)
        ),
    }
}

fn relation_json_array(keys: &[Value]) -> String {
    let mut out = String::with_capacity(2 + keys.len() * 4);
    out.push('[');
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        relation_json_key(k, &mut out);
    }
    out.push(']');
    out
}

/// Batch-load + hydrate ONE single-key relation onto the parent rows (native mirror of the
/// runtime `stitch_relation`): dedupe non-null parent keys (insertion order), resolve the PG
/// deferred array cast from the REAL keys, `?`→`$N`, bind (PG: ONE array param; mysql/sqlite: ONE
/// JSON text param), group child rows by target key, distribute per cardinality.
fn stitch_relation_native(
    rel: &Relation,
    mut parents: Vec<Value>,
    driver: &dyn Driver,
) -> Vec<Value> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut keys: Vec<Value> = Vec::new();
    for p in &parents {
        match obj_get(p, rel.parent_key) {
            None | Some(Value::Null) => continue,
            Some(v) => {
                if seen.insert(stringify_key(v)) {
                    keys.push(v.clone());
                }
            }
        }
    }
    let mut sql = rel.sql.to_string();
    if rel.dialect == Dialect::Postgres {
        sql = resolve_pg_array_cast(&sql, &keys);
    }
    let sql = render_placeholders(&sql, rel.dialect);

    let mut batch: HashMap<String, Vec<Value>> = HashMap::new();
    if !keys.is_empty() {
        let bound: Vec<Value> = if rel.dialect == Dialect::Postgres {
            vec![Value::Arr(keys.clone())]
        } else {
            vec![Value::Str(relation_json_array(&keys))]
        };
        let rows = driver.query(&sql, &bound).unwrap_or_else(|e| {
            panic!("codegen native: relation '{}' batch failed: {e}", rel.name)
        });
        for row in rows {
            let k = match obj_get(&row, rel.target_key) {
                Some(v) => stringify_key(v),
                None => "null".to_string(),
            };
            batch.entry(k).or_default().push(row);
        }
    }

    for row in parents.iter_mut() {
        let children = match obj_get(row, rel.parent_key) {
            None | Some(Value::Null) => None,
            Some(v) => batch.get(&stringify_key(v)),
        };
        let child = if rel.kind == RelKind::HasMany {
            Value::Arr(children.cloned().unwrap_or_default())
        } else {
            match children {
                Some(r) if !r.is_empty() => r[0].clone(),
                _ => Value::Null,
            }
        };
        if let Value::Obj(pairs) = row {
            pairs.push((rel.name.to_string(), child));
        }
    }
    parents
}

// ═══════════════════════════════════════════════════════════════════════════════
// Case dispatch (through the bc-GENERATED modules — the ONLY exec entry)
// ═══════════════════════════════════════════════════════════════════════════════

/// One prepared case: the static companion plan + the input scope materialized ONCE at load.
struct PreparedCase {
    plan: &'static CasePlan,
    /// The bench input scope (native, from companion InVal data). #60 m1: writes now call
    /// `exec_tx` DIRECTLY with this SAME scope (no generated write module — the old `__sql`/
    /// `__sqlParams`/`__skip` surrogate wiring `wmi` carried is gone, along with that module).
    input: Scope,
}

fn in_val_to_value(v: &InVal) -> Value {
    match v {
        InVal::Null => Value::Null,
        InVal::Bool(b) => Value::Bool(*b),
        InVal::Int(i) => Value::Int(*i),
        InVal::Float(f) => Value::Float(*f),
        InVal::Str(s) => Value::Str((*s).to_string()),
        InVal::IntArr(a) => Value::Arr(a.iter().map(|i| Value::Int(*i)).collect()),
        InVal::StrArr(a) => Value::Arr(a.iter().map(|s| Value::Str((*s).to_string())).collect()),
    }
}

fn prepare_case(plan: &'static CasePlan) -> PreparedCase {
    let input: Scope = plan
        .input
        .iter()
        .map(|(k, v)| ((*k).to_string(), in_val_to_value(v)))
        .collect();
    PreparedCase { plan, input }
}

/// Execute ONE covered read THROUGH its typed-native module's `run_native_raw_struct_<Comp>`
/// (#60 m1): builds the module's own `InNR<Comp>` from the bench input scope, calls the runner with
/// this crate's `(&ReadPlan, &dyn Driver)` handler tuple (implements the module's `HandlerNR<Comp>`
/// — see above), and re-boxes the resulting `Vec<T0>` to `Value::Arr(Value::Obj(...))` ONLY at this
/// OUTPUT boundary (the verify/canon leg + relation-stitch consume `Value`) — the SQL row
/// materialization itself never touched `Value`/`RawValue` (query_rows → T0 fields directly).
macro_rules! call_typed_native_read {
    ($m:ident :: $runner:ident, $in_:expr, $plan:expr, $driver:expr, [$($field:ident),+ $(,)?]) => {{
        let handler: (&'static ReadPlan, &dyn Driver) = ($plan.read.expect("read plan"), $driver);
        let rows = $m::$runner(&handler, $in_).expect("codegen: typed-native read call");
        rows.into_iter()
            .map(|row| Value::Obj(vec![$((stringify!($field).to_string(), row_field_to_value(row.$field))),+]))
            .collect::<Vec<Value>>()
    }};
}

/// One `T0` field's native scalar → `Value` (boxed ONLY here, at the output boundary — not on the
/// SQL row-fetch plane). A tiny closed-set conversion (`i64`/`String` — the only field kinds the
/// covered corpus's outType produces).
trait RowFieldToValue {
    fn row_field_to_value(self) -> Value;
}
impl RowFieldToValue for i64 {
    fn row_field_to_value(self) -> Value {
        Value::Int(self)
    }
}
impl RowFieldToValue for String {
    fn row_field_to_value(self) -> Value {
        Value::Str(self)
    }
}
fn row_field_to_value<T: RowFieldToValue>(v: T) -> Value {
    v.row_field_to_value()
}

/// Execute ONE case. Reads route through the typed-native `HandlerNR<Comp>` seam (#60 m1, covered
/// cases only — find/belongsTo/hasMany/hasManyLimit); writes call the NATIVE gate-first tx executor
/// directly (no generated-module wrapper — writes have no codegen module anymore).
fn run_case(pc: &PreparedCase, driver: &dyn Driver) -> Value {
    let plan = pc.plan;
    match plan.case_id {
        "find" => {
            let in_ = cg_find::InNRFind {
                author_id: scope_i64(&pc.input, "author_id"),
                since: scope_str(&pc.input, "since"),
                status: scope_str(&pc.input, "status"),
            };
            Value::Arr(call_typed_native_read!(cg_find::run_native_raw_struct_Find, in_, plan, driver, [id, author_id, title, status, views, created_at]))
        }
        "belongsTo" | "hasMany" | "hasManyLimit" => {
            let in_belongs = cg_belongs_to::InNRPosts { author_id: scope_i64(&pc.input, "author_id") };
            let in_many = cg_has_many::InNRPosts { author_id: scope_i64(&pc.input, "author_id") };
            let in_many_limit = cg_has_many_limit::InNRPosts { author_id: scope_i64(&pc.input, "author_id") };
            let rows = match plan.case_id {
                "belongsTo" => call_typed_native_read!(cg_belongs_to::run_native_raw_struct_Posts, in_belongs, plan, driver, [id, author_id, title]),
                "hasMany" => call_typed_native_read!(cg_has_many::run_native_raw_struct_Posts, in_many, plan, driver, [id, author_id, title]),
                _ => call_typed_native_read!(cg_has_many_limit::run_native_raw_struct_Posts, in_many_limit, plan, driver, [id, author_id, title]),
            };
            let rel = plan.relation.expect("relation op");
            Value::Arr(stitch_relation_native(rel, rows, driver))
        }
        "complexWhere" | "inList" => panic!(
            "codegen: case '{}' is NOT typed-native-covered (bc#86 gap: an IN-list's array-typed head \
             has no native port type) — the harness must route this case through db_skip_reason, not run_case",
            plan.case_id
        ),
        "batchInsert" | "writeTxGate" => run_write(plan.tx.expect("tx plan"), plan.dialect, &pc.input, driver),
        other => panic!("unknown codegen case '{other}' (fail-closed)"),
    }
}

fn scope_i64(scope: &Scope, key: &str) -> i64 {
    match scope_get(scope, key) {
        Some(Value::Int(i)) => *i,
        other => {
            panic!("codegen native: bench input '{key}' is not an int (fail-closed): {other:?}")
        }
    }
}
fn scope_str(scope: &Scope, key: &str) -> String {
    match scope_get(scope, key) {
        Some(Value::Str(s)) => s.clone(),
        other => {
            panic!("codegen native: bench input '{key}' is not a string (fail-closed): {other:?}")
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Canonical JSON observation (verify leg) — hand-written, key-sorted, matching
// the interpreter runner's compact JSON form so the lm_bench canon compares equal
// ═══════════════════════════════════════════════════════════════════════════════

fn canon_into(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        // Rust's shortest-roundtrip float Debug matches the ryu form for the corpus
        // (which contains NO float results; ints ride Value::Int).
        Value::Float(f) => out.push_str(&format!("{f:?}")),
        Value::Str(s) => json_escape_into(s, out),
        Value::Arr(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canon_into(e, out);
            }
            out.push(']');
        }
        Value::Obj(pairs) => {
            let mut sorted: Vec<&(String, Value)> = pairs.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            out.push('{');
            for (i, (k, val)) in sorted.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_escape_into(k, out);
                out.push(':');
                canon_into(val, out);
            }
            out.push('}');
        }
    }
}

fn canon(v: &Value) -> String {
    let mut out = String::new();
    canon_into(v, &mut out);
    out
}

// ═══════════════════════════════════════════════════════════════════════════════
// Line protocol (hand-rolled text — the protocol layer; NO JSON crate in this crate)
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract a STRING field from a flat one-line JSON request (`"key":"value"`). The harness's
/// request fields are plain ASCII tokens (kind/case/dialect); an escaped char fails closed.
fn field_str(line: &str, key: &str) -> Option<String> {
    let pat = format!("\"{key}\"");
    let rest = &line[line.find(&pat)? + pat.len()..];
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    let val = &rest[..end];
    if val.contains('\\') {
        panic!(
            "codegen protocol: escaped string in request field '{key}' (unsupported — fail-closed)"
        );
    }
    Some(val.to_string())
}

/// Extract an unsigned integer field from a flat one-line JSON request (`"key":123`).
fn field_u64(line: &str, key: &str) -> Option<u64> {
    let pat = format!("\"{key}\"");
    let rest = &line[line.find(&pat)? + pat.len()..];
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn write_line(s: &str) {
    let mut out = std::io::stdout();
    out.write_all(s.as_bytes()).unwrap();
    out.write_all(b"\n").unwrap();
    out.flush().unwrap();
}

/// A JSON string literal for a response line (same escaping as the canon writer).
fn jstr(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    json_escape_into(s, &mut out);
    out
}

fn samples_json(samples: &[f64]) -> String {
    let mut out = String::with_capacity(samples.len() * 8 + 2);
    out.push('[');
    for (i, s) in samples.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("{s:?}"));
    }
    out.push(']');
    out
}

fn now_ms() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn collect<F: FnMut()>(warmup: u64, iters: u64, mut op: F) -> Vec<f64> {
    for _ in 0..warmup {
        op();
    }
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t0 = Instant::now();
        op();
        samples.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    samples
}

fn rss_bytes() -> u64 {
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
    {
        if let Ok(s) = String::from_utf8(out.stdout) {
            if let Ok(kb) = s.trim().parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}

// ── artifact: the prepared cases, materialized ONCE at load from the companion ──
struct Artifact {
    /// dialect → case id → prepared case.
    by_dialect: HashMap<&'static str, HashMap<&'static str, PreparedCase>>,
}

fn load_artifact() -> Artifact {
    let mut by_dialect = HashMap::new();
    for d in companion::DIALECTS {
        let mut cases = HashMap::new();
        for c in companion::CASE_IDS {
            let plan = companion::case_plan(d, c)
                .unwrap_or_else(|| panic!("companion missing plan for {d}/{c} (fail-closed)"));
            cases.insert(*c, prepare_case(plan));
        }
        by_dialect.insert(*d, cases);
    }
    Artifact { by_dialect }
}

impl Artifact {
    fn case(&self, dialect: &str, case_id: &str) -> &PreparedCase {
        self.by_dialect
            .get(dialect)
            .and_then(|m| m.get(case_id))
            .unwrap_or_else(|| panic!("unknown dialect/case '{dialect}/{case_id}' (fail-closed)"))
    }
}

fn seed_driver() -> SqliteDriver {
    let d = SqliteDriver::in_memory(companion::SCHEMA);
    for s in companion::SEED {
        d.execute(s, &[]).expect("seed");
    }
    d
}

// This binary IS the codegen cell: it drives the in-proc sqlite driver for the DB-backed axis
// (live PG/MySQL is not wired into the bench codegen cell — explicit skip, mirroring the other
// adapters), and the mock driver for the per-dialect MICRO axis.
fn db_skip_reason(dialect: &str) -> Option<String> {
    match dialect {
        "sqlite" => None,
        other => Some(format!(
            "rust codegen cell drives the in-proc sqlite driver only; live {other} DB-backed not wired for the generated cell"
        )),
    }
}

/// #60 milestone 1: `complexWhere`/`inList` are NOT typed-native-covered (bc#86 gap — an IN-list's
/// array-typed head has no native scalar port type bc's typed-native emitter can lower to). There
/// is no generated codegen module for them anymore (retired `-typed-raw` is NOT a fallback) — the
/// codegen cell reports this as an explicit SKIP (never a silent/incorrect result).
fn case_skip_reason(case_id: &str) -> Option<&'static str> {
    match case_id {
        "complexWhere" | "inList" => Some(
            "not typed-native-covered (bc#86 gap): this case's WHERE binds an IN-list, whose value \
             is an ARRAY — bc's typed-native emitter (rust-typed-native) has no native scalar port \
             type for it, so litedbmodel's codegen-only lowering fails closed at generation. NOT \
             regenerated on the retired '-typed-raw' path (no silent fallback).",
        ),
        _ => None,
    }
}

fn write_skipped(case: &str, dialect: &str, reason: &str) {
    write_line(&format!(
        "{{\"kind\":\"skipped\",\"case\":{},\"dialect\":{},\"reason\":{}}}",
        jstr(case),
        jstr(dialect),
        jstr(reason)
    ));
}

fn handle(kind: &str, line: &str, art: &Artifact) {
    match kind {
        "run" => {
            let case = field_str(line, "case").expect("run: case");
            let dialect = field_str(line, "dialect").unwrap_or_else(|| "sqlite".into());
            if let Some(reason) = case_skip_reason(&case) {
                write_skipped(&case, &dialect, reason);
                return;
            }
            if let Some(reason) = db_skip_reason(&dialect) {
                write_skipped(&case, &dialect, &reason);
                return;
            }
            let warmup = field_u64(line, "warmup").expect("run: warmup");
            let iters = field_u64(line, "iterations").expect("run: iterations");
            let d = seed_driver();
            let pc = art.case(&dialect, &case);
            let samples = collect(warmup, iters, || {
                run_case(pc, &d);
            });
            write_line(&format!(
                "{{\"kind\":\"run\",\"case\":{},\"dialect\":{},\"samplesMs\":{}}}",
                jstr(&case),
                jstr(&dialect),
                samples_json(&samples)
            ));
        }
        "throughput" => {
            let case = field_str(line, "case").expect("throughput: case");
            let dialect = field_str(line, "dialect").unwrap_or_else(|| "sqlite".into());
            if let Some(reason) = case_skip_reason(&case) {
                write_skipped(&case, &dialect, reason);
                return;
            }
            if let Some(reason) = db_skip_reason(&dialect) {
                write_skipped(&case, &dialect, &reason);
                return;
            }
            let iters = field_u64(line, "iterations").expect("throughput: iterations");
            let d = seed_driver();
            let pc = art.case(&dialect, &case);
            let t0 = Instant::now();
            for _ in 0..iters {
                run_case(pc, &d);
            }
            let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
            write_line(&format!(
                "{{\"kind\":\"throughput\",\"case\":{},\"dialect\":{},\"elapsedMs\":{:?},\"completed\":{}}}",
                jstr(&case),
                jstr(&dialect),
                elapsed,
                iters
            ));
        }
        "micro" => {
            let case = field_str(line, "case").expect("micro: case");
            let dialect = field_str(line, "dialect").unwrap_or_else(|| "sqlite".into());
            if let Some(reason) = case_skip_reason(&case) {
                write_skipped(&case, &dialect, reason);
                return;
            }
            let warmup = field_u64(line, "warmup").expect("micro: warmup");
            let iters = field_u64(line, "iterations").expect("micro: iterations");
            let mock = MockDriver;
            // PER-DIALECT native plan — the render/placeholder/array form differs.
            let pc = art.case(&dialect, &case);
            let samples = collect(warmup, iters, || {
                run_case(pc, &mock);
            });
            write_line(&format!(
                "{{\"kind\":\"micro\",\"case\":{},\"dialect\":{},\"samplesMs\":{}}}",
                jstr(&case),
                jstr(&dialect),
                samples_json(&samples)
            ));
        }
        "rss" => {
            write_line(&format!(
                "{{\"kind\":\"rss\",\"rssBytes\":{}}}",
                rss_bytes()
            ));
        }
        "cost" => {
            let case = field_str(line, "case").expect("cost: case");
            let dialect = field_str(line, "dialect").unwrap_or_else(|| "sqlite".into());
            if let Some(reason) = case_skip_reason(&case) {
                write_skipped(&case, &dialect, reason);
                return;
            }
            let base = seed_driver();
            let counter = CountingDriver {
                inner: &base,
                queries: Cell::new(0),
                rows: Cell::new(0),
            };
            let pc = art.case("sqlite", &case);
            run_case(pc, &counter);
            write_line(&format!(
                "{{\"kind\":\"cost\",\"case\":{},\"dialect\":{},\"queries\":{},\"rows\":{}}}",
                jstr(&case),
                jstr(&dialect),
                counter.queries.get(),
                counter.rows.get()
            ));
        }
        "verify" => {
            // Behaviour-equality observation: the codegen output's canonical JSON. The selfcheck
            // driver compares it against lm_bench(ir)'s canonical interpreter output per case.
            let case = field_str(line, "case").expect("verify: case");
            if let Some(reason) = case_skip_reason(&case) {
                write_skipped(&case, "sqlite", reason);
                return;
            }
            let d = seed_driver();
            let pc = art.case("sqlite", &case);
            let out = run_case(pc, &d);
            write_line(&format!(
                "{{\"kind\":\"verify\",\"case\":{},\"impl\":\"codegen\",\"canon\":{}}}",
                jstr(&case),
                jstr(&canon(&out))
            ));
        }
        "shutdown" => std::process::exit(0),
        _ => {}
    }
}

fn main() {
    // The prepared cases (native input scopes + plan refs) — materialized ONCE, outside any
    // timed loop. Warm each sqlite case once on the mock (module load + first-call cost). Skip a
    // case case_skip_reason reports (#60 m1: complexWhere/inList are NOT typed-native-covered).
    let art = load_artifact();
    for c in companion::CASE_IDS {
        if case_skip_reason(c).is_some() {
            continue;
        }
        let pc = art.case("sqlite", c);
        run_case(pc, &MockDriver);
    }

    write_line(&format!(
        "{{\"kind\":\"ready\",\"language\":\"rust\",\"impl\":\"codegen\",\"readyAtEpochMs\":{:?}}}",
        now_ms()
    ));

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let kind = match field_str(&line, "kind") {
            Some(k) => k,
            None => {
                write_line("{\"kind\":\"error\",\"message\":\"bad request: no kind\"}");
                continue;
            }
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle(&kind, &line, &art);
        }));
        if let Err(e) = result {
            let msg = e
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| e.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".into());
            write_line(&format!(
                "{{\"kind\":\"error\",\"message\":{}}}",
                jstr(&msg)
            ));
        }
    }
}
