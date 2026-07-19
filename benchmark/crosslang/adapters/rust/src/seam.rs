//! The exec seam — the ENTIRE native runtime a baked-SQL op needs, over a DRIVER ABSTRACTION.
//!
//! Owner's canonical model: "native runtime = 「SQL・params・SKIP引数の汎用クエリ実行関数」だけ".
//! The seam runs baked SQL and MATERIALIZES each result row into a driver-agnostic WIRE (by column
//! name): the 0.8.9-generated modules de-box that wire INLINE (strict, by declared type) into their
//! concrete row structs. OP-AGNOSTIC (knows only SQL + params + how to materialize a row) and
//! DIALECT-abstracted (one `Db` per built driver). One dialect per binary (cargo feature).
//!
//! Read and write share ONE flow. The only per-dialect concerns are the DRIVER BIND (scalar vs
//! `<elem>[]` array), the batch/relation array shape (v2 single-JSON `json_each(?)` vs v1 `= ANY($1)` /
//! `UNNEST`), and — on mysql (no native RETURNING) — the generic strip-marker + re-select mechanic.

// ── The bound-param closed set (dialect-independent) ──────────────────────────────────────────────
#[derive(Debug, Clone)]
pub enum Param {
    Int(i64),
    Real(f64),
    Text(String),
    ArrayInt(Vec<i64>),
    ArrayText(Vec<String>),
}

/// A process-wide count of DB queries the seam issued — the N+1-avoidance proof.
pub static QUERY_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[inline(always)]
fn count_query() {
    #[cfg(feature = "count-queries")]
    QUERY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// The single summary a NON-RETURNING write hands back (mode-2 `executeStaticWrite`'s `{changes,
/// lastInsertRowid}` shape) — the generated module's no-RETURNING outType.
#[derive(Clone, Copy)]
pub struct WriteSummary {
    pub changes: i64,
    pub last_insert_rowid: i64,
}

// ══ The WIRE model — a materialized result the generated de-box probes by column name ══════════════
/// One materialized column value (driver-agnostic). The probe accessors coerce to the DECLARED type the
/// generated de-box asks for (e.g. a pg BOOLEAN `published` answers `probe_number` with 0/1).
#[derive(Clone)]
pub enum WireCell {
    Int(i64),
    Real(f64),
    Text(String),
    Bool(bool),
    Null,
}
impl WireCell {
    fn num_raw(&self) -> Option<String> {
        match self {
            WireCell::Int(v) => Some(v.to_string()),
            WireCell::Real(v) => Some(v.to_string()),
            WireCell::Bool(b) => Some((*b as i64).to_string()),
            _ => None,
        }
    }
    fn str_val(&self) -> Option<String> {
        match self {
            WireCell::Text(s) => Some(s.clone()),
            _ => None,
        }
    }
    fn bool_val(&self) -> Option<bool> {
        match self {
            WireCell::Bool(b) => Some(*b),
            WireCell::Int(v) => Some(*v != 0),
            _ => None,
        }
    }
    fn is_null(&self) -> bool {
        matches!(self, WireCell::Null)
    }
    fn tag(&self) -> &'static str {
        match self {
            WireCell::Int(_) | WireCell::Real(_) => "number",
            WireCell::Text(_) => "string",
            WireCell::Bool(_) => "bool",
            WireCell::Null => "null",
        }
    }
    fn raw(&self) -> String {
        match self {
            WireCell::Int(v) => v.to_string(),
            WireCell::Real(v) => v.to_string(),
            WireCell::Text(s) => s.clone(),
            WireCell::Bool(b) => b.to_string(),
            WireCell::Null => "null".to_string(),
        }
    }
}

/// Neutral probe outcomes (the seam speaks these; the per-module glue macro maps them to that module's
/// own `Probe`/`NumProbe` enums — the wire traits are generated PER module).
pub enum Outcome<T> {
    Got(T),
    Wrong { wire: String, raw: String },
    Null { wire: String, raw: String },
    Absent,
}
pub enum NumOutcome {
    Got { raw: String, wire: String },
    Wrong { wire: String, raw: String },
    Null { wire: String, raw: String },
    Absent,
}

fn cell_num(c: &WireCell) -> NumOutcome {
    if c.is_null() {
        return NumOutcome::Null { wire: c.tag().to_string(), raw: c.raw() };
    }
    match c.num_raw() {
        Some(raw) => NumOutcome::Got { raw, wire: c.tag().to_string() },
        None => NumOutcome::Wrong { wire: c.tag().to_string(), raw: c.raw() },
    }
}
fn cell_str(c: &WireCell) -> Outcome<String> {
    if c.is_null() {
        return Outcome::Null { wire: c.tag().to_string(), raw: c.raw() };
    }
    match c.str_val() {
        Some(v) => Outcome::Got(v),
        None => Outcome::Wrong { wire: c.tag().to_string(), raw: c.raw() },
    }
}
fn cell_bool(c: &WireCell) -> Outcome<bool> {
    if c.is_null() {
        return Outcome::Null { wire: c.tag().to_string(), raw: c.raw() };
    }
    match c.bool_val() {
        Some(v) => Outcome::Got(v),
        None => Outcome::Wrong { wire: c.tag().to_string(), raw: c.raw() },
    }
}

/// A materialized wire ROW (column name → cell). Probed by field name by the generated de-box.
#[derive(Clone)]
pub struct WireRowData {
    pub cells: Vec<(String, WireCell)>,
}
impl WireRowData {
    fn get(&self, field: &str) -> Option<&WireCell> {
        self.cells.iter().find(|(k, _)| k == field).map(|(_, v)| v)
    }
    pub fn keys_vec(&self) -> Vec<String> {
        self.cells.iter().map(|(k, _)| k.clone()).collect()
    }
    // Direct typed reads for the hand-written SDK baseline (which formats raw rows itself, NOT via the
    // generated de-box). By column name (the SDK authors the SELECT list). A pg BOOLEAN read as i64 →
    // 0/1 (the `published` canonicalization the oracle's canonVal also applies).
    pub fn i64(&self, field: &str) -> i64 {
        match self.get(field) {
            Some(WireCell::Int(v)) => *v,
            Some(WireCell::Bool(b)) => *b as i64,
            Some(WireCell::Real(v)) => *v as i64,
            Some(WireCell::Text(s)) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }
    pub fn text(&self, field: &str) -> String {
        match self.get(field) {
            Some(c) => c.raw(),
            None => String::new(),
        }
    }
    pub fn probe_num(&self, field: &str) -> NumOutcome {
        match self.get(field) {
            Some(c) => cell_num(c),
            None => NumOutcome::Absent,
        }
    }
    pub fn probe_str(&self, field: &str) -> Outcome<String> {
        match self.get(field) {
            Some(c) => cell_str(c),
            None => Outcome::Absent,
        }
    }
    pub fn probe_boolean(&self, field: &str) -> Outcome<bool> {
        match self.get(field) {
            Some(c) => cell_bool(c),
            None => Outcome::Absent,
        }
    }
    // The bench rows are flat (no nested obj/list within a row — relations are separate map nodes).
    pub fn probe_rowv(&self, _field: &str) -> Outcome<WireRowData> {
        Outcome::Absent
    }
    pub fn probe_listv(&self, _field: &str) -> Outcome<WireListData> {
        Outcome::Absent
    }
}

/// A materialized wire LIST (the rows of a result). Probed per element by the generated de-box.
#[derive(Clone)]
pub struct WireListData {
    pub rows: Vec<WireRowData>,
}
impl WireListData {
    pub fn length(&self) -> usize {
        self.rows.len()
    }
    pub fn elem_rowv(&self, i: usize) -> Outcome<WireRowData> {
        match self.rows.get(i) {
            Some(r) => Outcome::Got(r.clone()),
            None => Outcome::Absent,
        }
    }
    // The bench lists are lists of rows (parents / children), never scalar-element lists.
    pub fn elem_str(&self, _i: usize) -> Outcome<String> {
        Outcome::Absent
    }
    pub fn elem_num(&self, _i: usize) -> NumOutcome {
        NumOutcome::Absent
    }
    pub fn elem_boolean(&self, _i: usize) -> Outcome<bool> {
        Outcome::Absent
    }
    pub fn elem_listv(&self, _i: usize) -> Outcome<WireListData> {
        Outcome::Absent
    }
}

/// The handler's uniform return (`Self::Wire`): a materialized result. `as_list` yields the rows (a read
/// / single-write list); `as_row` yields the first row (a tx-body single row / summary).
#[derive(Clone)]
pub struct WireResult {
    pub rows: Vec<WireRowData>,
}
impl WireResult {
    pub fn list(rows: Vec<WireRowData>) -> WireResult {
        WireResult { rows }
    }
    /// A no-RETURNING write's result: ONE summary row `{changes, lastInsertRowid}` (v1's mode-2 shape).
    pub fn summary(s: WriteSummary) -> WireResult {
        WireResult {
            rows: vec![WireRowData {
                cells: vec![
                    ("changes".to_string(), WireCell::Int(s.changes)),
                    ("lastInsertRowid".to_string(), WireCell::Int(s.last_insert_rowid)),
                ],
            }],
        }
    }
    pub fn v_list(&self) -> Outcome<WireListData> {
        Outcome::Got(WireListData { rows: self.rows.clone() })
    }
    pub fn v_row(&self) -> Outcome<WireRowData> {
        match self.rows.first() {
            Some(r) => Outcome::Got(r.clone()),
            None => Outcome::Null { wire: "null".to_string(), raw: "".to_string() },
        }
    }
    pub fn v_str(&self) -> Outcome<String> {
        Outcome::Wrong { wire: "row".to_string(), raw: "".to_string() }
    }
    pub fn v_num(&self) -> NumOutcome {
        NumOutcome::Wrong { wire: "row".to_string(), raw: "".to_string() }
    }
    pub fn v_bool(&self) -> Outcome<bool> {
        Outcome::Wrong { wire: "row".to_string(), raw: "".to_string() }
    }
}

// ── canonical JSON out (hand-rolled — the runtime carries no external JSON crate) ──
pub fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
/// The value of column-array `p` at row `i`, JSON-encoded for the batch zip (Int bare, Text quoted).
fn array_json_cell(p: &Param, i: usize) -> String {
    match p {
        Param::ArrayInt(v) => v[i].to_string(),
        Param::ArrayText(v) => json_str(&v[i]),
        _ => panic!("batch-write column array must be an int/text array"),
    }
}
fn array_len(p: &Param) -> usize {
    match p {
        Param::ArrayInt(v) => v.len(),
        Param::ArrayText(v) => v.len(),
        _ => 0,
    }
}
/// Zip the parallel column arrays into the `[{col:val,…},…]` JSON the v2 `json_each`/`JSON_TABLE` batch
/// form expands (sqlite/mysql). ONE JSON bound to every `?`.
fn batch_json(columns: &[&str], arrays: &[&Param]) -> String {
    let n = arrays.first().map(|a| array_len(a)).unwrap_or(0);
    let mut objs: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let fields: Vec<String> = columns.iter().enumerate().map(|(j, c)| format!("{}:{}", json_str(c), array_json_cell(arrays[j], i))).collect();
        objs.push(format!("{{{}}}", fields.join(",")));
    }
    format!("[{}]", objs.join(","))
}

// ══ MySQL RETURNING emulation — a generic strip-marker + re-select mechanic (dialect-independent: the
// marker is emitted into the SQL literal ONLY for mysql by the codegen `mysqlWriteReselect`) ═══════════
const MARK_OPEN: &str = " /*scp-reselect: ";
struct Reselect<'a> {
    write_sql: &'a str,
    select_sql: &'a str,
    binds: Vec<&'a str>,
}
fn parse_reselect(sql: &str) -> Option<Reselect<'_>> {
    let open = sql.find(MARK_OPEN)?;
    let write_sql = &sql[..open];
    let rest = &sql[open + MARK_OPEN.len()..];
    let close = rest.rfind("*/")?;
    let body = rest[..close].trim_end();
    let sep = body.find(" ::binds:: ")?;
    let select_sql = &body[..sep];
    let binds: Vec<&str> = body[sep + " ::binds:: ".len()..].split(',').filter(|s| !s.is_empty()).collect();
    Some(Reselect { write_sql, select_sql, binds })
}
/// Build the re-select params from the baked token list + the write's own params + the write result:
/// `L`/`H` = the LAST_INSERT_ID range `[id, id+affectedRows)`; `pN` = the write's param N.
fn reselect_params(binds: &[&str], write_params: &[Param], s: WriteSummary) -> Vec<Param> {
    binds
        .iter()
        .map(|t| match *t {
            "L" => Param::Int(s.last_insert_rowid),
            "H" => Param::Int(s.last_insert_rowid + s.changes),
            _ if t.starts_with('p') => {
                let i: usize = t[1..].parse().expect("reselect bind pN index");
                write_params[i].clone()
            }
            other => panic!("unknown reselect bind token '{other}'"),
        })
        .collect()
}

// ══ Top-level generic ops (call the built backend's materialize/exec) ══════════════════════════════════
/// Run a read / RETURNING write and MATERIALIZE its rows. On mysql a RETURNING write carries the
/// re-select marker: strip it, run the write, re-select by the baked SELECT (the ONE strip+reselect
/// mechanic; pg/sqlite never carry a marker so this is a no-op branch for them).
pub fn query(db: &Db, sql: &str, params: &[Param]) -> Result<WireResult, String> {
    if let Some(r) = parse_reselect(sql) {
        let s = run_exec(db, r.write_sql, params)?;
        let rp = reselect_params(&r.binds, params, s);
        return Ok(WireResult::list(materialize(db, r.select_sql, &rp)?));
    }
    Ok(WireResult::list(materialize(db, sql, params)?))
}
/// Run a NON-RETURNING write (defensive marker strip) → the summary.
pub fn execute(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
    let clean = parse_reselect(sql).map(|r| r.write_sql).unwrap_or(sql);
    run_exec(db, clean, params)
}
/// Run a NON-RETURNING BATCH write (v2 JSON on sqlite/mysql; v1 UNNEST arrays on pg) → the summary.
pub fn execute_batch(db: &Db, sql: &str, columns: &[&str], arrays: &[&Param]) -> Result<WriteSummary, String> {
    run_exec_batch(db, sql, columns, arrays)
}

// ── Relation dedup + group + align (E4/#119): dialect-independent ──
pub fn distinct_keys<Key: Eq + std::hash::Hash + Clone>(item_keys: &[Key]) -> Vec<Key> {
    let mut seen: std::collections::HashSet<Key> = std::collections::HashSet::with_capacity(item_keys.len());
    let mut distinct: Vec<Key> = Vec::with_capacity(item_keys.len());
    for k in item_keys {
        if seen.insert(k.clone()) {
            distinct.push(k.clone());
        }
    }
    distinct
}
/// The generic BATCHED-RELATION exec (N+1-avoided): dedup the parent keys, run the ONE child query,
/// group child ROWS by their target key, align per-parent to `item_keys`, and wrap each parent's child
/// list in a `WireResult` (the generated relation handler returns `Vec<Self::Wire>`, one per parent).
pub fn query_batched_relation<Key>(
    db: &Db,
    sql: &str,
    item_keys: &[Key],
    encode_json: impl Fn(&[Key]) -> String,
    key_arrays: impl Fn(&[Key]) -> Vec<Param>,
    child_key: impl Fn(&WireRowData) -> Key,
) -> Result<Vec<WireResult>, String>
where
    Key: Eq + std::hash::Hash + Clone,
{
    let distinct = distinct_keys(item_keys);
    let json = encode_json(&distinct);
    let arrays = key_arrays(&distinct);
    let children = relation_query(db, sql, &arrays, json)?;
    let mut groups: std::collections::HashMap<Key, Vec<WireRowData>> = std::collections::HashMap::new();
    for ch in children {
        groups.entry(child_key(&ch)).or_default().push(ch);
    }
    Ok(item_keys.iter().map(|k| WireResult::list(groups.get(k).cloned().unwrap_or_default())).collect())
}

// ══ SQLite backend (rusqlite) ═════════════════════════════════════════════════════════════════════════
#[cfg(feature = "sqlite")]
mod sqlite_backend {
    use super::*;
    use rusqlite::Connection;

    pub struct Db {
        pub conn: Connection,
    }
    impl Db {
        pub fn open(target: &str) -> Db {
            Db { conn: Connection::open(target).expect("open sqlite db") }
        }
    }
    fn encode_json_array(p: &Param) -> Option<String> {
        match p {
            Param::ArrayInt(v) => Some(format!("[{}]", v.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(","))),
            Param::ArrayText(v) => Some(format!("[{}]", v.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(","))),
            _ => None,
        }
    }
    impl rusqlite::ToSql for Param {
        fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
            if let Some(json) = encode_json_array(self) {
                return Ok(rusqlite::types::ToSqlOutput::from(json));
            }
            Ok(match self {
                Param::Int(v) => rusqlite::types::ToSqlOutput::from(*v),
                Param::Real(v) => rusqlite::types::ToSqlOutput::from(*v),
                Param::Text(v) => rusqlite::types::ToSqlOutput::from(v.as_str()),
                Param::ArrayInt(_) | Param::ArrayText(_) => unreachable!("handled above"),
            })
        }
    }
    fn cell_of(vr: rusqlite::types::ValueRef<'_>) -> WireCell {
        match vr {
            rusqlite::types::ValueRef::Null => WireCell::Null,
            rusqlite::types::ValueRef::Integer(v) => WireCell::Int(v),
            rusqlite::types::ValueRef::Real(v) => WireCell::Real(v),
            rusqlite::types::ValueRef::Text(t) => WireCell::Text(String::from_utf8_lossy(t).into_owned()),
            rusqlite::types::ValueRef::Blob(_) => WireCell::Null,
        }
    }
    pub fn materialize(db: &Db, sql: &str, params: &[Param]) -> Result<Vec<WireRowData>, String> {
        count_query();
        let mut stmt = db.conn.prepare_cached(sql).map_err(|e| e.to_string())?;
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let n = names.len();
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter())).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut cells = Vec::with_capacity(n);
            for i in 0..n {
                let vr = row.get_ref(i).map_err(|e| e.to_string())?;
                cells.push((names[i].clone(), cell_of(vr)));
            }
            out.push(WireRowData { cells });
        }
        Ok(out)
    }
    pub fn run_exec(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
        count_query();
        let changes = db.conn.prepare_cached(sql).map_err(|e| e.to_string())?.execute(rusqlite::params_from_iter(params.iter())).map_err(|e| e.to_string())?;
        Ok(WriteSummary { changes: changes as i64, last_insert_rowid: db.conn.last_insert_rowid() })
    }
    pub fn run_exec_batch(db: &Db, sql: &str, columns: &[&str], arrays: &[&Param]) -> Result<WriteSummary, String> {
        let json = batch_json(columns, arrays);
        let n = sql.matches('?').count();
        let params: Vec<Param> = (0..n).map(|_| Param::Text(json.clone())).collect();
        run_exec(db, sql, &params)
    }
    pub fn relation_query(db: &Db, sql: &str, _key_arrays: &[Param], json: String) -> Result<Vec<WireRowData>, String> {
        materialize(db, sql, &[Param::Text(json)])
    }
    pub fn transaction<T, E>(db: &Db, body: impl FnOnce(&Db) -> Result<T, E>) -> Result<T, E> {
        db.conn.execute_batch("BEGIN").expect("tx BEGIN");
        match body(db) {
            Ok(v) => {
                db.conn.execute_batch("COMMIT").expect("tx COMMIT");
                Ok(v)
            }
            Err(e) => {
                let _ = db.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }
}
#[cfg(feature = "sqlite")]
pub use sqlite_backend::*;

// ══ PostgreSQL backend (postgres crate) ════════════════════════════════════════════════════════════════
#[cfg(feature = "postgres")]
mod pg_backend {
    use super::*;
    use postgres::types::{IsNull, ToSql, Type};
    use postgres::{Client, NoTls, Row};
    use std::cell::RefCell;
    use std::error::Error;

    pub struct Db {
        pub client: RefCell<Client>,
    }
    impl Db {
        pub fn open(target: &str) -> Db {
            Db { client: RefCell::new(Client::connect(target, NoTls).expect("connect postgres")) }
        }
    }
    impl ToSql for Param {
        fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            match self {
                Param::Int(v) => match *ty {
                    Type::INT4 => (*v as i32).to_sql(ty, out),
                    Type::INT2 => (*v as i16).to_sql(ty, out),
                    Type::BOOL => (*v != 0).to_sql(ty, out),
                    _ => v.to_sql(ty, out),
                },
                Param::Real(v) => v.to_sql(ty, out),
                Param::Text(v) => v.to_sql(ty, out),
                Param::ArrayInt(v) => match *ty {
                    Type::INT4_ARRAY => v.iter().map(|x| *x as i32).collect::<Vec<i32>>().to_sql(ty, out),
                    Type::INT2_ARRAY => v.iter().map(|x| *x as i16).collect::<Vec<i16>>().to_sql(ty, out),
                    _ => v.to_sql(ty, out),
                },
                Param::ArrayText(v) => v.to_sql(ty, out),
            }
        }
        fn accepts(_ty: &Type) -> bool {
            true
        }
        postgres::types::to_sql_checked!();
    }
    fn param_refs<'a>(params: &'a [Param]) -> Vec<&'a (dyn ToSql + Sync)> {
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
    }
    fn cell_of(row: &Row, i: usize, ty: &Type) -> WireCell {
        match *ty {
            Type::INT8 => match row.try_get::<_, Option<i64>>(i) {
                Ok(Some(v)) => WireCell::Int(v),
                _ => WireCell::Null,
            },
            Type::INT2 => match row.try_get::<_, Option<i16>>(i) {
                Ok(Some(v)) => WireCell::Int(v as i64),
                _ => WireCell::Null,
            },
            Type::BOOL => match row.try_get::<_, Option<bool>>(i) {
                Ok(Some(v)) => WireCell::Bool(v),
                _ => WireCell::Null,
            },
            Type::TIMESTAMP => match row.try_get::<_, Option<chrono::NaiveDateTime>>(i) {
                Ok(Some(v)) => WireCell::Text(v.format("%Y-%m-%d %H:%M:%S").to_string()),
                _ => WireCell::Null,
            },
            Type::TEXT | Type::VARCHAR | Type::BPCHAR => match row.try_get::<_, Option<String>>(i) {
                Ok(Some(v)) => WireCell::Text(v),
                _ => WireCell::Null,
            },
            // INT4 (incl. SERIAL id / INTEGER fks) and any other int-shaped column.
            _ => match row.try_get::<_, Option<i32>>(i) {
                Ok(Some(v)) => WireCell::Int(v as i64),
                Ok(None) => WireCell::Null,
                Err(_) => match row.try_get::<_, Option<String>>(i) {
                    Ok(Some(v)) => WireCell::Text(v),
                    _ => WireCell::Null,
                },
            },
        }
    }
    pub fn materialize(db: &Db, sql: &str, params: &[Param]) -> Result<Vec<WireRowData>, String> {
        count_query();
        let refs = param_refs(params);
        let rows = db.client.borrow_mut().query(sql, &refs).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in &rows {
            let cols = row.columns();
            let mut cells = Vec::with_capacity(cols.len());
            for (i, c) in cols.iter().enumerate() {
                cells.push((c.name().to_string(), cell_of(row, i, c.type_())));
            }
            out.push(WireRowData { cells });
        }
        Ok(out)
    }
    pub fn run_exec(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
        count_query();
        let refs = param_refs(params);
        let changes = db.client.borrow_mut().execute(sql, &refs).map_err(|e| e.to_string())?;
        Ok(WriteSummary { changes: changes as i64, last_insert_rowid: 0 })
    }
    /// v1 UNNEST batch: bind each column's WHOLE array as a native `<elem>[]` param (`$n::T[]`).
    pub fn run_exec_batch(db: &Db, sql: &str, _columns: &[&str], arrays: &[&Param]) -> Result<WriteSummary, String> {
        let params: Vec<Param> = arrays.iter().map(|a| (*a).clone()).collect();
        run_exec(db, sql, &params)
    }
    /// v1 relation child: `= ANY(?::@@PG_ARRAY_CAST@@)` / composite `UNNEST(?::…, …)` — resolve the cast
    /// to `int[]` (the bench relation keys are all integer) and renumber `?`→`$N`, bind native arrays.
    pub fn relation_query(db: &Db, sql: &str, key_arrays: &[Param], _json: String) -> Result<Vec<WireRowData>, String> {
        let resolved = renumber(&sql.replace("@@PG_ARRAY_CAST@@", "int[]"));
        materialize(db, &resolved, key_arrays)
    }
    fn renumber(sql: &str) -> String {
        let mut out = String::with_capacity(sql.len() + 8);
        let mut n = 0;
        for c in sql.chars() {
            if c == '?' {
                n += 1;
                out.push('$');
                out.push_str(&n.to_string());
            } else {
                out.push(c);
            }
        }
        out
    }
    pub fn transaction<T, E>(db: &Db, body: impl FnOnce(&Db) -> Result<T, E>) -> Result<T, E> {
        db.client.borrow_mut().batch_execute("BEGIN").expect("tx BEGIN");
        match body(db) {
            Ok(v) => {
                db.client.borrow_mut().batch_execute("COMMIT").expect("tx COMMIT");
                Ok(v)
            }
            Err(e) => {
                let _ = db.client.borrow_mut().batch_execute("ROLLBACK");
                Err(e)
            }
        }
    }
}
#[cfg(feature = "postgres")]
pub use pg_backend::*;

// ══ MySQL backend (mysql crate) — no native RETURNING (handled by the marker mechanic in `query`) ══════
#[cfg(feature = "mysql")]
mod mysql_backend {
    use super::*;
    use mysql::prelude::Queryable;
    use mysql::{Conn, Opts, Value};

    pub struct Db {
        pub conn: std::cell::RefCell<Conn>,
    }
    impl Db {
        pub fn open(target: &str) -> Db {
            let opts = Opts::from_url(target).expect("parse mysql url");
            Db { conn: std::cell::RefCell::new(Conn::new(opts).expect("connect mysql")) }
        }
    }
    fn encode_json_array(p: &Param) -> Option<String> {
        match p {
            Param::ArrayInt(v) => Some(format!("[{}]", v.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(","))),
            Param::ArrayText(v) => Some(format!("[{}]", v.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(","))),
            _ => None,
        }
    }
    fn to_value(p: &Param) -> Value {
        if let Some(json) = encode_json_array(p) {
            return Value::Bytes(json.into_bytes());
        }
        match p {
            Param::Int(v) => Value::Int(*v),
            Param::Real(v) => Value::Double(*v),
            Param::Text(v) => Value::Bytes(v.clone().into_bytes()),
            Param::ArrayInt(_) | Param::ArrayText(_) => unreachable!("handled above"),
        }
    }
    fn params_vec(params: &[Param]) -> Vec<Value> {
        params.iter().map(to_value).collect()
    }
    fn cell_of(v: Option<&Value>) -> WireCell {
        match v {
            Some(Value::NULL) | None => WireCell::Null,
            Some(Value::Int(v)) => WireCell::Int(*v),
            Some(Value::UInt(v)) => WireCell::Int(*v as i64),
            Some(Value::Bytes(b)) => WireCell::Text(String::from_utf8_lossy(b).into_owned()),
            Some(Value::Float(f)) => WireCell::Real(*f as f64),
            Some(Value::Double(f)) => WireCell::Real(*f),
            Some(Value::Date(y, mo, d, h, mi, s, _us)) => WireCell::Text(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, mo, d, h, mi, s)),
            Some(other) => WireCell::Text(format!("{:?}", other)),
        }
    }
    pub fn materialize(db: &Db, sql: &str, params: &[Param]) -> Result<Vec<WireRowData>, String> {
        count_query();
        let mut conn = db.conn.borrow_mut();
        let rows: Vec<mysql::Row> = conn.exec(sql, params_vec(params)).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in &rows {
            let cols = row.columns_ref();
            let mut cells = Vec::with_capacity(cols.len());
            for (i, c) in cols.iter().enumerate() {
                cells.push((c.name_str().to_string(), cell_of(row.as_ref(i))));
            }
            out.push(WireRowData { cells });
        }
        Ok(out)
    }
    pub fn run_exec(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
        count_query();
        let mut conn = db.conn.borrow_mut();
        conn.exec_drop(sql, params_vec(params)).map_err(|e| e.to_string())?;
        Ok(WriteSummary { changes: conn.affected_rows() as i64, last_insert_rowid: conn.last_insert_id() as i64 })
    }
    pub fn run_exec_batch(db: &Db, sql: &str, columns: &[&str], arrays: &[&Param]) -> Result<WriteSummary, String> {
        let json = batch_json(columns, arrays);
        let n = sql.matches('?').count();
        let params: Vec<Param> = (0..n).map(|_| Param::Text(json.clone())).collect();
        run_exec(db, sql, &params)
    }
    pub fn relation_query(db: &Db, sql: &str, _key_arrays: &[Param], json: String) -> Result<Vec<WireRowData>, String> {
        materialize(db, sql, &[Param::Text(json)])
    }
    pub fn transaction<T, E>(db: &Db, body: impl FnOnce(&Db) -> Result<T, E>) -> Result<T, E> {
        db.conn.borrow_mut().query_drop("BEGIN").expect("tx BEGIN");
        match body(db) {
            Ok(v) => {
                db.conn.borrow_mut().query_drop("COMMIT").expect("tx COMMIT");
                Ok(v)
            }
            Err(e) => {
                let _ = db.conn.borrow_mut().query_drop("ROLLBACK");
                Err(e)
            }
        }
    }
}
#[cfg(feature = "mysql")]
pub use mysql_backend::*;
