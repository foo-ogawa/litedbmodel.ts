//! The exec seam — the ENTIRE native runtime a baked-SQL op needs, over a DRIVER ABSTRACTION.
//!
//! Owner's canonical model: "native runtime = 「SQL・params・SKIP引数の汎用クエリ実行関数」だけ".
//! This module is that generic query-exec function and nothing else. It is OP-AGNOSTIC (knows only a
//! SQL string, an ordered param list, and how to decode one row) AND now DIALECT-AGNOSTIC: a single
//! `Db` backend + a `Cell` row-accessor trait abstract over the built driver, so the SAME handlers run
//! against sqlite (rusqlite) or postgres (postgres crate). One dialect per binary (cargo feature).
//!
//! Read and write share ONE flow: the module bakes the SQL either way; the seam runs it. The only
//! per-dialect concerns the seam owns are the DRIVER BIND (a scalar vs a `<elem>[]` array) and, for a
//! batch/relation, the array-param shape (v2 single-JSON `json_each(?)` vs v1 `= ANY($1)` / `UNNEST`).

// ── The bound-param closed set (dialect-independent) ──────────────────────────────────────────────
/// A bound param — the closed set the baked native ports lower to. `ArrayInt`/`ArrayText` are the
/// array-bound heads (IN-list bc#110 relations, and the pg `__batchArray` UNNEST columns): sqlite/mysql
/// bind them as ONE single-JSON param (`encode_json_array`), postgres binds them as a native `<elem>[]`.
#[derive(Debug)]
pub enum Param {
    Int(i64),
    Real(f64),
    Text(String),
    ArrayInt(Vec<i64>),
    ArrayText(Vec<String>),
}

/// JSON-encode an array param for the single-JSON IN-list bind (sqlite/mysql). Byte-equal to the TS
/// runtime's `JSON.stringify(arr)` for the scalar element types the lowering admits.
#[cfg(feature = "sqlite")]
fn encode_json_array(p: &Param) -> Option<String> {
    match p {
        Param::ArrayInt(v) => Some(format!("[{}]", v.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(","))),
        Param::ArrayText(v) => Some(format!("[{}]", v.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(","))),
        _ => None,
    }
}

/// A process-wide count of DB queries the seam issued — the N+1-avoidance proof (delta==2 for a batched
/// relation, not 1+N). Feature-gated so the latency bench never touches the atomic in the timed path.
pub static QUERY_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[inline(always)]
fn count_query() {
    #[cfg(feature = "count-queries")]
    QUERY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// The single summary row a NON-RETURNING write hands back — the shape mode-2 `executeStaticWrite`
/// returns (and the codegen bakes as a no-RETURNING write's outType).
pub struct WriteSummary {
    pub changes: i64,
    pub last_insert_rowid: i64,
}

// ══ Row-cell accessor abstraction — one decode closure over EITHER driver's row ═══════════════════════
/// A read cell accessor by column index — the ONLY row surface the handlers' decode closures use, so a
/// closure (`Foo { id: col(r, 0), .. }`) compiles against sqlite or postgres unchanged. Each impl reads
/// the driver's native cell and coerces to the closed value set (a pg TIMESTAMP → the canonical
/// `YYYY-MM-DD HH:MM:SS` string the sqlite oracle stores as TEXT; a pg int4 → i64; a pg BOOLEAN → bool).
pub trait Cell {
    fn i64(&self, i: usize) -> i64;
    fn text(&self, i: usize) -> String;
    fn opt_text(&self, i: usize) -> Option<String>;
    fn boolean(&self, i: usize) -> bool;
}

/// Decode ONE column into `T`, dispatched by the target field type — so a decode closure is written once
/// (`col(r, 3)`) and the field's type (i64 / String / bool for the pg-only `published`) selects the read.
pub trait FromCell: Sized {
    fn from_cell(c: &dyn Cell, i: usize) -> Self;
}
impl FromCell for i64 {
    fn from_cell(c: &dyn Cell, i: usize) -> Self { c.i64(i) }
}
impl FromCell for String {
    fn from_cell(c: &dyn Cell, i: usize) -> Self { c.text(i) }
}
impl FromCell for bool {
    fn from_cell(c: &dyn Cell, i: usize) -> Self { c.boolean(i) }
}
impl FromCell for Option<String> {
    fn from_cell(c: &dyn Cell, i: usize) -> Self { c.opt_text(i) }
}
/// `col(r, i)` — the decode-closure accessor (type inferred from the destination field).
#[inline]
pub fn col<T: FromCell>(c: &dyn Cell, i: usize) -> T {
    T::from_cell(c, i)
}

/// Serialize the `published` projection as the canonical `0`/`1` int the oracle emits, regardless of the
/// dialect's column type (sqlite/mysql `INTEGER`/`TINYINT` → i64; pg `BOOLEAN` → bool). `canonVal`
/// (oracle.ts) already maps a boolean to `'1'/'0'`, so this is the native twin of that canonicalization.
pub trait AsI64 {
    fn as_i64(&self) -> i64;
}
impl AsI64 for i64 {
    fn as_i64(&self) -> i64 { *self }
}
impl AsI64 for bool {
    fn as_i64(&self) -> i64 { *self as i64 }
}

/// ONE WHERE fragment for [`query_skip`]: bare predicate SQL (baked, no connector), its params, and the
/// SKIP ARG `present` (a required frag passes `true`; a skip-optional frag passes `<head>.is_some()`).
pub struct WhereFrag<'a> {
    pub sql: &'a str,
    pub present: bool,
    pub params: Vec<Param>,
}

// ══ SQLite backend (rusqlite) ═════════════════════════════════════════════════════════════════════════
#[cfg(feature = "sqlite")]
mod sqlite_backend {
    use super::*;
    use rusqlite::Connection;

    /// The seam's DB handle — a rusqlite connection (baked SQL → `prepare_cached` compiles once, reused).
    pub struct Db {
        pub conn: Connection,
    }
    impl Db {
        pub fn open(target: &str) -> Db {
            Db { conn: Connection::open(target).expect("open sqlite db") }
        }
    }

    impl rusqlite::ToSql for Param {
        fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
            // An array param binds as ONE single-JSON value (the baked `json_each(?)` expands it).
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

    impl Cell for rusqlite::Row<'_> {
        fn i64(&self, i: usize) -> i64 { self.get(i).expect("cell i64") }
        fn text(&self, i: usize) -> String { self.get(i).expect("cell text") }
        fn opt_text(&self, i: usize) -> Option<String> { self.get(i).expect("cell opt_text") }
        fn boolean(&self, i: usize) -> bool { self.get::<_, i64>(i).expect("cell bool") != 0 }
    }

    pub fn query<T>(db: &Db, sql: &str, params: &[Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        count_query();
        let mut stmt = db.conn.prepare_cached(sql).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| Ok(decode(r as &dyn Cell)))
            .map_err(|e| e.to_string())?;
        rows.collect::<rusqlite::Result<Vec<T>>>().map_err(|e| e.to_string())
    }

    pub fn query_skip<T>(db: &Db, head: &str, frags: &[WhereFrag], tail: &str, tail_params: &[Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        let mut sql = String::from(head);
        let mut params: Vec<&Param> = Vec::new();
        let mut first = true;
        for f in frags {
            if !f.present { continue; }
            sql.push_str(if first { " WHERE " } else { " AND " });
            sql.push_str(f.sql);
            first = false;
            for p in &f.params { params.push(p); }
        }
        sql.push_str(tail);
        for p in tail_params { params.push(p); }
        count_query();
        let mut stmt = db.conn.prepare_cached(&sql).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.into_iter()), |r| Ok(decode(r as &dyn Cell)))
            .map_err(|e| e.to_string())?;
        rows.collect::<rusqlite::Result<Vec<T>>>().map_err(|e| e.to_string())
    }

    pub fn execute(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
        count_query();
        let changes = db.conn.prepare_cached(sql).map_err(|e| e.to_string())?.execute(rusqlite::params_from_iter(params.iter())).map_err(|e| e.to_string())?;
        Ok(WriteSummary { changes: changes as i64, last_insert_rowid: db.conn.last_insert_rowid() })
    }

    pub fn transaction<T, E>(db: &Db, body: impl FnOnce(&Db) -> Result<T, E>) -> Result<T, E> {
        db.conn.execute_batch("BEGIN").expect("tx BEGIN");
        match body(db) {
            Ok(v) => { db.conn.execute_batch("COMMIT").expect("tx COMMIT"); Ok(v) }
            Err(e) => { let _ = db.conn.execute_batch("ROLLBACK"); Err(e) }
        }
    }

    /// BATCH-WRITE bind (v2): ZIP the parallel column arrays into the `[{col:val,…},…]` JSON the baked
    /// `json_each(?)` / `JSON_TABLE(?)` expands, and bind the SAME JSON to every `?` (createMany: one;
    /// updateMany: one per SET clause + the WHERE). ONE statement for N records. `arrays[j]` is column
    /// `columns[j]`'s whole array (Int keys bare, Text quoted — the type-aware JSON encoding).
    pub fn query_batch_write<T>(db: &Db, sql: &str, columns: &[&str], arrays: &[&Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        let n = arrays.first().map(|a| array_len(a)).unwrap_or(0);
        let mut objs: Vec<String> = Vec::with_capacity(n);
        for i in 0..n {
            let fields: Vec<String> = columns.iter().enumerate().map(|(j, c)| format!("{}:{}", json_str(c), array_json_cell(arrays[j], i))).collect();
            objs.push(format!("{{{}}}", fields.join(",")));
        }
        let json = format!("[{}]", objs.join(","));
        let n_params = sql.matches('?').count();
        let mut params: Vec<Param> = Vec::with_capacity(n_params);
        if n_params > 0 {
            for _ in 1..n_params { params.push(Param::Text(json.clone())); }
            params.push(Param::Text(json));
        }
        query(db, sql, &params, decode)
    }

    /// BATCHED-RELATION bind (v2): bind the deduped keys as the ONE single-JSON param the baked
    /// `json_each(?)` form expects. Unified signature with the pg backend (which ignores `json` and uses
    /// `key_arrays`) so the per-op relation handler is dialect-agnostic.
    pub fn relation_child<Child>(db: &Db, sql: &str, _key_arrays: &[Param], json: String, decode: impl Fn(&dyn Cell) -> Child) -> Result<Vec<Child>, String> {
        query(db, sql, &[Param::Text(json)], decode)
    }
    /// The value of column-array `p` at row `i`, JSON-encoded for the zip (Int bare, Text quoted).
    fn array_json_cell(p: &Param, i: usize) -> String {
        match p {
            Param::ArrayInt(v) => v[i].to_string(),
            Param::ArrayText(v) => json_str(&v[i]),
            _ => panic!("batch-write column array must be Array{{Int,Text}}"),
        }
    }
    fn array_len(p: &Param) -> usize {
        match p { Param::ArrayInt(v) => v.len(), Param::ArrayText(v) => v.len(), _ => 0 }
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

    /// The seam's DB handle — a blocking postgres client (the query needs `&mut`, so it rides a `RefCell`
    /// to keep the seam's `&Db` shape identical to sqlite's).
    pub struct Db {
        pub client: RefCell<Client>,
    }
    impl Db {
        pub fn open(target: &str) -> Db {
            Db { client: RefCell::new(Client::connect(target, NoTls).expect("connect postgres")) }
        }
    }

    // Bind a Param to postgres, coercing an i64/i64-array to the column's NARROWER int type (pg parses
    // `WHERE id = $1` as int4, `$1::int[]` as int4[]) so the wire format matches — v1's `::int[]` cast
    // stays byte-identical while the seam supplies the right width.
    impl ToSql for Param {
        fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            match self {
                Param::Int(v) => match *ty {
                    Type::INT4 => (*v as i32).to_sql(ty, out),
                    Type::INT2 => (*v as i16).to_sql(ty, out),
                    Type::BOOL => (*v != 0).to_sql(ty, out), // `published = $1` binds a BOOLEAN (1→true)
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
        fn accepts(_ty: &Type) -> bool { true } // the baked SQL casts (`$1::int[]`) drive coercion in to_sql
        postgres::types::to_sql_checked!();
    }

    impl Cell for Row {
        fn i64(&self, i: usize) -> i64 {
            match *self.columns()[i].type_() {
                Type::INT8 => self.get::<usize, i64>(i),
                Type::INT2 => self.get::<usize, i16>(i) as i64,
                _ => self.get::<usize, i32>(i) as i64, // INT4 (incl. SERIAL id / INTEGER fks)
            }
        }
        fn text(&self, i: usize) -> String {
            match *self.columns()[i].type_() {
                // A TIMESTAMP read canonicalizes to the `YYYY-MM-DD HH:MM:SS` string the sqlite oracle
                // stores as TEXT (the coltype `date` read-materialization contract).
                Type::TIMESTAMP => self.get::<usize, chrono::NaiveDateTime>(i).format("%Y-%m-%d %H:%M:%S").to_string(),
                _ => self.get::<usize, String>(i),
            }
        }
        fn opt_text(&self, i: usize) -> Option<String> { self.get::<usize, Option<String>>(i) }
        fn boolean(&self, i: usize) -> bool { self.get::<usize, bool>(i) }
    }

    fn param_refs<'a>(params: &'a [Param]) -> Vec<&'a (dyn ToSql + Sync)> {
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
    }

    pub fn query<T>(db: &Db, sql: &str, params: &[Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        count_query();
        let refs = param_refs(params);
        let rows = db.client.borrow_mut().query(sql, &refs).map_err(|e| e.to_string())?;
        Ok(rows.iter().map(|r| decode(r as &dyn Cell)).collect())
    }

    pub fn query_skip<T>(db: &Db, head: &str, frags: &[WhereFrag], tail: &str, tail_params: &[Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        // Assemble baked fragments (same order/connectors as mode-2), then renumber `?`→`$N` for pg.
        let mut sql = String::from(head);
        let mut params: Vec<Param> = Vec::new();
        let mut first = true;
        for f in frags {
            if !f.present { continue; }
            sql.push_str(if first { " WHERE " } else { " AND " });
            sql.push_str(f.sql);
            first = false;
            for p in &f.params { params.push(clone_param(p)); }
        }
        sql.push_str(tail);
        for p in tail_params { params.push(clone_param(p)); }
        let sql = renumber(&sql);
        query(db, &sql, &params, decode)
    }

    pub fn execute(db: &Db, sql: &str, params: &[Param]) -> Result<WriteSummary, String> {
        count_query();
        let refs = param_refs(params);
        let changes = db.client.borrow_mut().execute(sql, &refs).map_err(|e| e.to_string())?;
        Ok(WriteSummary { changes: changes as i64, last_insert_rowid: 0 }) // pg has no lastInsertRowid; RETURNING is used
    }

    pub fn transaction<T, E>(db: &Db, body: impl FnOnce(&Db) -> Result<T, E>) -> Result<T, E> {
        db.client.borrow_mut().batch_execute("BEGIN").expect("tx BEGIN");
        match body(db) {
            Ok(v) => { db.client.borrow_mut().batch_execute("COMMIT").expect("tx COMMIT"); Ok(v) }
            Err(e) => { let _ = db.client.borrow_mut().batch_execute("ROLLBACK"); Err(e) }
        }
    }

    /// BATCH-WRITE bind (v1 UNNEST): bind each column's WHOLE array as a native `<elem>[]` param — one
    /// `$n::T[]` per column (the baked `UNNEST($1::int[],$2::text[])` form). ONE statement for N records.
    pub fn query_batch_write<T>(db: &Db, sql: &str, _columns: &[&str], arrays: &[&Param], decode: impl Fn(&dyn Cell) -> T) -> Result<Vec<T>, String> {
        let params: Vec<Param> = arrays.iter().map(|a| clone_param(a)).collect();
        query(db, sql, &params, decode)
    }

    /// BATCHED-RELATION bind (v1): the child SQL is `= ANY(?::@@PG_ARRAY_CAST@@)` / composite
    /// `UNNEST(?::@@PG_ARRAY_CAST@@, …)` with `?` placeholders + the deferred cast marker (#46). Resolve
    /// the cast(s) to `int[]` (the bench's relation keys are all integer) and renumber `?`→`$N`, then bind
    /// the deduped key column array(s) as native `int[]`. Unified signature with the sqlite backend (which
    /// ignores `key_arrays` and uses `json`).
    pub fn relation_child<Child>(db: &Db, sql: &str, key_arrays: &[Param], _json: String, decode: impl Fn(&dyn Cell) -> Child) -> Result<Vec<Child>, String> {
        let resolved = renumber(&sql.replace("@@PG_ARRAY_CAST@@", "int[]"));
        query(db, &resolved, key_arrays, decode)
    }

    fn clone_param(p: &Param) -> Param {
        match p {
            Param::Int(v) => Param::Int(*v),
            Param::Real(v) => Param::Real(*v),
            Param::Text(v) => Param::Text(v.clone()),
            Param::ArrayInt(v) => Param::ArrayInt(v.clone()),
            Param::ArrayText(v) => Param::ArrayText(v.clone()),
        }
    }
    /// Rewrite positional `?` placeholders to pg `$1..$N` (left to right). Baked batch/main SQL already
    /// emits `$N`; only the relation child fragments carry `?`.
    fn renumber(sql: &str) -> String {
        let mut out = String::with_capacity(sql.len() + 8);
        let mut n = 0;
        for c in sql.chars() {
            if c == '?' { n += 1; out.push('$'); out.push_str(&n.to_string()); } else { out.push(c); }
        }
        out
    }
}
#[cfg(feature = "postgres")]
pub use pg_backend::*;

// ══ Dialect-independent relation dedup + group + align (E4/#119) ══════════════════════════════════════
/// Group the ONE batched child query's rows by their target key, then align to `item_keys` (one child
/// list per parent, in order). MOVE each parent's list out (`remove`) — parents are distinct, zero clones.
pub fn align_children<Child, Key>(item_keys: &[Key], distinct_children: Vec<Child>, child_key: impl Fn(&Child) -> Key) -> Vec<Vec<Child>>
where
    Key: Eq + std::hash::Hash + Clone,
{
    let mut groups: std::collections::HashMap<Key, Vec<Child>> = std::collections::HashMap::new();
    for ch in distinct_children {
        groups.entry(child_key(&ch)).or_default().push(ch);
    }
    item_keys.iter().map(|k| groups.remove(k).unwrap_or_default()).collect()
}
/// The DISTINCT keys of `item_keys`, first-seen order (the dedup a batched relation binds).
pub fn distinct_keys<Key: Eq + std::hash::Hash + Clone>(item_keys: &[Key]) -> Vec<Key> {
    let mut seen: std::collections::HashSet<Key> = std::collections::HashSet::with_capacity(item_keys.len());
    let mut distinct: Vec<Key> = Vec::with_capacity(item_keys.len());
    for k in item_keys {
        if seen.insert(k.clone()) { distinct.push(k.clone()); }
    }
    distinct
}

/// The generic BATCHED-RELATION exec (E4/#119, N+1-avoided): dedup the parent keys, run the ONE child
/// query (the backend [`relation_child`] binds the deduped keys — sqlite as one JSON param, pg as native
/// `int[]`), group by target key, and align per-parent to `item_keys`. Dialect-INDEPENDENT: the caller
/// supplies BOTH the sqlite JSON encoding AND the pg key-array(s) (the backend uses whichever it needs),
/// plus the key extraction — the whole relation runtime, ONE query, no N+1.
pub fn query_batched_relation<Child, Key>(
    db: &Db,
    sql: &str,
    item_keys: &[Key],
    encode_json: impl Fn(&[Key]) -> String,
    key_arrays: impl Fn(&[Key]) -> Vec<Param>,
    decode: impl Fn(&dyn Cell) -> Child,
    child_key: impl Fn(&Child) -> Key,
) -> Result<Vec<Vec<Child>>, String>
where
    Key: Eq + std::hash::Hash + Clone,
{
    let distinct = distinct_keys(item_keys);
    let json = encode_json(&distinct);
    let arrays = key_arrays(&distinct);
    let children = relation_child(db, sql, &arrays, json, decode)?;
    Ok(align_children(item_keys, children, child_key))
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
