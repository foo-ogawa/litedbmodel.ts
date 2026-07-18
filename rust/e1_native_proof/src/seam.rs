//! The exec seam — the ENTIRE native runtime a baked-SQL op needs.
//!
//! Owner's canonical model: "native runtime = 「SQL・params・SKIP引数の汎用クエリ実行関数」だけ".
//! This module is that generic query-exec function and nothing else. It is OP-AGNOSTIC: it knows
//! only a SQL string, an ordered param list, and (for a read) how to decode one row. It has no
//! knowledge of the op, the table, the projection, the behavior, or any IR — all of that lives in
//! the SQL the generated module baked as a native literal.
//!
//! Read and write share ONE flow (owner: "ReadもWriteもSQLを作ってBCで実行するという流れに違いは
//! ない。明確に分ける必要はないし、分けてはいけない"): the module bakes the SQL either way, and the
//! seam runs it. The ONLY difference is which driver call collects the result — `query` for a row
//! list (SELECT, or a write with RETURNING) vs `execute` for an affected-row count. That is a
//! driver-shape detail, not a separate path.

use rusqlite::Connection;

/// A bound param — the closed set the baked native ports lower to.
///
/// `Array` is the IN-list / array-bound head (bc#110 bakes it as a native `Vec<ElemT>` port). SQL
/// text for the IN-list is already baked (`id IN (SELECT value FROM json_each(?))` on sqlite/mysql,
/// `= ANY(?)` on pg), so the seam's only job is the driver BIND: sqlite/mysql take the array as ONE
/// single-JSON param, which is what `encode_json_array` produces. This is a binding concern, not a
/// SQL-text concern — the same reason the TS runtime encodes it at bind time (`evalSpec`'s
/// `__jsonArray` marker), so the two agree by construction.
pub enum Param {
    Int(i64),
    Real(f64),
    Text(String),
    ArrayInt(Vec<i64>),
    ArrayText(Vec<String>),
}

/// JSON-encode an array param for the single-JSON IN-list bind (sqlite/mysql). Byte-equal to the TS
/// runtime's `JSON.stringify(arr)` for the scalar element types the lowering admits.
fn encode_json_array(p: &Param) -> Option<String> {
    match p {
        Param::ArrayInt(v) => Some(format!(
            "[{}]",
            v.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        )),
        Param::ArrayText(v) => Some(format!(
            "[{}]",
            v.iter().map(|s| json_str(s)).collect::<Vec<_>>().join(",")
        )),
        _ => None,
    }
}

impl rusqlite::ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        // An array param binds as ONE single-JSON value (the baked `json_each(?)` expands it
        // server-side) — exactly the TS runtime's sqlite/mysql IN-list bind.
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

/// A process-wide count of DB queries the seam has issued — the definitive proof that a batched
/// relation runs ONE child query for all parents (not N+1). The relation proof reads it before/after
/// and asserts the delta is 2 (one parent read + one batched child), not 1+N.
pub static QUERY_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The generic READ exec: run `sql` with `params`, decode each row via `decode`.
pub fn query<T>(
    conn: &Connection,
    sql: &str,
    params: &[Param],
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    QUERY_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| decode(r))?;
    rows.collect()
}

/// ONE WHERE fragment handed to [`query_skip`]: the bare predicate SQL (baked literal, no connector),
/// its bound params, and the SKIP ARG — `present`. A required fragment passes `present: true`; a
/// skip-optional fragment passes `present: <optional head>.is_some()` (the bc#139 Option presence).
pub struct WhereFrag<'a> {
    pub sql: &'a str,
    pub present: bool,
    pub params: Vec<Param>,
}

/// The generic SKIP-aware READ exec (owner's model: "SQL・params・SKIP引数の汎用クエリ実行関数だけ").
/// It ASSEMBLES the query from baked literals: `head` + the PRESENT `frags` joined with ` WHERE `/
/// ` AND ` + `tail`, binding each present fragment's params in order followed by the tail params. This
/// is NATIVE string assembly over baked fragments — no IR walk, no JSON, no dispatch; the seam knows
/// only fragments + presence bits + params. The resulting SQL is byte-identical to the mode-2 runtime
/// (same fragment order, same connectors), so a skipped fragment drops in place exactly as mode-2
/// drops it. (sqlite/mysql keep `?`; a pg `?`→`$N` renumber would happen HERE, post-assembly.)
pub fn query_skip<T>(
    conn: &Connection,
    head: &str,
    frags: &[WhereFrag],
    tail: &str,
    tail_params: &[Param],
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    let mut sql = String::from(head);
    let mut params: Vec<&Param> = Vec::new();
    let mut first = true;
    for f in frags {
        if !f.present {
            continue;
        }
        sql.push_str(if first { " WHERE " } else { " AND " });
        sql.push_str(f.sql);
        first = false;
        for p in &f.params {
            params.push(p);
        }
    }
    sql.push_str(tail);
    for p in tail_params {
        params.push(p);
    }
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.into_iter()), |r| decode(r))?;
    rows.collect()
}

/// The generic BATCHED-RELATION exec (E4/#119 — the N+1-avoided native relation). Given the ONE
/// baked batched child SQL + the per-parent key(s), it: collects the DISTINCT parent keys, runs the
/// query ONCE (binding the deduped keys as the single JSON-array param the baked `json_each(?)` /
/// `= ANY(?)` form expects), groups the returned children by their target key, and returns the
/// per-parent child lists ALIGNED to `item_keys` (one list per parent, in order). This is the whole
/// relation runtime — one query, no N+1, no per-row `= ?`. The caller (per-op glue) supplies the
/// key extraction, the dialect JSON encoding, and the row decode; the dedup + group + align live HERE.
pub fn query_batched_relation<Child, Key>(
    conn: &Connection,
    sql: &str,
    item_keys: &[Key],
    encode_json: impl Fn(&[Key]) -> String,
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<Child>,
    child_key: impl Fn(&Child) -> Key,
) -> rusqlite::Result<Vec<Vec<Child>>>
where
    Key: Eq + std::hash::Hash + Clone,
    Child: Clone,
{
    // DISTINCT parent keys — the ONE query binds only these (the dedup the runtime relation does).
    let mut seen: std::collections::HashSet<Key> = std::collections::HashSet::new();
    let mut distinct: Vec<Key> = Vec::new();
    for k in item_keys {
        if seen.insert(k.clone()) {
            distinct.push(k.clone());
        }
    }
    let json = encode_json(&distinct);
    let children = query(conn, sql, &[Param::Text(json)], decode)?; // <-- ONE query for all parents
    // group children by their target key
    let mut groups: std::collections::HashMap<Key, Vec<Child>> = std::collections::HashMap::new();
    for ch in children {
        groups.entry(child_key(&ch)).or_default().push(ch);
    }
    // align a child list to EACH parent item, in order
    Ok(item_keys.iter().map(|k| groups.get(k).cloned().unwrap_or_default()).collect())
}

/// The generic BATCH-WRITE exec (E3/#118 — createMany / updateMany / upsertMany, ONE statement for N
/// records). Given the ONE baked json_each batch `sql` + the records as PARALLEL columns (`columns[j]`
/// names `cells[j]`), it ZIPS them into the `[{col:val,…},…]` JSON the baked `json_each(?)` /
/// `JSON_TABLE(?)` expands and runs the statement ONCE. `cells[j][i]` is the ALREADY-JSON-ENCODED
/// value of column j, row i — a string column is pre-quoted (`"foo"`), a numeric KEY column is bare
/// (`42`) so `json_extract(…) = <int column>` matches (type-aware encoding is the caller's job, since
/// only it knows each column's type). The seam owns the zip + the bind; the write twin of
/// `query_batched_relation`.
pub fn query_batch_write<Row>(
    conn: &Connection,
    sql: &str,
    columns: &[&str],
    cells: &[&[String]],
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<Row>,
) -> rusqlite::Result<Vec<Row>> {
    let n = if cells.is_empty() { 0 } else { cells[0].len() };
    let mut objs: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let fields: Vec<String> = columns.iter().enumerate().map(|(j, c)| format!("{}:{}", json_str(c), cells[j][i])).collect();
        objs.push(format!("{{{}}}", fields.join(",")));
    }
    let json = format!("[{}]", objs.join(","));
    // The baked batch SQL binds the SAME records-JSON to EVERY `?` (createMany: one; updateMany: one
    // per SET clause + the WHERE). None of these baked batch SQLs carry a `?` inside a string literal,
    // so counting `?` gives the bind count. Still ONE statement for N records either way.
    let n_params = sql.matches('?').count();
    let params: Vec<Param> = (0..n_params).map(|_| Param::Text(json.clone())).collect();
    query(conn, sql, &params, decode)
}

/// The single summary row a NON-RETURNING write hands back: `{changes, lastInsertRowid}` — exactly
/// the shape the mode-2 `executeStaticWrite` returns (and the shape the codegen lowering bakes as the
/// write node's outType when there is no RETURNING clause).
pub struct WriteSummary {
    pub changes: i64,
    pub last_insert_rowid: i64,
}

/// The generic WRITE exec: run `sql` with `params`, return the affected-row summary. SAME flow as
/// [`query`] — same baked SQL, same params, same seam. Read and write are ONE flow (owner): the
/// module bakes the SQL either way; the only difference is that a bare write collects an affected-row
/// summary while a SELECT / RETURNING write collects a row list (that is a driver-shape detail).
pub fn execute(conn: &Connection, sql: &str, params: &[Param]) -> rusqlite::Result<WriteSummary> {
    let changes = conn.execute(sql, rusqlite::params_from_iter(params.iter()))?;
    Ok(WriteSummary { changes: changes as i64, last_insert_rowid: conn.last_insert_rowid() })
}

/// The TRANSACTION ENVELOPE (E5/#120 — RETURNING-chained multi-statement writes). The generated chain
/// runner orchestrates the statements + the RETURNING→next wiring (baked); the BEGIN … COMMIT / ROLLBACK
/// envelope is the SEAM's concern (owner: the statements + wiring are the chain's, the transaction is the
/// runtime's). Run `body` between BEGIN and COMMIT on `conn`; on any `Err` from the chain (a statement
/// failed under its fail policy — a constraint violation, etc.) ROLLBACK and propagate the error, so a
/// partial chain leaves the DB UNCHANGED (atomicity). Generic over the chain's Ok/Err types — it knows
/// only "run, commit-or-rollback", never the op. BEGIN/COMMIT/ROLLBACK are `execute_batch` (no bound
/// params, and — unlike [`query`] — they do NOT touch `QUERY_COUNT`, which counts only data queries).
pub fn transaction<T, E>(conn: &Connection, body: impl FnOnce(&Connection) -> Result<T, E>) -> Result<T, E> {
    conn.execute_batch("BEGIN").expect("tx BEGIN");
    match body(conn) {
        Ok(v) => {
            conn.execute_batch("COMMIT").expect("tx COMMIT");
            Ok(v)
        }
        Err(e) => {
            // Best-effort ROLLBACK (mirrors the mode-2 executeTransaction catch); surface the chain error.
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
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
