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

/// The generic READ exec: run `sql` with `params`, decode each row via `decode`.
pub fn query<T>(
    conn: &Connection,
    sql: &str,
    params: &[Param],
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| decode(r))?;
    rows.collect()
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
