//! E1 (#116, epic #115) PROOF-OF-APPROACH — execute a BAKED-SQL native read against real SQLite.
//!
//! The generated module (`generated_findunique.rs`, emitted by `generateCodegenArtifact(bundle,
//! 'rust', …)` through bc 0.8.0's `rust-typed-native` endpoint) carries the read's per-dialect SQL as
//! a NATIVE STRING LITERAL on its concrete ports struct:
//!
//! ```ignore
//! let ports_n0 = PortsNRFindUniqueN0 {
//!     f_sql: "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?".to_string(),
//!     f_p0: in_.email.clone(),
//!     f_p1: 1i64,
//! };
//! ```
//!
//! So there is NO JSON catalog companion to read at runtime: the query IS the module. This binary
//! supplies the other half — the thin, op-agnostic `exec(sql, params)` seam that consumes the baked
//! ports and drives the driver — and prints the rows as canonical JSON so the TS leg can assert
//! byte-equality against the mode-2 oracle (`executeBundle`) over the SAME seeded DB file.

mod generated_findunique;

use generated_findunique::{
    run_native_raw_struct_FindUnique, HandlerNRFindUnique, InNRFindUnique, PortsNRFindUniqueN0,
    RawRowNRFindUniqueN0, T0,
};
use rusqlite::Connection;

// ── the exec seam ────────────────────────────────────────────────────────────────────────────
//
// OP-AGNOSTIC: `exec` knows only "a SQL string, an ordered param list, and how to decode one row".
// It has no knowledge of the op, the table, the projection, or the behavior — those all live in the
// SQL the generated module baked. This is the whole runtime surface the native read needs.

/// A bound scalar param — the closed set the baked ports lower to (bc's native port types).
pub enum Param {
    Int(i64),
    Real(f64),
    Text(String),
}

impl rusqlite::ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(match self {
            Param::Int(v) => rusqlite::types::ToSqlOutput::from(*v),
            Param::Real(v) => rusqlite::types::ToSqlOutput::from(*v),
            Param::Text(v) => rusqlite::types::ToSqlOutput::from(v.as_str()),
        })
    }
}

/// The thin, op-agnostic query-exec primitive: run `sql` with `params`, decode each row via `decode`.
fn exec<T>(
    conn: &Connection,
    sql: &str,
    params: &[Param],
    decode: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| decode(r))?;
    rows.collect()
}

// ── the per-op adapter: baked ports → params, driver rows → the module's concrete row struct ──
//
// This is the ONLY per-op glue: it maps the node's typed ports onto the ordered param list and
// decodes the projected columns into the module's own `T0` outType struct. Hand-written for the E1
// proof; it is uniform enough to be generated alongside the module (E2+).

struct SqliteSeam<'a> {
    conn: &'a Connection,
}

impl HandlerNRFindUnique for SqliteSeam<'_> {
    fn node_n0(
        &self,
        ports: &PortsNRFindUniqueN0,
        _bound: Option<String>,
    ) -> Option<RawRowNRFindUniqueN0> {
        // The baked ports, in placeholder order: `?1` = f_p0 (the email head), `?2` = f_p1 (LIMIT).
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        // `ports.f_sql` is the module's OWN baked SQL — nothing was read from a companion.
        let decoded = exec(self.conn, &ports.f_sql, &params, |r| {
            Ok(T0 {
                id: r.get(0)?,
                email: r.get(1)?,
                name: r.get(2)?,
            })
        });
        Some(match decoded {
            Ok(val) => RawRowNRFindUniqueN0 {
                is_error: false,
                err: String::new(),
                val,
            },
            Err(e) => RawRowNRFindUniqueN0 {
                is_error: true,
                err: e.to_string(),
                ..Default::default()
            },
        })
    }
}

// ── canonical JSON (hand-rolled — no external JSON crate, mirroring the runtime's native-only rule) ──

fn json_str(s: &str) -> String {
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

/// Serialize the rows in the projection's field order — matching TS `JSON.stringify` byte-for-byte.
fn rows_json(rows: &[T0]) -> String {
    let items: Vec<String> = rows
        .iter()
        .map(|r| {
            format!(
                "{{\"id\":{},\"email\":{},\"name\":{}}}",
                r.id,
                json_str(&r.email),
                json_str(&r.name)
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn main() {
    let db_path = std::env::args().nth(1).expect("usage: e1_native_proof <db> <email>");
    let email = std::env::args().nth(2).expect("usage: e1_native_proof <db> <email>");
    let conn = Connection::open(&db_path).expect("open db");
    let seam = SqliteSeam { conn: &conn };

    // Drive the GENERATED module: it builds its own ports (baked SQL + typed params) and calls the seam.
    let out = run_native_raw_struct_FindUnique(&seam, InNRFindUnique { email })
        .unwrap_or_else(|e| panic!("behavior failed: {e}"));

    println!("{}", rows_json(&out));
}
