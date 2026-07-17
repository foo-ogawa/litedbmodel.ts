//! E1/E2 (#116, epic #115) PROOF-OF-APPROACH — execute BAKED-SQL native ops against real SQLite.
//!
//! Each generated module (emitted by `generateCodegenArtifact(bundle, 'rust', …, {nativeSql:true})`
//! through bc 0.8.0's `rust-typed-native` endpoint) carries its op's per-dialect SQL as a NATIVE
//! STRING LITERAL on its concrete ports struct, e.g.
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
//! supplies the other half — the thin, op-agnostic seam (`seam.rs`) — and prints rows as canonical
//! JSON so the TS leg can assert byte-equality against the mode-2 oracle.
//!
//! Read and write go through the SAME lowering and the SAME seam (owner decision): the module bakes
//! the SQL either way; only the driver's result collection differs (`query` vs `execute`).

mod generated_byids;
mod generated_findunique;
mod seam;

use rusqlite::Connection;
use seam::{json_str, query, Param};

// ── per-op adapters ──────────────────────────────────────────────────────────────────────────
//
// The ONLY per-op glue: map the node's baked typed ports onto the ordered param list, and decode
// the projected columns into the module's own outType struct. Hand-written for the proof; uniform
// enough to be generated alongside the module.

struct FindUniqueSeam<'a> {
    conn: &'a Connection,
}

impl generated_findunique::HandlerNRFindUnique for FindUniqueSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_findunique::PortsNRFindUniqueN0,
        _bound: Option<String>,
    ) -> Option<generated_findunique::RawRowNRFindUniqueN0> {
        // Baked ports in placeholder order: ?1 = f_p0 (email head), ?2 = f_p1 (LIMIT literal).
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let decoded = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_findunique::T0 {
                id: r.get(0)?,
                email: r.get(1)?,
                name: r.get(2)?,
            })
        });
        Some(match decoded {
            Ok(val) => generated_findunique::RawRowNRFindUniqueN0 {
                is_error: false,
                err: String::new(),
                val,
            },
            Err(e) => generated_findunique::RawRowNRFindUniqueN0 {
                is_error: true,
                err: e.to_string(),
                ..Default::default()
            },
        })
    }
}

struct ByIdsSeam<'a> {
    conn: &'a Connection,
}

impl generated_byids::HandlerNRByIds for ByIdsSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_byids::PortsNRByIdsN0,
        _bound: Option<String>,
    ) -> Option<generated_byids::RawRowNRByIdsN0> {
        // The IN-list head is a NATIVE Vec<i64> port (bc#110). The baked SQL already carries the
        // `json_each(?)` expansion; the seam performs the single-JSON array BIND.
        let params = [Param::ArrayInt(ports.f_p0.clone())];
        let decoded = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_byids::T0 {
                id: r.get(0)?,
                email: r.get(1)?,
                name: r.get(2)?,
            })
        });
        Some(match decoded {
            Ok(val) => generated_byids::RawRowNRByIdsN0 {
                is_error: false,
                err: String::new(),
                val,
            },
            Err(e) => generated_byids::RawRowNRByIdsN0 {
                is_error: true,
                err: e.to_string(),
                ..Default::default()
            },
        })
    }
}

fn rows_json_fu(rows: &[generated_findunique::T0]) -> String {
    let items: Vec<String> = rows
        .iter()
        .map(|r| format!("{{\"id\":{},\"email\":{},\"name\":{}}}", r.id, json_str(&r.email), json_str(&r.name)))
        .collect();
    format!("[{}]", items.join(","))
}

fn rows_json_bi(rows: &[generated_byids::T0]) -> String {
    let items: Vec<String> = rows
        .iter()
        .map(|r| format!("{{\"id\":{},\"email\":{},\"name\":{}}}", r.id, json_str(&r.email), json_str(&r.name)))
        .collect();
    format!("[{}]", items.join(","))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let op = args.get(1).expect("usage: e1_native_proof <op> <db> <args...>");
    let db_path = args.get(2).expect("usage: e1_native_proof <op> <db> <args...>");
    let conn = Connection::open(db_path).expect("open db");

    match op.as_str() {
        // findUnique: <email>
        "findunique" => {
            let email = args.get(3).expect("findunique needs <email>").clone();
            let out = generated_findunique::run_native_raw_struct_FindUnique(
                &FindUniqueSeam { conn: &conn },
                generated_findunique::InNRFindUnique { email },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!("{}", rows_json_fu(&out));
        }
        // byIds: <comma-separated ids>  ("" = the empty IN-list)
        "byids" => {
            let raw = args.get(3).expect("byids needs <ids>");
            let ids: Vec<i64> = if raw.is_empty() {
                vec![]
            } else {
                raw.split(',').map(|s| s.parse().expect("id")).collect()
            };
            let out = generated_byids::run_native_raw_struct_ByIds(
                &ByIdsSeam { conn: &conn },
                generated_byids::InNRByIds { ids },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!("{}", rows_json_bi(&out));
        }
        other => panic!("unknown op '{other}'"),
    }
}
