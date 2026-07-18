//! E1/E2/E3 (#116, epic #115) PROOF-OF-APPROACH — execute BAKED-SQL native ops against real SQLite.
//!
//! Each generated module (emitted by `generateCodegenArtifact(bundle, 'rust', …, {nativeSql:true})`
//! through bc 0.8.5's `rust-typed-native` endpoint) carries its op's per-dialect SQL as a NATIVE
//! STRING LITERAL on its concrete ports struct. So there is NO JSON catalog companion to read at
//! runtime: the query IS the module. This binary supplies the other half — the thin, op-agnostic
//! seam (`seam.rs`) — and prints rows / write effects as canonical JSON so the TS leg can assert
//! byte-equality against the mode-2 oracle.
//!
//! Read and write go through the SAME lowering and the SAME seam (owner decision — read/write are one
//! flow): the module bakes the SQL either way; only the driver's result collection differs
//! (`query` for a row list — SELECT or RETURNING — vs `execute` for an affected-row summary).

mod generated_byids;
mod generated_createuser;
mod generated_deleteuser;
mod generated_findunique;
mod generated_recent;
mod generated_renameuser;
mod seam;

use rusqlite::Connection;
use seam::{json_str, query, Param};

// ── per-op adapters ──────────────────────────────────────────────────────────────────────────
//
// The ONLY per-op glue: map the node's baked typed ports onto the ordered param list, and decode the
// projected columns into the module's own outType struct. Hand-written for the proof; uniform enough
// to be generated alongside the module.

struct FindUniqueSeam<'a> {
    conn: &'a Connection,
}
impl generated_findunique::HandlerNRFindUnique for FindUniqueSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_findunique::PortsNRFindUniqueN0,
        _bound: Option<String>,
    ) -> Option<generated_findunique::RawRowNRFindUniqueN0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_findunique::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(row_or_err_fu(val))
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
        // The IN-list head is a NATIVE Vec<i64> port (bc#110); the seam performs the single-JSON bind.
        let params = [Param::ArrayInt(ports.f_p0.clone())];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_byids::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(row_or_err_bi(val))
    }
}

struct RecentSeam<'a> {
    conn: &'a Connection,
}
impl generated_recent::HandlerNRRecent for RecentSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_recent::PortsNRRecentN0,
        _bound: Option<String>,
    ) -> Option<generated_recent::RawRowNRRecentN0> {
        // #122: the baked LIMIT param is `in_.limit.unwrap_or(20)` — already an i64 by the time the
        // seam sees it (the default resolved natively in the module). The seam binds it plainly.
        let params = [Param::Int(ports.f_p0)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_recent::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_recent::RawRowNRRecentN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_recent::RawRowNRRecentN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct CreateUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_createuser::HandlerNRCreateUser for CreateUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_createuser::PortsNRCreateUserN0,
        _bound: Option<String>,
    ) -> Option<generated_createuser::RawRowNRCreateUserN0> {
        // A RETURNING write is a row-returning op — `query`, same as a read. Params in placeholder
        // order: values.email, values.name (the write's SoT ports, not the SQL text).
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_createuser::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_createuser::RawRowNRCreateUserN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_createuser::RawRowNRCreateUserN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct RenameUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_renameuser::HandlerNRRenameUser for RenameUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_renameuser::PortsNRRenameUserN0,
        _bound: Option<String>,
    ) -> Option<generated_renameuser::RawRowNRRenameUserN0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_renameuser::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_renameuser::RawRowNRRenameUserN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_renameuser::RawRowNRRenameUserN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct DeleteUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_deleteuser::HandlerNRDeleteUser for DeleteUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_deleteuser::PortsNRDeleteUserN0,
        _bound: Option<String>,
    ) -> Option<generated_deleteuser::RawRowNRDeleteUserN0> {
        // A NON-RETURNING write hands back the summary row `[{changes, lastInsertRowid}]` — SAME baked
        // SQL, SAME seam, only the collection differs (`execute`, not `query`).
        let params = [Param::Int(ports.f_p0)];
        match seam::execute(self.conn, &ports.f_sql, &params) {
            Ok(s) => Some(generated_deleteuser::RawRowNRDeleteUserN0 {
                is_error: false,
                err: String::new(),
                val: vec![generated_deleteuser::T0 { changes: s.changes, lastInsertRowid: s.last_insert_rowid }],
            }),
            Err(e) => Some(generated_deleteuser::RawRowNRDeleteUserN0 { is_error: true, err: e.to_string(), ..Default::default() }),
        }
    }
}

fn row_or_err_fu(v: rusqlite::Result<Vec<generated_findunique::T0>>) -> generated_findunique::RawRowNRFindUniqueN0 {
    match v {
        Ok(val) => generated_findunique::RawRowNRFindUniqueN0 { is_error: false, err: String::new(), val },
        Err(e) => generated_findunique::RawRowNRFindUniqueN0 { is_error: true, err: e.to_string(), ..Default::default() },
    }
}
fn row_or_err_bi(v: rusqlite::Result<Vec<generated_byids::T0>>) -> generated_byids::RawRowNRByIdsN0 {
    match v {
        Ok(val) => generated_byids::RawRowNRByIdsN0 { is_error: false, err: String::new(), val },
        Err(e) => generated_byids::RawRowNRByIdsN0 { is_error: true, err: e.to_string(), ..Default::default() },
    }
}

fn user_rows_json(items: &[(i64, String, String)]) -> String {
    let s: Vec<String> = items
        .iter()
        .map(|(id, email, name)| format!("{{\"id\":{},\"email\":{},\"name\":{}}}", id, json_str(email), json_str(name)))
        .collect();
    format!("[{}]", s.join(","))
}

/// The resulting table state, for the write DB-state assertion — a raw generic read via the seam.
fn table_state(conn: &Connection) -> String {
    let rows = query(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
    })
    .expect("state read");
    user_rows_json(&rows)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let op = args.get(1).expect("usage: e1_native_proof <op> <db> <args...>");
    let db_path = args.get(2).expect("usage: e1_native_proof <op> <db> <args...>");
    let conn = Connection::open(db_path).expect("open db");

    match op.as_str() {
        "findunique" => {
            let email = args.get(3).expect("findunique needs <email>").clone();
            let out = generated_findunique::run_native_raw_struct_FindUnique(&FindUniqueSeam { conn: &conn }, generated_findunique::InNRFindUnique { email })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "byids" => {
            let raw = args.get(3).expect("byids needs <ids>");
            let ids: Vec<i64> = if raw.is_empty() { vec![] } else { raw.split(',').map(|s| s.parse().expect("id")).collect() };
            let out = generated_byids::run_native_raw_struct_ByIds(&ByIdsSeam { conn: &conn }, generated_byids::InNRByIds { ids })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        // recent: <limit>  ("" = absent → the baked .unwrap_or(20) default takes effect)
        "recent" => {
            let raw = args.get(3).expect("recent needs <limit or ''>");
            let limit: Option<i64> = if raw.is_empty() { None } else { Some(raw.parse().expect("limit")) };
            let out = generated_recent::run_native_raw_struct_Recent(&RecentSeam { conn: &conn }, generated_recent::InNRRecent { limit })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        // A write MUTATES `db_path` (the harness passes a fresh copy). It prints {result, state} so
        // the leg asserts BOTH the returned rows/summary AND the resulting DB state vs the oracle.
        "createuser" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let out = generated_createuser::run_native_raw_struct_CreateUser(&CreateUserSeam { conn: &conn }, generated_createuser::InNRCreateUser { email, name })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        "renameuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let name = args.get(4).expect("name").clone();
            let out = generated_renameuser::run_native_raw_struct_RenameUser(&RenameUserSeam { conn: &conn }, generated_renameuser::InNRRenameUser { id, name })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        "deleteuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let out = generated_deleteuser::run_native_raw_struct_DeleteUser(&DeleteUserSeam { conn: &conn }, generated_deleteuser::InNRDeleteUser { id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            // The non-returning summary row: [{changes, lastInsertRowid}].
            let s: Vec<String> = out.iter().map(|r| format!("{{\"changes\":{},\"lastInsertRowid\":{}}}", r.changes, r.lastInsertRowid)).collect();
            println!("{{\"result\":[{}],\"state\":{}}}", s.join(","), table_state(&conn));
        }
        other => panic!("unknown op '{other}'"),
    }
}
