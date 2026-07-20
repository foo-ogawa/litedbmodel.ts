//! Native-codegen REFERENCE CELL (epic #123 / #124) — run the litedbmodel-generated native modules
//! end to end and print canonical JSON so the TS leg asserts byte-equality vs the mode-2 oracle.
//!
//! This binary is a litedbmodel-CONSUMER: for each op it builds the typed input, obtains the
//! runtime-backed handler from the GENERATED companion (`companion_<op>::handler` / `::run`), and calls
//! the GENERATED runner (`generated_<op>::run_native_raw_struct_<Comp>`). It supplies NO `node_*` of its
//! own — those come from litedbmodel (the generated companion + litedbmodel_runtime). Every DB access
//! goes through litedbmodel_runtime's `Driver`; there is NO `rusqlite` here (the old hand-written
//! `seam.rs` is retired) and NO per-op handler glue (the old per-op `*Seam` structs are retired).

// The bc-generated native modules (runtime-free) + the litedbmodel-generated companions (the
// boundary-injected node_* handlers + wire adapter). Paired 1:1; a companion refers to its module as
// `super::generated_<op>`.
mod companion_byids;
mod companion_bymaybe;
mod companion_createmany;
mod companion_createuser;
mod companion_deleteuser;
mod companion_feed;
mod companion_findunique;
mod companion_recent;
mod companion_relbatch;
mod companion_relsingle;
mod companion_renameuser;
mod companion_tenantfeed;
mod companion_txdelete;
mod companion_txnestedcreate;
mod companion_txnestedupdate;
mod companion_txnestedupsert;
mod companion_txrollback;
mod companion_updatemany;
mod companion_upsert;
mod companion_upsertmany;
mod generated_byids;
mod generated_bymaybe;
mod generated_createmany;
mod generated_createuser;
mod generated_deleteuser;
mod generated_feed;
mod generated_findunique;
mod generated_recent;
mod generated_relbatch;
mod generated_relsingle;
mod generated_renameuser;
mod generated_tenantfeed;
mod generated_txdelete;
mod generated_txnestedcreate;
mod generated_txnestedupdate;
mod generated_txnestedupsert;
mod generated_txrollback;
mod generated_updatemany;
mod generated_upsert;
mod generated_upsertmany;

use litedbmodel_runtime::driver::PreparedStatement;
use litedbmodel_runtime::exec_context::TxConnection;
use litedbmodel_runtime::{
    encode_value, execute_bundle, execute_transaction_bundle, read_bundle_pooled, stitch_relation,
    Driver, Node, SqlFailure, SqliteDriver, Value,
};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};
use std::sync::atomic::{AtomicUsize, Ordering};

// ── query counter (consumer-side observability) ────────────────────────────────────────────────
//
// The N+1 proof: a batched relation must run 1 parent + 1 batched child = 2 queries (not 1+N), and a
// batch write 1 statement + 1 state-read = 2 (not N+1). Counting lives HERE (the litedbmodel-consumer),
// NOT in the runtime/companion: a `CountingDriver` decorator over the runtime `SqliteDriver` increments
// on each `prepare` (one per statement the runtime issues). The runtime + companion stay unchanged;
// this is a consumer-side measurement at the Driver seam.
static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

struct CountingDriver {
    inner: Box<dyn Driver>,
}
impl Driver for CountingDriver {
    fn dialect(&self) -> &'static str {
        self.inner.dialect()
    }
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        self.inner.prepare(sql)
    }
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.inner.begin_tx()
    }
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.inner.acquire_tx()
    }
}

// Open the concrete runtime Driver the consumer runs against — the ONE place the reference cell picks
// a DB. `pg:<libpq-conn>` / `mysql:<url>` (the `livedb` feature) route to the live PostgresDriver /
// MysqlDriver; anything else is a sqlite file path. The SAME generated module + companion + runtime
// path runs on whichever Driver this returns — the dialect difference is the baked SQL + the Driver,
// never the executor.
fn open_driver(spec: &str) -> Box<dyn Driver> {
    #[cfg(feature = "livedb")]
    {
        if let Some(conn) = spec.strip_prefix("pg:") {
            return Box::new(PostgresDriver::connect(conn).expect("connect postgres"));
        }
        if let Some(url) = spec.strip_prefix("mysql:") {
            return Box::new(MysqlDriver::connect(url).expect("connect mysql"));
        }
    }
    Box::new(SqliteDriver::open(spec).expect("open sqlite db"))
}

// ── canonical JSON out (byte-equal to the mode-2 oracle shapes) ─────────────────────────────────

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

fn user_rows_json(items: &[(i64, String, String)]) -> String {
    let s: Vec<String> = items
        .iter()
        .map(|(id, email, name)| {
            format!(
                "{{\"id\":{},\"email\":{},\"name\":{}}}",
                id,
                json_str(email),
                json_str(name)
            )
        })
        .collect();
    format!("[{}]", s.join(","))
}

// ── raw state reads through the Driver (for the write/tx {result, state} assertions) ─────────────

fn obj_get<'a>(row: &'a Value, key: &str) -> Option<&'a Value> {
    match row {
        Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}
fn cell_i64(row: &Value, key: &str) -> i64 {
    match obj_get(row, key) {
        Some(Value::Int(i)) => *i,
        Some(Value::Float(f)) => *f as i64,
        _ => 0,
    }
}
fn cell_str(row: &Value, key: &str) -> String {
    match obj_get(row, key) {
        Some(Value::Str(s)) => s.clone(),
        _ => String::new(),
    }
}
fn state_rows(driver: &dyn Driver, sql: &str) -> Vec<Value> {
    driver.prepare(sql).all(&[]).expect("state read")
}

fn table_state(driver: &dyn Driver) -> String {
    let rows = state_rows(
        driver,
        "SELECT id, email, name FROM benchmark_users ORDER BY id",
    );
    let items: Vec<(i64, String, String)> = rows
        .iter()
        .map(|r| (cell_i64(r, "id"), cell_str(r, "email"), cell_str(r, "name")))
        .collect();
    user_rows_json(&items)
}

fn tx_state(driver: &dyn Driver) -> String {
    let users = state_rows(
        driver,
        "SELECT id, email, name FROM benchmark_users ORDER BY id",
    );
    let posts = state_rows(
        driver,
        "SELECT id, title, author_id FROM benchmark_posts ORDER BY id",
    );
    let u: Vec<String> = users
        .iter()
        .map(|r| {
            format!(
                "{{\"id\":{},\"email\":{},\"name\":{}}}",
                cell_i64(r, "id"),
                json_str(&cell_str(r, "email")),
                json_str(&cell_str(r, "name"))
            )
        })
        .collect();
    let p: Vec<String> = posts
        .iter()
        .map(|r| {
            format!(
                "{{\"id\":{},\"title\":{},\"author_id\":{}}}",
                cell_i64(r, "id"),
                json_str(&cell_str(r, "title")),
                cell_i64(r, "author_id")
            )
        })
        .collect();
    format!(
        "{{\"users\":[{}],\"posts\":[{}]}}",
        u.join(","),
        p.join(",")
    )
}

fn print_queries() {
    eprintln!("queries={}", QUERY_COUNT.load(Ordering::SeqCst));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let op = args
        .get(1)
        .expect("usage: e1_native_proof <op> <db> <args...>");
    let db_path = args
        .get(2)
        .expect("usage: e1_native_proof <op> <db> <args...>");
    let driver = CountingDriver {
        inner: open_driver(db_path),
    };
    let d: &dyn Driver = &driver;

    match op.as_str() {
        // The mode-2 INTERPRETER oracle (epic #123/#124 commit 3): run the SAME op via the runtime's
        // mode-2 entry (`execute_bundle` for read/write, `execute_transaction_bundle` for tx) on the
        // SAME live connection, and print the SAME canonical shape the native leg prints. The livedb
        // harness runs native (codegen path) AND this (interpreter path) against the same DB and asserts
        // byte-equal — a REAL conformance check (two DISTINCT code paths, one real DB), NOT a circular
        // self-comparison. Usage: `mode2 <spec> <read|write|tx> <bundle.json> <input.json>`.
        "mode2" => {
            let kind = args.get(3).expect("mode2 <kind>");
            let bundle = Node::parse(
                &std::fs::read_to_string(args.get(4).expect("mode2 <bundle.json>"))
                    .expect("read bundle"),
            )
            .expect("parse bundle json");
            let input = Node::parse(
                &std::fs::read_to_string(args.get(5).expect("mode2 <input.json>"))
                    .expect("read input"),
            )
            .expect("parse input json");
            match kind.as_str() {
                "read" => {
                    let v = execute_bundle(&bundle, &input, d)
                        .unwrap_or_else(|e| panic!("mode2 read: {}", e.message()));
                    println!("{}", encode_value(&v).to_json_string());
                }
                "readrel" => {
                    // The mode-2 INTERPRETER relation read: the SAME hydrated `read_bundle_pooled` path
                    // the livedb_runner uses at 132/132 (primary via `execute_bundle_pooled` + the
                    // op-independent stitch SSoT `run_relation_op`/`distribute_to_parent`). The stitch is
                    // SHARED with the native companion by design (like the Driver) — NOT re-implemented
                    // here; the DISTINCT comparison is the parent+child de-box (interpreter vs codegen).
                    // Emits the hydrated `[{...parent, <rel>:[children]}]` envelope; the harness normalizes
                    // it and the native `{rows,posts}` to the common {parents, per-parent children} canon.
                    let with_names: Vec<String> = bundle
                        .get("relations")
                        .and_then(|r| r.as_object())
                        .map(|pairs| pairs.iter().map(|(k, _)| k.clone()).collect())
                        .unwrap_or_default();
                    let empty: std::collections::HashMap<String, &(dyn Driver + Sync)> =
                        std::collections::HashMap::new();
                    #[cfg(feature = "livedb")]
                    {
                        let v = if let Some(conn) = db_path.strip_prefix("pg:") {
                            let drv = PostgresDriver::connect(conn).expect("connect postgres");
                            read_bundle_pooled(&bundle, &input, &drv, &with_names, &empty)
                        } else if let Some(url) = db_path.strip_prefix("mysql:") {
                            let drv = MysqlDriver::connect(url).expect("connect mysql");
                            read_bundle_pooled(&bundle, &input, &drv, &with_names, &empty)
                        } else {
                            panic!("mode2 readrel requires a pg:/mysql: spec");
                        }
                        .unwrap_or_else(|e| panic!("mode2 readrel: {}", e.message()));
                        println!("{}", encode_value(&v).to_json_string());
                    }
                    #[cfg(not(feature = "livedb"))]
                    {
                        let _ = (&with_names, &empty);
                        panic!("mode2 readrel requires --features livedb");
                    }
                }
                "write" => {
                    let v = execute_bundle(&bundle, &input, d)
                        .unwrap_or_else(|e| panic!("mode2 write: {}", e.message()));
                    println!(
                        "{{\"result\":{},\"state\":{}}}",
                        encode_value(&v).to_json_string(),
                        table_state(d)
                    );
                }
                "tx" => {
                    // Ok ⇒ committed; Err (a statement failed under its policy — e.g. the rollback control's
                    // UNIQUE clash) ⇒ rolled back. SAME commit/rollback semantics as the native envelope.
                    let committed = execute_transaction_bundle(&bundle, &input, d).is_ok();
                    println!(
                        "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                        committed,
                        tx_state(d)
                    );
                }
                other => panic!("unknown mode2 kind '{other}'"),
            }
        }
        "findunique" => {
            let email = args.get(3).expect("findunique needs <email>").clone();
            let out = generated_findunique::run_native_raw_struct_FindUnique(
                &companion_findunique::handler(d),
                generated_findunique::InNRFindUnique { email },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "byids" => {
            let raw = args.get(3).expect("byids needs <ids>");
            let ids: Vec<i64> = if raw.is_empty() {
                vec![]
            } else {
                raw.split(',').map(|s| s.parse().expect("id")).collect()
            };
            let out = generated_byids::run_native_raw_struct_ByIds(
                &companion_byids::handler(d),
                generated_byids::InNRByIds { ids },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "recent" => {
            let raw = args.get(3).expect("recent needs <limit or ''>");
            let limit: Option<i64> = if raw.is_empty() {
                None
            } else {
                Some(raw.parse().expect("limit"))
            };
            let out = generated_recent::run_native_raw_struct_Recent(
                &companion_recent::handler(d),
                generated_recent::InNRRecent { limit },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "bymaybe" => {
            let author_id: i64 = args
                .get(3)
                .expect("author_id")
                .parse()
                .expect("author_id int");
            let raw = args.get(4).expect("published or ''");
            let published: Option<i64> = if raw.is_empty() {
                None
            } else {
                Some(raw.parse().expect("published"))
            };
            let out = generated_bymaybe::run_native_raw_struct_ByAuthorMaybePublished(
                &companion_bymaybe::handler(d),
                generated_bymaybe::InNRByAuthorMaybePublished {
                    author_id,
                    published,
                },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<String> = out
                .iter()
                .map(|r| {
                    format!(
                        "{{\"id\":{},\"title\":{},\"author_id\":{},\"published\":{}}}",
                        r.id,
                        json_str(&r.title),
                        r.author_id,
                        r.published
                    )
                })
                .collect();
            println!("[{}]", items.join(","));
        }
        "feed" => {
            let author_id: i64 = args
                .get(3)
                .expect("author_id")
                .parse()
                .expect("author_id int");
            let out = generated_feed::run_native_raw_struct_PostsWithAuthor(
                &companion_feed::handler(d),
                generated_feed::InNRPostsWithAuthor { author_id },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let authors: Vec<String> = out
                .authors
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner
                        .iter()
                        .map(|a| format!("{{\"id\":{},\"name\":{}}}", a.id, json_str(&a.name)))
                        .collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            let posts: Vec<String> = out
                .posts
                .iter()
                .map(|p| {
                    format!(
                        "{{\"id\":{},\"title\":{},\"author_id\":{}}}",
                        p.id,
                        json_str(&p.title),
                        p.author_id
                    )
                })
                .collect();
            println!(
                "{{\"authors\":[{}],\"posts\":[{}]}}",
                authors.join(","),
                posts.join(",")
            );
        }
        "tenantfeed" => {
            let tenant_id: i64 = args
                .get(3)
                .expect("tenant_id")
                .parse()
                .expect("tenant_id int");
            let out = generated_tenantfeed::run_native_raw_struct_UsersWithPosts(
                &companion_tenantfeed::handler(d),
                generated_tenantfeed::InNRUsersWithPosts { tenant_id },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let posts: Vec<String> = out
                .posts
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner
                        .iter()
                        .map(|p| {
                            format!(
                                "{{\"tenant_id\":{},\"post_id\":{},\"title\":{}}}",
                                p.tenant_id,
                                p.post_id,
                                json_str(&p.title)
                            )
                        })
                        .collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            let users: Vec<String> = out
                .users
                .iter()
                .map(|u| {
                    format!(
                        "{{\"tenant_id\":{},\"user_id\":{},\"name\":{}}}",
                        u.tenant_id,
                        u.user_id,
                        json_str(&u.name)
                    )
                })
                .collect();
            println!(
                "{{\"posts\":[{}],\"users\":[{}]}}",
                posts.join(","),
                users.join(",")
            );
        }
        // relbatch/relsingle (#131): the native module carries the PRIMARY read ONLY. The relation is v1
        // lazy-batch loading — a RUNTIME concern: after the native de-boxed parent read, the runtime loader
        // `stitch_relation` resolves the DECLARED relation op (companion `relation_ops_json`, litedbmodel
        // metadata) over the single query primitive (dedupe → ONE batched child query → group → distribute).
        // 1 parent + 1 batched child = 2 queries (no N+1). The relation is NOT an executor primitive.
        "relbatch" => {
            let tenant_id: i64 = args
                .get(3)
                .expect("tenant_id")
                .parse()
                .expect("tenant_id int");
            let out = generated_relbatch::run_native_raw_struct_ByTenant(
                &companion_relbatch::handler(d),
                generated_relbatch::InNRByTenant { tenant_id },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let rows: Vec<String> = out
                .iter()
                .map(|u| {
                    format!(
                        "{{\"tenant_id\":{},\"user_id\":{},\"name\":{}}}",
                        u.tenant_id,
                        u.user_id,
                        json_str(&u.name)
                    )
                })
                .collect();
            let parents: Vec<Value> = out
                .iter()
                .map(|u| {
                    Value::Obj(vec![
                        ("tenant_id".to_string(), Value::Int(u.tenant_id)),
                        ("user_id".to_string(), Value::Int(u.user_id)),
                        ("name".to_string(), Value::Str(u.name.clone())),
                    ])
                })
                .collect();
            let ops = Node::parse(companion_relbatch::relation_ops_json()).expect("parse relation ops");
            let op = ops.get("posts").expect("relation op 'posts'");
            let stitched = stitch_relation(op, parents, d)
                .unwrap_or_else(|e| panic!("stitch relation: {}", e.message()));
            let posts: Vec<String> = stitched
                .iter()
                .map(|p| {
                    let children = match obj_get(p, "posts") {
                        Some(Value::Arr(c)) => c.as_slice(),
                        _ => &[],
                    };
                    let items: Vec<String> = children
                        .iter()
                        .map(|c| {
                            format!(
                                "{{\"tenant_id\":{},\"post_id\":{},\"user_id\":{},\"title\":{}}}",
                                cell_i64(c, "tenant_id"),
                                cell_i64(c, "post_id"),
                                cell_i64(c, "user_id"),
                                json_str(&cell_str(c, "title"))
                            )
                        })
                        .collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            println!(
                "{{\"rows\":[{}],\"posts\":[{}]}}",
                rows.join(","),
                posts.join(",")
            );
            print_queries();
        }
        "relsingle" => {
            let author_id: i64 = args
                .get(3)
                .expect("author_id")
                .parse()
                .expect("author_id int");
            let out = generated_relsingle::run_native_raw_struct_ByAuthor(
                &companion_relsingle::handler(d),
                generated_relsingle::InNRByAuthor { author_id },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let rows: Vec<String> = out
                .iter()
                .map(|p| {
                    format!(
                        "{{\"id\":{},\"title\":{},\"author_id\":{}}}",
                        p.id,
                        json_str(&p.title),
                        p.author_id
                    )
                })
                .collect();
            let parents: Vec<Value> = out
                .iter()
                .map(|p| {
                    Value::Obj(vec![
                        ("id".to_string(), Value::Int(p.id)),
                        ("title".to_string(), Value::Str(p.title.clone())),
                        ("author_id".to_string(), Value::Int(p.author_id)),
                    ])
                })
                .collect();
            let ops =
                Node::parse(companion_relsingle::relation_ops_json()).expect("parse relation ops");
            let op = ops.get("comments").expect("relation op 'comments'");
            let stitched = stitch_relation(op, parents, d)
                .unwrap_or_else(|e| panic!("stitch relation: {}", e.message()));
            let comments: Vec<String> = stitched
                .iter()
                .map(|p| {
                    let children = match obj_get(p, "comments") {
                        Some(Value::Arr(c)) => c.as_slice(),
                        _ => &[],
                    };
                    let items: Vec<String> = children
                        .iter()
                        .map(|c| {
                            format!(
                                "{{\"id\":{},\"body\":{},\"post_id\":{}}}",
                                cell_i64(c, "id"),
                                json_str(&cell_str(c, "body")),
                                cell_i64(c, "post_id")
                            )
                        })
                        .collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            println!(
                "{{\"rows\":[{}],\"comments\":[{}]}}",
                rows.join(","),
                comments.join(",")
            );
            print_queries();
        }
        "createuser" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let out = generated_createuser::run_native_raw_struct_CreateUser(
                &companion_createuser::handler(d),
                generated_createuser::InNRCreateUser { email, name },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
        }
        "createmany" => {
            let emails: Vec<String> = args
                .get(3)
                .expect("emails")
                .split(',')
                .map(|s| s.to_string())
                .collect();
            let names: Vec<String> = args
                .get(4)
                .expect("names")
                .split(',')
                .map(|s| s.to_string())
                .collect();
            let out = generated_createmany::run_native_raw_struct_CreateMany(
                &companion_createmany::handler(d),
                generated_createmany::InNRCreateMany { emails, names },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
            print_queries();
        }
        "upsertmany" => {
            let emails: Vec<String> = args
                .get(3)
                .expect("emails")
                .split(',')
                .map(|s| s.to_string())
                .collect();
            let names: Vec<String> = args
                .get(4)
                .expect("names")
                .split(',')
                .map(|s| s.to_string())
                .collect();
            let out = generated_upsertmany::run_native_raw_struct_UpsertMany(
                &companion_upsertmany::handler(d),
                generated_upsertmany::InNRUpsertMany { emails, names },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
            print_queries();
        }
        "updatemany" => {
            let ids: Vec<i64> = args
                .get(3)
                .expect("ids")
                .split(',')
                .map(|s| s.parse().expect("id"))
                .collect();
            let names: Vec<String> = args
                .get(4)
                .expect("names")
                .split(',')
                .map(|s| s.to_string())
                .collect();
            let out = generated_updatemany::run_native_raw_struct_UpdateMany(
                &companion_updatemany::handler(d),
                generated_updatemany::InNRUpdateMany { ids, names },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
        }
        "upsert" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let out = generated_upsert::run_native_raw_struct_UpsertUser(
                &companion_upsert::handler(d),
                generated_upsert::InNRUpsertUser { email, name },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
        }
        "renameuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let name = args.get(4).expect("name").clone();
            let out = generated_renameuser::run_native_raw_struct_RenameUser(
                &companion_renameuser::handler(d),
                generated_renameuser::InNRRenameUser { id, name },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!(
                "{{\"result\":{},\"state\":{}}}",
                user_rows_json(&items),
                table_state(d)
            );
        }
        "deleteuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let out = generated_deleteuser::run_native_raw_struct_DeleteUser(
                &companion_deleteuser::handler(d),
                generated_deleteuser::InNRDeleteUser { id },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let s: Vec<String> = out
                .iter()
                .map(|r| {
                    format!(
                        "{{\"changes\":{},\"lastInsertRowid\":{}}}",
                        r.changes, r.lastInsertRowid
                    )
                })
                .collect();
            println!(
                "{{\"result\":[{}],\"state\":{}}}",
                s.join(","),
                table_state(d)
            );
        }
        "txdelete" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let committed =
                companion_txdelete::run(d, generated_txdelete::InNRTxDelete { email, name })
                    .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        "txnestedcreate" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let committed = companion_txnestedcreate::run(
                d,
                generated_txnestedcreate::InNRTxNestedCreate { email, name, title },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        "txnestedupdate" => {
            let user_id: i64 = args.get(3).expect("user_id").parse().expect("user_id int");
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let committed = companion_txnestedupdate::run(
                d,
                generated_txnestedupdate::InNRTxNestedUpdate {
                    name,
                    user_id,
                    title,
                },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        "txnestedupsert" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let committed = companion_txnestedupsert::run(
                d,
                generated_txnestedupsert::InNRTxNestedUpsert { email, name, title },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        "txrollback" => {
            let email = args.get(3).expect("email").clone();
            let dup_email = args.get(4).expect("dup_email").clone();
            let name = args.get(5).expect("name").clone();
            let committed = companion_txrollback::run(
                d,
                generated_txrollback::InNRTxRollback {
                    email,
                    name,
                    dup_email,
                },
            )
            .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        other => panic!("unknown op '{other}'"),
    }
}
