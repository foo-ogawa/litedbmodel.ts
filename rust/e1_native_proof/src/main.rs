//! Native-codegen REFERENCE CELL (epic #123 / #124) — run the litedbmodel-generated native modules
//! end to end and print canonical JSON so the TS leg asserts byte-equality vs the mode-2 oracle.
//!
//! This binary is a litedbmodel-CONSUMER: for each op it builds the typed input, obtains the
//! runtime-backed adapter co-located in the GENERATED module, and calls the public
//! `generated_<op>::run`. It supplies NO `node_*` of its own — those come from litedbmodel codegen and
//! litedbmodel_runtime. Every DB access
//! goes through litedbmodel_runtime's `Driver`; there is NO `rusqlite` here (the old hand-written
//! `seam.rs` is retired) and NO per-op handler glue (the old per-op `*Seam` structs are retired).

// The bc-generated native modules (runtime-free) + the litedbmodel-generated adapters (the
// boundary-injected node_* handlers + wire adapter). Paired 1:1; a adapter refers to its module as
// `super::generated_<op>`.
mod generated_byids;
mod generated_bymaybe;
mod generated_capped;
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
use litedbmodel_runtime::{Driver, SqlFailure, SqliteDriver, Value};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};
use std::sync::atomic::{AtomicUsize, Ordering};

// ── query counter (consumer-side observability) ────────────────────────────────────────────────
//
// The N+1 proof: a batched relation must run 1 parent + 1 batched child = 2 queries (not 1+N), and a
// batch write 1 statement + 1 state-read = 2 (not N+1). Counting lives HERE (the litedbmodel-consumer),
// NOT in the runtime/adapter: a `CountingDriver` decorator over the runtime `SqliteDriver` increments
// on each `prepare` (one per statement the runtime issues). The runtime + adapter stay unchanged;
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
// MysqlDriver; anything else is a sqlite file path. The SAME generated module + adapter + runtime
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

/// The tx dialect for the isolation prelude, derived from the connection spec (#136). Inert while the
/// e1 tx cells pass the default `TransactionOptions` (isolation `None` ⇒ empty prelude), but kept
/// correct per connection so a future isolation option renders the right SET.
fn tx_dialect(db_path: &str) -> litedbmodel_runtime::TxDialect {
    if db_path.starts_with("pg:") {
        litedbmodel_runtime::TxDialect::Postgres
    } else if db_path.starts_with("mysql:") {
        litedbmodel_runtime::TxDialect::Mysql
    } else {
        litedbmodel_runtime::TxDialect::Sqlite
    }
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
        "findunique" => {
            let email = args.get(3).expect("findunique needs <email>").clone();
            let out = generated_findunique::run(d, generated_findunique::InNRFindUnique { email })
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
            let out = generated_byids::run(d, generated_byids::InNRByIds { ids })
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
            let out = generated_recent::run(d, generated_recent::InNRRecent { limit })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> =
                out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "capped" => {
            // #135/#136: the GUARDED find entry enforces the baked hardLimit (2). The seed has > 2 users,
            // so the cap trips ⇒ `run` returns RuntimeError::Limit (context=find), byte-equal to mode-2.
            // This EXERCISES the auto-wired guarded adapter `run` (compiled by this crate build).
            match generated_capped::run(d, generated_capped::InNRCappedFind {}) {
                Ok(rows) => println!("OK:{}", rows.len()),
                Err(litedbmodel_runtime::RuntimeError::Limit(l)) => {
                    println!("LIMIT:{}:{}:{}", l.context, l.limit, l.count)
                }
                Err(litedbmodel_runtime::RuntimeError::Sql(e)) => panic!("capped: {}", e.message),
            }
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
            let out = generated_bymaybe::run(
                d,
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
            let out = generated_feed::run(d, generated_feed::InNRPostsWithAuthor { author_id })
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
            let out = generated_tenantfeed::run(
                d,
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
        // relbatch/relsingle (#131 → #140): the native module carries the PRIMARY read ONLY. The relation is
        // v1 lazy-batch loading — a RUNTIME concern: after the native de-boxed parent read, the adapter's
        // TYPED hydrator (`hydrate_<rel>`) calls `execute_relation_batch` for empty/dedupe/cast/bind/exec/limit,
        // de-boxes child rows via BC, then calls `hydrate_children` for group/distribute. The children remain
        // typed (NO `Value::Obj` grouped/retained). 1 parent + 1 batched child = 2 queries (no
        // N+1). The relation is NOT an executor primitive; the child de-box is bc's (same as a primary read).
        "relbatch" => {
            let tenant_id: i64 = args
                .get(3)
                .expect("tenant_id")
                .parse()
                .expect("tenant_id int");
            let out = generated_relbatch::run(d, generated_relbatch::InNRByTenant { tenant_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let hydrated = generated_relbatch::hydrate_posts(out, d)
                .unwrap_or_else(|e| panic!("hydrate posts: {}", e.message()));
            let rows: Vec<String> = hydrated
                .iter()
                .map(|(u, _)| {
                    format!(
                        "{{\"tenant_id\":{},\"user_id\":{},\"name\":{}}}",
                        u.tenant_id,
                        u.user_id,
                        json_str(&u.name)
                    )
                })
                .collect();
            let posts: Vec<String> = hydrated
                .iter()
                .map(|(_, children)| {
                    let items: Vec<String> = children
                        .iter()
                        .map(|c| {
                            format!(
                                "{{\"tenant_id\":{},\"post_id\":{},\"user_id\":{},\"title\":{}}}",
                                c.tenant_id,
                                c.post_id,
                                c.user_id,
                                json_str(&c.title)
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
            let out = generated_relsingle::run(d, generated_relsingle::InNRByAuthor { author_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let hydrated = generated_relsingle::hydrate_comments(out, d)
                .unwrap_or_else(|e| panic!("hydrate comments: {}", e.message()));
            let rows: Vec<String> = hydrated
                .iter()
                .map(|(p, _)| {
                    format!(
                        "{{\"id\":{},\"title\":{},\"author_id\":{}}}",
                        p.id,
                        json_str(&p.title),
                        p.author_id
                    )
                })
                .collect();
            let comments: Vec<String> = hydrated
                .iter()
                .map(|(_, children)| {
                    let items: Vec<String> = children
                        .iter()
                        .map(|c| {
                            format!(
                                "{{\"id\":{},\"body\":{},\"post_id\":{}}}",
                                c.id,
                                json_str(&c.body),
                                c.post_id
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
            let out =
                generated_createuser::run(d, generated_createuser::InNRCreateUser { email, name })
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
            let out = generated_createmany::run(
                d,
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
            let out = generated_upsertmany::run(
                d,
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
            let out =
                generated_updatemany::run(d, generated_updatemany::InNRUpdateMany { ids, names })
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
            let out = generated_upsert::run(d, generated_upsert::InNRUpsertUser { email, name })
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
            let out =
                generated_renameuser::run(d, generated_renameuser::InNRRenameUser { id, name })
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
            let out = generated_deleteuser::run(d, generated_deleteuser::InNRDeleteUser { id })
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
            // #136: the RETRYING/options tx entry — the input builder rebuilds the input per attempt so a
            // retryable failure re-runs the whole tx (bc-independent). A non-retryable error ⇒ Err ⇒
            // committed:false (mirrors mode-2 `execute_transaction_bundle(...).is_ok()`).
            let committed = generated_txdelete::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                tx_dialect(db_path),
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_txdelete::InNRTxDelete {
                    email: email.clone(),
                    name: name.clone(),
                },
            )
            .unwrap_or(false);
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
            let committed = generated_txnestedcreate::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                tx_dialect(db_path),
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_txnestedcreate::InNRTxNestedCreate {
                    email: email.clone(),
                    name: name.clone(),
                    title: title.clone(),
                },
            )
            .unwrap_or(false);
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
            let committed = generated_txnestedupdate::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                tx_dialect(db_path),
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_txnestedupdate::InNRTxNestedUpdate {
                    name: name.clone(),
                    user_id,
                    title: title.clone(),
                },
            )
            .unwrap_or(false);
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
            let committed = generated_txnestedupsert::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                tx_dialect(db_path),
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_txnestedupsert::InNRTxNestedUpsert {
                    email: email.clone(),
                    name: name.clone(),
                    title: title.clone(),
                },
            )
            .unwrap_or(false);
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
            let committed = generated_txrollback::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                tx_dialect(db_path),
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_txrollback::InNRTxRollback {
                    email: email.clone(),
                    name: name.clone(),
                    dup_email: dup_email.clone(),
                },
            )
            .unwrap_or(false);
            println!(
                "{{\"result\":{{\"committed\":{}}},\"state\":{}}}",
                committed,
                tx_state(d)
            );
        }
        other => panic!("unknown op '{other}'"),
    }
}
