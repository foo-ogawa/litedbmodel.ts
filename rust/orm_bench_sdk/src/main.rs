//! SDK-baseline ORM-bench cell (#129) — the raw-driver comparison cell for the collector's `sdk` vs
//! native latency comparison. It runs the 19 ORM ops over the shared benchmark seed
//! (the compiled `generated_setup::STATEMENTS`), with the SAME CLI, CSV schema, op list/order, per-iteration
//! unique-key strategy, warmup/reps defaults, and re-seed-before-each-op behaviour — but it does NOT go
//! through litedbmodel: every op is hand-written SQL issued straight at the plain driver (rusqlite for
//! sqlite; the `postgres` / `mysql` crates behind `livedb`). The CSV cell label is `sdk`.
//!
//! Relations are N+1-avoided by hand: one parent read + one batched child read per level (batched via an
//! `IN (…)` over the collected parent keys), grouped in memory. Batch writes are a single multi-row
//! statement. The `safety` mode proves those query counts (2 / 2 / 3 / 3 / 1 / 1) via a per-statement
//! counter incremented in the one exec seam (`Db`).
//!
//! Usage: `orm_bench_sdk <dialect> <spec> [reps=300] [warmup=30]`  (spec = sqlite file path, or — with
//! `--features livedb` — `pg:<libpq-conn>` / `mysql:<url>`); or `orm_bench_sdk safety <dialect> <spec>`.

use std::sync::atomic::{AtomicUsize, Ordering};

mod generated_setup;
use std::time::Instant;

// ── per-statement query counter (safety proof) — every prepared statement the crate issues bumps this
//    in the ONE exec seam below, so the N+1 proof is measured, not asserted. ─────────────────────────
static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, PartialEq)]
enum Dialect {
    Sqlite,
    // Pg/Mysql are only constructed by the `livedb`-gated drivers; live there, not dead.
    #[cfg_attr(not(feature = "livedb"), allow(dead_code))]
    Pg,
    #[cfg_attr(not(feature = "livedb"), allow(dead_code))]
    Mysql,
}

/// A bind parameter, dialect-agnostic; each driver lowers it to its own param type in the exec seam.
enum P {
    I(i64),
    S(String),
    B(bool),
}

/// A decoded result cell. Reads materialise every selected column (fair vs the native cell, which
/// decodes into typed structs) — the non-key payloads (`F`/`S`/`B`) are intentionally decoded-then-held
/// to pay the real allocation/parse cost even though only `id`/key columns (`I`) are read downstream for
/// batching, so `dead_code` on those payloads is expected.
#[allow(dead_code)]
enum Cell {
    I(i64),
    F(f64),
    S(String),
    B(bool),
    Null,
}
fn cell_i64(c: &Cell) -> i64 {
    match c {
        Cell::I(n) => *n,
        _ => 0,
    }
}

/// Placeholder emitter: `?` for sqlite/mysql, `$1,$2,…` for postgres (positional per statement).
struct Ph {
    dialect: Dialect,
    n: usize,
}
impl Ph {
    fn new(d: Dialect) -> Self {
        Ph { dialect: d, n: 0 }
    }
    fn next(&mut self) -> String {
        self.n += 1;
        match self.dialect {
            Dialect::Pg => format!("${}", self.n),
            _ => "?".to_string(),
        }
    }
    /// `ph,ph,…` for a flat `IN (…)` list of `count` scalars.
    fn list(&mut self, count: usize) -> String {
        (0..count).map(|_| self.next()).collect::<Vec<_>>().join(",")
    }
    /// A row-tuple IN body over `rows` tuples of `cols` columns each, in the form each dialect accepts:
    /// pg/mysql `((?,?),(?,?),…)`, sqlite `(VALUES (?,?),(?,?),…)`.
    fn tuple_in(&mut self, rows: usize, cols: usize) -> String {
        let body = (0..rows)
            .map(|_| format!("({})", self.list(cols)))
            .collect::<Vec<_>>()
            .join(",");
        match self.dialect {
            Dialect::Sqlite => format!("(VALUES {body})"),
            _ => format!("({body})"),
        }
    }
}

// ── the ONE exec seam. All DB access in this crate rides these three methods, so the query counter and
//    the per-driver param/decode lowering each live in exactly one place per driver. ─────────────────
trait Db {
    fn dialect(&self) -> Dialect;
    fn query(&mut self, sql: &str, params: &[P]) -> Vec<Vec<Cell>>;
    fn exec(&mut self, sql: &str, params: &[P]);
    /// INSERT one row and return its generated `id` (pg appends `RETURNING id`; sqlite/mysql read the
    /// driver's last-insert-id).
    fn insert_returning_id(&mut self, sql: &str, params: &[P]) -> i64;
}

// ── sqlite (rusqlite) ───────────────────────────────────────────────────────────────────────────────
struct SqliteDb {
    conn: rusqlite::Connection,
}
fn sqlite_value(p: &P) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match p {
        P::I(n) => Value::Integer(*n),
        P::S(s) => Value::Text(s.clone()),
        P::B(b) => Value::Integer(if *b { 1 } else { 0 }),
    }
}
impl Db for SqliteDb {
    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }
    fn query(&mut self, sql: &str, params: &[P]) -> Vec<Vec<Cell>> {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        // prepare_cached reuses the compiled statement across iterations (rusqlite's built-in per-conn
        // statement cache) — the fair "competent raw-driver user" baseline, matching native's prepared
        // cache. re-preparing per call was the strawman asymmetry.
        let mut stmt = self.conn.prepare_cached(sql).expect("prepare");
        let ncols = stmt.column_count();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter().map(sqlite_value)), |row| {
                Ok((0..ncols)
                    .map(|i| match row.get_ref(i).unwrap() {
                        rusqlite::types::ValueRef::Null => Cell::Null,
                        rusqlite::types::ValueRef::Integer(n) => Cell::I(n),
                        rusqlite::types::ValueRef::Real(f) => Cell::F(f),
                        rusqlite::types::ValueRef::Text(t) => {
                            Cell::S(String::from_utf8_lossy(t).into_owned())
                        }
                        rusqlite::types::ValueRef::Blob(_) => Cell::Null,
                    })
                    .collect::<Vec<Cell>>())
            })
            .expect("query");
        rows.map(|r| r.unwrap()).collect()
    }
    fn exec(&mut self, sql: &str, params: &[P]) {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        if params.is_empty() {
            self.conn.execute_batch(sql).unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        } else {
            self.conn
                .prepare_cached(sql)
                .expect("prepare")
                .execute(rusqlite::params_from_iter(params.iter().map(sqlite_value)))
                .unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        }
    }
    fn insert_returning_id(&mut self, sql: &str, params: &[P]) -> i64 {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        self.conn
            .prepare_cached(sql)
            .expect("prepare")
            .execute(rusqlite::params_from_iter(params.iter().map(sqlite_value)))
            .unwrap_or_else(|e| panic!("insert `{sql}`: {e}"));
        self.conn.last_insert_rowid()
    }
}

// ── postgres ─────────────────────────────────────────────────────────────────────────────────────────
#[cfg(feature = "livedb")]
struct PgDb {
    client: postgres::Client,
    // Per-SQL prepared-statement cache (postgres::Statement is a cheap Arc-backed handle) — reused across
    // iterations so the SDK, like native's prepare_cached, parses each SQL once. Keyed by SQL text.
    stmts: std::collections::HashMap<String, postgres::Statement>,
}
#[cfg(feature = "livedb")]
impl PgDb {
    fn prep(&mut self, sql: &str) -> postgres::Statement {
        if let Some(s) = self.stmts.get(sql) {
            return s.clone();
        }
        let s = self.client.prepare(sql).unwrap_or_else(|e| panic!("prepare `{sql}`: {e}"));
        self.stmts.insert(sql.to_string(), s.clone());
        s
    }
}
#[cfg(feature = "livedb")]
fn pg_params(params: &[P]) -> Vec<Box<dyn postgres::types::ToSql + Sync>> {
    params
        .iter()
        .map(|p| match p {
            // int4 columns everywhere in the fixture; values are small, so i32 matches the column type.
            P::I(n) => Box::new(*n as i32) as Box<dyn postgres::types::ToSql + Sync>,
            P::S(s) => Box::new(s.clone()),
            P::B(b) => Box::new(*b),
        })
        .collect()
}
#[cfg(feature = "livedb")]
fn pg_decode(row: &postgres::Row) -> Vec<Cell> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, col)| match col.type_().name() {
            "int2" => row.get::<_, Option<i16>>(i).map(|v| Cell::I(v as i64)).unwrap_or(Cell::Null),
            "int4" => row.get::<_, Option<i32>>(i).map(|v| Cell::I(v as i64)).unwrap_or(Cell::Null),
            "int8" => row.get::<_, Option<i64>>(i).map(Cell::I).unwrap_or(Cell::Null),
            "bool" => row.get::<_, Option<bool>>(i).map(Cell::B).unwrap_or(Cell::Null),
            "timestamp" | "timestamptz" => {
                // Pull + decode the value (wire cost is what matters); content is unused downstream.
                let _: Option<std::time::SystemTime> = row.get(i);
                Cell::Null
            }
            _ => row.get::<_, Option<String>>(i).map(Cell::S).unwrap_or(Cell::Null),
        })
        .collect()
}
#[cfg(feature = "livedb")]
impl Db for PgDb {
    fn dialect(&self) -> Dialect {
        Dialect::Pg
    }
    fn query(&mut self, sql: &str, params: &[P]) -> Vec<Vec<Cell>> {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        let boxed = pg_params(params);
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            boxed.iter().map(|b| b.as_ref()).collect();
        let stmt = self.prep(sql);
        self.client
            .query(&stmt, &refs)
            .unwrap_or_else(|e| panic!("query `{sql}`: {e}"))
            .iter()
            .map(pg_decode)
            .collect()
    }
    fn exec(&mut self, sql: &str, params: &[P]) {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        if params.is_empty() {
            // BEGIN/COMMIT + param-free seed statements: run outside the extended protocol.
            self.client.batch_execute(sql).unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        } else {
            let boxed = pg_params(params);
            let refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
                boxed.iter().map(|b| b.as_ref()).collect();
            let stmt = self.prep(sql);
            self.client.execute(&stmt, &refs).unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        }
    }
    fn insert_returning_id(&mut self, sql: &str, params: &[P]) -> i64 {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        let sql = format!("{sql} RETURNING id");
        let boxed = pg_params(params);
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            boxed.iter().map(|b| b.as_ref()).collect();
        let stmt = self.prep(&sql);
        let rows = self.client.query(&stmt, &refs).unwrap_or_else(|e| panic!("insert `{sql}`: {e}"));
        rows[0].get::<_, i32>(0) as i64
    }
}

// ── mysql ────────────────────────────────────────────────────────────────────────────────────────────
#[cfg(feature = "livedb")]
fn my_params(params: &[P]) -> Vec<mysql::Value> {
    params
        .iter()
        .map(|p| match p {
            P::I(n) => mysql::Value::Int(*n),
            P::S(s) => mysql::Value::Bytes(s.clone().into_bytes()),
            P::B(b) => mysql::Value::Int(if *b { 1 } else { 0 }),
        })
        .collect()
}
#[cfg(feature = "livedb")]
fn my_decode(row: mysql::Row) -> Vec<Cell> {
    row.unwrap()
        .into_iter()
        .map(|v| match v {
            mysql::Value::NULL => Cell::Null,
            mysql::Value::Int(n) => Cell::I(n),
            mysql::Value::UInt(n) => Cell::I(n as i64),
            mysql::Value::Float(f) => Cell::F(f as f64),
            mysql::Value::Double(f) => Cell::F(f),
            mysql::Value::Bytes(b) => Cell::S(String::from_utf8_lossy(&b).into_owned()),
            _ => Cell::Null,
        })
        .collect()
}
#[cfg(feature = "livedb")]
struct MyDb {
    conn: mysql::Conn,
}
#[cfg(feature = "livedb")]
impl Db for MyDb {
    fn dialect(&self) -> Dialect {
        Dialect::Mysql
    }
    fn query(&mut self, sql: &str, params: &[P]) -> Vec<Vec<Cell>> {
        use mysql::prelude::Queryable;
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        let rows: Vec<mysql::Row> = self
            .conn
            .exec(sql, my_params(params))
            .unwrap_or_else(|e| panic!("query `{sql}`: {e}"));
        rows.into_iter().map(my_decode).collect()
    }
    fn exec(&mut self, sql: &str, params: &[P]) {
        use mysql::prelude::Queryable;
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        if params.is_empty() {
            self.conn.query_drop(sql).unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        } else {
            self.conn.exec_drop(sql, my_params(params)).unwrap_or_else(|e| panic!("exec `{sql}`: {e}"));
        }
    }
    fn insert_returning_id(&mut self, sql: &str, params: &[P]) -> i64 {
        use mysql::prelude::Queryable;
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        self.conn.exec_drop(sql, my_params(params)).unwrap_or_else(|e| panic!("insert `{sql}`: {e}"));
        self.conn.last_insert_id() as i64
    }
}

fn open_db(spec: &str) -> Box<dyn Db> {
    #[cfg(feature = "livedb")]
    {
        if let Some(conn) = spec.strip_prefix("pg:") {
            return Box::new(PgDb {
                client: postgres::Client::connect(conn, postgres::NoTls).expect("connect postgres"),
                stmts: std::collections::HashMap::new(),
            });
        }
        if let Some(url) = spec.strip_prefix("mysql:") {
            let opts = mysql::Opts::from_url(url).expect("parse mysql url");
            return Box::new(MyDb { conn: mysql::Conn::new(opts).expect("connect mysql") });
        }
    }
    Box::new(SqliteDb {
        conn: rusqlite::Connection::open(spec).expect("open sqlite db"),
    })
}

// ── setup: exec the selected dialect's compiled static statements ──────────────────────────────────
fn reseed(db: &mut dyn Db, _dialect: &str) {
    for sql in generated_setup::STATEMENTS {
        db.exec(sql, &[]);
    }
}

// ── batch-write inputs (mirror ops.ts / the native cell) ──────────────────────────────────────────────
fn batch_emails(it: u64) -> Vec<String> {
    (0..10).map(|k| format!("many{it}_{k}@bench.com")).collect()
}
fn batch_names() -> Vec<String> {
    (0..10).map(|k| format!("Many {k}")).collect()
}

// ── upsert bodies differ only in the conflict clause; the column list + VALUES are shared. ───────────
fn upsert_conflict(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Mysql => " ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)",
        _ => " ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name",
    }
}

// ── the 19 ORM ops (contract.ts order). Each runs ONE logical op for iteration `it`; mutating ops vary
//    their UNIQUE column by `it`. Fixed inputs mirror ops.ts (the SCP SSoT). ──────────────────────────
fn run_op(op: &str, it: u64, db: &mut dyn Db) {
    let dialect = db.dialect();
    match op {
        "findAll" => {
            db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[]);
        }
        "filterPaginateSort" => {
            let mut ph = Ph::new(dialect);
            let published = if dialect == Dialect::Pg { P::B(true) } else { P::I(1) };
            let sql = format!(
                "SELECT id, title, content, published, author_id, created_at FROM benchmark_posts \
                 WHERE published = {} ORDER BY created_at DESC LIMIT 20 OFFSET 10",
                ph.next()
            );
            db.query(&sql, &[published]);
        }
        "findFirst" => {
            let mut ph = Ph::new(dialect);
            let sql = format!(
                "SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1",
                ph.next()
            );
            db.query(&sql, &[P::S("User%".into())]);
        }
        "findUnique" => {
            let mut ph = Ph::new(dialect);
            let sql = format!(
                "SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1",
                ph.next()
            );
            db.query(&sql, &[P::S("user500@example.com".into())]);
        }
        // ── nested reads: primary + ONE batched child (2 queries), grouped in memory. ──────────────
        "nestedFindAll" => {
            let users = db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[]);
            nested_posts_for(db, users);
        }
        "nestedFindFirst" => {
            let mut ph = Ph::new(dialect);
            let sql = format!("SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1", ph.next());
            let users = db.query(&sql, &[P::S("User%".into())]);
            nested_posts_for(db, users);
        }
        "nestedFindUnique" => {
            let mut ph = Ph::new(dialect);
            let sql = format!("SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1", ph.next());
            let users = db.query(&sql, &[P::S("user1@example.com".into())]);
            nested_posts_for(db, users);
        }
        // ── 3-level chain: users → posts → comments (3 queries). ──────────────────────────────────
        "nestedRelations" => {
            let users = db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[]);
            let post_ids = nested_posts_collect_ids(db, &users);
            batched_comments(db, &post_ids);
        }
        // ── composite 3-level: tenant_users → tenant_posts → tenant_comments (3 queries). ─────────
        "compositeRelations" => composite_relations(db),
        "create" => {
            let mut ph = Ph::new(dialect);
            let sql = format!(
                "INSERT INTO benchmark_users (email, name) VALUES ({}, {})",
                ph.next(),
                ph.next()
            );
            db.exec(&sql, &[P::S(format!("new{it}@bench.com")), P::S("New".into())]);
        }
        "nestedCreate" => {
            db.exec("BEGIN", &[]);
            let uid = insert_user(db, format!("nc{it}@bench.com"), "NC");
            insert_post(db, uid, "NC Post");
            db.exec("COMMIT", &[]);
        }
        "update" => {
            let mut ph = Ph::new(dialect);
            let sql = format!(
                "UPDATE benchmark_users SET name = {} WHERE id = {}",
                ph.next(),
                ph.next()
            );
            db.exec(&sql, &[P::S("Updated 100".into()), P::I(100)]);
        }
        "nestedUpdate" => {
            db.exec("BEGIN", &[]);
            let mut ph = Ph::new(dialect);
            let s1 = format!("UPDATE benchmark_users SET name = {} WHERE id = {}", ph.next(), ph.next());
            db.exec(&s1, &[P::S("NU".into()), P::I(7)]);
            let mut ph = Ph::new(dialect);
            let s2 = format!("UPDATE benchmark_posts SET title = {} WHERE author_id = {}", ph.next(), ph.next());
            db.exec(&s2, &[P::S("NU Post".into()), P::I(7)]);
            db.exec("COMMIT", &[]);
        }
        "upsert" => {
            let mut ph = Ph::new(dialect);
            let sql = format!(
                "INSERT INTO benchmark_users (email, name) VALUES ({}, {}){}",
                ph.next(),
                ph.next(),
                upsert_conflict(dialect)
            );
            db.exec(&sql, &[P::S("user1@example.com".into()), P::S("Upserted One".into())]);
        }
        "nestedUpsert" => {
            db.exec("BEGIN", &[]);
            let mut ph = Ph::new(dialect);
            let up = format!(
                "INSERT INTO benchmark_users (email, name) VALUES ({}, {}){}",
                ph.next(),
                ph.next(),
                upsert_conflict(dialect)
            );
            db.exec(&up, &[P::S("user1@example.com".into()), P::S("NUp".into())]);
            // Re-select the id by the unique email (upsert has no portable RETURNING for the update path).
            let mut ph = Ph::new(dialect);
            let sel = format!("SELECT id FROM benchmark_users WHERE email = {}", ph.next());
            let rows = db.query(&sel, &[P::S("user1@example.com".into())]);
            let uid = cell_i64(&rows[0][0]);
            insert_post(db, uid, "NUp Post");
            db.exec("COMMIT", &[]);
        }
        "delete" => {
            db.exec("BEGIN", &[]);
            let uid = insert_user(db, format!("del{it}@bench.com"), "Del");
            let mut ph = Ph::new(dialect);
            let del = format!("DELETE FROM benchmark_users WHERE id = {}", ph.next());
            db.exec(&del, &[P::I(uid)]);
            db.exec("COMMIT", &[]);
        }
        "createMany" => {
            let emails = batch_emails(it);
            let names = batch_names();
            let mut ph = Ph::new(dialect);
            let rows: Vec<String> = (0..10).map(|_| format!("({}, {})", ph.next(), ph.next())).collect();
            let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {}", rows.join(","));
            let mut params = Vec::with_capacity(20);
            for k in 0..10 {
                params.push(P::S(emails[k].clone()));
                params.push(P::S(names[k].clone()));
            }
            db.exec(&sql, &params);
        }
        "upsertMany" => {
            let mut emails: Vec<String> = vec!["user1@example.com".into(), "user2@example.com".into()];
            emails.extend((0..8).map(|k| format!("many{k}@bench.com")));
            let names = batch_names();
            let mut ph = Ph::new(dialect);
            let rows: Vec<String> = (0..10).map(|_| format!("({}, {})", ph.next(), ph.next())).collect();
            let sql = format!(
                "INSERT INTO benchmark_users (email, name) VALUES {}{}",
                rows.join(","),
                upsert_conflict(dialect)
            );
            let mut params = Vec::with_capacity(20);
            for k in 0..10 {
                params.push(P::S(emails[k].clone()));
                params.push(P::S(names[k].clone()));
            }
            db.exec(&sql, &params);
        }
        "updateMany" => update_many(db),
        other => panic!("unknown op '{other}'"),
    }
}

// ── read helpers (one batched child query per level) ──────────────────────────────────────────────────
/// Given parent user rows (col0 = id), run ONE batched child posts read and group in memory.
fn nested_posts_for(db: &mut dyn Db, users: Vec<Vec<Cell>>) {
    let ids: Vec<i64> = users.iter().map(|r| cell_i64(&r[0])).collect();
    if ids.is_empty() {
        return;
    }
    let mut ph = Ph::new(db.dialect());
    let sql = format!(
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({}) ORDER BY id ASC",
        ph.list(ids.len())
    );
    let posts = db.query(&sql, &ids.iter().map(|i| P::I(*i)).collect::<Vec<_>>());
    group_by(&posts, 2); // author_id at col2
}
/// nestedRelations middle level: batched posts, returning the collected post ids for the comments level.
fn nested_posts_collect_ids(db: &mut dyn Db, users: &[Vec<Cell>]) -> Vec<i64> {
    let ids: Vec<i64> = users.iter().map(|r| cell_i64(&r[0])).collect();
    if ids.is_empty() {
        return vec![];
    }
    let mut ph = Ph::new(db.dialect());
    let sql = format!(
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({}) ORDER BY id ASC",
        ph.list(ids.len())
    );
    let posts = db.query(&sql, &ids.iter().map(|i| P::I(*i)).collect::<Vec<_>>());
    group_by(&posts, 2);
    posts.iter().map(|r| cell_i64(&r[0])).collect()
}
/// nestedRelations leaf level: ONE batched comments read by post_id IN (…).
fn batched_comments(db: &mut dyn Db, post_ids: &[i64]) {
    if post_ids.is_empty() {
        return;
    }
    let mut ph = Ph::new(db.dialect());
    let sql = format!(
        "SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN ({}) ORDER BY id ASC",
        ph.list(post_ids.len())
    );
    let comments = db.query(&sql, &post_ids.iter().map(|i| P::I(*i)).collect::<Vec<_>>());
    group_by(&comments, 2); // post_id at col2
}

/// compositeRelations: tenant_users(tenant=1) → batched tenant_posts by (tenant_id,user_id) → batched
/// tenant_comments by (tenant_id,post_id). 3 queries.
fn composite_relations(db: &mut dyn Db) {
    let mut ph = Ph::new(db.dialect());
    let sql = format!(
        "SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = {} ORDER BY user_id ASC",
        ph.next()
    );
    let tusers = db.query(&sql, &[P::I(1)]);
    if tusers.is_empty() {
        return;
    }
    // batched posts by (tenant_id, user_id)
    let mut ph = Ph::new(db.dialect());
    let body = ph.tuple_in(tusers.len(), 2);
    let psql = format!(
        "SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE (tenant_id, user_id) IN {body}"
    );
    let mut pparams = Vec::new();
    for r in &tusers {
        pparams.push(P::I(cell_i64(&r[0]))); // tenant_id
        pparams.push(P::I(cell_i64(&r[1]))); // user_id
    }
    let tposts = db.query(&psql, &pparams);
    if tposts.is_empty() {
        return;
    }
    // batched comments by (tenant_id, post_id)
    let mut ph = Ph::new(db.dialect());
    let body = ph.tuple_in(tposts.len(), 2);
    let csql = format!(
        "SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments WHERE (tenant_id, post_id) IN {body}"
    );
    let mut cparams = Vec::new();
    for r in &tposts {
        cparams.push(P::I(cell_i64(&r[0]))); // tenant_id
        cparams.push(P::I(cell_i64(&r[1]))); // post_id
    }
    db.query(&csql, &cparams);
}

/// Group child rows by the parent-key column (in-memory stitch work, mirrors the runtime distribute).
fn group_by(rows: &[Vec<Cell>], key_col: usize) {
    use std::collections::HashMap;
    let mut map: HashMap<i64, Vec<usize>> = HashMap::new();
    for (idx, r) in rows.iter().enumerate() {
        map.entry(cell_i64(&r[key_col])).or_default().push(idx);
    }
    std::hint::black_box(&map);
}

// ── write helpers ─────────────────────────────────────────────────────────────────────────────────────
fn insert_user(db: &mut dyn Db, email: String, name: &str) -> i64 {
    let mut ph = Ph::new(db.dialect());
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {})", ph.next(), ph.next());
    db.insert_returning_id(&sql, &[P::S(email), P::S(name.into())])
}
fn insert_post(db: &mut dyn Db, author_id: i64, title: &str) {
    let mut ph = Ph::new(db.dialect());
    let sql = format!("INSERT INTO benchmark_posts (author_id, title) VALUES ({}, {})", ph.next(), ph.next());
    db.exec(&sql, &[P::I(author_id), P::S(title.into())]);
}

/// updateMany: ONE statement setting names for ids 1..=10. sqlite/mysql use a `CASE id` expression with
/// an `id IN (…)` guard; pg uses a `FROM (VALUES …)` join. All single-statement (N+1-avoided).
fn update_many(db: &mut dyn Db) {
    let names = batch_names();
    match db.dialect() {
        Dialect::Pg => {
            let mut ph = Ph::new(Dialect::Pg);
            // Cast the first tuple so postgres infers the VALUES column types.
            let mut tuples: Vec<String> = Vec::with_capacity(10);
            for k in 0..10 {
                if k == 0 {
                    tuples.push(format!("({}::integer, {}::varchar)", ph.next(), ph.next()));
                } else {
                    tuples.push(format!("({}, {})", ph.next(), ph.next()));
                }
            }
            let sql = format!(
                "UPDATE benchmark_users AS t SET name = v.name FROM (VALUES {}) AS v(id, name) WHERE t.id = v.id",
                tuples.join(",")
            );
            let mut params = Vec::with_capacity(20);
            for k in 0..10 {
                params.push(P::I((k + 1) as i64));
                params.push(P::S(names[k].clone()));
            }
            db.exec(&sql, &params);
        }
        _ => {
            let mut ph = Ph::new(db.dialect());
            let mut whens = String::new();
            let mut params: Vec<P> = Vec::with_capacity(30);
            for k in 0..10 {
                whens.push_str(&format!(" WHEN {} THEN {}", ph.next(), ph.next()));
                params.push(P::I((k + 1) as i64));
                params.push(P::S(names[k].clone()));
            }
            let in_list = ph.list(10);
            for k in 0..10 {
                params.push(P::I((k + 1) as i64));
            }
            let sql = format!(
                "UPDATE benchmark_users SET name = CASE id{whens} END WHERE id IN ({in_list})"
            );
            db.exec(&sql, &params);
        }
    }
}

const OPS: &[&str] = &[
    "findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst",
    "nestedFindUnique", "create", "nestedCreate", "update", "nestedUpdate", "upsert", "nestedUpsert",
    "delete", "createMany", "upsertMany", "updateMany", "nestedRelations", "compositeRelations",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("safety") {
        let dialect = args.get(2).expect("safety <dialect> <spec>").clone();
        let spec = args.get(3).expect("safety <dialect> <spec>").clone();
        run_safety(&dialect, &spec);
        return;
    }
    let dialect = args.get(1).expect("usage: orm_bench_sdk <dialect> <spec> [reps] [warmup]").clone();
    let spec = args.get(2).expect("usage: orm_bench_sdk <dialect> <spec> [reps] [warmup]").clone();
    let reps: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(300);
    let warmup: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    let mut db = open_db(&spec);
    println!("cell,dialect,op,iter,us");
    for op in OPS {
        // Re-seed before each op so reads see the seed state and writes start clean.
        reseed(db.as_mut(), &dialect);
        for it in 0..warmup {
            run_op(op, it, db.as_mut());
        }
        for it in 0..reps {
            let g = it + warmup;
            let t = Instant::now();
            run_op(op, g, db.as_mut());
            let us = t.elapsed().as_micros();
            println!("sdk,{dialect},{op},{it},{us}");
        }
    }
}

// ── #129 safety proof: N+1-avoidance (query counts) via the per-statement QUERY_COUNT. ────────────────
fn run_safety(dialect: &str, spec: &str) {
    let mut db = open_db(spec);
    reseed(db.as_mut(), dialect);
    let count = |op: &str, db: &mut dyn Db| {
        QUERY_COUNT.store(0, Ordering::SeqCst);
        run_op(op, 0, db);
        QUERY_COUNT.load(Ordering::SeqCst)
    };
    println!("nestedFindAll queries={} (expect 2: 1 parent + 1 batched child)", count("nestedFindAll", db.as_mut()));
    println!("nestedFindUnique queries={} (expect 2)", count("nestedFindUnique", db.as_mut()));
    println!("nestedRelations queries={} (expect 3: users + posts + comments)", count("nestedRelations", db.as_mut()));
    println!("compositeRelations queries={} (expect 3)", count("compositeRelations", db.as_mut()));
    reseed(db.as_mut(), dialect);
    println!("createMany queries={} (expect 1: one batched INSERT for 10 records)", count("createMany", db.as_mut()));
    reseed(db.as_mut(), dialect);
    println!("updateMany queries={} (expect 1)", count("updateMany", db.as_mut()));
}
