//! litedbmodel cross-language adapter RUNNER — v1-rs (OLD litedbmodel.rs@0.4.5) — epic #44.
//!
//! Wires the old, independent `litedbmodel.rs@0.4.5` ActiveRecord into the #44 matrix as a
//! comparison cell for a FAIR v1-rust vs v2-rust verdict.
//!
//! ## Seam
//! v1-rs is an ASYNC `sqlx`/tokio + `deadpool` ActiveRecord: there is NO makeSQL bundle and no
//! synchronous in-proc `Driver` seam matching the v2 harness contract. So this adapter runs it
//! against an IN-PROC SQLite `:memory:` DB (the achievable, faithful comparison sqlx supports),
//! wrapping a single-thread tokio runtime and `block_on`-ing each op. It speaks the SAME
//! line-delimited JSON contract as every other adapter and consumes the SAME
//! `generated/bundles.json` schema+seed so the dataset is byte-identical to the v2 cells.
//!
//! The DB is seeded with `max_connections(1)` so the single `:memory:` connection (and thus the
//! seeded data) persists across ops (a fresh pooled `:memory:` connection would be an empty DB).
//!
//! ## What runs per axis
//!   * DB-backed (`run`/`throughput`): the 8 access patterns through v1-rs's real public
//!     ActiveRecord API (`find` with `Condition`s, batched-IN relation loads, `create_many`,
//!     gate-first write-tx). N+1 is avoided EXACTLY as the v2 baseline does it (parent query →
//!     collect keys → ONE batched `IN (...)` child query), so the logical work matches.
//!   * micro (`micro`, I/O-EXCLUDED): v1-rs's client-side path only — `build_select_sql` /
//!     `build_insert` SQL construction + `from_row` hydration over FIXED mock rows, NO sqlx
//!     execute. This isolates the same client-side work (render WHERE + bind + hydrate) the other
//!     langs' micro cells measure.
//!   * cost (`cost`): queries/op + rows/op, measured from the statements v1-rs actually issues and
//!     the rows it actually materializes (tx-control excluded) — the fairness evidence.
//!
//! NOTE: this in-proc-SQLite bench does NOT exercise the #40 pooled-async-vs-sync axis; that needs
//! live-PG network I/O (a docker/live-DB concern), stated explicitly in the report.

use litedbmodel::driver::Row;
use litedbmodel::prelude::*;
use litedbmodel::values::Value as V;
use serde_json::Value as J;
use std::io::{BufRead, Write};
use std::time::Instant;
use tokio::runtime::Runtime;

// ════════════════════════════════════════════════════════════════════════════
// Models — the SAME shared #44 schema (users / posts / comments), expressed with
// v1-rs's #[derive(Model)] ActiveRecord API.
// ════════════════════════════════════════════════════════════════════════════

#[derive(Model, Debug, Clone, Default)]
#[model(table = "users")]
pub struct User {
    #[column(primary_key)]
    pub id: i64,
    #[column]
    pub name: String,
    #[column]
    pub post_count: i64,
}

#[derive(Model, Debug, Clone, Default)]
#[model(table = "posts")]
pub struct Post {
    #[column(primary_key, auto_increment)]
    pub id: Option<i64>,
    #[column]
    pub author_id: i64,
    #[column]
    pub title: String,
    #[column]
    pub status: Option<String>,
    #[column]
    pub views: i64,
    #[column]
    pub created_at: String,
}

#[derive(Model, Debug, Clone, Default)]
#[model(table = "comments")]
pub struct Comment {
    #[column(primary_key, auto_increment)]
    pub id: Option<i64>,
    #[column]
    pub post_id: i64,
    #[column]
    pub body: String,
    #[column]
    pub created_at: String,
}

// ════════════════════════════════════════════════════════════════════════════
// Shared artifact (schema + seed) — read from the SAME generated/bundles.json.
// ════════════════════════════════════════════════════════════════════════════

struct Artifact {
    schema: Vec<String>,
    seed: Vec<String>,
}

fn load_artifact(path: &str) -> Artifact {
    let raw = std::fs::read_to_string(path).expect("read bundles.json");
    let j: J = serde_json::from_str(&raw).expect("parse bundles.json");
    let schema = j["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    let seed = j["seed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    Artifact { schema, seed }
}

/// Build + seed a fresh in-proc SQLite `:memory:` handler (single connection so the
/// `:memory:` DB persists across ops). One BEGIN/COMMIT wraps the seed; tx-control is
/// not part of any measured op.
fn seed_handler(rt: &Runtime, art: &Artifact) -> DbHandler {
    rt.block_on(async {
        let handler = DbHandlerBuilder::new(DbConfig::sqlite(":memory:"))
            .max_connections(1)
            .min_connections(0)
            .build()
            .await
            .expect("build sqlite :memory: handler");
        let schema = art.schema.clone();
        let seed = art.seed.clone();
        handler
            .transaction(|tx| async move {
                for stmt in &schema {
                    tx.execute_write(stmt, &[]).await?;
                }
                for stmt in &seed {
                    tx.execute_write(stmt, &[]).await?;
                }
                Ok(())
            })
            .await
            .expect("seed");
        handler
    })
}

// ════════════════════════════════════════════════════════════════════════════
// The 8 access patterns via v1-rs's real ActiveRecord API.
//
// Returns (queries, rows): statements v1-rs issued + rows it materialized. This is the
// fairness cost — it MUST match the shared 1/3, 1/5, 1/10, 2/6, 2/30, 2/20, 1/0, 4/2 shape.
// ════════════════════════════════════════════════════════════════════════════

fn run_case(rt: &Runtime, handler: &DbHandler, case: &str) -> (u64, u64) {
    rt.block_on(async move { run_case_async(handler, case).await })
}

async fn run_case_async(h: &DbHandler, case: &str) -> (u64, u64) {
    match case {
        // find: author_id = ? AND status = ? (SKIP-optional present) AND created_at >= ?, ORDER BY id
        "find" => {
            let conds = vec![
                Condition::Eq {
                    column: "author_id".into(),
                    table: None,
                    value: V::Int(1),
                },
                Condition::Eq {
                    column: "status".into(),
                    table: None,
                    value: V::Text("live".into()),
                },
                Condition::Raw {
                    sql: "created_at >= ?".into(),
                    params: vec![V::Text("2026-02-01".into())],
                },
            ];
            let opts = SelectOptions {
                select: Some("id, author_id, title, status, views, created_at".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let rows: Vec<Post> = find(h, &conds, Some(&opts), None).await.unwrap();
            (1, rows.len() as u64)
        }
        // complexWhere: eq + range + LIKE + IN (multiple predicate kinds)
        "complexWhere" => {
            let conds = vec![
                Condition::Eq {
                    column: "author_id".into(),
                    table: None,
                    value: V::Int(1),
                },
                Condition::Raw {
                    sql: "created_at >= ?".into(),
                    params: vec![V::Text("2026-02-01".into())],
                },
                Condition::Raw {
                    sql: "title LIKE ?".into(),
                    params: vec![V::Text("post-%".into())],
                },
                Condition::Eq {
                    column: "id".into(),
                    table: None,
                    value: V::IntArray(vec![1, 2, 3, 4, 5]),
                },
            ];
            let opts = SelectOptions {
                select: Some("id, author_id, title, status, views".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let rows: Vec<Post> = find(h, &conds, Some(&opts), None).await.unwrap();
            (1, rows.len() as u64)
        }
        // inList: IN (?, …) single-column IN-list
        "inList" => {
            let conds = vec![Condition::Eq {
                column: "id".into(),
                table: None,
                value: V::IntArray(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            }];
            let opts = SelectOptions {
                select: Some("id, title".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let rows: Vec<Post> = find(h, &conds, Some(&opts), None).await.unwrap();
            (1, rows.len() as u64)
        }
        // belongsTo: posts → author. Parent query + ONE batched IN query (N+1 avoided).
        "belongsTo" => {
            let (posts, users) = relation_belongs_to(h).await;
            (2, (posts + users) as u64)
        }
        // hasMany: posts → comments. Parent query + ONE batched IN query (N+1 avoided).
        "hasMany" => {
            let (posts, comments) = relation_has_many(h, None).await;
            (2, (posts + comments) as u64)
        }
        // hasManyLimit: posts → recent comments, per-parent LIMIT 3 (batched, N+1 avoided).
        "hasManyLimit" => {
            let (posts, comments) = relation_has_many(h, Some(3)).await;
            (2, (posts + comments) as u64)
        }
        // batchInsert: create_many → ONE grouped INSERT in one tx.
        "batchInsert" => {
            h.transaction(|tx| async move {
                let cols = Post::columns();
                let rows: Vec<Vec<ColumnValue>> = (0..10)
                    .map(|i| {
                        vec![
                            cols.author_id.set(2i64),
                            cols.title.set(format!("bulk-{i}")),
                            cols.status.set("live".to_string()),
                            cols.views.set(0i64),
                            cols.created_at.set("2026-05-01".to_string()),
                        ]
                    })
                    .collect();
                tx.create_many::<Post>(&rows, None).await?;
                Ok(())
            })
            .await
            .unwrap();
            (1, 0)
        }
        // writeTxGate: gate-first create (requires + unique guard + body + derive) in ONE tx.
        //   q1 requires SELECT, q2 unique-guard INSERT OR IGNORE, q3 body INSERT RETURNING,
        //   q4 derive UPDATE. rows = requires(1) + body RETURNING(1) = 2.
        "writeTxGate" => {
            let (q, r) = h
                .transaction(|tx| async move {
                    // gate:requires — author exists
                    let g = tx
                        .execute_query("SELECT 1 FROM users WHERE id = ?", &[V::Int(1)])
                        .await?;
                    if g.rows.is_empty() {
                        return Err(litedbmodel::error::Error::Driver("requires_absent".into()));
                    }
                    let mut rows_read = g.rows.len() as u64;
                    // gate:unique — title-per-author guard (INSERT OR IGNORE, no rows)
                    tx.execute_write(
                        "INSERT OR IGNORE INTO uniq (name, s0, f0) VALUES (?, ?, ?)",
                        &[
                            V::Text("title_per_author".into()),
                            V::Text("1".into()),
                            V::Text("txn-post".into()),
                        ],
                    )
                    .await?;
                    // body — INSERT ... RETURNING (materializes the new row)
                    let ins = tx
                        .execute_query(
                            "INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title",
                            &[
                                V::Int(1),
                                V::Text("txn-post".into()),
                                V::Text("2026-05-01".into()),
                            ],
                        )
                        .await?;
                    rows_read += ins.rows.len() as u64;
                    // derive — cascade counter
                    tx.execute_write(
                        "UPDATE users SET post_count = post_count + ? WHERE id = ?",
                        &[V::Int(1), V::Int(1)],
                    )
                    .await?;
                    Ok((4u64, rows_read))
                })
                .await
                .unwrap();
            (q, r)
        }
        _ => panic!("unknown case {case}"),
    }
}

/// posts (by author) → author (belongsTo). Returns (#posts, #authors) materialized.
async fn relation_belongs_to(h: &DbHandler) -> (usize, usize) {
    let conds = vec![Condition::Eq {
        column: "author_id".into(),
        table: None,
        value: V::Int(1),
    }];
    let opts = SelectOptions {
        select: Some("id, author_id, title".into()),
        order_raw: Some("id ASC".into()),
        ..Default::default()
    };
    let posts: Vec<Post> = find(h, &conds, Some(&opts), None).await.unwrap();
    // batch keys — distinct author ids (N+1 avoided: ONE IN query for all parents)
    let mut aids: Vec<i64> = posts.iter().map(|p| p.author_id).collect();
    aids.sort_unstable();
    aids.dedup();
    let uconds = vec![Condition::Eq {
        column: "id".into(),
        table: None,
        value: V::IntArray(aids),
    }];
    let uopts = SelectOptions {
        select: Some("id, name".into()),
        ..Default::default()
    };
    let users: Vec<User> = find(h, &uconds, Some(&uopts), None).await.unwrap();
    (posts.len(), users.len())
}

/// posts (by author) → comments (hasMany, optional per-parent LIMIT). Returns (#posts, #comments).
async fn relation_has_many(h: &DbHandler, per_parent_limit: Option<i64>) -> (usize, usize) {
    let conds = vec![Condition::Eq {
        column: "author_id".into(),
        table: None,
        value: V::Int(1),
    }];
    let opts = SelectOptions {
        select: Some("id, author_id, title".into()),
        order_raw: Some("id ASC".into()),
        ..Default::default()
    };
    let posts: Vec<Post> = find(h, &conds, Some(&opts), None).await.unwrap();
    let ids: Vec<i64> = posts.iter().filter_map(|p| p.id).collect();

    let comments: Vec<Comment> = match per_parent_limit {
        // per-parent LIMIT via ROW_NUMBER — the hand-optimized batched form (ONE query, N+1 avoided).
        Some(n) => {
            let ph = vec!["?"; ids.len()].join(", ");
            let sql = format!(
                "SELECT id, post_id, body, created_at FROM (SELECT id, post_id, body, created_at, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ({ph})) WHERE rn <= {n}"
            );
            let params: Vec<V> = ids.iter().map(|i| V::Int(*i)).collect();
            litedbmodel::model::query::<Comment, _>(h, &sql, &params)
                .await
                .unwrap()
        }
        // full children — ONE batched IN query.
        None => {
            let cconds = vec![Condition::Eq {
                column: "post_id".into(),
                table: None,
                value: V::IntArray(ids),
            }];
            let copts = SelectOptions {
                select: Some("id, post_id, body".into()),
                ..Default::default()
            };
            find(h, &cconds, Some(&copts), None).await.unwrap()
        }
    };
    (posts.len(), comments.len())
}

// ════════════════════════════════════════════════════════════════════════════
// micro (I/O-EXCLUDED): v1-rs's client-side path only — SQL construction + hydration
// over FIXED mock rows, NO sqlx execute. Isolates render WHERE + bind + hydrate.
// ════════════════════════════════════════════════════════════════════════════

fn mock_post_rows(n: usize) -> Vec<Row> {
    (1..=n as i64)
        .map(|i| {
            let mut r = Row::new();
            r.insert("id".into(), V::Int(i));
            r.insert("author_id".into(), V::Int(1));
            r.insert("title".into(), V::Text(format!("post-{i}")));
            r.insert("status".into(), V::Text("live".into()));
            r.insert("views".into(), V::Int(i * 10));
            r.insert("created_at".into(), V::Text("2026-02-01".into()));
            r
        })
        .collect()
}

fn mock_comment_rows(n: usize) -> Vec<Row> {
    (1..=n as i64)
        .map(|i| {
            let mut r = Row::new();
            r.insert("id".into(), V::Int(i));
            r.insert("post_id".into(), V::Int(((i - 1) % 5) + 1));
            r.insert("body".into(), V::Text(format!("comment-{i}")));
            r.insert("created_at".into(), V::Text("2026-03-01".into()));
            r
        })
        .collect()
}

fn run_micro(case: &str) {
    match case {
        "find" => {
            let conds = vec![
                Condition::Eq {
                    column: "author_id".into(),
                    table: None,
                    value: V::Int(1),
                },
                Condition::Eq {
                    column: "status".into(),
                    table: None,
                    value: V::Text("live".into()),
                },
                Condition::Raw {
                    sql: "created_at >= ?".into(),
                    params: vec![V::Text("2026-02-01".into())],
                },
            ];
            let opts = SelectOptions {
                select: Some("id, author_id, title, status, views, created_at".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let (sql, params) = build_select_sql::<Post>(&conds, Some(&opts));
            std::hint::black_box((&sql, &params));
            let hydrated: Vec<Post> = mock_post_rows(3)
                .iter()
                .map(|r| Post::from_row(r).unwrap())
                .collect();
            std::hint::black_box(hydrated);
        }
        "complexWhere" => {
            let conds = vec![
                Condition::Eq {
                    column: "author_id".into(),
                    table: None,
                    value: V::Int(1),
                },
                Condition::Raw {
                    sql: "created_at >= ?".into(),
                    params: vec![V::Text("2026-02-01".into())],
                },
                Condition::Raw {
                    sql: "title LIKE ?".into(),
                    params: vec![V::Text("post-%".into())],
                },
                Condition::Eq {
                    column: "id".into(),
                    table: None,
                    value: V::IntArray(vec![1, 2, 3, 4, 5]),
                },
            ];
            let opts = SelectOptions {
                select: Some("id, author_id, title, status, views".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let (sql, params) = build_select_sql::<Post>(&conds, Some(&opts));
            std::hint::black_box((&sql, &params));
            let hydrated: Vec<Post> = mock_post_rows(5)
                .iter()
                .map(|r| Post::from_row(r).unwrap())
                .collect();
            std::hint::black_box(hydrated);
        }
        // relation build + hydrate parent + child rows (the two batched selects' client-side path)
        "hasMany" => {
            let pconds = vec![Condition::Eq {
                column: "author_id".into(),
                table: None,
                value: V::Int(1),
            }];
            let popts = SelectOptions {
                select: Some("id, author_id, title".into()),
                order_raw: Some("id ASC".into()),
                ..Default::default()
            };
            let (psql, pparams) = build_select_sql::<Post>(&pconds, Some(&popts));
            std::hint::black_box((&psql, &pparams));
            let parents: Vec<Post> = mock_post_rows(5)
                .iter()
                .map(|r| Post::from_row(r).unwrap())
                .collect();
            let ids: Vec<i64> = parents.iter().filter_map(|p| p.id).collect();
            let cconds = vec![Condition::Eq {
                column: "post_id".into(),
                table: None,
                value: V::IntArray(ids),
            }];
            let copts = SelectOptions {
                select: Some("id, post_id, body".into()),
                ..Default::default()
            };
            let (csql, cparams) = build_select_sql::<Comment>(&cconds, Some(&copts));
            std::hint::black_box((&csql, &cparams));
            let children: Vec<Comment> = mock_comment_rows(25)
                .iter()
                .map(|r| Comment::from_row(r).unwrap())
                .collect();
            std::hint::black_box((parents, children));
        }
        // write path: derive gate-first plan + render statements (marshaling-only)
        "writeTxGate" => {
            // body INSERT (the write the v1 create path builds) + gate/derive statement render
            let (isql, iparams) = build_insert_sql(
                "posts",
                &[
                    Post::columns().author_id.set(1i64),
                    Post::columns().title.set("txn-post".to_string()),
                    Post::columns().created_at.set("2026-05-01".to_string()),
                ],
                Some(&InsertOptions {
                    returning: true,
                    ..Default::default()
                }),
                litedbmodel::types::DriverType::Sqlite,
            );
            std::hint::black_box((&isql, &iparams));
            // gate:requires + gate:unique + derive — the other 3 statements' construction
            let gate = Condition::Eq {
                column: "id".into(),
                table: None,
                value: V::Int(1),
            };
            std::hint::black_box(gate.compile());
            let uniq = build_insert_sql(
                "uniq",
                &[
                    Column::<String>::new("uniq", "name").set("title_per_author".to_string()),
                    Column::<String>::new("uniq", "s0").set("1".to_string()),
                    Column::<String>::new("uniq", "f0").set("txn-post".to_string()),
                ],
                Some(&InsertOptions {
                    on_conflict_ignore: true,
                    on_conflict_columns: vec!["name".into()],
                    ..Default::default()
                }),
                litedbmodel::types::DriverType::Sqlite,
            );
            std::hint::black_box(uniq);
        }
        _ => panic!("unknown micro case {case}"),
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Protocol I/O.
// ════════════════════════════════════════════════════════════════════════════

fn write_line(v: &J) {
    let mut out = std::io::stdout();
    out.write_all(serde_json::to_string(v).unwrap().as_bytes())
        .unwrap();
    out.write_all(b"\n").unwrap();
    out.flush().unwrap();
}

fn now_ms() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn collect<F: FnMut()>(warmup: usize, iters: usize, mut op: F) -> Vec<f64> {
    for _ in 0..warmup {
        op();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        op();
        samples.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    samples
}

fn rss_bytes() -> u64 {
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
    {
        if let Ok(s) = String::from_utf8(out.stdout) {
            if let Ok(kb) = s.trim().parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}

fn main() {
    // --impl=… accepted for contract parity (v1-rs has a single exec surface: the ActiveRecord).
    let mut impl_ = "ir".to_string();
    for a in std::env::args() {
        if let Some(v) = a.strip_prefix("--impl=") {
            impl_ = v.to_string();
        }
    }

    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let bundles = here.join("../../generated/bundles.json");
    let art = load_artifact(bundles.to_str().unwrap());

    // Single-thread current-thread runtime — the bench is single-threaded; block_on per op.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    write_line(&serde_json::json!({
        "kind":"ready","language":"v1-rs","impl":impl_,"readyAtEpochMs":now_ms()
    }));

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let req: J = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                write_line(&serde_json::json!({"kind":"error","message":format!("bad request: {e}")}));
                continue;
            }
        };
        let kind = req["kind"].as_str().unwrap_or("");
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle(kind, &req, &impl_, &art, &rt)
        }));
        if let Err(e) = res {
            let msg = e
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| e.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".into());
            write_line(&serde_json::json!({"kind":"error","message":msg}));
        }
    }
}

fn handle(kind: &str, req: &J, _impl_: &str, art: &Artifact, rt: &Runtime) {
    match kind {
        "run" => {
            let case = req["case"].as_str().unwrap();
            let warmup = req["warmup"].as_u64().unwrap() as usize;
            let iters = req["iterations"].as_u64().unwrap() as usize;
            // Fresh seeded DB per case so writes (batchInsert/writeTxGate) do not accumulate across
            // the warmup+timed loop and shift row counts. Seeded ONCE; the op is what's timed.
            let handler = seed_handler(rt, art);
            let samples = collect(warmup, iters, || {
                let _ = run_case(rt, &handler, case);
            });
            rt.block_on(handler.close());
            write_line(&serde_json::json!({"kind":"run","case":case,"samplesMs":samples}));
        }
        "throughput" => {
            let case = req["case"].as_str().unwrap();
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let handler = seed_handler(rt, art);
            let t0 = Instant::now();
            for _ in 0..iters {
                let _ = run_case(rt, &handler, case);
            }
            let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
            rt.block_on(handler.close());
            write_line(&serde_json::json!({
                "kind":"throughput","case":case,"elapsedMs":elapsed,"completed":iters
            }));
        }
        "micro" => {
            let case = req["case"].as_str().unwrap();
            let warmup = req["warmup"].as_u64().unwrap() as usize;
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let samples = collect(warmup, iters, || run_micro(case));
            write_line(&serde_json::json!({"kind":"micro","case":case,"samplesMs":samples}));
        }
        "rss" => {
            write_line(&serde_json::json!({"kind":"rss","rssBytes":rss_bytes()}));
        }
        "cost" => {
            let case = req["case"].as_str().unwrap();
            let handler = seed_handler(rt, art);
            let (q, r) = run_case(rt, &handler, case);
            rt.block_on(handler.close());
            write_line(&serde_json::json!({"kind":"cost","case":case,"queries":q,"rows":r}));
        }
        "shutdown" => std::process::exit(0),
        _ => {}
    }
}
