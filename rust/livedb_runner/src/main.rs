//! litedbmodel SCP LIVE-DB conformance — Rust runner (WS7g, #36).
//!
//! The Rust leg of the coordinated cross-language live-DB pass (spec §10 dialect axis). It loads
//! the WS7g live-DB corpus (`conformance/vectors-livedb/livedb.json` — the exec/tx bundles compiled
//! for `postgres` + `mysql`), connects to REAL dockerized Postgres + MySQL via the live `Driver`
//! seam (`PostgresDriver` / `MysqlDriver`), creates the needed tables in an ISOLATED per-language
//! namespace (Postgres schema / MySQL database `scp_rust`), and runs each bundle through the SAME
//! `execute_bundle` / `execute_transaction_bundle` the SQLite conformance uses. It asserts the
//! assembled result equals the frozen SQLite reference (`expectedResult` / `expectedDbState`) — the
//! §10 promise (same IR + input → same RESULT regardless of dialect).
//!
//! REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable it ERRORS OUT (exit 3). Prints the
//! machine JSON summary as its LAST stdout line:
//!   {"lang":"rust-livedb","suites":{"livedb-pg":{..},"livedb-mysql":{..}},"total_pass",...}
//! exit 0 all pass / 1 any fail / 2 corpus-version mismatch / 3 DB unreachable.

use std::path::PathBuf;
use std::process::ExitCode;

use litedbmodel_runtime::livedb::{MysqlDriver, PostgresDriver};
use litedbmodel_runtime::value::encode_value;
use litedbmodel_runtime::{
    execute_bundle_pooled, execute_transaction_bundle, read_bundle_pooled, Driver,
};
use serde_json::{json, Value as J};

const SUPPORTED_CORPUS_VERSION: i64 = 2;
const PG_SCHEMA: &str = "scp_rust";
const MYSQL_DB: &str = "scp_rust";

const ALL_TABLES: &[&str] = &[
    "post_tags",
    "order_lines",
    "comments",
    "posts",
    "tags",
    "docs",
    "docs2",
    "revs",
    "typed",
    "users",
    "users2",
    "idem",
    "uniq",
    "outbox",
];

fn corpus_path() -> PathBuf {
    if let Ok(env) = std::env::var("LITEDBMODEL_LIVEDB_VECTORS") {
        return PathBuf::from(env);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")); // rust/livedb_runner
    manifest
        .parent() // rust/
        .and_then(|p| p.parent()) // repo root
        .map(|p| {
            p.join("conformance")
                .join("vectors-livedb")
                .join("livedb.json")
        })
        .unwrap_or_else(|| PathBuf::from("conformance/vectors-livedb/livedb.json"))
}

fn env(k: &str, def: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| def.to_string())
}

// ── numeric-canon + structural equality (mirror of vectors_runner) ─────────────

fn numeric_canon(x: &J) -> J {
    match x {
        J::Object(o) => {
            if o.len() == 1 {
                if let Some(J::String(s)) = o.get("$bigint") {
                    if let Ok(i) = s.parse::<i64>() {
                        return J::Number(i.into());
                    }
                }
            }
            let mut m = serde_json::Map::new();
            for (k, v) in o {
                m.insert(k.clone(), numeric_canon(v));
            }
            J::Object(m)
        }
        J::Array(a) => J::Array(a.iter().map(numeric_canon).collect()),
        other => other.clone(),
    }
}

fn canonical_string(x: &J) -> String {
    match x {
        J::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", J::String((*k).clone()), canonical_string(&o[*k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        J::Array(a) => {
            let inner: Vec<String> = a.iter().map(canonical_string).collect();
            format!("[{}]", inner.join(","))
        }
        other => other.to_string(),
    }
}

fn eq(a: &J, b: &J) -> bool {
    canonical_string(&numeric_canon(a)) == canonical_string(&numeric_canon(b))
}

// ── schema lifecycle ───────────────────────────────────────────────────────────

fn schema_of(v: &J, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|s| s.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn reset_pg(pg: &PostgresDriver, schema: &[String]) -> Result<(), String> {
    let drops: Vec<String> = ALL_TABLES
        .iter()
        .map(|t| format!("DROP TABLE IF EXISTS {t} CASCADE"))
        .collect();
    pg.exec_ddl(&drops).map_err(|e| e.message)?;
    pg.exec_ddl(schema).map_err(|e| e.message)?;
    Ok(())
}

fn reset_mysql(my: &MysqlDriver, schema: &[String]) -> Result<(), String> {
    let mut stmts: Vec<String> = vec!["SET FOREIGN_KEY_CHECKS = 0".into()];
    stmts.extend(
        ALL_TABLES
            .iter()
            .map(|t| format!("DROP TABLE IF EXISTS {t}")),
    );
    stmts.push("SET FOREIGN_KEY_CHECKS = 1".into());
    my.exec_ddl(&stmts).map_err(|e| e.message)?;
    my.exec_ddl(schema).map_err(|e| e.message)?;
    Ok(())
}

// ── per-vector runs (driver is the live Driver trait object) ───────────────────

fn run_exec(driver: &(dyn Driver + Sync), bundle: &J, v: &J) -> Result<(), String> {
    let input = numeric_canon(&v["input"]);
    // The PRODUCTION live PG/MySQL read path: the pooled executor fans out independent sibling read
    // nodes of a plan stage concurrently (capped at the plan concurrency); a single-relation read
    // graph runs serially, byte-identical to `execute_bundle` (#40).
    let result = execute_bundle_pooled(bundle, &input, driver)
        .map_err(|e| format!("execute threw: {}", e.message))?;
    let got = encode_value(&result);
    if eq(&got, &v["expectedResult"]) {
        Ok(())
    } else {
        Err(format!("result {got} != {}", v["expectedResult"]))
    }
}

/// A read-RELATION EXECUTION vector: run the parent read (pooled #40 fan-out) + batch-load/hydrate
/// the `with` relations, compare to the PER-DIALECT golden (`expected_key` = expectedResultPg /
/// expectedResultMysql — a limited hasMany's `_rn` window column is present on MySQL but projected
/// away by PG's LATERAL form).
fn run_read(
    driver: &(dyn Driver + Sync),
    bundle: &J,
    v: &J,
    expected_key: &str,
) -> Result<(), String> {
    let input = numeric_canon(&v["input"]);
    let with_names: Vec<String> = v
        .get("with")
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let empty = std::collections::HashMap::new();
    let result = read_bundle_pooled(bundle, &input, driver, &with_names, &empty)
        .map_err(|e| format!("read threw: {}", e.message))?;
    let got = encode_value(&result);
    if eq(&got, &v[expected_key]) {
        Ok(())
    } else {
        Err(format!("result {got} != {}", v[expected_key]))
    }
}

/// A CROSS-DB read-RELATION vector (V0 R1): the parent runs on the PRIMARY driver and a TAGGED
/// relation on the SECONDARY driver (the target model's own DB). The caller seeds the secondary DB
/// with its own schema BEFORE this call (the parent DB has NO target table — a mis-route would fail
/// loudly), then the tagged relation is routed via the `connections` registry. A green hydrated
/// result is unforgeable proof the `connection` tag routed the batch to the secondary connection.
fn run_crossdb(
    driver: &(dyn Driver + Sync),
    secondary: &(dyn Driver + Sync),
    bundle: &J,
    v: &J,
    expected_key: &str,
) -> Result<(), String> {
    let input = numeric_canon(&v["input"]);
    let with_names: Vec<String> = v
        .get("with")
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let tag = v
        .get("connectionTag")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let mut connections: std::collections::HashMap<String, &(dyn Driver + Sync)> =
        std::collections::HashMap::new();
    connections.insert(tag, secondary);
    let result = read_bundle_pooled(bundle, &input, driver, &with_names, &connections)
        .map_err(|e| format!("cross-DB read threw: {}", e.message))?;
    let got = encode_value(&result);
    if eq(&got, &v[expected_key]) {
        Ok(())
    } else {
        Err(format!("result {got} != {}", v[expected_key]))
    }
}

fn run_tx(
    driver: &(dyn Driver + Sync),
    bundle: &J,
    v: &J,
    tx_expected_key: &str,
) -> Result<(), String> {
    let input = numeric_canon(&v["input"]);
    let result = execute_transaction_bundle(bundle, &input, driver)
        .map_err(|e| format!("tx threw: {}", e.message))?;
    let got = encode_value(&result);
    // A write may GENUINELY diverge by dialect (DELETE…RETURNING returns rows on PG, [] on MySQL);
    // the mysql leg then carries `expectedResultMysql`. Fall back to the shared `expectedResult`.
    let expected = v.get(tx_expected_key).unwrap_or(&v["expectedResult"]);
    if !eq(&got, expected) {
        return Err(format!("result {got} != {expected}"));
    }
    if let Some(states) = v.get("expectedDbState").and_then(|s| s.as_array()) {
        for s in states {
            let query = s["query"].as_str().unwrap_or("");
            let mut stmt = driver.prepare(query);
            let rows = stmt
                .all(&[])
                .map_err(|e| format!("db-state '{query}' threw: {}", e.message))?;
            let got_rows = J::Array(rows.iter().map(encode_value).collect());
            if !eq(&got_rows, &s["rows"]) {
                return Err(format!("db-state '{query}': {got_rows} != {}", s["rows"]));
            }
        }
    }
    Ok(())
}

struct Tally {
    pass: i64,
    fail: i64,
}

#[allow(clippy::too_many_arguments)]
fn run_dialect_leg(
    name: &str,
    driver: &(dyn Driver + Sync),
    reset: &mut dyn FnMut(&[String]) -> Result<(), String>,
    secondary: &(dyn Driver + Sync),
    secondary_reset: &mut dyn FnMut(&[String]) -> Result<(), String>,
    secondary_schema_key: &str,
    vectors: &[J],
    bundle_key: &str,
    schema_key: &str,
    read_expected_key: &str,
) -> Tally {
    let mut t = Tally { pass: 0, fail: 0 };
    eprintln!("\nlivedb-{name} — {} vectors (real {name})", vectors.len());
    for v in vectors {
        let vname = v.get("name").and_then(|n| n.as_str()).unwrap_or("?");
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        // CROSS-DB vectors carry their OWN primary schema key (the parent DB — NO target table).
        let primary_schema_key = if kind == "crossdb" {
            if name == "pg" {
                "primarySchemaPg"
            } else {
                "primarySchemaMysql"
            }
        } else {
            schema_key
        };
        if let Err(e) = reset(&schema_of(v, primary_schema_key)) {
            t.fail += 1;
            eprintln!("  XX  {vname}\n      reset: {e}");
            continue;
        }
        if kind == "crossdb" {
            if let Err(e) = secondary_reset(&schema_of(v, secondary_schema_key)) {
                t.fail += 1;
                eprintln!("  XX  {vname}\n      secondary reset: {e}");
                continue;
            }
        }
        let bundle = &v[bundle_key];
        let res = match kind {
            "exec" => run_exec(driver, bundle, v),
            "read" => run_read(driver, bundle, v, read_expected_key),
            "crossdb" => run_crossdb(driver, secondary, bundle, v, read_expected_key),
            "tx" => run_tx(driver, bundle, v, read_expected_key),
            other => Err(format!("unknown kind {other}")),
        };
        match res {
            Ok(()) => {
                t.pass += 1;
                eprintln!("  ok  {vname}");
            }
            Err(e) => {
                t.fail += 1;
                eprintln!("  XX  {vname}\n      {e}");
            }
        }
    }
    t
}

fn main() -> ExitCode {
    eprintln!("litedbmodel SCP LIVE-DB conformance — Rust runner (real PG + MySQL)");

    let corpus: J = match std::fs::read_to_string(corpus_path())
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(c) => c,
        None => {
            eprintln!("FAIL: cannot load live-DB corpus");
            println!(
                "{}",
                json!({"lang":"rust-livedb","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true})
            );
            return ExitCode::from(2);
        }
    };
    if corpus.get("corpusVersion").and_then(|c| c.as_i64()) != Some(SUPPORTED_CORPUS_VERSION) {
        eprintln!("FAIL-CLOSED: corpusVersion mismatch");
        println!(
            "{}",
            json!({"lang":"rust-livedb","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true})
        );
        return ExitCode::from(2);
    }
    let vectors = corpus
        .get("vectors")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Postgres: connect to base testdb, create + enter the per-language schema.
    let pg_host = env("TEST_DB_HOST", "localhost");
    let pg_port = env("TEST_DB_PORT", "5433");
    let pg_conn = format!(
        "host={} port={} user={} password={} dbname={} options=--search_path={}",
        pg_host,
        pg_port,
        env("TEST_DB_USER", "testuser"),
        env("TEST_DB_PASSWORD", "testpass"),
        env("TEST_DB_NAME", "testdb"),
        PG_SCHEMA
    );
    let pg = match PostgresDriver::connect(&pg_conn) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "FATAL: Postgres unreachable at {pg_host}:{pg_port} — {}",
                e.message
            );
            return ExitCode::from(3);
        }
    };
    if let Err(e) = pg.exec_ddl(&[
        format!("CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}"),
        format!("SET search_path TO {PG_SCHEMA}"),
    ]) {
        eprintln!("FATAL: cannot create PG schema — {}", e.message);
        return ExitCode::from(3);
    }

    // MySQL: connect to base testdb, create the per-language database, reconnect into it.
    let my_host = env("TEST_MYSQL_HOST", "127.0.0.1");
    let my_port = env("TEST_MYSQL_PORT", "3307");
    let my_user = env("TEST_MYSQL_USER", "testuser");
    let my_pass = env("TEST_MYSQL_PASSWORD", "testpass");
    let boot_url = format!(
        "mysql://{my_user}:{my_pass}@{my_host}:{my_port}/{}",
        env("TEST_MYSQL_DB", "testdb")
    );
    let boot = match MysqlDriver::connect(&boot_url) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "FATAL: MySQL unreachable at {my_host}:{my_port} — {}",
                e.message
            );
            return ExitCode::from(3);
        }
    };
    if let Err(e) = boot.exec_ddl(&[format!("CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")]) {
        eprintln!("FATAL: cannot create MySQL database — {}", e.message);
        return ExitCode::from(3);
    }
    drop(boot);
    let my_url = format!("mysql://{my_user}:{my_pass}@{my_host}:{my_port}/{MYSQL_DB}");
    let my = match MysqlDriver::connect(&my_url) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("FATAL: MySQL (scp_rust) unreachable — {}", e.message);
            return ExitCode::from(3);
        }
    };

    // CROSS-DB (V0 R1): each leg's SECONDARY connection is the OTHER live DB (pg leg → my; mysql leg
    // → pg), reset with the OTHER dialect's reset fn + the vector's per-leg secondary schema.
    let pg_t = {
        let mut reset = |schema: &[String]| reset_pg(&pg, schema);
        let mut secondary_reset = |schema: &[String]| reset_mysql(&my, schema);
        run_dialect_leg(
            "pg",
            &pg,
            &mut reset,
            &my,
            &mut secondary_reset,
            "secondarySchemaPg",
            &vectors,
            "bundlePg",
            "schemaPg",
            "expectedResultPg",
        )
    };
    let my_t = {
        let mut reset = |schema: &[String]| reset_mysql(&my, schema);
        let mut secondary_reset = |schema: &[String]| reset_pg(&pg, schema);
        run_dialect_leg(
            "mysql",
            &my,
            &mut reset,
            &pg,
            &mut secondary_reset,
            "secondarySchemaMysql",
            &vectors,
            "bundleMysql",
            "schemaMysql",
            "expectedResultMysql",
        )
    };

    let total_pass = pg_t.pass + my_t.pass;
    let total_fail = pg_t.fail + my_t.fail;
    eprintln!(
        "\n{total_pass} passed, {total_fail} failed / {} live-DB vectors",
        total_pass + total_fail
    );
    println!(
        "{}",
        json!({
            "lang": "rust-livedb",
            "suites": {
                "livedb-pg": {"pass": pg_t.pass, "fail": pg_t.fail},
                "livedb-mysql": {"pass": my_t.pass, "fail": my_t.fail},
            },
            "total_pass": total_pass,
            "total_fail": total_fail,
            "version_mismatch": false,
        })
    );
    if total_fail > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
