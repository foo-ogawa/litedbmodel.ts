//! Test-only native/interpreter parity harness. No bundle or parser dependency reaches native crates.

#[path = "generated/create_mysql.rs"]
mod create_mysql;
#[path = "generated/create_postgres.rs"]
mod create_postgres;
#[path = "generated/create_sqlite.rs"]
mod create_sqlite;
#[path = "generated/fixture.rs"]
mod fixture;
#[path = "generated/nestedCreate_mysql.rs"]
mod nested_create_mysql;
#[path = "generated/nestedCreate_postgres.rs"]
mod nested_create_postgres;
#[path = "generated/nestedCreate_sqlite.rs"]
mod nested_create_sqlite;
#[path = "generated/nestedRelations_mysql.rs"]
mod nested_relations_mysql;
#[path = "generated/nestedRelations_postgres.rs"]
mod nested_relations_postgres;
#[path = "generated/nestedRelations_sqlite.rs"]
mod nested_relations_sqlite;
#[path = "generated/relation_limit_mysql.rs"]
mod relation_limit_mysql;
#[path = "generated/relation_limit_postgres.rs"]
mod relation_limit_postgres;
#[path = "generated/relation_limit_sqlite.rs"]
mod relation_limit_sqlite;
#[path = "generated/setup_mysql.rs"]
mod setup_mysql;
#[path = "generated/setup_postgres.rs"]
mod setup_postgres;
#[path = "generated/setup_sqlite.rs"]
mod setup_sqlite;

use litedbmodel_interpreter::{
    execute_bundle, execute_transaction_bundle, read_bundle, RuntimeError, Value,
};
use litedbmodel_runtime::{Driver, SqliteDriver};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};

fn open(spec: &str) -> Box<dyn Driver> {
    #[cfg(feature = "livedb")]
    {
        if let Some(conn) = spec.strip_prefix("pg:") {
            return Box::new(PostgresDriver::connect(conn).expect("connect postgres"));
        }
        if let Some(url) = spec.strip_prefix("mysql:") {
            return Box::new(MysqlDriver::connect(url).expect("connect mysql"));
        }
    }
    Box::new(SqliteDriver::open(spec).expect("open sqlite"))
}

fn reseed(dialect: &str, driver: &dyn Driver) {
    let statements = match dialect {
        "sqlite" => setup_sqlite::STATEMENTS,
        "postgres" => setup_postgres::STATEMENTS,
        "mysql" => setup_mysql::STATEMENTS,
        _ => panic!("unsupported dialect"),
    };
    for sql in statements {
        driver.prepare(sql).run(&[]).expect("oracle seed statement");
    }
}

fn canonical(value: &Value) -> Vec<u8> {
    fn put(value: &Value, out: &mut Vec<u8>) {
        match value {
            Value::Null => out.push(b'0'),
            Value::Bool(v) => out.extend_from_slice(if *v { b"b1" } else { b"b0" }),
            Value::Int(v) => out.extend_from_slice(format!("i{v};").as_bytes()),
            Value::Float(v) => out.extend_from_slice(format!("f{:016x};", v.to_bits()).as_bytes()),
            Value::Str(v) => {
                out.extend_from_slice(format!("s{}:", v.len()).as_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            Value::Arr(values) => {
                out.extend_from_slice(format!("a{}[", values.len()).as_bytes());
                for value in values {
                    put(value, out);
                }
                out.push(b']');
            }
            Value::Obj(fields) => {
                out.extend_from_slice(format!("o{}{{", fields.len()).as_bytes());
                for (key, value) in fields {
                    out.extend_from_slice(format!("{}:", key.len()).as_bytes());
                    out.extend_from_slice(key.as_bytes());
                    put(value, out);
                }
                out.push(b'}');
            }
        }
    }
    let mut out = Vec::new();
    put(value, &mut out);
    out
}

fn state(driver: &dyn Driver) -> Value {
    Value::Obj(vec![
        (
            "users".into(),
            Value::Arr(
                driver
                    .prepare("SELECT id, email, name FROM benchmark_users ORDER BY id")
                    .all(&[])
                    .expect("users state"),
            ),
        ),
        (
            "posts".into(),
            Value::Arr(
                driver
                    .prepare("SELECT id, title, author_id FROM benchmark_posts ORDER BY id")
                    .all(&[])
                    .expect("posts state"),
            ),
        ),
    ])
}

fn native_nested(dialect: &str, driver: &dyn Driver) -> Value {
    macro_rules! run {
        ($module:ident) => {{
            let parents =
                $module::run(driver, $module::InNRFindAll).expect("native nested parents");
            let tree = $module::hydrate_posts(parents, driver).expect("native nested hydrate");
            Value::Arr(
                tree.into_iter()
                    .map(|(user, posts)| {
                        Value::Obj(vec![
                            ("id".into(), Value::Int(user.id)),
                            ("email".into(), Value::Str(user.email)),
                            ("name".into(), Value::Str(user.name)),
                            (
                                "posts".into(),
                                Value::Arr(
                                    posts.into_iter().map(|(post, comments)| {
                                        Value::Obj(vec![
                                            ("id".into(), Value::Int(post.id)),
                                            ("title".into(), Value::Str(post.title)),
                                            ("author_id".into(), Value::Int(post.author_id)),
                                            ("comments".into(), Value::Arr(comments.into_iter().map(|comment| {
                                                Value::Obj(vec![
                                                    ("id".into(), Value::Int(comment.id)),
                                                    ("body".into(), Value::Str(comment.body)),
                                                    ("post_id".into(), Value::Int(comment.post_id)),
                                                ])
                                            }).collect())),
                                        ])
                                    }).collect()),
                            ),
                        ])
                    }).collect(),
            )
        }};
    }
    match dialect {
        "sqlite" => run!(nested_relations_sqlite),
        "postgres" => run!(nested_relations_postgres),
        "mysql" => run!(nested_relations_mysql),
        _ => panic!("unsupported dialect"),
    }
}

fn native_create(dialect: &str, driver: &dyn Driver) -> Value {
    macro_rules! run {
        ($module:ident) => {{
            let rows = $module::run(
                driver,
                $module::InNRCreate {
                    email: "new@bench.com".into(),
                    name: "New".into(),
                },
            )
            .expect("native create");
            Value::Arr(
                rows.into_iter()
                    .map(|row| {
                        Value::Obj(vec![
                            ("changes".into(), Value::Int(row.changes)),
                            ("lastInsertRowid".into(), Value::Int(row.lastInsertRowid)),
                        ])
                    })
                    .collect(),
            )
        }};
    }
    match dialect {
        "sqlite" => run!(create_sqlite),
        "postgres" => run!(create_postgres),
        "mysql" => run!(create_mysql),
        _ => panic!("unsupported dialect"),
    }
}

fn native_nested_create(dialect: &str, driver: &dyn Driver) -> bool {
    macro_rules! run {
        ($module:ident) => {{
            $module::run(
                driver,
                $module::InNRNestedCreate {
                    email: "nc@bench.com".into(),
                    name: "NC".into(),
                    title: "NC Post".into(),
                },
            )
            .expect("native tx")
        }};
    }
    match dialect {
        "sqlite" => run!(nested_create_sqlite),
        "postgres" => run!(nested_create_postgres),
        "mysql" => run!(nested_create_mysql),
        _ => panic!("unsupported dialect"),
    }
}

fn relation_limit_native(dialect: &str, driver: &dyn Driver) -> RuntimeError {
    macro_rules! run {
        ($m:ident) => {{
            let parents = $m::run(driver, $m::InNRFindAll).expect("limit parents");
            match $m::hydrate_posts(parents, driver) {
                Err(error) => error,
                Ok(_) => panic!("relation limit must fail"),
            }
        }};
    }
    match dialect {
        "sqlite" => run!(relation_limit_sqlite),
        "postgres" => run!(relation_limit_postgres),
        "mysql" => run!(relation_limit_mysql),
        _ => panic!("unsupported dialect"),
    }
}

fn limit_fields(error: RuntimeError) -> (i64, i64, String, Option<String>, Option<String>) {
    let limit = error.as_limit().expect("RuntimeError::Limit");
    (
        limit.limit,
        limit.count,
        limit.context.clone(),
        limit.model.clone(),
        limit.relation.clone(),
    )
}

fn run(dialect: &str, spec: &str) {
    let driver = open(spec);
    let d = driver.as_ref();

    reseed(dialect, d);
    let native = native_nested(dialect, d);
    reseed(dialect, d);
    let interpreted = read_bundle(
        &fixture::bundle("nestedRelations", dialect),
        &fixture::input("nestedRelations", dialect),
        d,
        &["posts".into()],
    )
    .expect("interpreter nested");
    assert_eq!(
        canonical(&native),
        canonical(&interpreted),
        "full nested relation result"
    );

    reseed(dialect, d);
    let native_create = native_create(dialect, d);
    let native_create_state = state(d);
    reseed(dialect, d);
    let interpreted_create = execute_bundle(
        &fixture::bundle("create", dialect),
        &fixture::input("create", dialect),
        d,
    )
    .expect("interpreter create");
    let interpreted_create_state = state(d);
    assert_eq!(
        (canonical(&native_create), canonical(&native_create_state)),
        (
            canonical(&interpreted_create),
            canonical(&interpreted_create_state)
        ),
        "write return/state"
    );

    reseed(dialect, d);
    let native_tx = native_nested_create(dialect, d);
    let native_tx_state = state(d);
    reseed(dialect, d);
    let interpreted_tx = execute_transaction_bundle(
        &fixture::bundle("nestedCreate", dialect),
        &fixture::input("nestedCreate", dialect),
        d,
    )
    .expect("interpreter tx");
    let interpreted_committed = match &interpreted_tx {
        Value::Obj(fields) => fields
            .iter()
            .find(|(key, _)| key == "committed")
            .is_some_and(|(_, value)| matches!(value, Value::Bool(true))),
        _ => false,
    };
    let interpreted_tx_state = state(d);
    assert_eq!(
        (native_tx, canonical(&native_tx_state)),
        (interpreted_committed, canonical(&interpreted_tx_state)),
        "tx return/state"
    );

    reseed(dialect, d);
    let native_limit = relation_limit_native(dialect, d);
    reseed(dialect, d);
    let interpreted_limit = read_bundle(
        &fixture::bundle("relationLimit", dialect),
        &fixture::input("relationLimit", dialect),
        d,
        &["posts".into()],
    )
    .expect_err("interpreter relation limit");
    assert_eq!(
        limit_fields(native_limit),
        limit_fields(interpreted_limit),
        "relation limit variant/fields"
    );
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    run(args.get(1).expect("dialect"), args.get(2).expect("spec"));
}
