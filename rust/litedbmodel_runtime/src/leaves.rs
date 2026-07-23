//! litedbmodel v2 SCP — the op-INDEPENDENT runtime leaves (#141 / #164), Rust wire-passthrough port
//! of `src/scp/leaves.ts`.
//!
//! The whole per-DSL execution surface is THREE op-agnostic leaves — the transport symbols the
//! native codegen calls DIRECTLY at each covered node (`execute_sql` / `pluck_keys` /
//! `group_children`, matching `LEAF_TRANSPORT_SYMBOLS` in leaves.ts). They are NOT per-op and own NO
//! grouping logic of their own: `pluck_keys` / `group_children` delegate to the shared grouping CORE
//! ([`crate::grouping`]) — the SAME SSoT the runtime lazy/relation path ([`crate::relation`])
//! consumes (no duplicated dedupe/grouping). `execute_sql` funnels through the central execute/run
//! seam ([`crate::exec_context`]) — the ONLY driver contact.
//!
//! ## Wire-passthrough (#164) — the leaves speak `WireValue`, the core speaks `Value`
//!
//! Under #164 the covered runner holds intermediate node results as RAW wire (`Vec<WireValue>`) and
//! NEVER de-boxes them; only the terminal node de-boxes to its concrete outType. So these leaves
//! take/return the BC-owned [`WireValue`] (crate::wire) and convert `WireValue`↔[`Value`] at the
//! boundary to reach the Value-based grouping core — the codec is the ONLY place the two value models
//! meet (the grouping stays single-sourced; the wire types add a boundary codec, not a second core).
//!
//! ## Ambient driver — the leaves are free functions, the driver is scoped
//!
//! The covered runner (`run_native_raw_struct_<comp>`) takes NO driver argument — it calls the leaf
//! transport symbols as free functions. `execute_sql` resolves the driver from a thread-scoped
//! ambient set by [`with_ambient_driver`] (the consumer brackets each op call). This is the rust
//! analogue of the TS `LeafContext.exec` bc injects at `bindBehaviors` time (C4 — never on the IR).

use std::cell::Cell;

use behavior_contracts::Value;

use crate::driver::Driver;
use crate::exec_context::{self, StatementIntent};
use crate::sql_render::render_placeholders;
use crate::wire::{BehaviorError, WireList, WireRow, WireValue};

// ── Ambient driver (thread-scoped) ───────────────────────────────────────────────────────────────

/// A type-erased pointer to the ambient [`Driver`]. Set only for the duration of a
/// [`with_ambient_driver`] scope (which brackets the whole covered-op call), then restored — so the
/// pointer never outlives the borrow it was made from.
#[derive(Clone, Copy)]
struct DriverPtr(*const (dyn Driver + 'static));

thread_local! {
    static AMBIENT_DRIVER: Cell<Option<DriverPtr>> = const { Cell::new(None) };
}

/// Run `f` with `driver` installed as the thread's ambient driver (the covered runner's
/// `execute_sql` transport resolves it). The previous ambient is restored on return / unwind, so
/// scopes nest. The consumer brackets each covered-op call with this (the driver argument the
/// op-agnostic leaves no longer take explicitly).
pub fn with_ambient_driver<R>(driver: &dyn Driver, f: impl FnOnce() -> R) -> R {
    // SAFETY: the raw pointer is installed ONLY for the span of `f` and cleared before this function
    // returns (the `Restore` guard runs on normal return AND on unwind), so it can never be
    // dereferenced after `driver`'s borrow ends. The lifetime is erased to `'static` to store it in
    // the thread-local; every read (`current_driver`) reborrows it with a shorter, call-scoped
    // lifetime bounded by this frame.
    let erased: *const (dyn Driver + 'static) =
        unsafe { std::mem::transmute::<*const dyn Driver, *const (dyn Driver + 'static)>(driver) };
    let prev = AMBIENT_DRIVER.with(|c| c.replace(Some(DriverPtr(erased))));

    struct Restore(Option<DriverPtr>);
    impl Drop for Restore {
        fn drop(&mut self) {
            AMBIENT_DRIVER.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(prev);
    f()
}

/// The current ambient driver, or a fail-closed [`BehaviorError`] if none is installed (the consumer
/// must bracket the op call with [`with_ambient_driver`]). The returned reference is bounded by the
/// caller's frame (SAFETY note on [`with_ambient_driver`]).
fn current_driver() -> Result<&'static dyn Driver, BehaviorError> {
    AMBIENT_DRIVER.with(|c| c.get()).map(|p| unsafe { &*p.0 }).ok_or_else(|| {
        BehaviorError::new(
            "NO_AMBIENT_DRIVER",
            "scp leaf: execute_sql called with no ambient driver — bracket the op with with_ambient_driver",
        )
    })
}

// ── Transaction scope for the covered plane (the CONSUMER's tx-boundary responsibility) ────────────
//
// The DB transaction boundary (BEGIN/COMMIT/ROLLBACK + atomicity) is litedbmodel's job, NOT a bc
// feature and NOT emitted into the generated runner — the covered runner just runs its body statements
// via `execute_sql` and returns `Result`. THIS wrapper brackets that runner in a transaction using the
// EXISTING tx primitives ([`Driver::begin_tx`] issues BEGIN; [`TxConnection::commit`]/[`rollback`] issue
// COMMIT/ROLLBACK on the owned connection) and the EXISTING ambient-driver mechanism
// ([`with_ambient_driver`]) — no new tx execution engine, no parallel exec path. Statement execution
// stays the ONE seam ([`execute_sql`] → [`exec_context`]); only the tx-control is added around it.

/// A [`Driver`] adapter over a tx's OWNED [`TxConnection`]: it forwards every prepared statement to the
/// pinned tx connection, so a covered runner's `execute_sql` (which resolves the ambient driver and runs
/// through the central seam) executes ON the transaction. Installed as the ambient driver for the span
/// of the tx body by [`with_ambient_transaction`]. `dialect` mirrors the underlying driver so
/// `execute_sql`'s `?`→`$N` placeholder render is unchanged.
struct TxDriver<'a> {
    tx: std::cell::RefCell<Box<dyn crate::exec_context::TxConnection + 'a>>,
    dialect: &'static str,
}

/// A [`PreparedStatement`] over the tx connection: `all`/`run` forward the (already placeholder-rendered)
/// SQL to the pinned tx connection ([`crate::exec_context::TxConnection`]). The tx connection is the ONE
/// connection the whole BEGIN…COMMIT runs on (the owned-connection contract).
struct TxPrepared<'a, 'd> {
    driver: &'d TxDriver<'a>,
    sql: String,
}

impl crate::driver::PreparedStatement for TxPrepared<'_, '_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, crate::errors::SqlFailure> {
        self.driver.tx.borrow_mut().execute(&self.sql, params)
    }
    fn run(&mut self, params: &[Value]) -> Result<crate::driver::RunInfo, crate::errors::SqlFailure> {
        self.driver.tx.borrow_mut().run(&self.sql, params)
    }
}

impl Driver for TxDriver<'_> {
    fn dialect(&self) -> &'static str {
        self.dialect
    }
    fn prepare(&self, sql: &str) -> Box<dyn crate::driver::PreparedStatement + '_> {
        Box::new(TxPrepared { driver: self, sql: sql.to_string() })
    }
    // A covered tx body never opens a NESTED transaction (the ambient IS the tx); fail closed rather
    // than silently begin a second BEGIN on the same connection.
    fn begin_tx(&self) -> Result<Box<dyn crate::exec_context::TxConnection + '_>, crate::errors::SqlFailure> {
        Err(nested_tx_unsupported())
    }
    fn acquire_tx(&self) -> Result<Box<dyn crate::exec_context::TxConnection + '_>, crate::errors::SqlFailure> {
        Err(nested_tx_unsupported())
    }
}

/// A tx-pinned driver has no nested-tx path (the ambient IS the open transaction) — fail closed.
fn nested_tx_unsupported() -> crate::errors::SqlFailure {
    crate::errors::SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: "scp tx-scope: nested transaction on a tx-pinned driver is not supported".into(),
    }
}

/// Run `body` inside a transaction on `driver`, threading the tx connection as the ambient driver so a
/// covered runner's `execute_sql` executes on it: [`Driver::begin_tx`] (BEGIN) → run `body` under the
/// tx-pinned ambient → COMMIT on `Ok` / ROLLBACK on `Err` (the atomicity guarantee). A body error rolls
/// back and re-raises; a COMMIT that itself fails rolls back and surfaces the error. This is the covered
/// plane's tx boundary — the runtime owns it (the generated runner emits NO BEGIN/COMMIT).
pub fn with_ambient_transaction<R>(
    driver: &dyn Driver,
    body: impl FnOnce() -> Result<R, BehaviorError>,
) -> Result<R, BehaviorError> {
    let tx = driver.begin_tx().map_err(sql_failure_to_behavior_error)?; // BEGIN issued on the owned connection
    let tx_driver = TxDriver { tx: std::cell::RefCell::new(tx), dialect: driver.dialect() };
    let result = with_ambient_driver(&tx_driver, body);
    let tx = tx_driver.tx.into_inner();
    match result {
        Ok(r) => tx.commit().map(|_| r).map_err(sql_failure_to_behavior_error),
        Err(e) => {
            let _ = tx.rollback(); // best-effort; surface the ORIGINAL body error
            Err(e)
        }
    }
}

// ── WireValue ↔ Value boundary codec (the ONLY place the two value models meet) ───────────────────

/// A BC-owned [`WireValue`] → bc [`Value`] (the input to the SQL param binder + the grouping core).
/// A `Num` rides as raw text: an integral literal → [`Value::Int`] (the driver's INTEGER model, so a
/// key round-trips to the SAME identity the grouping core keys on), else [`Value::Float`].
fn wire_to_value(w: &WireValue) -> Value {
    match w {
        WireValue::Str(s) => Value::Str(s.clone()),
        WireValue::Num(s) => {
            if let Ok(i) = s.parse::<i64>() {
                Value::Int(i)
            } else if let Ok(f) = s.parse::<f64>() {
                Value::Float(f)
            } else {
                Value::Str(s.clone())
            }
        }
        WireValue::Bool(b) => Value::Bool(*b),
        WireValue::Null => Value::Null,
        WireValue::Row(r) => Value::Obj(r.entries.iter().map(|(k, v)| (k.clone(), wire_to_value(v))).collect()),
        WireValue::List(l) => Value::Arr(l.items.iter().map(wire_to_value).collect()),
    }
}

/// A bc [`Value`] → BC-owned [`WireValue`] (the leaf output the covered runner de-boxes). Numbers
/// ride as raw text (the de-box parses + range-checks — overflow is BC's to detect).
fn value_to_wire(v: Value) -> WireValue {
    match v {
        Value::Null => WireValue::Null,
        Value::Bool(b) => WireValue::Bool(b),
        Value::Int(i) => WireValue::Num(i.to_string()),
        Value::Float(f) => WireValue::Num(f.to_string()),
        Value::Str(s) => WireValue::Str(s),
        Value::Arr(a) => WireValue::List(WireList { items: a.into_iter().map(value_to_wire).collect() }),
        Value::Obj(o) => WireValue::Row(WireRow { entries: o.into_iter().map(|(k, v)| (k, value_to_wire(v))).collect() }),
    }
}

/// Adapt a transport-level SQL failure to the shared [`BehaviorError`] the covered runner transports.
/// A SQL failure carries no de-box Error Value (that is the type-mismatch classifier's job), so
/// `detail` is `None`.
fn sql_failure_to_behavior_error(e: crate::errors::SqlFailure) -> BehaviorError {
    BehaviorError::new("SQL_FAILURE", e.message)
}

// ── execute_sql — the SOLE op-independent SQL transport ────────────────────────────────────────────

/// The SOLE SQL transport leaf (leaves.ts `executeSQL`). Binds `params` and runs `sql` through the
/// central seam ([`exec_context::execute`] / [`exec_context::run`]) on the AMBIENT driver — the ONLY
/// driver contact. `write` selects `run` (INSERT/UPDATE/DELETE) vs `execute` (SELECT / RETURNING); a
/// non-returning write returns a one-row `[{changes,lastInsertRowid}]` summary so the leaf output
/// shape is uniform (a `List` of `Row`). `?`→`$N` is rendered here (the transport's placeholder SSoT,
/// matching the TS `prepareSql`); an array param (a relation key set) rides as [`Value::Arr`], which
/// the driver encodes per dialect (json_each / native array). Ports are spread alphabetically by the
/// native emitter: `(bigint, params, returning, sql, write)`.
pub fn execute_sql(
    bigint: bool,
    params: &[WireValue],
    returning: bool,
    sql: &str,
    write: bool,
) -> Result<WireValue, BehaviorError> {
    // `bigint` is the better-sqlite3 #59 safe-integers toggle; rust/PG/MySQL return BIGINT natively
    // (i64), so there is no exact-integer read mode to select (see exec_context docs) — the port is
    // accepted for signature parity with the TS leaf and does not branch the rust seam.
    let _ = bigint;
    let driver = current_driver()?;
    let rendered = render_placeholders(sql, driver.dialect());
    let value_params: Vec<Value> = params.iter().map(wire_to_value).collect();
    let ctx = exec_context::for_driver(driver);
    if write && !returning {
        let info = exec_context::run(&ctx, &rendered, &value_params, &StatementIntent::write())
            .map_err(sql_failure_to_behavior_error)?;
        // The affected-write summary row (uniform `items` output shape — TS `writeSummary`).
        Ok(WireValue::List(WireList {
            items: vec![WireValue::Row(WireRow {
                entries: vec![
                    ("changes".to_string(), WireValue::int(info.changes)),
                    ("lastInsertRowid".to_string(), WireValue::int(info.last_insert_rowid)),
                ],
            })],
        }))
    } else {
        let rows = exec_context::execute(&ctx, &rendered, &value_params, &StatementIntent::read())
            .map_err(sql_failure_to_behavior_error)?;
        Ok(WireValue::List(WireList { items: rows.into_iter().map(value_to_wire).collect() }))
    }
}

// ── pluck_keys — rows + column → the deduped key array (the `= ANY($1)` batch key set) ──────────────

/// Extract the deduped, non-null key array from `rows[col]` — the batch key set a relation child
/// fetch binds to `WHERE fk = ANY($1)` / `json_each(?)`. Insertion order preserved; a null/absent key
/// is dropped (no partial keys). Dedupe is the shared grouping core ([`crate::grouping::dedupe_key_tuples`])
/// — the SAME SSoT the runtime relation path uses (no duplicated grouping). `col` is the ordered
/// parent-key column TUPLE (single-key → 1 column; composite → the tuple): single-key emits a flat
/// scalar key array (`json_each` scalar `value`), composite emits an array-of-tuples (`json_each`
/// per-ordinal `$[i]`) — the SAME shape `relation.ts bindKeys` produces for the MySQL/SQLite JSON
/// param. Ports are spread alphabetically by the native emitter: `(col, rows)`.
pub fn pluck_keys(col: &[String], rows: &[WireValue]) -> Result<WireValue, BehaviorError> {
    // The grouping core keys DIRECTLY on `WireValue` — no `WireValue`→`Value` conversion (the read path
    // never boxes into bc's `Value`). `col` is the ordered key-column tuple spread as an owned `Vec<String>`.
    let tuples = crate::grouping::dedupe_key_tuples(rows, col);
    let keys: Vec<WireValue> = if col.len() == 1 {
        tuples.into_iter().map(|mut t| t.remove(0)).collect()
    } else {
        tuples
            .into_iter()
            .map(|t| WireValue::List(WireList { items: t }))
            .collect()
    };
    Ok(WireValue::List(WireList { items: keys }))
}

// ── group_children — parents + flat children → each parent with its children nested ────────────────

/// Distribute a flat `children` list onto `parents` by matching `child[fk]` to `parent[pk]`, nesting
/// the result under `into`. `single == true` (belongsTo/hasOne) nests the one matching child (or
/// null); otherwise (hasMany) nests the child list (`[]` when none). Grouping is the shared core
/// ([`crate::grouping::group_by_key`] / [`crate::grouping::attach_to_parent`]) — the SAME SSoT the
/// runtime relation path uses (no duplicated grouping). `pk`/`fk` are the ordered parent/child key-
/// column TUPLES (single-key → 1 column; composite → the tuple) — the core keys on the WHOLE tuple
/// identity, so a composite relation nests by the full key (no scalar-collapse cartesian). Each parent
/// is shallow-copied before the own-key set (matching the TS `{...par, [into]: …}` spread — the input
/// is not mutated). Ports are spread alphabetically by the native emitter: `(children, fk, into,
/// parents, pk, single)`.
pub fn group_children(
    children: &[WireValue],
    fk: &[String],
    into: &str,
    parents: &[WireValue],
    pk: &[String],
    single: bool,
) -> Result<WireValue, BehaviorError> {
    // The grouping core keys DIRECTLY on `WireValue` (no `WireValue`↔`Value` conversion). The buckets
    // hold REFERENCES into `children` — no per-child clone; a matched child is cloned exactly once, when
    // `attach_to_parent` nests it into a parent's output.
    let by_key = crate::grouping::group_by_key(children, fk);
    let out: Vec<WireValue> = parents
        .iter()
        .map(|p| {
            let nested = crate::grouping::attach_to_parent(p, pk, &by_key, single);
            match p {
                // {...p, [into]: nested}: shallow-copy the parent's entries, then set an existing `into`
                // in place (keeps its position) or append a new one — the TS `{...par, [into]: …}` spread.
                WireValue::Row(r) => {
                    let mut entries = r.entries.clone();
                    match entries.iter_mut().find(|(k, _)| k == into) {
                        Some(slot) => slot.1 = nested,
                        None => entries.push((into.to_string(), nested)),
                    }
                    WireValue::Row(WireRow { entries })
                }
                // Records are rows by contract (SQL rows); a non-row passes through untouched.
                _ => p.clone(),
            }
        })
        .collect();
    Ok(WireValue::List(WireList { items: out }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrow(pairs: &[(&str, WireValue)]) -> WireValue {
        WireValue::Row(WireRow { entries: pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect() })
    }
    fn items(w: &WireValue) -> Vec<WireValue> {
        match w {
            WireValue::List(l) => l.items.clone(),
            _ => panic!("not a list"),
        }
    }
    // The key-column tuple ports arrive as owned `Vec<String>` (bc 0.9.0 declared `{arr:'string'}`).
    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    // ── with_ambient_transaction atomicity (#142): Ok → COMMIT (all rows persist), Err → ROLLBACK
    //    (NO rows persist). Proves the tx boundary the covered runner relies on is genuinely atomic. ──
    #[test]
    fn tx_commits_on_ok_and_rolls_back_on_err() {
        use crate::driver::{PreparedStatement, SqliteDriver};
        let d = SqliteDriver::in_memory(&["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)".to_string()]).unwrap();
        let ins = |id: i64, v: &str| -> Result<(), BehaviorError> {
            execute_sql(false, &[WireValue::int(id), WireValue::Str(v.to_string())], false, "INSERT INTO t (id, v) VALUES (?, ?)", true).map(|_| ())
        };
        let row_count = |d: &SqliteDriver| -> i64 {
            let rows = d.prepare("SELECT COUNT(*) AS c FROM t").all(&[]).unwrap();
            match &rows[0] {
                Value::Obj(pairs) => match pairs.iter().find(|(k, _)| k == "c").map(|(_, v)| v) {
                    Some(Value::Int(n)) => *n,
                    other => panic!("unexpected count cell: {other:?}"),
                },
                other => panic!("unexpected count row: {other:?}"),
            }
        };

        // Ok body: two inserts on the tx connection → COMMIT → both rows persist.
        with_ambient_transaction(&d, || {
            ins(1, "a")?;
            ins(2, "b")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(row_count(&d), 2, "a committed tx must persist all its writes");

        // Err body: insert row 3 then fail mid-tx → ROLLBACK → row 3 must NOT persist (still 2 rows).
        let outcome: Result<(), BehaviorError> = with_ambient_transaction(&d, || {
            ins(3, "c")?; // this write is issued inside the tx…
            Err(BehaviorError::new("BOOM", "mid-tx failure")) // …then the body errors → rollback
        });
        assert!(outcome.is_err(), "the body error must propagate");
        assert_eq!(row_count(&d), 2, "a rolled-back tx must leave NO rows committed (row 3 gone)");
    }

    // Single-key pluck emits a FLAT scalar key array (json_each scalar `value`).
    #[test]
    fn pluck_single_key_is_flat_scalars() {
        let rows = [wrow(&[("id", WireValue::int(2))]), wrow(&[("id", WireValue::int(1))]), wrow(&[("id", WireValue::int(2))])];
        let out = pluck_keys(&cols(&["id"]), &rows).unwrap();
        let ks = items(&out);
        assert_eq!(ks.len(), 2); // deduped, order preserved
        assert!(matches!(&ks[0], WireValue::Num(n) if n == "2"));
        assert!(!matches!(&ks[0], WireValue::List(_))); // scalar, NOT a 1-tuple
    }

    // Composite pluck emits an array-of-TUPLES (json_each per-ordinal `$[i]`).
    #[test]
    fn pluck_composite_key_is_tuples() {
        let rows = [
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(9))]),
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(9))]), // dup tuple
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(8))]),
        ];
        let out = pluck_keys(&cols(&["tenant_id", "user_id"]), &rows).unwrap();
        let ks = items(&out);
        assert_eq!(ks.len(), 2); // deduped on the whole tuple
        assert_eq!(items(&ks[0]).len(), 2); // each key is a 2-element tuple
    }

    // group_children keyed on a COMPOSITE tuple nests by the FULL key — NOT a cartesian cross. A parent
    // (t=1,u=9) must receive only its (t=1,u=9) child, never the (t=1,u=8) one (which a `''`-collapse or
    // first-column-only key would wrongly attach).
    #[test]
    fn group_composite_is_not_cartesian() {
        let parents = [
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(9))]),
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(8))]),
        ];
        let children = [
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(9)), ("title", WireValue::Str("p9".into()))]),
            wrow(&[("tenant_id", WireValue::int(1)), ("user_id", WireValue::int(8)), ("title", WireValue::Str("p8".into()))]),
        ];
        let out = group_children(&children, &cols(&["tenant_id", "user_id"]), "posts", &parents, &cols(&["tenant_id", "user_id"]), false).unwrap();
        let ps = items(&out);
        for p in &ps {
            let posts = match p {
                WireValue::Row(r) => r.entries.iter().find(|(k, _)| k == "posts").map(|(_, v)| v.clone()).unwrap(),
                _ => panic!(),
            };
            // each parent nests EXACTLY its own one matching post (cartesian would nest both).
            assert_eq!(items(&posts).len(), 1, "composite grouping must not be cartesian");
        }
    }
}
