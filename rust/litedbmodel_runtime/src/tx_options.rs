//! litedbmodel v2 SCP — the **tx-completeness contract** (Phase B / #82, rust port of
//! `src/scp/tx-options.ts`): the [`TransactionOptions`] shape + defaults, the [`IsolationLevel`]
//! enum + per-dialect isolation-level SQL, the retryable-error classifier, and the write=tx guard
//! errors ([`WriteOutsideTransactionError`] / [`WriteInReadOnlyContextError`]).
//!
//! This is the rust mirror of the Phase B **API REFERENCE** (`tx-options.ts`, #81): the option
//! field names + defaults, guard-error semantics, isolation-level→SQL mapping per dialect,
//! retryable-error classification, and retry-loop policy all match that contract exactly. It is
//! dialect-neutral and driver-agnostic on purpose: [`crate::exec_context::transaction`] consumes it;
//! nothing here touches a connection. It layers the options/guards/retry/isolation on top of the
//! Phase A [`crate::exec_context::ExecutionContext`] + owned-connection ownership
//! ([`crate::exec_context::with_transaction_decided`]); it does NOT re-implement connection
//! ownership. It mirrors v1 `litedbmodel.rs` `transaction_with_options` (`handler.rs:216`) + the v1
//! `TransactionOptions` (`types.rs:299`) + the write=tx guard (`model.rs:1045`) but on the SCP seam.

use crate::errors::SqlFailure;

// ── Isolation level (the portable enum + per-dialect SQL) ─────────────────────

/// The three portable SQL isolation levels the tx API exposes (matching the v2 public surface).
/// READ UNCOMMITTED is deliberately NOT offered — neither PG (which silently upgrades it to READ
/// COMMITTED) nor a correctness-minded default wants it, and omitting it keeps the ports' enum
/// identical. The value maps to the canonical SQL phrase via [`IsolationLevel::phrase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `READ COMMITTED` — a statement sees rows committed before it began (no dirty reads).
    ReadCommitted,
    /// `REPEATABLE READ` — a re-read inside one tx sees the SAME snapshot (no non-repeatable reads).
    RepeatableRead,
    /// `SERIALIZABLE` — the strongest level; concurrent txs serialize (PG raises 40001 on write-skew).
    Serializable,
}

impl IsolationLevel {
    /// The canonical SQL phrase for this level (e.g. `SERIALIZABLE`). Mirrors `isolationPhrase`.
    pub fn phrase(self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// The dialect a tx runs against (the isolation-SQL is per-dialect). Mirrors the makeSQL `Dialect`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mysql,
    Sqlite,
}

impl Dialect {
    /// Parse the dialect name the bundle carries (fail-closed on an unknown value).
    pub fn parse(name: &str) -> Result<Dialect, SqlFailure> {
        match name {
            "postgres" => Ok(Dialect::Postgres),
            "mysql" => Ok(Dialect::Mysql),
            "sqlite" => Ok(Dialect::Sqlite),
            other => Err(tx_failure(&format!("scp tx: unknown dialect '{other}'"))),
        }
    }
}

/// The tx-start statements for `dialect` at `isolation` (in issue order). Per-dialect because the
/// three engines express per-transaction isolation DIFFERENTLY (mirror `beginStatements`):
///
///   - **postgres**: `BEGIN ISOLATION LEVEL <phrase>` — one statement, the level rides the BEGIN.
///   - **mysql**: `SET TRANSACTION ISOLATION LEVEL <phrase>` MUST precede `BEGIN` (it scopes the
///     very NEXT transaction only), so this returns TWO statements: the SET, then a bare `BEGIN`.
///   - **sqlite**: SQLite has NO per-transaction isolation-level knob — its isolation is a
///     process/PRAGMA property (`journal_mode=WAL` for snapshot reads), NOT a `BEGIN` clause. So an
///     isolation request on SQLite is a HARD ERROR here (we do NOT silently drop it — that would
///     fake honoring the level). Absent `isolation` SQLite emits a bare `BEGIN`.
///
/// With no `isolation`, every dialect emits a single bare `BEGIN` (the Phase A behavior).
///
/// NB: the actual `BEGIN` is issued by the driver's [`crate::driver::Driver::begin_tx`] (which owns
/// the connection). So this returns the PRELUDE statements to run on the owned connection BEFORE the
/// tx body — for PG the isolation-aware BEGIN REPLACES the plain BEGIN, for MySQL the SET precedes
/// the plain BEGIN. The runtime uses [`isolation_prelude`] (the driver-facing split) to bridge the
/// two; [`begin_statements`] is retained as the TS-parity / conformance-facing SQL-text form.
pub fn begin_statements(
    dialect: Dialect,
    isolation: Option<IsolationLevel>,
) -> Result<Vec<String>, SqlFailure> {
    let Some(level) = isolation else {
        return Ok(vec!["BEGIN".to_string()]);
    };
    let phrase = level.phrase();
    match dialect {
        Dialect::Postgres => Ok(vec![format!("BEGIN ISOLATION LEVEL {phrase}")]),
        // The SET scopes ONLY the next tx; it must be issued before BEGIN, on the SAME connection.
        Dialect::Mysql => Ok(vec![
            format!("SET TRANSACTION ISOLATION LEVEL {phrase}"),
            "BEGIN".to_string(),
        ]),
        Dialect::Sqlite => Err(tx_failure(&format!(
            "scp tx: SQLite does not support a per-transaction isolation level ('{phrase}'). \
             SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot \
             reads), not a BEGIN clause — set it on the connection, and omit \
             TransactionOptions.isolation for SQLite."
        ))),
    }
}

/// The isolation prelude split into (before-BEGIN, after-BEGIN) statements the driver runs around
/// its own `BEGIN` (Phase B / #82). This is the DRIVER-facing form of [`begin_statements`]: because
/// each driver's [`crate::driver::Driver::begin_tx`] issues the plain `BEGIN` itself, the isolation
/// SET is delivered as prelude statements it runs BEFORE (MySQL — the SET scopes the next tx) or
/// AFTER (Postgres — the SET is valid as the first in-tx statement) that `BEGIN`.
///
///   - **postgres**: `([], ["SET TRANSACTION ISOLATION LEVEL <phrase>"])` — runs post-BEGIN.
///   - **mysql**: `(["SET TRANSACTION ISOLATION LEVEL <phrase>"], [])` — runs pre-BEGIN.
///   - **sqlite** with isolation: a HARD ERROR (no per-tx level); no isolation ⇒ both empty.
///
/// No isolation ⇒ `([], [])` for every dialect (the Phase A bare `BEGIN`, byte-identical).
pub fn isolation_prelude(
    dialect: Dialect,
    isolation: Option<IsolationLevel>,
) -> Result<(Vec<String>, Vec<String>), SqlFailure> {
    let Some(level) = isolation else {
        return Ok((Vec::new(), Vec::new()));
    };
    let set = format!("SET TRANSACTION ISOLATION LEVEL {}", level.phrase());
    match dialect {
        Dialect::Postgres => Ok((Vec::new(), vec![set])),
        Dialect::Mysql => Ok((vec![set], Vec::new())),
        Dialect::Sqlite => Err(tx_failure(&format!(
            "scp tx: SQLite does not support a per-transaction isolation level ('{}'). \
             SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot \
             reads), not a BEGIN clause — set it on the connection, and omit \
             TransactionOptions.isolation for SQLite.",
            level.phrase()
        ))),
    }
}

// ── TransactionOptions (the Phase B public option shape) ──────────────────────

/// Options for a transaction (the Phase B contract; mirrors v1 `TransactionOptions` in `types.rs`
/// plus the new `isolation`, and the TS `TransactionOptions`). Every field has a stable default via
/// [`TransactionOptions::default`]; the field names + defaults match the other ports EXACTLY.
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    /// Per-transaction isolation level. Issued via [`begin_statements`] on the tx-owned connection
    /// (PG: on BEGIN; MySQL: a preceding SET). `None` ⇒ the engine default. SQLite has no per-tx
    /// level ⇒ passing this on SQLite is an error.
    pub isolation: Option<IsolationLevel>,
    /// Retry the whole tx on a retryable error (deadlock / serialization / connection). Default true.
    pub retry_on_error: bool,
    /// Max attempts before giving up (the FIRST try counts as attempt 1). Default 3.
    pub retry_limit: u32,
    /// Backoff base in ms; attempt k waits `retry_duration_ms * 2^(k-1)` (exponential). Default 200.
    pub retry_duration_ms: u64,
    /// ROLLBACK instead of COMMIT at the end of a SUCCESSFUL body (dry-run / preview): the body runs
    /// and its result is returned, but NO change is committed. A body error still ROLLBACKs +
    /// re-raises as usual. Default false.
    pub rollback_only: bool,
}

impl Default for TransactionOptions {
    /// The Phase B defaults (mirror TS `resolveTxOptions`: retryOnError=true, retryLimit=3,
    /// retryDuration=200). NB: this DIFFERS from the v1 `.rs` `TransactionOptions::default`
    /// (`retry_on_error=false`, `retry_duration_ms=100`) — the v2 Phase B contract (`tx-options.ts`)
    /// is the SSoT the 5 ports mirror, and it defaults retry ON with a 200ms base.
    fn default() -> Self {
        TransactionOptions {
            isolation: None,
            retry_on_error: true,
            retry_limit: 3,
            retry_duration_ms: 200,
            rollback_only: false,
        }
    }
}

// ── Retryable-error classification (per dialect) ──────────────────────────────

/// Is `error` a RETRYABLE transaction failure — a deadlock, a serialization failure, or a broken
/// connection — for which re-running the whole transaction can succeed? Classification is by the
/// driver's stable error CODE first (PG `SQLSTATE`, MySQL `errno`), with the v1 message substrings
/// as a fallback (mirrors TS `isRetryableTxError`). A data conflict (unique/FK/check) is NOT
/// retryable — re-running would fail identically.
///
/// Codes (per dialect), carried in the [`SqlFailure::message`] (the live drivers embed the driver
/// error text, which includes the SQLSTATE / errno):
///   - **postgres** SQLSTATE: `40001` serialization_failure, `40P01` deadlock_detected.
///   - **mysql** errno: `1213` ER_LOCK_DEADLOCK, `1205` ER_LOCK_WAIT_TIMEOUT.
///   - **connection errors** (either dialect): a dropped/reset/refused connection is retryable
///     (reconnect on the next attempt) — [`is_connection_error`].
pub fn is_retryable_tx_error(error: &SqlFailure) -> bool {
    if is_connection_error(error) {
        return true;
    }
    let m = &error.message;
    // PG SQLSTATE / MySQL errno appear in the driver error text the live drivers embed.
    if m.contains("40001") || m.contains("40P01") {
        return true; // serialization_failure / deadlock_detected
    }
    if m.contains("1213") || m.contains("1205") {
        return true; // ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT
    }
    // Fallback: the v1 message substrings (driver-version-independent phrasing).
    m.contains("The transaction might succeed if retried")
        || m.contains("try restarting transaction")
        || m.contains("could not serialize access due to concurrent update")
        || m.contains("could not serialize access")
        || m.contains("Deadlock found")
        || m.contains("deadlock detected")
        || m.contains("Lock wait timeout exceeded")
}

/// Is `error` a broken/stale connection (retryable via reconnect)? A message/-code heuristic
/// matching `src/connection-errors.ts` (`isConnectionError`) — a dropped/reset/refused connection
/// on the next attempt reconnects on a fresh pooled connection.
pub fn is_connection_error(error: &SqlFailure) -> bool {
    let m = &error.message;
    m.contains("Connection terminated")
        || m.contains("Client has encountered a connection error")
        || m.contains("ECONNRESET")
        || m.contains("ECONNREFUSED")
        || m.contains("Connection lost")
        || m.contains("This socket has been ended by the other party")
        || m.contains("EPIPE")
        || m.contains("PROTOCOL_CONNECTION_LOST")
        // sqlx / tokio-postgres connection-closed phrasings.
        || m.contains("connection closed")
        || m.contains("connection was closed")
        || m.contains("pool has been closed")
}

// ── write=tx guards (mirror v1 `create`/`update`/… guard, model.rs:1045) ───────

/// The write=tx guard error: a write issued OUTSIDE a `transaction()` boundary. Mirrors v1
/// `Error::WriteOutsideTransaction` (`error.rs:25`) + the TS `WriteOutsideTransactionError`. A
/// [`SqlFailure`] with `kind = "write_outside_transaction"` so the runtime surfaces it uniformly.
pub fn write_outside_transaction(operation: &str, model: Option<&str>) -> SqlFailure {
    let model = model.unwrap_or("");
    SqlFailure {
        kind: "write_outside_transaction".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: format!("Write operation \"{operation}\" on {model} requires a transaction"),
    }
}

/// The write=tx guard error: a write issued in a READ-ONLY scope. Mirrors v1
/// `Error::WriteInReadOnlyContext` (`error.rs:28`) + the TS `WriteInReadOnlyContextError`.
pub fn write_in_read_only(operation: &str, model: Option<&str>) -> SqlFailure {
    let model = model.unwrap_or("");
    SqlFailure {
        kind: "write_in_read_only_context".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: format!(
            "Write operation \"{operation}\" on {model} is not allowed in read-only context"
        ),
    }
}

/// Enforce the write=tx guard (mirror v1 `create`/`update`/`delete` guard, `model.rs:1045-1056`, +
/// the TS `checkWriteAllowed`): a write in a read-only scope → [`write_in_read_only`]; a write with
/// NO active transaction → [`write_outside_transaction`]. Called at every write ENTRY BEFORE any
/// SQL. The order matches v1: read-only is checked FIRST (the more specific rejection).
///
/// `in_transaction` / `read_only` come from the CALLER's [`crate::exec_context::ExecutionContext`]
/// (the explicit-ctx analogue of the TS async-local markers): a write inside `transaction()` runs
/// with a tx-scoped ctx (`in_transaction = true`), a bare write with a base ctx (`false`).
pub fn check_write_allowed(
    operation: &str,
    model: Option<&str>,
    in_transaction: bool,
    read_only: bool,
) -> Result<(), SqlFailure> {
    if read_only {
        return Err(write_in_read_only(operation, model));
    }
    if !in_transaction {
        return Err(write_outside_transaction(operation, model));
    }
    Ok(())
}

/// A plain tx-layer [`SqlFailure`] (unknown dialect / unsupported isolation).
fn tx_failure(message: &str) -> SqlFailure {
    SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: message.to_string(),
    }
}

// ── Unit tests (no DB) — the contract shapes ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_phase_b_contract() {
        let o = TransactionOptions::default();
        assert!(o.retry_on_error);
        assert_eq!(o.retry_limit, 3);
        assert_eq!(o.retry_duration_ms, 200);
        assert!(!o.rollback_only);
        assert!(o.isolation.is_none());
    }

    #[test]
    fn begin_statements_per_dialect() {
        assert_eq!(
            begin_statements(Dialect::Postgres, None).unwrap(),
            vec!["BEGIN".to_string()]
        );
        assert_eq!(
            begin_statements(Dialect::Postgres, Some(IsolationLevel::Serializable)).unwrap(),
            vec!["BEGIN ISOLATION LEVEL SERIALIZABLE".to_string()]
        );
        assert_eq!(
            begin_statements(Dialect::Mysql, Some(IsolationLevel::RepeatableRead)).unwrap(),
            vec![
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".to_string(),
                "BEGIN".to_string(),
            ]
        );
        assert_eq!(
            begin_statements(Dialect::Mysql, None).unwrap(),
            vec!["BEGIN".to_string()]
        );
    }

    #[test]
    fn sqlite_isolation_is_a_hard_error() {
        assert!(begin_statements(Dialect::Sqlite, Some(IsolationLevel::Serializable)).is_err());
        // …but a bare BEGIN with no isolation is fine (the Phase A path).
        assert_eq!(
            begin_statements(Dialect::Sqlite, None).unwrap(),
            vec!["BEGIN".to_string()]
        );
    }

    fn err(msg: &str) -> SqlFailure {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: msg.into(),
        }
    }

    #[test]
    fn retryable_classification() {
        // PG serialization / deadlock (SQLSTATE in the message).
        assert!(is_retryable_tx_error(&err(
            "postgres tx execute: db error: ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)"
        )));
        assert!(is_retryable_tx_error(&err("deadlock detected (40P01)")));
        // MySQL deadlock / lock-wait (errno in the message).
        assert!(is_retryable_tx_error(&err(
            "mysql exec: 1213 Deadlock found"
        )));
        assert!(is_retryable_tx_error(&err(
            "Lock wait timeout exceeded (1205)"
        )));
        // Connection error → retryable.
        assert!(is_retryable_tx_error(&err(
            "Connection terminated unexpectedly"
        )));
        // A data conflict is NOT retryable.
        assert!(!is_retryable_tx_error(&err(
            "duplicate key value violates unique constraint (SQLSTATE 23505)"
        )));
        assert!(!is_retryable_tx_error(&err("some unrelated driver error")));
    }

    #[test]
    fn guard_order_read_only_first() {
        // Read-only is rejected FIRST (more specific), even with no active tx.
        let e = check_write_allowed("INSERT", Some("users"), false, true).unwrap_err();
        assert_eq!(e.kind, "write_in_read_only_context");
        // No active tx (not read-only) → outside-transaction.
        let e = check_write_allowed("INSERT", Some("users"), false, false).unwrap_err();
        assert_eq!(e.kind, "write_outside_transaction");
        // Inside a tx (not read-only) → allowed.
        assert!(check_write_allowed("INSERT", Some("users"), true, false).is_ok());
    }
}
