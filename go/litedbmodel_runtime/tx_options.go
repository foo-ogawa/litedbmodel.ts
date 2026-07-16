// litedbmodel v2 SCP — the **tx-completeness contract** (Phase B / #83, go port of
// `src/scp/tx-options.ts` + the rust `tx_options.rs`): the [TransactionOptions] shape + defaults,
// the [IsolationLevel] enum + per-dialect isolation-level SQL, the retryable-error classifier, and
// the write=tx guard errors ([WriteOutsideTransaction] / [WriteInReadOnly]).
//
// This is the go mirror of the Phase B **API REFERENCE** (`tx-options.ts`, #81): the option field
// names + defaults, guard-error semantics, isolation-level→SQL mapping per dialect,
// retryable-error classification, and retry-loop policy all match that contract exactly. It is
// dialect-neutral and driver-agnostic on purpose: [Transaction] (exec_context.go) consumes it;
// nothing here touches a connection. It layers the options/guards/retry/isolation on top of the
// Phase A [ExecutionContext] + owned-connection ownership ([WithTransactionDecided]); it does NOT
// re-implement connection ownership. It mirrors v1 `DBModel.ts` (`transaction` :2787,
// `checkWriteAllowed` :886, `isRetryableError` :2865) but on the SCP seam.

package litedbmodel_runtime

import (
	"errors"
	"fmt"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
	pgconn "github.com/jackc/pgx/v5/pgconn"
)

// ── Isolation level (the portable enum + per-dialect SQL) ─────────────────────

// IsolationLevel is one of the three portable SQL isolation levels the tx API exposes (matching the
// v2 public surface). READ UNCOMMITTED is deliberately NOT offered — neither PG (which silently
// upgrades it to READ COMMITTED) nor a correctness-minded default wants it, and omitting it keeps
// the ports' enum identical. The value maps to the canonical SQL phrase via [IsolationLevel.Phrase].
// The zero value [IsolationNone] means "no per-tx level requested" (the engine default; a bare
// BEGIN) — Go has no Option type, so the enum carries an explicit unset variant.
type IsolationLevel int

const (
	// IsolationNone requests NO per-transaction level (the engine default; a bare BEGIN). The zero
	// value, so an omitted TransactionOptions.Isolation naturally means "engine default".
	IsolationNone IsolationLevel = iota
	// IsolationReadCommitted — a statement sees rows committed before it began (no dirty reads).
	IsolationReadCommitted
	// IsolationRepeatableRead — a re-read inside one tx sees the SAME snapshot (no non-repeatable reads).
	IsolationRepeatableRead
	// IsolationSerializable — the strongest level; concurrent txs serialize (PG raises 40001 on write-skew).
	IsolationSerializable
)

// Phrase is the canonical SQL phrase for this level (e.g. `SERIALIZABLE`). Mirrors `isolationPhrase`.
// Fails-closed on an unknown/unset level (a corrupt value must NOT silently run at the engine
// default — that would hide a mis-set isolation), returning ("", err); [IsolationNone] is handled by
// the callers (a bare BEGIN) and never reaches here.
func (l IsolationLevel) Phrase() (string, error) {
	switch l {
	case IsolationReadCommitted:
		return "READ COMMITTED", nil
	case IsolationRepeatableRead:
		return "REPEATABLE READ", nil
	case IsolationSerializable:
		return "SERIALIZABLE", nil
	default:
		return "", txFailure(fmt.Sprintf("scp tx: unknown isolation level '%d'", int(l)))
	}
}

// BeginStatements returns the tx-start statements for `dialectName` at `isolation` (in issue order).
// Per-dialect because the three engines express per-transaction isolation DIFFERENTLY (mirror
// `beginStatements`):
//
//   - postgres: `BEGIN ISOLATION LEVEL <phrase>` — one statement, the level rides the BEGIN.
//   - mysql: `SET TRANSACTION ISOLATION LEVEL <phrase>` MUST precede `BEGIN` (it scopes the very
//     NEXT transaction only), so this returns TWO statements: the SET, then a bare `BEGIN`.
//   - sqlite: SQLite has NO per-transaction isolation-level knob — its isolation is a process/PRAGMA
//     property (`journal_mode=WAL` for snapshot reads), NOT a `BEGIN` clause. So an isolation
//     request on SQLite is a HARD ERROR here (we do NOT silently drop it — that would fake honoring
//     the level). Absent `isolation` SQLite emits a bare `BEGIN`.
//
// With [IsolationNone], every dialect emits a single bare `BEGIN` (the Phase A behavior).
//
// NB: the actual `BEGIN` is issued by database/sql's db.Begin() (which owns the *sql.Tx connection).
// So the runtime uses [isolationPrelude] (the driver-facing split) to bridge — MySQL's SET runs
// pre-BEGIN, PG's SET runs post-BEGIN as the first in-tx statement. [BeginStatements] is retained as
// the TS-parity / conformance-facing SQL-text form.
func BeginStatements(dialectName string, isolation IsolationLevel) ([]string, error) {
	if isolation == IsolationNone {
		return []string{"BEGIN"}, nil
	}
	phrase, err := isolation.Phrase()
	if err != nil {
		return nil, err
	}
	switch dialectName {
	case "postgres":
		return []string{fmt.Sprintf("BEGIN ISOLATION LEVEL %s", phrase)}, nil
	case "mysql":
		// The SET scopes ONLY the next tx; it must be issued before BEGIN, on the SAME connection.
		return []string{fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", phrase), "BEGIN"}, nil
	case "sqlite":
		return nil, txFailure(fmt.Sprintf(
			"scp tx: SQLite does not support a per-transaction isolation level ('%s'). "+
				"SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot "+
				"reads), not a BEGIN clause — set it on the connection, and omit "+
				"TransactionOptions.Isolation for SQLite.", phrase))
	default:
		return nil, txFailure(fmt.Sprintf("scp tx: unknown dialect '%s'", dialectName))
	}
}

// isolationPrelude splits the isolation prelude into (before-BEGIN, after-BEGIN) statements the
// runtime runs around database/sql's own `BEGIN` (Phase B / #83). This is the DRIVER-facing form of
// [BeginStatements]: because db.Begin() issues the plain `BEGIN` itself, the isolation SET is
// delivered as prelude statements run BEFORE (MySQL — the SET scopes the next tx) or AFTER
// (Postgres — the SET is valid as the first in-tx statement) that `BEGIN`.
//
//   - postgres: `([], ["SET TRANSACTION ISOLATION LEVEL <phrase>"])` — runs post-BEGIN.
//   - mysql: `(["SET TRANSACTION ISOLATION LEVEL <phrase>"], [])` — runs pre-BEGIN.
//   - sqlite with isolation: a HARD ERROR (no per-tx level); no isolation ⇒ both empty.
//
// [IsolationNone] ⇒ `([], [])` for every dialect (the Phase A bare `BEGIN`, byte-identical).
func isolationPrelude(dialectName string, isolation IsolationLevel) (before []string, after []string, err error) {
	if isolation == IsolationNone {
		return nil, nil, nil
	}
	phrase, perr := isolation.Phrase()
	if perr != nil {
		return nil, nil, perr
	}
	set := fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", phrase)
	switch dialectName {
	case "postgres":
		return nil, []string{set}, nil
	case "mysql":
		return []string{set}, nil, nil
	case "sqlite":
		return nil, nil, txFailure(fmt.Sprintf(
			"scp tx: SQLite does not support a per-transaction isolation level ('%s'). "+
				"SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot "+
				"reads), not a BEGIN clause — set it on the connection, and omit "+
				"TransactionOptions.Isolation for SQLite.", phrase))
	default:
		return nil, nil, txFailure(fmt.Sprintf("scp tx: unknown dialect '%s'", dialectName))
	}
}

// ── TransactionOptions (the Phase B public option shape) ──────────────────────

// TransactionOptions is the Phase B tx contract (mirrors v1 `TransactionOptions` in `src/types.ts`
// plus the new `isolation`, and the TS/rust `TransactionOptions`). Every field has a stable default
// via [DefaultTransactionOptions]; the field names + defaults match the other ports EXACTLY. NB the
// zero value is NOT the intended default (a zero RetryLimit would mean "never even try once") — a
// caller building options by hand should start from [DefaultTransactionOptions]. The [Transaction]
// entry itself normalizes (a zero RetryLimit floors to 1).
type TransactionOptions struct {
	// Isolation is the per-transaction isolation level. Issued via [isolationPrelude] on the tx-owned
	// connection (PG: post-BEGIN SET; MySQL: a preceding SET). [IsolationNone] ⇒ the engine default.
	// SQLite has no per-tx level ⇒ passing this on SQLite is an error.
	Isolation IsolationLevel
	// RetryOnError retries the whole tx on a retryable error (deadlock / serialization / connection).
	// Default true.
	RetryOnError bool
	// RetryLimit is the max attempts before giving up (the FIRST try counts as attempt 1). Default 3.
	RetryLimit int
	// RetryDurationMs is the backoff base in ms; attempt k waits `RetryDurationMs * 2^(k-1)`
	// (exponential). Default 200.
	RetryDurationMs int
	// RollbackOnly ROLLBACKs instead of COMMITting at the end of a SUCCESSFUL body (dry-run /
	// preview): the body runs and its result is returned, but NO change is committed. A body error
	// still ROLLBACKs + re-raises as usual. Default false.
	RollbackOnly bool
}

// DefaultTransactionOptions returns the Phase B defaults (mirror TS `resolveTxOptions` / rust
// `TransactionOptions::default`: RetryOnError=true, RetryLimit=3, RetryDurationMs=200,
// RollbackOnly=false, Isolation=none). NB this DIFFERS from the v1 defaults (retry OFF, 100ms) — the
// v2 Phase B contract (`tx-options.ts`) is the SSoT the 5 ports mirror, and it defaults retry ON
// with a 200ms base.
func DefaultTransactionOptions() TransactionOptions {
	return TransactionOptions{
		Isolation:       IsolationNone,
		RetryOnError:    true,
		RetryLimit:      3,
		RetryDurationMs: 200,
		RollbackOnly:    false,
	}
}

// ── Retryable-error classification (per dialect) ──────────────────────────────

// IsRetryableTxError reports whether `err` is a RETRYABLE transaction failure — a deadlock, a
// serialization failure, or a broken connection — for which re-running the whole transaction can
// succeed. Classification is by the driver's stable error CODE first (PG SQLSTATE via
// *pgconn.PgError.Code, MySQL errno via *mysql.MySQLError.Number), with the v1 message substrings as
// a fallback (mirrors TS `isRetryableTxError` / rust `is_retryable_tx_error`). A data conflict
// (unique/FK/check) is NOT retryable — re-running would fail identically.
//
// Codes (per dialect):
//   - postgres SQLSTATE: `40001` serialization_failure, `40P01` deadlock_detected.
//   - mysql errno: `1213` ER_LOCK_DEADLOCK, `1205` ER_LOCK_WAIT_TIMEOUT.
//   - connection errors (either dialect): via [IsConnectionError] — a dropped/reset/refused
//     connection is retryable (reconnect on the next attempt).
//
// The typed-code extraction (errors.As on the concrete driver error) is the PRIMARY, load-bearing
// mechanism — NOT dead code behind the string match. The live drivers' errors reach here wrapped in a
// [SqlFailure] (mapSqliteError flattens the driver TEXT into Msg), but SqlFailure.Unwrap() re-exposes
// the ORIGINAL concrete error, so errors.As traverses to the *pgconn.PgError / *mysql.MySQLError even
// for a serialization failure raised at tx.Commit() time. The string match is a belt-and-suspenders
// FALLBACK for an error that lost its type (e.g. a doubly-wrapped or re-constructed failure) — it is
// deliberately EXERCISE-DISABLABLE (disableRetryStringFallback) so a live regression test can prove
// the typed path alone still catches PG 40001 + MySQL 1213 (guarding against the typed block silently
// rotting back to dead code). Mirrors TS `isRetryableTxError` / rust `is_retryable_tx_error`. A data
// conflict (unique/FK/check) is NOT retryable — re-running would fail identically.
func IsRetryableTxError(err error) bool {
	if err == nil {
		return false
	}
	if IsConnectionError(err) {
		return true
	}
	// PRIMARY: stable CODE via the concrete driver error type (reachable through SqlFailure.Unwrap()).
	if retryableByTypedCode(err) {
		return true
	}
	// FALLBACK: the code / v1 message substrings (driver-version-independent phrasing) — belt-and-
	// suspenders for a type-erased error. Disablable in tests to prove the typed path is load-bearing.
	if disableRetryStringFallback {
		return false
	}
	m := err.Error()
	if strings.Contains(m, "40001") || strings.Contains(m, "40P01") {
		return true
	}
	if strings.Contains(m, "1213") || strings.Contains(m, "1205") {
		return true
	}
	return strings.Contains(m, "The transaction might succeed if retried") ||
		strings.Contains(m, "try restarting transaction") ||
		strings.Contains(m, "could not serialize access due to concurrent update") ||
		strings.Contains(m, "could not serialize access") ||
		strings.Contains(m, "Deadlock found") ||
		strings.Contains(m, "deadlock detected") ||
		strings.Contains(m, "Lock wait timeout exceeded")
}

// disableRetryStringFallback, when set, makes [IsRetryableTxError] skip the message-substring
// FALLBACK so the PRIMARY typed-code path ([retryableByTypedCode] via errors.As on the concrete
// driver error) must stand on its own. It exists ONLY for the live regression test that proves the
// typed extraction is genuinely load-bearing (the concrete *pgconn.PgError / *mysql.MySQLError is
// reachable through SqlFailure.Unwrap()); production never sets it. Package-level (not concurrency-
// safe) — the regression test runs it serially.
var disableRetryStringFallback bool

// retryableByTypedCode reports whether `err` (traversed via errors.As, so a mapped [SqlFailure]
// reaches its wrapped concrete driver error through Unwrap()) carries a retryable PG SQLSTATE
// (40001/40P01) or MySQL errno (1213/1205). This is the driver-version-independent PRIMARY classifier
// — it does NOT string-match. It is the load-bearing live path: a PG 40001 raised at tx.Commit() is
// wrapped by mapSqliteError into a SqlFailure whose Unwrap() re-exposes the *pgconn.PgError, and this
// errors.As reaches its .Code.
func retryableByTypedCode(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		if pg.Code == "40001" || pg.Code == "40P01" {
			return true // serialization_failure / deadlock_detected
		}
	}
	var my *gomysql.MySQLError
	if errors.As(err, &my) {
		if my.Number == 1213 || my.Number == 1205 {
			return true // ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT
		}
	}
	return false
}

// IsConnectionError reports whether `err` is a broken/stale connection (retryable via reconnect) — a
// message/-code heuristic matching `src/connection-errors.ts` (`isConnectionError`) plus the go
// driver-closed phrasings. A dropped/reset/refused connection reconnects on the next attempt (a
// fresh pooled connection / *sql.Tx). database/sql's own sentinels (sql.ErrConnDone / driver.ErrBadConn)
// are also treated as connection errors.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "Connection terminated") ||
		strings.Contains(m, "Client has encountered a connection error") ||
		strings.Contains(m, "ECONNRESET") ||
		strings.Contains(m, "ECONNREFUSED") ||
		strings.Contains(m, "Connection lost") ||
		strings.Contains(m, "This socket has been ended by the other party") ||
		strings.Contains(m, "EPIPE") ||
		strings.Contains(m, "PROTOCOL_CONNECTION_LOST") ||
		// go database/sql + pgx/go-sql-driver connection-closed phrasings.
		strings.Contains(m, "connection closed") ||
		strings.Contains(m, "connection was closed") ||
		strings.Contains(m, "bad connection") ||
		strings.Contains(m, "invalid connection") ||
		strings.Contains(m, "broken pipe") ||
		strings.Contains(m, "connection reset by peer") ||
		strings.Contains(m, "sql: connection is already closed")
}

// ── write=tx guards (mirror v1 `checkWriteAllowed`, DBModel.ts:886) ────────────

// WriteOutsideTransaction is the write=tx guard error: a write issued OUTSIDE a [Transaction]
// boundary. Mirrors v1 `WriteOutsideTransactionError` (the TS `WriteOutsideTransactionError` / rust
// `write_outside_transaction`). A [SqlFailure] with Kind = "write_outside_transaction" so the
// runtime surfaces it uniformly.
func WriteOutsideTransaction(operation string, model string) *SqlFailure {
	return &SqlFailure{
		Kind:   "write_outside_transaction",
		Policy: "fail",
		Msg:    fmt.Sprintf("Write operation %q on %s requires a transaction", operation, model),
	}
}

// WriteInReadOnly is the write=tx guard error: a write issued in a READ-ONLY scope. Mirrors v1
// `WriteInReadOnlyContextError` (the TS `WriteInReadOnlyContextError` / rust `write_in_read_only`).
func WriteInReadOnly(operation string, model string) *SqlFailure {
	return &SqlFailure{
		Kind:   "write_in_read_only_context",
		Policy: "fail",
		Msg:    fmt.Sprintf("Write operation %q on %s is not allowed in read-only context", operation, model),
	}
}

// CheckWriteAllowed enforces the write=tx guard (mirror v1 `DBModel._checkWriteAllowed`,
// DBModel.ts:886-896, + the TS `checkWriteAllowed` / rust `check_write_allowed`): a write in a
// read-only scope → [WriteInReadOnly]; a write with NO active transaction → [WriteOutsideTransaction].
// Called at every write ENTRY (create/update/delete/upsert/batch) BEFORE any SQL. The order matches
// v1: read-only is checked FIRST (a read-only scope is the more specific rejection).
//
// `inTransaction` / `readOnly` come from the CALLER's [ExecutionContext] (the explicit-ctx analogue
// of the TS async-local markers): a write inside [Transaction] runs with a tx-scoped ctx
// (inTransaction = true), a bare write with a base ctx (false).
func CheckWriteAllowed(operation string, model string, inTransaction bool, readOnly bool) error {
	if readOnly {
		return WriteInReadOnly(operation, model)
	}
	if !inTransaction {
		return WriteOutsideTransaction(operation, model)
	}
	return nil
}

// txFailure is a plain tx-layer [SqlFailure] (unknown dialect / unsupported isolation).
func txFailure(message string) *SqlFailure {
	return &SqlFailure{Kind: KindDriverError, Policy: "fail", Msg: message}
}
