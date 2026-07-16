// litedbmodel v2 SCP — Error Mapping (Go port of src/scp/errors.ts; spec §11 item 5).
//
// Maps a SQLite driver error to an SCP SqlFailure: a structured Failure with a stable `Kind` and
// the bc Execution-Plan Policy the runtime honors (fail / retry / continue). The mapping is closed
// and explicit (no silent catch-all): an unrecognized SQLite code maps to `driver_error` /
// `fail` — loud, carrying the original code + message for diagnosis. Ported byte-true to errors.ts.

package litedbmodel_runtime

import (
	"errors"
	"fmt"

	sqlite "modernc.org/sqlite"
)

// SqlFailureKind is the SCP failure kind a SQL driver error maps to (errors.ts SqlFailureKind).
type SqlFailureKind string

const (
	KindConstraintViolation SqlFailureKind = "constraint_violation"  // UNIQUE/PK/CHECK/NOT NULL
	KindForeignKeyViolation SqlFailureKind = "foreign_key_violation" // FK constraint
	KindRetryable           SqlFailureKind = "retryable"             // BUSY/LOCKED (Policy: retry)
	KindDriverError         SqlFailureKind = "driver_error"          // anything else (loud, fail)
)

// SqlFailure is a mapped SCP failure: the SCP Kind, the honored bc Policy, the original SQLite
// code, and a human message (errors.ts SqlFailure).
type SqlFailure struct {
	Kind       SqlFailureKind
	Policy     string // bc PolicyKind: "fail" / "retry" / "continue"
	SqliteCode string
	Msg        string
	// wrapped retains the ORIGINAL concrete driver error (a live *pgconn.PgError / *mysql.MySQLError)
	// when this failure maps a live-DB driver error (Phase B / #83). mapSqliteError flattens the driver
	// error's TEXT into Msg, but a text string is opaque to errors.As — so IsRetryableTxError's typed
	// SQLSTATE/errno extraction (the robust, driver-version-independent classifier) would be DEAD unless
	// the concrete error stays reachable. Unwrap() re-exposes it so errors.As(sqlFailure, &pg) traverses
	// to the *pgconn.PgError even at COMMIT time (where mapSqliteError wraps tx.Commit()'s error). nil
	// for a synthetically-constructed failure or an in-proc SQLite error (which carries its own typed
	// path via sqliteCodeString). This field is UNEXPORTED so it never touches the byte-identical
	// conformance surface (the corpus compares the encoded result, never the error struct).
	wrapped error
}

func (e *SqlFailure) Error() string { return e.Msg }

// Unwrap re-exposes the original wrapped driver error (Phase B / #83) so errors.As can reach a
// concrete live-DB error type (*pgconn.PgError / *mysql.MySQLError) through the mapped SqlFailure —
// keeping IsRetryableTxError's typed SQLSTATE/errno classifier LOAD-BEARING (not dead code behind the
// string fallback). Returns nil when no driver error was wrapped.
func (e *SqlFailure) Unwrap() error { return e.wrapped }

// primary result codes (code & 0xFF). Ported to the `SQLITE_*` string family errors.ts branches on.
const (
	rcConstraint = 19
	rcBusy       = 5
	rcLocked     = 6
	// extended: SQLITE_CONSTRAINT_FOREIGNKEY = 787 (19 | (3<<8))
	rcConstraintForeignKey = 787
)

// sqliteCodeString returns the `SQLITE_*` string code for a caught driver error, or "" if the
// error is not a modernc sqlite Error. It reproduces the string codes errors.ts branches on
// (better-sqlite3 exposes them as strings).
func sqliteCodeString(err error) string {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return ""
	}
	ext := se.Code()
	switch ext {
	case rcConstraintForeignKey:
		return "SQLITE_CONSTRAINT_FOREIGNKEY"
	}
	switch ext & 0xFF {
	case rcConstraint:
		return "SQLITE_CONSTRAINT" // UNIQUE/PRIMARYKEY/CHECK/NOTNULL/… family
	case rcBusy:
		return "SQLITE_BUSY"
	case rcLocked:
		return "SQLITE_LOCKED"
	}
	return fmt.Sprintf("SQLITE_%d", ext)
}

// mapSqliteError maps a caught driver error to an SqlFailure (errors.ts mapSqliteError, byte-true
// on kind/policy). The message embeds the `[SQLITE_*]` code so it survives being wrapped.
func mapSqliteError(err error) *SqlFailure {
	code := sqliteCodeString(err)
	if code == "" {
		// A live-DB (non-modernc-SQLite) driver error — e.g. a live *pgconn.PgError / *mysql.MySQLError.
		// Retain the concrete error (wrapped) so errors.As can reach its SQLSTATE/errno through the
		// mapped failure (Phase B / #83 typed-retryable path). This is the branch a PG 40001 raised at
		// tx.Commit() lands in.
		return &SqlFailure{Kind: KindDriverError, Policy: "fail", SqliteCode: "", Msg: fmt.Sprintf("non-SQLite driver error: %s", err.Error()), wrapped: err}
	}
	tagged := fmt.Sprintf("[%s] %s", code, err.Error())
	if code == "SQLITE_CONSTRAINT_FOREIGNKEY" {
		return &SqlFailure{Kind: KindForeignKeyViolation, Policy: "fail", SqliteCode: code, Msg: tagged, wrapped: err}
	}
	if len(code) >= len("SQLITE_CONSTRAINT") && code[:len("SQLITE_CONSTRAINT")] == "SQLITE_CONSTRAINT" {
		return &SqlFailure{Kind: KindConstraintViolation, Policy: "fail", SqliteCode: code, Msg: tagged, wrapped: err}
	}
	if code == "SQLITE_BUSY" || code == "SQLITE_LOCKED" {
		return &SqlFailure{Kind: KindRetryable, Policy: "retry", SqliteCode: code, Msg: tagged, wrapped: err}
	}
	return &SqlFailure{Kind: KindDriverError, Policy: "fail", SqliteCode: code, Msg: tagged, wrapped: err}
}
