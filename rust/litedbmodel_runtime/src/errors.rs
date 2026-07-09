//! litedbmodel v2 SCP — Error Mapping (Rust port of `src/scp/errors.ts`, spec §11 item 5).
//!
//! Maps a SQLite driver error (`rusqlite::Error`) to a structured [`SqlFailure`] with a stable
//! `kind` + the bc Execution-Plan Policy Kind the runtime honors (fail / retry / continue),
//! mirroring the audited Python/PHP ports.
//!
//! The mapping is closed and explicit (no silent catch-all that hides a driver error): an
//! unrecognized error maps to `kind = "driver_error"` / `policy = "fail"` — loud, and carrying the
//! original `SQLITE_*` code + message. The `SQLITE_*` code family is mirrored from the TS
//! reference; the `rusqlite` extended result code is translated to the canonical `SQLITE_*` name so
//! the same kind/policy is produced as the better-sqlite3 / stdlib-sqlite3 seams.

use rusqlite::ffi::ErrorCode;

/// A mapped SCP failure: SCP `kind`, honored bc Policy Kind, the SQLite code, a message.
#[derive(Debug, Clone)]
pub struct SqlFailure {
    pub kind: String,
    pub policy: String,
    pub sqlite_code: Option<String>,
    pub message: String,
}

impl std::fmt::Display for SqlFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for SqlFailure {}

/// Extended-result-code → canonical `SQLITE_*` name (subset the reference distinguishes).
fn code_name_from_extended(primary: ErrorCode, extended: i32) -> Option<String> {
    // Extended constraint codes (primary = SQLITE_CONSTRAINT = 19).
    const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = 787; // (19 | (3<<8))
    match extended {
        SQLITE_CONSTRAINT_FOREIGNKEY => return Some("SQLITE_CONSTRAINT_FOREIGNKEY".to_string()),
        5 | 261 => return Some("SQLITE_BUSY".to_string()),
        6 | 262 => return Some("SQLITE_LOCKED".to_string()),
        _ => {}
    }
    match primary {
        ErrorCode::ConstraintViolation => Some("SQLITE_CONSTRAINT".to_string()),
        ErrorCode::DatabaseBusy => Some("SQLITE_BUSY".to_string()),
        ErrorCode::DatabaseLocked => Some("SQLITE_LOCKED".to_string()),
        _ => None,
    }
}

/// Map a caught `rusqlite` error to a [`SqlFailure`] (byte-for-byte kind/policy with the TS/Py/PHP
/// references).
pub fn map_sqlite_error(e: &rusqlite::Error) -> SqlFailure {
    let code: Option<String> = match e {
        rusqlite::Error::SqliteFailure(ffi_err, _) => {
            code_name_from_extended(ffi_err.code, ffi_err.extended_code)
        }
        _ => extract_code_from_message(&e.to_string()),
    };

    let Some(code) = code else {
        return SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!("non-SQLite driver error: {e}"),
        };
    };

    let tagged = format!("[{code}] {e}");
    if code == "SQLITE_CONSTRAINT_FOREIGNKEY" {
        SqlFailure {
            kind: "foreign_key_violation".into(),
            policy: "fail".into(),
            sqlite_code: Some(code),
            message: tagged,
        }
    } else if code.starts_with("SQLITE_CONSTRAINT") {
        SqlFailure {
            kind: "constraint_violation".into(),
            policy: "fail".into(),
            sqlite_code: Some(code),
            message: tagged,
        }
    } else if code == "SQLITE_BUSY" || code == "SQLITE_LOCKED" {
        SqlFailure {
            kind: "retryable".into(),
            policy: "retry".into(),
            sqlite_code: Some(code),
            message: tagged,
        }
    } else {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: Some(code),
            message: tagged,
        }
    }
}

/// Recover a `SQLITE_*` code embedded in a bc `OP_FAILED` message (the runtime re-surfaces a
/// structured failure from a handler `{error}` that carries the tag), mirroring the Python/PHP
/// `re_error_to_sql_failure`.
pub fn extract_code_from_message(message: &str) -> Option<String> {
    let start = message.find("SQLITE_")?;
    let rest = &message[start..];
    let end = rest
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_uppercase() || *c == '_'))
        .map(|(i, _)| i)
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Re-surface a structured [`SqlFailure`] from a message whose text embeds a `SQLITE_*` code.
/// If no code is present, the original message is preserved as a plain driver error.
pub fn re_error_to_sql_failure(message: &str) -> SqlFailure {
    if let Some(code) = extract_code_from_message(message) {
        let tagged = format!("[{code}] {message}");
        if code == "SQLITE_CONSTRAINT_FOREIGNKEY" {
            return SqlFailure {
                kind: "foreign_key_violation".into(),
                policy: "fail".into(),
                sqlite_code: Some(code),
                message: tagged,
            };
        }
        if code.starts_with("SQLITE_CONSTRAINT") {
            return SqlFailure {
                kind: "constraint_violation".into(),
                policy: "fail".into(),
                sqlite_code: Some(code),
                message: tagged,
            };
        }
        if code == "SQLITE_BUSY" || code == "SQLITE_LOCKED" {
            return SqlFailure {
                kind: "retryable".into(),
                policy: "retry".into(),
                sqlite_code: Some(code),
                message: tagged,
            };
        }
    }
    SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: message.to_string(),
    }
}
