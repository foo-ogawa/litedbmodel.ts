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

/// The context a [`LimitExceededError`] was raised from (Rust port of the TS `LimitExceededContext`,
/// spec §E-2). `"find"` — a top-level read exceeded the find hard limit (the reported `count` is the
/// `LIMIT hardLimit + 1` N+1 fetch size — the TRUE total is only known to be MORE than the cap).
/// `"relation"` — a hasMany relation batch exceeded the hasMany hard limit (the batch is fetched in
/// full, so the reported `count` is the EXACT batch total).
pub const LIMIT_CONTEXT_FIND: &str = "find";
/// See [`LIMIT_CONTEXT_FIND`].
pub const LIMIT_CONTEXT_RELATION: &str = "relation";

/// The SHARED cross-language runaway-prevention contract (Phase E-2, epic #74; Rust port of the TS
/// reference `LimitExceededError` in `src/scp/errors.ts`, #99). Raised by the runtime post-fetch
/// guard when a read / relation batch returns MORE rows than the baked hard limit, so an accidental
/// missing-WHERE / N+1 pattern fails LOUD instead of loading an unbounded result set.
///
/// Byte-for-byte mirror of the reference error shape:
///   - fields: `limit` (the cap), `count` (rows fetched — see [`LIMIT_CONTEXT_FIND`]),
///     `context` (`"find"` | `"relation"`), `model` (the read/parent model — the relation's TARGET
///     TABLE in the relation context), `relation` (the relation NAME, `"relation"` context only);
///   - message: `Query limit exceeded: <where> returned <count-phrase> records, but limit is
///     <limit>. This usually indicates a missing WHERE clause or an N+1 query pattern. Set a higher
///     limit or use pagination.` — `find` reports `more than <limit>` (N+1 fetch), `relation`
///     reports the exact `<count>`.
///
/// NOT a [`SqlFailure`]: a runaway guard is a litedbmodel-level policy error, not a mapped driver
/// failure, and it carries no `SQLITE_*` code — so it propagates as its OWN error (the read path's
/// `re_error_to_sql_failure` never re-wraps it).
#[derive(Debug, Clone)]
pub struct LimitExceededError {
    /// The row cap.
    pub limit: i64,
    /// Rows fetched. `find`: the `LIMIT hardLimit + 1` fetch size. `relation`: the EXACT batch total.
    pub count: i64,
    /// `"find"` or `"relation"` ([`LIMIT_CONTEXT_FIND`] / [`LIMIT_CONTEXT_RELATION`]).
    pub context: String,
    /// The read/parent model (find) or the relation's TARGET TABLE (relation).
    pub model: Option<String>,
    /// The relation NAME — present only in the `"relation"` context.
    pub relation: Option<String>,
    /// The rendered message (built once at construction — byte-identical to the TS reference).
    pub message: String,
}

impl LimitExceededError {
    /// Build a [`LimitExceededError`], rendering the SHARED message byte-for-byte with the TS
    /// reference (`errors.ts` `LimitExceededError` constructor).
    pub fn new(
        limit: i64,
        count: i64,
        context: &str,
        model: Option<String>,
        relation: Option<String>,
    ) -> Self {
        let unknown = "unknown".to_string();
        let where_ = if context == LIMIT_CONTEXT_FIND {
            format!("find() on {}", model.as_ref().unwrap_or(&unknown))
        } else {
            format!(
                "relation '{}' on {}",
                relation.as_ref().unwrap_or(&unknown),
                model.as_ref().unwrap_or(&unknown)
            )
        };
        let count_phrase = if context == LIMIT_CONTEXT_FIND {
            format!("more than {limit}")
        } else {
            count.to_string()
        };
        let message = format!(
            "Query limit exceeded: {where_} returned {count_phrase} records, but limit is {limit}. \
             This usually indicates a missing WHERE clause or an N+1 query pattern. Set a higher \
             limit or use pagination."
        );
        LimitExceededError {
            limit,
            count,
            context: context.to_string(),
            model,
            relation,
            message,
        }
    }

    /// The SHARED post-fetch runaway check (SSoT) — the ONE `count > limit ⇒ throw` primitive both
    /// the FIND-context guard and the native-codegen read guard [`check_find_hard_limit`], plus the RELATION-context guard
    /// ([`crate::relation`] `run_relation_op`) call — so no path re-implements the comparison or the
    /// error assembly. The caller supplies the resolved `limit` (the cap baked onto the compiled
    /// artifact at generation time), the fetched `count`, and the context/model/relation identity; the
    /// byte-identical message is rendered by [`LimitExceededError::new`]. `Ok(())` when within the cap.
    pub fn check(
        limit: i64,
        count: i64,
        context: &str,
        model: Option<String>,
        relation: Option<String>,
    ) -> Result<(), LimitExceededError> {
        if count > limit {
            Err(LimitExceededError::new(
                limit, count, context, model, relation,
            ))
        } else {
            Ok(())
        }
    }
}

/// The FIND-context runaway guard shared by BOTH execution paths (SSoT): the mode-2 read-graph guard
/// The native-codegen guarded entry bakes the
/// SAME cap from that `findGuard` meta and passes the de-boxed row count). Both call THIS — the compile
/// injects `LIMIT hardLimit + 1`, so a `count` of `hardLimit + 1` means the TRUE total exceeds the cap
/// and the read fails LOUD (`context: find`) instead of loading an unbounded set. A thin find-context
/// adapter over [`LimitExceededError::check`]; the relation guard calls that core directly with its own
/// context. `Ok(())` when within the cap.
pub fn check_find_hard_limit(
    limit: i64,
    count: i64,
    model: Option<&str>,
) -> Result<(), LimitExceededError> {
    LimitExceededError::check(
        limit,
        count,
        LIMIT_CONTEXT_FIND,
        model.map(str::to_string),
        None,
    )
}

impl std::fmt::Display for LimitExceededError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for LimitExceededError {}

/// The read-path error carrier (Phase E-2): a mapped driver [`SqlFailure`] OR a runaway
/// [`LimitExceededError`]. The two are DISTINCT (the limit guard is not a SqlFailure) — a caller
/// (e.g. the conformance runner's expect-error leg) matches on the variant to assert the raised
/// error is a `LimitExceededError` with the exact fields, exactly as the TS runner's
/// `thrown instanceof LimitExceededError` check. `SqlFailure` converts in via `?` so the many
/// existing `Result<_, SqlFailure>` call sites on the read path port unchanged.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    /// A mapped SQL driver failure (the pre-existing error kind).
    Sql(SqlFailure),
    /// A hard-limit runaway guard trip (`context: find | relation`).
    Limit(LimitExceededError),
}

impl RuntimeError {
    /// The human message of either variant (so `.map_err(|e| … e.message())` ports from the old
    /// `SqlFailure.message` field access unchanged).
    pub fn message(&self) -> &str {
        match self {
            RuntimeError::Sql(e) => &e.message,
            RuntimeError::Limit(e) => &e.message,
        }
    }

    /// The [`LimitExceededError`] iff this is the limit variant (the expect-error leg reads it).
    pub fn as_limit(&self) -> Option<&LimitExceededError> {
        match self {
            RuntimeError::Limit(e) => Some(e),
            _ => None,
        }
    }
}

impl From<SqlFailure> for RuntimeError {
    fn from(e: SqlFailure) -> Self {
        RuntimeError::Sql(e)
    }
}

impl From<LimitExceededError> for RuntimeError {
    fn from(e: LimitExceededError) -> Self {
        RuntimeError::Limit(e)
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}
impl std::error::Error for RuntimeError {}

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
