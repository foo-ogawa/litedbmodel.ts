//! litedbmodel v2 SCP — dialect strategy table (Rust port of `src/scp/dialect.ts`).
//!
//! The SINGLE SOURCE OF TRUTH for every SQL-dialect difference the render pipeline needs,
//! ported byte-for-byte from the TS reference (spec §4/§5/§8/§10), and mirroring the audited
//! Python/PHP sibling ports. The dialect axis is compiled ONCE TS-side; the published bundle
//! carries `?` placeholders and a `dialect` tag, and this module only needs the render-time
//! concerns a thin runtime touches:
//!
//!   - [`Dialect::finalize_placeholders`] — the `?`→`$N` final one-pass (Postgres only;
//!     SQLite/MySQL identity).
//!   - [`Dialect::order_by_nulls`] — deterministic NULLS ordering (native for PG/SQLite,
//!     `IS NULL` emulation for MySQL) — the WS6-flagged dialect primitive exercised by the
//!     conformance `dialect` suite.
//!
//! The INSERT-conflict / guard-INSERT strategy methods are NOT needed by the runtime: those are
//! a compile-time concern (the published bundle's `operations[*].sql` already carries the fully
//! rendered conflict clause — e.g. `ON CONFLICT DO NOTHING`), so the runtime never re-derives
//! them. This mirrors the TS runtime, which likewise only calls `finalizePlaceholders` /
//! `orderByNulls`.

/// A frozen dialect strategy: the render-time text producers a thin runtime consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
    Mysql,
}

impl Dialect {
    /// The dialect's registered name.
    pub fn name(self) -> &'static str {
        match self {
            Dialect::Sqlite => "sqlite",
            Dialect::Postgres => "postgres",
            Dialect::Mysql => "mysql",
        }
    }

    /// Replace each `?` with `$1, $2, …` for Postgres; identity for SQLite/MySQL.
    pub fn finalize_placeholders(self, sql: &str) -> String {
        match self {
            Dialect::Postgres => to_dollar_placeholders(sql),
            Dialect::Sqlite | Dialect::Mysql => sql.to_string(),
        }
    }

    /// Deterministic NULLS ordering (native for PG/SQLite, `IS NULL` emulation for MySQL).
    pub fn order_by_nulls(self, expr: &str, direction: &str, nulls: &str) -> String {
        match self {
            // Postgres / SQLite (3.30+): native `NULLS FIRST/LAST`.
            Dialect::Postgres | Dialect::Sqlite => format!("{expr} {direction} NULLS {nulls}"),
            // MySQL has no NULLS FIRST/LAST — emulate with a leading `IS NULL` sort key.
            // In MySQL NULL sorts LOWEST; `expr IS NULL` is 1 for null, 0 otherwise.
            //   NULLS FIRST: nulls must come first → order the IS-NULL flag DESC (1 before 0).
            //   NULLS LAST:  nulls must come last  → order the IS-NULL flag ASC  (0 before 1).
            Dialect::Mysql => {
                let flag_dir = if nulls == "FIRST" { "DESC" } else { "ASC" };
                format!("{expr} IS NULL {flag_dir}, {expr} {direction}")
            }
        }
    }
}

/// Replace each `?` with `$1, $2, …` left-to-right (Postgres §8 final one-pass).
///
/// Byte-identical to the TS `toDollarPlaceholders`: it runs ONCE over the fully-assembled,
/// param-flattened SQL text, so placeholder numbering is a plain running counter (the
/// number-reassignment problem cannot reappear). Every `?` on the compiled surface is a bound
/// param position — the render pipeline never emits a literal `?` inside a string literal.
pub fn to_dollar_placeholders(sql: &str) -> String {
    let mut n = 0u32;
    let mut out = String::with_capacity(sql.len());
    for ch in sql.chars() {
        if ch == '?' {
            n += 1;
            out.push('$');
            out.push_str(&n.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Resolve a dialect name to its strategy (fail-closed — no silent default).
pub fn dialect_for(name: &str) -> Result<Dialect, String> {
    match name {
        "sqlite" => Ok(Dialect::Sqlite),
        "postgres" => Ok(Dialect::Postgres),
        "mysql" => Ok(Dialect::Mysql),
        other => Err(format!(
            "scp dialect: unknown dialect '{other}' (known: sqlite, postgres, mysql)"
        )),
    }
}
