// litedbmodel v2 SCP — dialect strategy table (Go port of src/scp/dialect.ts; spec §4/§5/§8/§10).
//
// The SINGLE SOURCE OF TRUTH for every SQL-dialect difference the render pipeline needs, ported
// EXACTLY from the TS reference so the compiled SQL text is byte-identical across languages. No
// scattered dialect `switch` in the render code — a renderer asks the Dialect for the
// dialect-specific text and never branches on the dialect name inline.
//
// For the WS7c vector-conformance surface the render axis only exercises finalizePlaceholders
// (the `?`→`$N` PG one-pass) and orderByNulls; the INSERT-conflict/guard-insert renderings are
// compiled TS-side into the published bundle SQL (spec §10: the dialect axis is compiled once,
// TS-side), so a consuming Go runtime never re-derives them. They are ported here anyway for
// parity + a clean seam should a future WS lower them Go-side.

package litedbmodel_runtime

import (
	"fmt"
	"strings"
)

// Dialect is a closed dialect strategy (the SSoT), one frozen record per dialect. Every method is
// a pure text producer — no side effects, no hidden fallback.
type Dialect interface {
	// Name is the dialect name ('sqlite'/'postgres'/'mysql').
	Name() string
	// FinalizePlaceholders converts the fully-assembled `?`-placeholder SQL text to this dialect's
	// final placeholder style. SQLite/MySQL: identity. Postgres: a single left-to-right pass
	// replacing the Nth `?` with `$N` (spec §8 final one-pass). Runs ONCE over the final text.
	FinalizePlaceholders(sql string) string
	// OrderByNulls renders a deterministic NULL-ordering ORDER BY term (spec §13). PG/SQLite use
	// native `NULLS FIRST/LAST`; MySQL emulates with a leading `<expr> IS NULL` sort key.
	OrderByNulls(expr, dir, nulls string) string
	// InsertVerb returns the INSERT verb+prefix (up to `INTO`) for the given conflict action.
	InsertVerb(ignore bool) string
	// InsertConflictClause renders an INSERT's trailing conflict clause (empty when handled by the
	// verb). `updateColumns == nil` means the `ignore` action; a non-nil slice means an upsert.
	InsertConflictClause(conflictColumns []string, ignore bool, updateColumns []string) string
	// GuardInsert renders a bare do-nothing GUARD INSERT (the gate-first idempotency/unique guard,
	// spec §6). SQLite/Postgres emit a trailing `ON CONFLICT DO NOTHING`; MySQL emits `INSERT IGNORE`.
	GuardInsert(table string, columns []string, placeholders string) string
}

// toDollarPlaceholders replaces each `?` with `$1, $2, …` left-to-right (Postgres, spec §8 final
// one-pass). Ported EXACTLY from dialect.ts toDollarPlaceholders. Runs once over the final flat SQL.
func toDollarPlaceholders(sql string) string {
	n := 0
	var out strings.Builder
	for _, ch := range sql {
		if ch == '?' {
			n++
			fmt.Fprintf(&out, "$%d", n)
		} else {
			out.WriteRune(ch)
		}
	}
	return out.String()
}

// onConflictClause renders the "has-a-column-target" upsert/ignore clause shared by SQLite/Postgres
// (`ON CONFLICT (cols) DO NOTHING | DO UPDATE SET …`). `excludedRef` produces the per-column RHS.
func onConflictClause(conflictColumns []string, ignore bool, updateColumns []string, excludedRef func(string) string) string {
	target := ""
	if len(conflictColumns) > 0 {
		target = " (" + strings.Join(conflictColumns, ", ") + ")"
	}
	if ignore {
		return " ON CONFLICT" + target + " DO NOTHING"
	}
	sets := make([]string, len(updateColumns))
	for i, c := range updateColumns {
		sets[i] = c + " = " + excludedRef(c)
	}
	return " ON CONFLICT" + target + " DO UPDATE SET " + strings.Join(sets, ", ")
}

// guardInsertText renders a bare do-nothing guard INSERT.
func guardInsertText(table string, columns []string, placeholders string, insertIgnore bool) string {
	head := "INSERT INTO"
	tail := " ON CONFLICT DO NOTHING"
	if insertIgnore {
		head = "INSERT IGNORE INTO"
		tail = ""
	}
	return fmt.Sprintf("%s %s (%s) VALUES (%s)%s", head, table, strings.Join(columns, ", "), placeholders, tail)
}

// ── SQLite ─────────────────────────────────────────────────────────────────────

type sqliteDialect struct{}

func (sqliteDialect) Name() string                           { return "sqlite" }
func (sqliteDialect) FinalizePlaceholders(sql string) string { return sql }
func (sqliteDialect) OrderByNulls(expr, dir, nulls string) string {
	return fmt.Sprintf("%s %s NULLS %s", expr, dir, nulls)
}
func (sqliteDialect) InsertVerb(ignore bool) string {
	if ignore {
		return "INSERT OR IGNORE INTO"
	}
	return "INSERT INTO"
}
func (sqliteDialect) InsertConflictClause(conflictColumns []string, ignore bool, updateColumns []string) string {
	if ignore {
		return "" // handled by the `INSERT OR IGNORE` verb
	}
	return onConflictClause(conflictColumns, false, updateColumns, func(c string) string { return "excluded." + c })
}
func (sqliteDialect) GuardInsert(table string, columns []string, placeholders string) string {
	return guardInsertText(table, columns, placeholders, false)
}

// ── Postgres ─────────────────────────────────────────────────────────────────

type postgresDialect struct{}

func (postgresDialect) Name() string                           { return "postgres" }
func (postgresDialect) FinalizePlaceholders(sql string) string { return toDollarPlaceholders(sql) }
func (postgresDialect) OrderByNulls(expr, dir, nulls string) string {
	return fmt.Sprintf("%s %s NULLS %s", expr, dir, nulls)
}
func (postgresDialect) InsertVerb(ignore bool) string { return "INSERT INTO" }
func (postgresDialect) InsertConflictClause(conflictColumns []string, ignore bool, updateColumns []string) string {
	return onConflictClause(conflictColumns, ignore, updateColumns, func(c string) string { return "EXCLUDED." + c })
}
func (postgresDialect) GuardInsert(table string, columns []string, placeholders string) string {
	return guardInsertText(table, columns, placeholders, false)
}

// ── MySQL ───────────────────────────────────────────────────────────────────

type mysqlDialect struct{}

func (mysqlDialect) Name() string                           { return "mysql" }
func (mysqlDialect) FinalizePlaceholders(sql string) string { return sql }
func (mysqlDialect) OrderByNulls(expr, dir, nulls string) string {
	// MySQL: emulate NULLS FIRST/LAST with a leading `IS NULL` key (NULL sorts LOWEST by default).
	flagDir := "ASC"
	if nulls == "FIRST" {
		flagDir = "DESC"
	}
	return fmt.Sprintf("%s IS NULL %s, %s %s", expr, flagDir, expr, dir)
}
func (mysqlDialect) InsertVerb(ignore bool) string {
	if ignore {
		return "INSERT IGNORE INTO"
	}
	return "INSERT INTO"
}
func (mysqlDialect) InsertConflictClause(_ []string, ignore bool, updateColumns []string) string {
	if ignore {
		return "" // handled by the `INSERT IGNORE` verb
	}
	sets := make([]string, len(updateColumns))
	for i, c := range updateColumns {
		sets[i] = fmt.Sprintf("%s = VALUES(%s)", c, c)
	}
	return " ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
}
func (mysqlDialect) GuardInsert(table string, columns []string, placeholders string) string {
	return guardInsertText(table, columns, placeholders, true)
}

// dialects is the closed strategy registry (SSoT).
var dialects = map[string]Dialect{
	"sqlite":   sqliteDialect{},
	"postgres": postgresDialect{},
	"mysql":    mysqlDialect{},
}

// DialectFor resolves a dialect name to its strategy (fail-closed — no silent default).
func DialectFor(name string) (Dialect, error) {
	d, ok := dialects[name]
	if !ok {
		return nil, fmt.Errorf("scp dialect: unknown dialect '%s' (known: sqlite, postgres, mysql)", name)
	}
	return d, nil
}
