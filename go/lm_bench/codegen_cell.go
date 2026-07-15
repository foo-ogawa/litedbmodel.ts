// TRUE-codegen cell (#44 perf integration; #60 milestone 1 — typed-NATIVE READ codegen).
//
// WIRED onto bc's go-typed-native modules (bc 0.7.3): the covered-read runner is now EXPORTED
// (`RunNativeRawStruct_<Comp>`, uppercase-initial — bc#102 fixed), so every read case executes
// THROUGH its generated module (`runCodegenCase` below implements each module's `Handler_<Comp>`,
// rendering + executing the static SQL and scanning rows into the module's concrete `T0` struct, then
// drives the read via `RunNativeRawStruct_<Comp>`; relations stitch via `stitchNative`). Writes
// (`batchInsert`/`writeTxGate`) have no codegen module (#60 m1) — they call `execTxNative` directly.
// All 8 cases verify byte-identical to the ir path.
//
// OWNER ORDERS (absolute): the codegen EXECUTION path touches NO IR data and NO JSON-handling
// library — no encoding/json, no litedbmodel_runtime bundle/IR types (rt.BundleFromJObj /
// rt.ReadGraph carry JSON-decoded IR data). Everything the timed op needs is the GENERATED native
// companion (`cgplans` — pre-decoded statement/tx/relation plans + bench inputs, emitted by
// benchmark/crosslang/generate.ts through a CLOSED-SET fail-closed decoder) + database/sql as the
// driver seam. The native SQL render engine / relation stitch / gate-first tx executor below are
// kept (language-neutral, `bc.Value`-based) — they are NOT what's blocked; only the typed-native
// module WIRING is.
//
// Fail-closed: an unknown case / spec kind / non-scalar driver arg PANICS loudly — never a silent
// degrade (companion generation already fail-closed on out-of-set shapes).
package main

import (
	"database/sql"
	"strconv"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	"github.com/foo-ogawa/litedbmodel/go/lm_bench/cgplans"

	// The bc-GENERATED typed-native READ modules (bc 0.7.3 go-typed-native — RUNTIME-FREE: zero
	// boxed *Obj/Value on the module's own surface, no bc-runtime import in the generated module).
	// This cell implements each module's Handler_<Comp> (Node_* renders + executes the static SQL via
	// renderReadNative and scans rows into the module's concrete T0 struct) and drives the read THROUGH
	// each module's exported RunNativeRawStruct_<Comp> runner (bc 0.7.3 fixes bc#102 — the runner is
	// now EXPORTED, uppercase-initial, callable cross-package). Writes stay on execTxNative (no codegen
	// write module — #60 m1).
	cgBelongsTo "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/belongsTo"
	cgComplexWhere "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/complexWhere"
	cgFind "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/find"
	cgHasMany "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/hasMany"
	cgHasManyLimit "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/hasManyLimit"
	cgInList "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/inList"
)

// cgDB is the SQL driver seam the codegen cell drives (a *sql.DB satisfies it — the real seeded
// sqlite, the mock micro driver, and the trace cost probe all ride database/sql).
type cgDB interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
	Begin() (*sql.Tx, error)
}

// ── prepared cases: the native input scopes, materialized ONCE at package init ──

type preparedCase struct {
	plan  *cgplans.CasePlan
	input *bc.Obj // the bench input scope (native cgplans data → bc values; NO JSON decode)
}

// cgPrepared maps dialect -> case id -> the prepared case.
var cgPrepared = func() map[string]map[string]*preparedCase {
	out := make(map[string]map[string]*preparedCase, len(cgplans.Plans))
	for dialect, cases := range cgplans.Plans {
		m := make(map[string]*preparedCase, len(cases))
		for id, plan := range cases {
			in := bc.NewObj()
			for _, kv := range plan.Input {
				in.Set(kv.K, kvValue(kv.V))
			}
			m[id] = &preparedCase{plan: plan, input: in}
		}
		out[dialect] = m
	}
	return out
}()

// kvValue converts a cgplans native input value to a bc Value (closed set — fail-closed).
func kvValue(v any) bc.Value {
	switch t := v.(type) {
	case nil, bool, int64, float64, string:
		return t
	case []int64:
		out := make([]bc.Value, len(t))
		for i, e := range t {
			out[i] = e
		}
		return out
	case []string:
		out := make([]bc.Value, len(t))
		for i, e := range t {
			out[i] = e
		}
		return out
	default:
		panic("codegen native: input value outside the closed set (fail-closed)")
	}
}

func preparedFor(dialect, caseID string) *preparedCase {
	pc, ok := cgPrepared[dialect][caseID]
	if !ok {
		panic("codegen native: unknown dialect/case '" + dialect + "/" + caseID + "' (fail-closed)")
	}
	return pc
}

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE value-spec / statement render engine (mirror of the runtime's
// renderStatements semantics for the CLOSED companion set — byte-identical SQL
// text + params for the shapes in scope; no JSON, no IR)
// ═══════════════════════════════════════════════════════════════════════════════

// scopeRef walks a bc scope object by path (bc `{ref:[..]}` semantics). Fail-closed: a missing
// head/segment panics (mirrors bc UNKNOWN_BINDING — never a silent nil).
func scopeRef(scope *bc.Obj, path []string) bc.Value {
	cur, ok := scope.Get(path[0])
	if !ok {
		panic("codegen native: unknown binding '" + path[0] + "' (fail-closed)")
	}
	for _, seg := range path[1:] {
		obj, isObj := cur.(*bc.Obj)
		if !isObj {
			panic("codegen native: ref path '." + seg + "' into a non-object (fail-closed)")
		}
		cur, ok = obj.Get(seg)
		if !ok {
			panic("codegen native: unknown property '." + seg + "' (fail-closed)")
		}
	}
	return cur
}

// jsonEscapeInto appends a JSON string literal (native writer — JS JSON.stringify form).
func jsonEscapeInto(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		case '\b':
			sb.WriteString("\\b")
		case '\f':
			sb.WriteString("\\f")
		default:
			if r < 0x20 {
				sb.WriteString("\\u")
				hex := strconv.FormatInt(int64(r), 16)
				for len(hex) < 4 {
					hex = "0" + hex
				}
				sb.WriteString(hex)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
}

// jsonInListText builds the single-JSON-text IN-list param natively (mirror of the runtime
// evalSpec encode: mysql booleans as 1/0; closed scalar set — anything else fail-closed).
func jsonInListText(arr []bc.Value, mysql bool) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, e := range arr {
		if i > 0 {
			sb.WriteByte(',')
		}
		switch t := e.(type) {
		case nil:
			sb.WriteString("null")
		case bool:
			if mysql {
				if t {
					sb.WriteByte('1')
				} else {
					sb.WriteByte('0')
				}
			} else if t {
				sb.WriteString("true")
			} else {
				sb.WriteString("false")
			}
		case int64:
			sb.WriteString(strconv.FormatInt(t, 10))
		case float64:
			if t == float64(int64(t)) {
				sb.WriteString(strconv.FormatInt(int64(t), 10))
			} else {
				sb.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
			}
		case string:
			jsonEscapeInto(&sb, t)
		default:
			panic("codegen native: IN-list element outside the closed scalar set (fail-closed)")
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

// evalSpecArg evaluates one companion value-spec against the scope into a DRIVER arg (native
// mirror of the runtime evalSpec + toDriverParam). A non-scalar arg (a PG array param) cannot
// bind through database/sql — fail-closed panic (the go bench's PG DB-backed/micro legs are
// protocol-level skips, so this is unreachable in a valid run — never a silent degrade).
func evalSpecArg(spec *cgplans.Spec, scope *bc.Obj) any {
	switch spec.Kind {
	case cgplans.SpecRef:
		return driverArg(scopeRef(scope, spec.Path))
	case cgplans.SpecStr:
		return spec.Str
	case cgplans.SpecInt:
		return spec.Int
	case cgplans.SpecArrLit:
		panic("codegen native: literal-array param (PG UNNEST) cannot bind through database/sql — fail-closed")
	case cgplans.SpecJSONArray:
		v := scopeRef(scope, spec.Path)
		arr, ok := v.([]bc.Value)
		if !ok {
			panic("codegen native: IN-list value-spec did not evaluate to an array (fail-closed)")
		}
		if spec.ArrDialect == cgplans.Postgres {
			panic("codegen native: postgres array param cannot bind through database/sql — fail-closed")
		}
		return jsonInListText(arr, spec.ArrDialect == cgplans.Mysql)
	default:
		panic("codegen native: unknown value-spec kind (fail-closed)")
	}
}

// driverArg converts a bc scalar Value to a database/sql arg (closed set — fail-closed).
func driverArg(v bc.Value) any {
	switch t := v.(type) {
	case nil, bool, int64, float64, string:
		return t
	default:
		panic("codegen native: a non-scalar reached the param binder (fail-closed)")
	}
}

// renderPlaceholdersNative rewrites `?` → `$N` for postgres (quote-aware; byte-port of the
// runtime renderPlaceholders). MySQL/SQLite keep `?`.
func renderPlaceholdersNative(sqlText string, dialect cgplans.Dialect) string {
	if dialect != cgplans.Postgres {
		return sqlText
	}
	var sb strings.Builder
	sb.Grow(len(sqlText) + 8)
	index := 0
	inString := false
	for _, r := range sqlText {
		switch {
		case inString:
			sb.WriteRune(r)
			if r == '\'' {
				inString = false
			}
		case r == '\'':
			sb.WriteRune(r)
			inString = true
		case r == '?':
			index++
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(index))
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// renderReadNative renders a native read plan against the evaluated `__scope` (mirror of the
// runtime renderStatements): SKIP-if-null statement drop (absent == null), ` WHERE `/` AND `
// connector, `?`→`$N`. The `?`/param arity was asserted at generation time (static data).
func renderReadNative(plan *cgplans.ReadPlan, scope *bc.Obj) (string, []any) {
	var sb strings.Builder
	args := make([]any, 0, 8)
	whereSeen := false
	for i := range plan.Stmts {
		stmt := &plan.Stmts[i]
		if stmt.HasSkip {
			v, ok := scope.Get(stmt.SkipIfNullHead)
			if !ok || v == nil {
				continue // refOpt(head) == null → skip (absent reads as null — the SKIP contract)
			}
		}
		for j := range stmt.Params {
			args = append(args, evalSpecArg(&stmt.Params[j], scope))
		}
		if stmt.WhereFragment {
			if whereSeen {
				sb.WriteString(" AND ")
			} else {
				sb.WriteString(" WHERE ")
			}
			whereSeen = true
		}
		sb.WriteString(stmt.SQL)
	}
	return renderPlaceholdersNative(sb.String(), plan.Dialect), args
}

// ── native row scan (database/sql → bc values; int columns stay int64) ─────────

// scanCell normalizes one scanned column: []byte → string; an INTEGRAL float64 → int64 (the
// typed de-box outType for INTEGER columns is int64 — this is what a real Go wire consumer
// scanning an int column produces); everything else passes through.
func scanCell(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	default:
		return v
	}
}

type sqlQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// queryNative runs a SELECT/RETURNING natively: ordered column names + normalized cells.
func queryNative(db sqlQuerier, query string, args []any) ([]string, [][]any) {
	rows, err := db.Query(query, args...)
	must(err)
	defer rows.Close()
	cols, err := rows.Columns()
	must(err)
	var out [][]any
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		must(rows.Scan(ptrs...))
		for i := range raw {
			raw[i] = scanCell(raw[i])
		}
		out = append(out, raw)
	}
	must(rows.Err())
	return cols, out
}

func rowsToObjs(cols []string, data [][]any) []bc.Value {
	out := make([]bc.Value, len(data))
	for i, row := range data {
		o := bc.NewObj()
		for j, c := range cols {
			o.Set(c, row[j])
		}
		out[i] = o
	}
	return out
}

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE handlers at the generated modules' makeSQL seam
// ═══════════════════════════════════════════════════════════════════════════════

// execTxNative is the NATIVE gate-first transaction execution (mirror of the runtime's
// ExecuteTransactionBundle for the companion's closed set): BEGIN → per-statement native param
// eval + render + execute → gate short-circuit (ROLLBACK + committed:false result) →
// entityFrom/binds RETURNING-row scope binds → batch returnedRows accumulation → COMMIT. Returns the
// TransactionResult as a plain *bc.Obj (committed/executed/shortCircuit/entity always present —
// present-as-null for an absent optional — plus returnedRows only when a batch RETURNING produced
// rows), the SAME canonical shape the ir path's txResultToObj produces. #60 m1: writes have no codegen
// module de-box (no RawValue ABI) — this is called directly and its *bc.Obj is the verify output.
func execTxNative(plan *cgplans.TxPlan, dialect cgplans.Dialect, input *bc.Obj, db cgDB) bc.Value {
	tx, err := db.Begin()
	must(err)
	done := false
	defer func() {
		if !done {
			_ = tx.Rollback() // best-effort on a panic path; the panic itself surfaces
		}
	}()

	// The tx scope: the input bindings + later __entity / binds rows (a shallow copy — the
	// prepared input stays immutable across iterations).
	scope := bc.NewObj()
	for _, k := range input.Keys {
		scope.Set(k, input.Vals[k])
	}

	executed := make([]bc.Value, 0, len(plan.Statements))
	var entity *bc.Obj
	var returnedRows []bc.Value

	for i := range plan.Statements {
		stmt := &plan.Statements[i]
		args := make([]any, 0, len(stmt.Params))
		for j := range stmt.Params {
			args = append(args, evalSpecArg(&stmt.Params[j], scope))
		}
		query := renderPlaceholdersNative(stmt.SQL, dialect)

		var rows []bc.Value
		var changes int64
		if stmt.IsReturn {
			cols, data := queryNative(tx, query, args)
			rows = rowsToObjs(cols, data)
			changes = int64(len(rows))
		} else {
			res, execErr := tx.Exec(query, args...)
			must(execErr)
			n, affErr := res.RowsAffected()
			must(affErr)
			changes = n
		}
		executed = append(executed, stmt.ID)

		if stmt.Gate != cgplans.GateNone {
			var reason string
			switch stmt.Gate {
			case cgplans.GateExistsElseRollback:
				if len(rows) == 0 {
					reason = "requires_absent"
				}
			case cgplans.GateInsertedElseRollback:
				if changes == 0 {
					reason = "unique_collision"
				}
			case cgplans.GateInsertedElseNoop:
				if changes == 0 {
					reason = "idempotent_duplicate"
				}
			}
			if reason != "" {
				must(tx.Rollback())
				done = true // the rollback consumed the tx; suppress the deferred rollback
				sc := bc.NewObj()
				sc.Set("statementId", stmt.ID)
				sc.Set("reason", reason)
				out := bc.NewObj()
				out.Set("committed", false)
				out.Set("executed", executed)
				out.Set("shortCircuit", sc)
				out.Set("entity", nil)
				return out
			}
		}

		if plan.IsBatch && len(rows) > 0 {
			group := make([]bc.Value, len(rows))
			copy(group, rows)
			returnedRows = append(returnedRows, group)
		}
		var firstRow *bc.Obj
		if len(rows) > 0 {
			firstRow = rows[0].(*bc.Obj)
		}
		if plan.EntityFrom != "" && plan.EntityFrom == stmt.ID {
			entity = firstRow
			if entity != nil {
				scope.Set("__entity", entity)
			}
		}
		if stmt.Binds != "" && firstRow != nil {
			scope.Set(stmt.Binds, firstRow)
		}
	}

	must(tx.Commit())
	done = true
	out := bc.NewObj()
	out.Set("committed", true)
	out.Set("executed", executed)
	out.Set("shortCircuit", nil)
	if entity != nil {
		out.Set("entity", entity)
	} else {
		out.Set("entity", nil)
	}
	if len(returnedRows) > 0 {
		out.Set("returnedRows", returnedRows)
	}
	return out
}

// ═══════════════════════════════════════════════════════════════════════════════
// NATIVE relation batch stitch (mirror of the runtime StitchRelation for the
// companion's single-key closed set)
// ═══════════════════════════════════════════════════════════════════════════════

// stringifyKeyNative mirrors the runtime's key identity (int64 / integral float / bool / string).
func stringifyKeyNative(v bc.Value) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'g', -1, 64)
	case string:
		return t
	default:
		panic("codegen native: relation key outside the closed scalar set (fail-closed)")
	}
}

// stitchNative batch-loads + hydrates ONE single-key relation onto the parent rows: dedupe
// non-null parent keys (insertion order), bind (mysql/sqlite: ONE JSON text param; PG cannot
// bind arrays through database/sql — fail-closed, and the go bench's PG legs are protocol-level
// skips), group child rows by target key, distribute per cardinality.
func stitchNative(rel *cgplans.Relation, parents []bc.Value, db cgDB) []bc.Value {
	seen := make(map[string]bool, len(parents))
	keys := make([]bc.Value, 0, len(parents))
	for _, p := range parents {
		obj, ok := p.(*bc.Obj)
		if !ok {
			panic("codegen native: relation parent is not a row object (fail-closed)")
		}
		v, has := obj.Get(rel.ParentKey)
		if !has || v == nil {
			continue
		}
		id := stringifyKeyNative(v)
		if !seen[id] {
			seen[id] = true
			keys = append(keys, v)
		}
	}

	batch := make(map[string][]bc.Value)
	if len(keys) > 0 {
		if rel.Dialect == cgplans.Postgres {
			panic("codegen native: postgres relation array binding not wired through database/sql — fail-closed")
		}
		sqlText := renderPlaceholdersNative(rel.SQL, rel.Dialect)
		arg := jsonInListText(keys, rel.Dialect == cgplans.Mysql)
		cols, data := queryNative(db, sqlText, []any{arg})
		for _, row := range rowsToObjs(cols, data) {
			obj := row.(*bc.Obj)
			k := "null"
			if v, has := obj.Get(rel.TargetKey); has {
				k = stringifyKeyNative(v)
			}
			batch[k] = append(batch[k], row)
		}
	}

	for _, p := range parents {
		obj := p.(*bc.Obj)
		var children []bc.Value
		if v, has := obj.Get(rel.ParentKey); has && v != nil {
			children = batch[stringifyKeyNative(v)]
		}
		if rel.Kind == "hasMany" {
			if children == nil {
				children = []bc.Value{}
			}
			obj.Set(rel.Name, children)
		} else {
			if len(children) > 0 {
				obj.Set(rel.Name, children[0])
			} else {
				obj.Set(rel.Name, nil)
			}
		}
	}
	return parents
}

// ═══════════════════════════════════════════════════════════════════════════════
// Case dispatch (through the bc-GENERATED modules — the ONLY exec entry)
// ═══════════════════════════════════════════════════════════════════════════════

// nativeReadRows renders the covered read node's static plan against `scope` and scans REAL SQL rows
// as ordered column-name → cell pairs (native; no *Obj/Value boxing on the module's own surface —
// each per-module handler decodes these directly into its concrete T0 struct). Shared by every
// Handler_<Comp> below so all covered reads run the SAME native render+execute the ir path runs.
func nativeReadRows(plan *cgplans.ReadPlan, scope *bc.Obj, db cgDB) ([]string, [][]any) {
	query, args := renderReadNative(plan, scope)
	return queryNative(db, query, args)
}

// cellString/cellInt read one scanned cell by column name as its native scalar (the scanned cells are
// already int64/string via scanCell). Fail-closed on a wrong type.
func cellInt(cols []string, row []any, col string) int64 {
	for i, c := range cols {
		if c == col {
			if v, ok := row[i].(int64); ok {
				return v
			}
			panic("codegen native: column '" + col + "' is not an int64 (fail-closed)")
		}
	}
	panic("codegen native: column '" + col + "' missing from row (fail-closed)")
}
func cellStr(cols []string, row []any, col string) string {
	for i, c := range cols {
		if c == col {
			if v, ok := row[i].(string); ok {
				return v
			}
			panic("codegen native: column '" + col + "' is not a string (fail-closed)")
		}
	}
	panic("codegen native: column '" + col + "' missing from row (fail-closed)")
}

// scopeObj materializes a native ports value set into a *bc.Obj render scope (the small handful of
// WHERE-bound scalars/arrays the covered node reads — cheap, not a per-row cost).
func scopeObj(pairs ...[2]any) *bc.Obj {
	o := bc.NewObj()
	for _, kv := range pairs {
		o.Set(kv[0].(string), bc.Value(kv[1]))
	}
	return o
}

// ── per-module Handler_<Comp> impls (Node_* renders + executes + scans into the module's T0) ──

type findHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h findHandler) Node_Find_n0(ports cgFind.PortsNR_Find_n0, _ *string) (cgFind.Row_Find_n0, bool) {
	scope := scopeObj([2]any{"author_id", ports.Author_id}, [2]any{"status", ports.Status}, [2]any{"since", ports.Since})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgFind.T0, len(data))
	for i, row := range data {
		val[i] = cgFind.T0{
			Id: cellInt(cols, row, "id"), Author_id: cellInt(cols, row, "author_id"),
			Title: cellStr(cols, row, "title"), Status: cellStr(cols, row, "status"),
			Views: cellInt(cols, row, "views"), Created_at: cellStr(cols, row, "created_at"),
		}
	}
	return cgFind.Row_Find_n0{Val: val}, true
}

// The Posts entry (belongsTo / hasMany / hasManyLimit share it) — one scalar author_id WHERE port.
// Each module declares a DISTINCT (though structurally identical) type set, so one handler per module.
type postsBelongsToHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h postsBelongsToHandler) Node_Posts_n0(ports cgBelongsTo.PortsNR_Posts_n0, _ *string) (cgBelongsTo.Row_Posts_n0, bool) {
	scope := scopeObj([2]any{"author_id", ports.Author_id})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgBelongsTo.T0, len(data))
	for i, row := range data {
		val[i] = cgBelongsTo.T0{Id: cellInt(cols, row, "id"), Author_id: cellInt(cols, row, "author_id"), Title: cellStr(cols, row, "title")}
	}
	return cgBelongsTo.Row_Posts_n0{Val: val}, true
}

type postsHasManyHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h postsHasManyHandler) Node_Posts_n0(ports cgHasMany.PortsNR_Posts_n0, _ *string) (cgHasMany.Row_Posts_n0, bool) {
	scope := scopeObj([2]any{"author_id", ports.Author_id})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgHasMany.T0, len(data))
	for i, row := range data {
		val[i] = cgHasMany.T0{Id: cellInt(cols, row, "id"), Author_id: cellInt(cols, row, "author_id"), Title: cellStr(cols, row, "title")}
	}
	return cgHasMany.Row_Posts_n0{Val: val}, true
}

type postsHasManyLimitHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h postsHasManyLimitHandler) Node_Posts_n0(ports cgHasManyLimit.PortsNR_Posts_n0, _ *string) (cgHasManyLimit.Row_Posts_n0, bool) {
	scope := scopeObj([2]any{"author_id", ports.Author_id})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgHasManyLimit.T0, len(data))
	for i, row := range data {
		val[i] = cgHasManyLimit.T0{Id: cellInt(cols, row, "id"), Author_id: cellInt(cols, row, "author_id"), Title: cellStr(cols, row, "title")}
	}
	return cgHasManyLimit.Row_Posts_n0{Val: val}, true
}

type inListHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h inListHandler) Node_ByIds_n0(ports cgInList.PortsNR_ByIds_n0, _ *string) (cgInList.Row_ByIds_n0, bool) {
	arr := make([]bc.Value, len(ports.Ids))
	for i, id := range ports.Ids {
		arr[i] = id
	}
	scope := scopeObj([2]any{"ids", arr})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgInList.T0, len(data))
	for i, row := range data {
		val[i] = cgInList.T0{Id: cellInt(cols, row, "id"), Title: cellStr(cols, row, "title")}
	}
	return cgInList.Row_ByIds_n0{Val: val}, true
}

type complexWhereHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h complexWhereHandler) Node_ComplexWhere_n0(ports cgComplexWhere.PortsNR_ComplexWhere_n0, _ *string) (cgComplexWhere.Row_ComplexWhere_n0, bool) {
	arr := make([]bc.Value, len(ports.Ids))
	for i, id := range ports.Ids {
		arr[i] = id
	}
	scope := scopeObj([2]any{"author_id", ports.Author_id}, [2]any{"since", ports.Since}, [2]any{"titleLike", ports.TitleLike}, [2]any{"ids", arr})
	cols, data := nativeReadRows(h.plan, scope, h.db)
	val := make([]cgComplexWhere.T0, len(data))
	for i, row := range data {
		val[i] = cgComplexWhere.T0{
			Id: cellInt(cols, row, "id"), Author_id: cellInt(cols, row, "author_id"),
			Title: cellStr(cols, row, "title"), Status: cellStr(cols, row, "status"), Views: cellInt(cols, row, "views"),
		}
	}
	return cgComplexWhere.Row_ComplexWhere_n0{Val: val}, true
}

// ── typed row → bc.Value (boxed ONLY at this OUTPUT boundary — not on the SQL row-fetch plane) ──

func postsRowsToValues[T any](rows []T, id func(T) int64, aid func(T) int64, title func(T) string) []bc.Value {
	out := make([]bc.Value, len(rows))
	for i, r := range rows {
		o := bc.NewObj()
		o.Set("id", id(r))
		o.Set("author_id", aid(r))
		o.Set("title", title(r))
		out[i] = o
	}
	return out
}

// runCodegenCase executes ONE case THROUGH its bc-GENERATED typed-native module (bc 0.7.3): reads route
// through RunNativeRawStruct_<Comp> with this cell's Handler_<Comp> (native render+execute+scan into the
// module's concrete T0); relations stitch via stitchNative; writes call execTxNative directly (no write
// codegen module — #60 m1). Fail-closed on an unknown case.
func runCodegenCase(dialect, caseID string, db cgDB) bc.Value {
	pc := preparedFor(dialect, caseID)
	switch caseID {
	case "find":
		in := cgFind.In_Find{Author_id: scopeI64(pc.input, "author_id"), Status: scopeStr(pc.input, "status"), Since: scopeStr(pc.input, "since")}
		rows, err := cgFind.RunNativeRawStruct_Find(findHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		out := make([]bc.Value, len(rows))
		for i, r := range rows {
			o := bc.NewObj()
			o.Set("id", r.Id)
			o.Set("author_id", r.Author_id)
			o.Set("title", r.Title)
			o.Set("status", r.Status)
			o.Set("views", r.Views)
			o.Set("created_at", r.Created_at)
			out[i] = o
		}
		return out
	case "belongsTo":
		in := cgBelongsTo.In_Posts{Author_id: scopeI64(pc.input, "author_id")}
		rows, err := cgBelongsTo.RunNativeRawStruct_Posts(postsBelongsToHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		parents := postsRowsToValues(rows, func(r cgBelongsTo.T0) int64 { return r.Id }, func(r cgBelongsTo.T0) int64 { return r.Author_id }, func(r cgBelongsTo.T0) string { return r.Title })
		return stitchNative(pc.plan.Rel, parents, db)
	case "hasMany":
		in := cgHasMany.In_Posts{Author_id: scopeI64(pc.input, "author_id")}
		rows, err := cgHasMany.RunNativeRawStruct_Posts(postsHasManyHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		parents := postsRowsToValues(rows, func(r cgHasMany.T0) int64 { return r.Id }, func(r cgHasMany.T0) int64 { return r.Author_id }, func(r cgHasMany.T0) string { return r.Title })
		return stitchNative(pc.plan.Rel, parents, db)
	case "hasManyLimit":
		in := cgHasManyLimit.In_Posts{Author_id: scopeI64(pc.input, "author_id")}
		rows, err := cgHasManyLimit.RunNativeRawStruct_Posts(postsHasManyLimitHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		parents := postsRowsToValues(rows, func(r cgHasManyLimit.T0) int64 { return r.Id }, func(r cgHasManyLimit.T0) int64 { return r.Author_id }, func(r cgHasManyLimit.T0) string { return r.Title })
		return stitchNative(pc.plan.Rel, parents, db)
	case "inList":
		in := cgInList.In_ByIds{Ids: scopeI64Arr(pc.input, "ids")}
		rows, err := cgInList.RunNativeRawStruct_ByIds(inListHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		out := make([]bc.Value, len(rows))
		for i, r := range rows {
			o := bc.NewObj()
			o.Set("id", r.Id)
			o.Set("title", r.Title)
			out[i] = o
		}
		return out
	case "complexWhere":
		in := cgComplexWhere.In_ComplexWhere{Author_id: scopeI64(pc.input, "author_id"), Since: scopeStr(pc.input, "since"), TitleLike: scopeStr(pc.input, "titleLike"), Ids: scopeI64Arr(pc.input, "ids")}
		rows, err := cgComplexWhere.RunNativeRawStruct_ComplexWhere(complexWhereHandler{plan: pc.plan.Read, db: db}, in)
		must(err)
		out := make([]bc.Value, len(rows))
		for i, r := range rows {
			o := bc.NewObj()
			o.Set("id", r.Id)
			o.Set("author_id", r.Author_id)
			o.Set("title", r.Title)
			o.Set("status", r.Status)
			o.Set("views", r.Views)
			out[i] = o
		}
		return out
	case "batchInsert", "writeTxGate":
		return execTxNative(pc.plan.Tx, pc.plan.Dialect, pc.input, db)
	default:
		panic("codegen native: unknown case '" + caseID + "' (fail-closed)")
	}
}

// scope helpers (native input scope → typed values for the module's In_<Comp>).
func scopeI64(scope *bc.Obj, key string) int64 {
	v, _ := scope.Get(key)
	if i, ok := v.(int64); ok {
		return i
	}
	panic("codegen native: input '" + key + "' is not an int64 (fail-closed)")
}
func scopeStr(scope *bc.Obj, key string) string {
	v, _ := scope.Get(key)
	if s, ok := v.(string); ok {
		return s
	}
	panic("codegen native: input '" + key + "' is not a string (fail-closed)")
}
func scopeI64Arr(scope *bc.Obj, key string) []int64 {
	v, _ := scope.Get(key)
	arr, ok := v.([]bc.Value)
	if !ok {
		panic("codegen native: input '" + key + "' is not an array (fail-closed)")
	}
	out := make([]int64, len(arr))
	for i, e := range arr {
		if n, ok := e.(int64); ok {
			out[i] = n
		} else {
			panic("codegen native: IN-list element is not an int64 (fail-closed)")
		}
	}
	return out
}

// runCodegen is the timed codegen op (output discarded).
func runCodegen(dialect, caseID string, db cgDB) {
	_ = runCodegenCase(dialect, caseID, db)
}
