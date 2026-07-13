// TRUE-codegen cell (#44 perf integration; #60 milestone 1 — typed-NATIVE READ codegen).
//
// ══════════════════════════════════════════════════════════════════════════════════════════════
// TODO(#60 m1, Go BLOCKED — bc emitter defect, ESCALATE to bc, do not work around locally):
//
// bc's go-typed-native emitter (bc#102) generates the covered-read runner function
// `run_native_raw_struct_<Comp>` with a LOWERCASE-initial name (verified against the actual
// generated source, e.g. `go/lm_bench/cgmods/find/gen.go`) — in Go, a lowercase-initial identifier
// is UNEXPORTED and cannot be called from another package. Every OTHER symbol the module needs
// (`T0`, `In_<Comp>`, `Row_<Comp>_<node>`, `Handler_<Comp>`, `PortsNR_<Comp>_<node>`) IS exported,
// so this looks like a straightforward emitter naming bug (the function should be
// `RunNativeRawStruct<Comp>`), not a deliberate non-exported-by-design internal — but as generated
// today the runner is UNCALLABLE from this cell (a separate package), so Go's codegen cell CANNOT
// be wired onto typed-native in this pass. This is a genuine bc capability gap discovered during
// #60 m1 implementation — ESCALATE to bc; do NOT fork a local patched copy of the generated module.
//
// Consequently ALL codegen cases are UNAVAILABLE for Go right now (not just complexWhere/inList's
// bc#86 IN-list gap) — `runCodegenCase` panics naming this gap for every case if invoked. This is
// NOT a silent degrade: `impl=codegen` for Go now fails LOUDLY instead of running the retired
// RAW-ABI path (which has no generated module to bind against anymore — the old
// `cgmods/batchInsert`/`complexWhere`/`inList`/`writeTxGate` packages are no longer generated,
// #60 m1: writes are not codegen-module cases either). The bench harness/report MUST treat Go
// codegen as "blocked pending bc fix" until this lands upstream — never re-add a boxed/typed-raw
// fallback here locally.
// ══════════════════════════════════════════════════════════════════════════════════════════════
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
	wmi   *bc.Obj // a WRITE module's surrogate input (__sql/__sqlParams/__skip); nil for reads
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
			var wmi *bc.Obj
			if plan.Kind == "batch" || plan.Kind == "tx" {
				wmi = bc.NewObj()
				wmi.Set("__sql", plan.WriteSQL)
				wmi.Set("__sqlParams", []bc.Value{})
				wmi.Set("__skip", false)
			}
			m[id] = &preparedCase{plan: plan, input: in, wmi: wmi}
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

func rowsToRaw(cols []string, data [][]any) []bc.RawValue {
	out := make([]bc.RawValue, len(data))
	for i, row := range data {
		ro := bc.NewRawObj()
		for j, c := range cols {
			ro.Set(c, row[j])
		}
		out[i] = ro
	}
	return out
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

// nativeReadHandler renders the primary node's NATIVE statement plan against the evaluated
// `__scope`, runs REAL SQL, and returns the rows as []RawValue of RawObj for bc's de-box.
type nativeReadHandler struct {
	plan *cgplans.ReadPlan
	db   cgDB
}

func (h *nativeReadHandler) ExecRaw(component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	return h.ExecRawCtx("", component, ports, bound)
}

func (h *nativeReadHandler) ExecRawCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	scopeV, _ := ports.Get("__scope")
	scope, ok := scopeV.(*bc.Obj)
	if !ok {
		return bc.ErrRaw("codegen: __scope did not evaluate to an object"), true
	}
	query, args := renderReadNative(h.plan, scope)
	cols, data := queryNative(h.db, query, args)
	return bc.OkRaw(rowsToRaw(cols, data)), true
}

// nativeWriteHandler runs the NATIVE gate-first transaction plan and returns the
// TransactionResult as a RawObj (committed/executed/shortCircuit/entity always present —
// present-as-null for an absent optional — plus returnedRows only when a batch RETURNING
// produced rows), exactly the shape the generated write module's de-box marshal expects.
type nativeWriteHandler struct {
	plan    *cgplans.TxPlan
	dialect cgplans.Dialect
	input   *bc.Obj
	db      cgDB
}

func (h *nativeWriteHandler) ExecRaw(component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	return bc.OkRaw(execTxNative(h.plan, h.dialect, h.input, h.db)), true
}

func (h *nativeWriteHandler) ExecRawCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	return h.ExecRaw(component, ports, bound)
}

// objToRaw converts a scanned row object to a RawValue (native bc-value conversion; the cells are
// already int64/string/etc. from scanCell).
func objToRaw(o *bc.Obj) bc.RawValue {
	ro := bc.NewRawObj()
	for _, k := range o.Keys {
		v := o.Vals[k]
		if inner, isObj := v.(*bc.Obj); isObj {
			ro.Set(k, objToRaw(inner))
		} else {
			ro.Set(k, v)
		}
	}
	return ro
}

// execTxNative is the NATIVE gate-first transaction execution (mirror of the runtime's
// ExecuteTransactionBundle for the companion's closed set): BEGIN → per-statement native param
// eval + render + execute → gate short-circuit (ROLLBACK + committed:false result) →
// entityFrom/binds RETURNING-row scope binds → batch returnedRows accumulation → COMMIT.
func execTxNative(plan *cgplans.TxPlan, dialect cgplans.Dialect, input *bc.Obj, db cgDB) bc.RawValue {
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

	executed := make([]bc.RawValue, 0, len(plan.Statements))
	var entity *bc.Obj
	var returnedRows []bc.RawValue

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
				sc := bc.NewRawObj()
				sc.Set("statementId", stmt.ID)
				sc.Set("reason", reason)
				out := bc.NewRawObj()
				out.Set("committed", false)
				out.Set("executed", executed)
				out.Set("shortCircuit", sc)
				out.Set("entity", nil)
				return out
			}
		}

		if plan.IsBatch && len(rows) > 0 {
			group := make([]bc.RawValue, len(rows))
			for j, r := range rows {
				group[j] = objToRaw(r.(*bc.Obj))
			}
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
	out := bc.NewRawObj()
	out.Set("committed", true)
	out.Set("executed", executed)
	out.Set("shortCircuit", nil)
	if entity != nil {
		out.Set("entity", objToRaw(entity))
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

// runCodegenCase executes ONE case THROUGH its generated module (RAW ABI) with the native
// handlers, returning the produced output (the verify leg canonicalizes it; timed loops discard).
func runCodegenCase(dialect, caseID string, db cgDB) bc.Value {
	_ = preparedFor(dialect, caseID) // validates dialect/case exist before reporting the block
	panic(
		"codegen (go): BLOCKED on a bc go-typed-native emitter defect (bc#102) — the generated " +
			"covered-read runner `run_native_raw_struct_<Comp>` is emitted with a LOWERCASE-initial " +
			"name, so it is UNEXPORTED and uncallable from this cell's package. ALL codegen cases " +
			"(not just complexWhere/inList's bc#86 IN-list gap) are unavailable for Go until this is " +
			"fixed upstream (see the package doc TODO at the top of codegen_cell.go). ESCALATE to bc; " +
			"do NOT fork a local patched copy of the generated module, and do NOT fall back to the " +
			"retired RAW-ABI path (no generated write/complexWhere/inList module exists anymore either).",
	)
}

// runCodegen is the timed codegen op (output discarded).
func runCodegen(dialect, caseID string, db cgDB) {
	_ = runCodegenCase(dialect, caseID, db)
}
