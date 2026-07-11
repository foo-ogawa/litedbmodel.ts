// TRUE-codegen cell (#44 anti-sham fix) — execute THROUGH the bc-GENERATED module.
//
// The OLD codegen path was a DECORATION: runLM called the SAME ExecuteBundle/ReadBundle the `ir`
// cell calls, with only a cosmetic resident-bundle "verify" at load — so codegen was literally an
// alias of `ir` (and measured ~ir). This cell fixes that: it COMPILES the bc-GENERATED straight-
// line Go modules (materialized per-case as `cgmods/<case>` packages by generate.ts, emitted by
// litedbmodel generateCodegenArtifact = bc#75 straight-line, de-interpreted native source — the
// portable IR is NOT embedded, only its fingerprint) and executes each read case THROUGH the
// module's Bind(handler)[entry](input) — a DISTINCT code entry from `ir`'s ExecuteBundle, with NO
// RunBehavior tree-walk. It runs the REAL fail-closed skew gate (recompute
// FingerprintComponentGraph(live readGraph.IR) == baked IRFingerprint) mirroring the generated
// module header + the TS/Rust codegen cells.
package main

import (
	"fmt"

	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	cgBatchInsert "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/batchInsert"
	cgBelongsTo "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/belongsTo"
	cgComplexWhere "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/complexWhere"
	cgFind "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/find"
	cgHasMany "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/hasMany"
	cgHasManyLimit "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/hasManyLimit"
	cgInList "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/inList"
	cgWriteTxGate "github.com/foo-ogawa/litedbmodel/go/lm_bench/cgmods/writeTxGate"
)

// genModule is the uniform surface every per-case generated package exposes. `bindRaw` is the
// RAW-ABI de-boxed dispatch surface (bc#76): a RawComponentExec handler returns a RawValue and the
// generated `run_typed_raw_*` runner materializes the outType struct DIRECTLY from it (reads AND
// writes) — no dynamic *Obj/Value tree on the row/entity data plane.
type genModule struct {
	fingerprint string
	bindRaw     func(bc.RawComponentExec) map[string]func(*bc.Obj) (bc.Value, error)
	entry       string
}

// codegenModules maps case id -> its generated module surface (built from the imported packages).
var codegenModules = map[string]genModule{
	"find":         {cgFind.IRFingerprint, cgFind.BindRaw, cgFind.ComponentNames[0]},
	"complexWhere": {cgComplexWhere.IRFingerprint, cgComplexWhere.BindRaw, cgComplexWhere.ComponentNames[0]},
	"inList":       {cgInList.IRFingerprint, cgInList.BindRaw, cgInList.ComponentNames[0]},
	"belongsTo":    {cgBelongsTo.IRFingerprint, cgBelongsTo.BindRaw, cgBelongsTo.ComponentNames[0]},
	"hasMany":      {cgHasMany.IRFingerprint, cgHasMany.BindRaw, cgHasMany.ComponentNames[0]},
	"hasManyLimit": {cgHasManyLimit.IRFingerprint, cgHasManyLimit.BindRaw, cgHasManyLimit.ComponentNames[0]},
	"batchInsert":  {cgBatchInsert.IRFingerprint, cgBatchInsert.BindRaw, cgBatchInsert.ComponentNames[0]},
	"writeTxGate":  {cgWriteTxGate.IRFingerprint, cgWriteTxGate.BindRaw, cgWriteTxGate.ComponentNames[0]},
}

// codegenRawReadHandler is the RAW-ABI makeSQL handler for the GENERATED READ module's node boundary
// (bc#76 de-box): bc's generated raw runner calls it per SQL node with the evaluated `__scope`; we
// render the primary read node via the SAME runtime RenderExecuteNode the `ir` path uses, run REAL
// SQL, and return the row list as a native RawValue ([]RawValue of RawRow) — the generated runner
// de-boxes each row straight into the concrete struct (no *Obj on the row data plane).
type codegenRawReadHandler struct {
	graph     *rt.ReadGraph
	primaryID string
	db        rt.SQLDB
}

func (h *codegenRawReadHandler) ExecRaw(component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	return h.ExecRawCtx("", component, ports, bound)
}

func (h *codegenRawReadHandler) ExecRawCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	scopeV, _ := ports.Get("__scope")
	scope, ok := scopeV.(*bc.Obj)
	if !ok {
		return bc.ErrRaw("codegen: __scope did not evaluate to an object"), true
	}
	rows, err := rt.RenderExecuteNode(h.graph, h.primaryID, h.graph.Dialect, scope, h.db)
	if err != nil {
		return bc.ErrRaw(err.Error()), true
	}
	// Build the RawValue at the wire seam — bc's raw runner de-boxes it. rawFromValueDeboxed mirrors
	// bc's RawFromValue but coerces an INTEGRAL float64 back to int64: the go runtime's scanValue
	// floats INTEGER columns for JS-number parity (sqldb.go), but the typed de-box outType is `int`
	// (a strict RawValue.(int64) type-switch), so the int column's RawValue MUST be int64 — exactly
	// what a real go wire-consumer scanning an int column produces (the float shim is a boxed-path
	// artifact the raw seam undoes). A genuine float column keeps float64 (none in these cases).
	out := make([]bc.RawValue, len(rows))
	for i, r := range rows {
		out[i] = rawFromValueDeboxed(r)
	}
	return bc.OkRaw(out), true
}

// codegenRawWriteHandler is the RAW-ABI handler for a GENERATED WRITE module's single node boundary
// (bc#76 de-box): the node's outType IS the TransactionResult typed shape. We drive the derived
// transaction plan via the shared runtime ExecuteTransactionBundle (gate-first, byte-parity with the
// thin runtime) and return the TransactionResult as a native RawValue (RawRow), which the generated
// marshal_raw_T0 de-boxes into the concrete result struct — no *Obj on the entity/returnedRows plane.
type codegenRawWriteHandler struct {
	bundle *rt.SqlBundle
	input  *bc.Obj
	db     rt.SQLDB
}

func (h *codegenRawWriteHandler) ExecRaw(component string, ports *bc.Obj, bound bc.Value) (bc.RawOutcome, bool) {
	result, err := rt.ExecuteTransactionBundle(h.bundle, h.input, h.db.(rt.TxDB))
	if err != nil {
		return bc.ErrRaw(err.Error()), true
	}
	// The go runtime returns a typed TransactionResult STRUCT; present it as the canonical bc.Obj the
	// typed outType declares (committed/executed/shortCircuit/entity always present-as-null;
	// returnedRows only when populated — matching deriveWriteOutputType), then de-box via the raw seam
	// (int columns coerced back to int64). The de-box marshaller reads exactly these keys.
	return bc.OkRaw(rawFromValueDeboxed(txResultToObj(result))), true
}

// rawFromValueDeboxed mirrors bc.RawFromValue but coerces an INTEGRAL float64 to int64 (the go
// runtime's scanValue floats INTEGER columns for JS-number parity; the typed de-box outType for those
// columns is `int`, a strict int64 type-switch). Recurses through objects/arrays so nested row values
// (entity/returnedRows) coerce too. A non-integral float64 stays float64 (a genuine float column —
// none in the bench corpus, all numeric columns are INTEGER). Off the hot path (bench-adapter seam).
func rawFromValueDeboxed(v bc.Value) bc.RawValue {
	switch t := v.(type) {
	case *bc.Obj:
		ro := bc.NewRawObj()
		for _, k := range t.Keys {
			ro.Set(k, rawFromValueDeboxed(t.Vals[k]))
		}
		return ro
	case []bc.Value:
		out := make([]bc.RawValue, 0, len(t))
		for _, e := range t {
			out = append(out, rawFromValueDeboxed(e))
		}
		return out
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	default:
		return bc.RawFromValue(v)
	}
}

// txResultToObj presents the go runtime's typed TransactionResult STRUCT as the canonical bc.Obj the
// write outType declares: committed / executed / shortCircuit / entity ALWAYS present (shortCircuit &
// a null entity present-as-null, so the opt<..> de-box finds every key), plus returnedRows ONLY when
// the runtime populated it (a batch-with-RETURNING — matching deriveWriteOutputType, which types the
// field only for that shape). Field order + presence mirror the de-boxed `ser_T0` output exactly, so
// the codegen output and the interpreter reference (also routed through here) compare byte-for-byte.
func txResultToObj(r rt.TransactionResult) *bc.Obj {
	out := bc.NewObj()
	out.Set("committed", r.Committed)
	execVals := make([]bc.Value, len(r.Executed))
	for i, e := range r.Executed {
		execVals[i] = e
	}
	out.Set("executed", execVals)
	if r.ShortCircuit != nil {
		sc := bc.NewObj()
		sc.Set("statementId", r.ShortCircuit.StatementID)
		sc.Set("reason", string(r.ShortCircuit.Reason))
		out.Set("shortCircuit", sc)
	} else {
		out.Set("shortCircuit", nil)
	}
	if r.Entity == nil {
		out.Set("entity", nil)
	} else {
		out.Set("entity", r.Entity)
	}
	if r.ReturnedRows != nil {
		rr := make([]bc.Value, len(r.ReturnedRows))
		for i, group := range r.ReturnedRows {
			rr[i] = group
		}
		out.Set("returnedRows", rr)
	}
	return out
}

// coerceIntsValue returns a bc.Value with every INTEGRAL float64 rewritten to int64 (recursing
// through objects/arrays). The go de-boxed codegen output types INTEGER columns as int64 (ser_T0);
// the interpreter path floats them (scanValue). Both denote the same integers — this canonicalizes
// the interpreter side by VALUE for the behaviour-equality selfcheck. A fractional float64 (a genuine
// float column — none in the bench corpus) is left untouched. Representation-only; DATA is unchanged.
func coerceIntsValue(v bc.Value) bc.Value {
	switch t := v.(type) {
	case *bc.Obj:
		out := bc.NewObj()
		for _, k := range t.Keys {
			out.Set(k, coerceIntsValue(t.Vals[k]))
		}
		return out
	case []bc.Value:
		out := make([]bc.Value, len(t))
		for i, e := range t {
			out[i] = coerceIntsValue(e)
		}
		return out
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	default:
		return v
	}
}

// writeModuleInput is the input scope for a WRITE module's raw runner: bc's makeSqlComponentIR node
// evaluates `__sql`/`__sqlParams`/`__skip` port refs, so those heads MUST be present or slRef
// fail-closes (UNKNOWN_BINDING). The generated write runner passes them to the handler as ports, but
// our raw write handler ignores them (it drives the plan from the bundle), so present-as-empty is
// exact — the values are never read. This is the makeSQL surrogate input, not a fabricated default.
func writeModuleInput() *bc.Obj {
	in := bc.NewObj()
	in.Set("__sql", "")
	in.Set("__sqlParams", []bc.Value{})
	in.Set("__skip", false)
	return in
}

// codegenSkewGate — REAL fail-closed gate (bc#75 straight-line): recompute the fingerprint of the
// LIVE component-graph IR the runtime would execute and assert it equals the module's baked
// IRFingerprint. For reads the live IR is readGraph.IR; for writes the runtime does not surface the
// portable IR, so we compare against the case-artifact Fingerprint the generator computed from the
// SAME bundle (a real generated-const vs live comparison).
func codegenSkewGate(c *caseArt, bundle *rt.SqlBundle, baked string) {
	var live string
	if bundle.ReadGraph != nil {
		fp, err := bc.FingerprintComponentGraph(bundle.ReadGraph.IR)
		must(err)
		live = fp
	} else {
		live = c.Fingerprint
	}
	if live != baked {
		panic(fmt.Sprintf("codegen: generated %s fingerprint mismatch (%s != %s) — regenerate (fail-closed)", c.Case, live, baked))
	}
}

// runCodegen executes ONE case THROUGH the generated de-interpreted module. Reads/relations run the
// generated Bind(handler)[entry](input); the companion relation is hydrated via the shared runtime
// StitchRelation (same grouping SSoT as ReadBundle). Writes force the generated module's fail-closed
// load, then defer execution to the runtime tx path.
func runCodegen(c *caseArt, db rt.SQLDB) {
	mod, ok := codegenModules[c.Case]
	if !ok {
		panic("unknown codegen case " + c.Case)
	}
	bundle, err := rt.BundleFromJObj(c.bundleObj)
	must(err)
	codegenSkewGate(c, bundle, mod.fingerprint)

	switch c.Kind {
	case "read", "relation":
		pid, err := bundle.ReadGraph.PrimaryNodeID()
		must(err)
		handler := &codegenRawReadHandler{graph: bundle.ReadGraph, primaryID: pid, db: db}
		bound := mod.bindRaw(handler)
		run := bound[mod.entry]
		out, err := run(c.inputScope)
		must(err)
		if c.Kind == "relation" {
			parents, _ := out.([]bc.Value)
			relN, _ := c.relationsJObj.Get(c.WithRelation)
			relObj := relN.(*bc.JObj)
			_, err := rt.StitchRelation(relObj, parents, db)
			must(err)
		}
	case "batch":
		handler := &codegenRawWriteHandler{bundle: bundle, input: bc.NewObj(), db: db}
		bound := mod.bindRaw(handler)
		_, err := bound[mod.entry](writeModuleInput())
		must(err)
	case "tx":
		handler := &codegenRawWriteHandler{bundle: bundle, input: c.inputScope, db: db}
		bound := mod.bindRaw(handler)
		_, err := bound[mod.entry](writeModuleInput())
		must(err)
	default:
		panic("unknown kind " + c.Kind)
	}
}

// ── behaviour-equality selfcheck helpers ───────────────────────────────────────
// runCodegenValue / runLMValue return the produced output for the behaviour-equality selfcheck,
// each on its OWN freshly-seeded in-memory DB, encoded to a JSON-comparable string.
func runCodegenValue(a *artifact, c *caseArt) string {
	raw := seedDB(a)
	defer raw.Close()
	var db rt.SQLDB = raw
	mod := codegenModules[c.Case]
	bundle, err := rt.BundleFromJObj(c.bundleObj)
	must(err)
	codegenSkewGate(c, bundle, mod.fingerprint)
	var out bc.Value
	switch c.Kind {
	case "read", "relation":
		pid, e := bundle.ReadGraph.PrimaryNodeID()
		must(e)
		bound := mod.bindRaw(&codegenRawReadHandler{graph: bundle.ReadGraph, primaryID: pid, db: db})
		o, e2 := bound[mod.entry](c.inputScope)
		must(e2)
		if c.Kind == "relation" {
			parents, _ := o.([]bc.Value)
			relN, _ := c.relationsJObj.Get(c.WithRelation)
			rows, e3 := rt.StitchRelation(relN.(*bc.JObj), parents, db)
			must(e3)
			out = bcRows(rows)
		} else {
			out = o
		}
	case "batch":
		bound := mod.bindRaw(&codegenRawWriteHandler{bundle: bundle, input: bc.NewObj(), db: db})
		o, e := bound[mod.entry](writeModuleInput())
		must(e)
		out = o
	default:
		bound := mod.bindRaw(&codegenRawWriteHandler{bundle: bundle, input: c.inputScope, db: db})
		o, e := bound[mod.entry](writeModuleInput())
		must(e)
		out = o
	}
	// Canonicalize integral float64 → int64 uniformly (the relation-stitch path returns float rows
	// from the driver; the de-box read/write path returns int64) so cg and ir compare by VALUE.
	return encodeConformance(coerceIntsValue(out))
}

func runLMValue(a *artifact, c *caseArt) string {
	raw := seedDB(a)
	defer raw.Close()
	var db rt.SQLDB = raw
	bundle, err := rt.BundleFromJObj(c.bundleObj)
	must(err)
	var out bc.Value
	switch c.Kind {
	case "batch":
		o, e := rt.ExecuteTransactionBundle(bundle, bc.NewObj(), db.(rt.TxDB))
		must(e)
		out = txResultToObj(o) // the SAME canonical shape the de-boxed path emits (via ser_T0)
	case "tx":
		o, e := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
		must(e)
		out = txResultToObj(o)
	case "relation":
		o, e := rt.ReadBundle(bundle, c.relationsJObj, c.inputScope, db, []string{c.WithRelation}, nil)
		must(e)
		out = o
	default:
		o, e := rt.ExecuteBundle(bundle, c.inputScope, db)
		must(e)
		out = o
	}
	// The de-boxed codegen output types INTEGER columns as int64 (the typed outType); the interpreter
	// path floats them (scanValue's JS-number shim). Both encode the SAME integers — canonicalize the
	// interpreter side's integral float64 → int64 so the behaviour-equality selfcheck compares by VALUE
	// (the codegen output is already int64 via ser_T0). Representation-only; the row DATA is identical.
	out = coerceIntsValue(out)
	return encodeConformance(out)
}

func bcRows(rows []bc.Value) bc.Value {
	out := make([]bc.Value, len(rows))
	copy(out, rows)
	return out
}

// encodeConformance renders a bc Value to its conformance-JSON string (deterministic key order +
// $bigint codec), so codegen vs ir outputs compare byte-for-byte.
func encodeConformance(v bc.Value) string { return rt.EncodeConformanceJSON(v) }
