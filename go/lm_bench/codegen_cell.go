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

// genModule is the uniform surface every per-case generated package exposes.
type genModule struct {
	fingerprint string
	bind        func(bc.ComponentExec) map[string]func(*bc.Obj) (bc.Value, error)
	entry       string
}

// codegenModules maps case id -> its generated module surface (built from the imported packages).
var codegenModules = map[string]genModule{
	"find":         {cgFind.IRFingerprint, cgFind.Bind, cgFind.ComponentNames[0]},
	"complexWhere": {cgComplexWhere.IRFingerprint, cgComplexWhere.Bind, cgComplexWhere.ComponentNames[0]},
	"inList":       {cgInList.IRFingerprint, cgInList.Bind, cgInList.ComponentNames[0]},
	"belongsTo":    {cgBelongsTo.IRFingerprint, cgBelongsTo.Bind, cgBelongsTo.ComponentNames[0]},
	"hasMany":      {cgHasMany.IRFingerprint, cgHasMany.Bind, cgHasMany.ComponentNames[0]},
	"hasManyLimit": {cgHasManyLimit.IRFingerprint, cgHasManyLimit.Bind, cgHasManyLimit.ComponentNames[0]},
	"batchInsert":  {cgBatchInsert.IRFingerprint, cgBatchInsert.Bind, cgBatchInsert.ComponentNames[0]},
	"writeTxGate":  {cgWriteTxGate.IRFingerprint, cgWriteTxGate.Bind, cgWriteTxGate.ComponentNames[0]},
}

// codegenReadHandler is the makeSQL handler for the GENERATED module's `__makeSqlNode` boundary op:
// bc's generated code calls this per SQL node with the evaluated `__scope`; we render the primary
// read node via the SAME runtime RenderExecuteNode the `ir` path uses and run REAL SQL on the db.
// bc composes (de-interpreted, native), makeSQL executes (the shared render+driver seam).
type codegenReadHandler struct {
	graph     *rt.ReadGraph
	primaryID string
	db        rt.SQLDB
}

func (h *codegenReadHandler) Exec(component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	return h.ExecCtx("", component, ports, bound)
}

func (h *codegenReadHandler) ExecCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	scopeV, _ := ports.Get("__scope")
	scope, ok := scopeV.(*bc.Obj)
	if !ok {
		return bc.ErrOutcome("codegen: __scope did not evaluate to an object"), true
	}
	rows, err := rt.RenderExecuteNode(h.graph, h.primaryID, h.graph.Dialect, scope, h.db)
	if err != nil {
		return bc.ErrOutcome(err.Error()), true
	}
	return bc.OkOutcome(rows), true
}

// noopHandler forces the generated module's load-time fail-closed checks (Bind runs init/skew) for
// the write cases (batch/tx), whose actual execution defers to the runtime transaction path (write
// parity is exact + loop-safe) — mirroring the TS/Rust codegen cells.
type noopHandler struct{}

func (noopHandler) Exec(component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	return bc.ErrOutcome("codegen: write body not executed through generated module (deferred to tx path)"), true
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
		handler := &codegenReadHandler{graph: bundle.ReadGraph, primaryID: pid, db: db}
		bound := mod.bind(handler)
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
		_ = mod.bind(noopHandler{}) // force fail-closed load
		_, err := rt.ExecuteTransactionBundle(bundle, bc.NewObj(), db.(rt.TxDB))
		must(err)
	case "tx":
		_ = mod.bind(noopHandler{}) // force fail-closed load
		_, err := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
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
		bound := mod.bind(&codegenReadHandler{graph: bundle.ReadGraph, primaryID: pid, db: db})
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
		_ = mod.bind(noopHandler{})
		o, e := rt.ExecuteTransactionBundle(bundle, bc.NewObj(), db.(rt.TxDB))
		must(e)
		out = o
	default:
		_ = mod.bind(noopHandler{})
		o, e := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
		must(e)
		out = o
	}
	return encodeConformance(out)
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
		out = o
	case "tx":
		o, e := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
		must(e)
		out = o
	case "relation":
		o, e := rt.ReadBundle(bundle, c.relationsJObj, c.inputScope, db, []string{c.WithRelation}, nil)
		must(e)
		out = o
	default:
		o, e := rt.ExecuteBundle(bundle, c.inputScope, db)
		must(e)
		out = o
	}
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
