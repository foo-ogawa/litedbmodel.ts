// Package litedbmodel_runtime is the Go leg of the litedbmodel v2 SCP multi-language runtime
// (WS7c, #32).
//
// It interprets the language-neutral §8 published bundle (SqlBundle: sql + fragment tree +
// closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it against
// a database/sql driver, semantics-identical to the TS reference (src/scp). The generic
// Expression-IR evaluation + the plan/map/wire/output orchestration are delegated to the shared
// common core behavior-contracts (Go module), mirroring the TS reference's npm dependency — this
// package re-implements NO generic evaluator and NO generic executor.
//
// Execution pipeline (spec §3), byte-true to runtime.ts:
//
//	validate → fragment select (SKIP) → array expand → Expression eval → bind → SQL execute → assembly
//
// bc's RunBehavior owns the orchestration (which node runs when, map iteration, wire binding,
// output merge). The bundle's surrogate component collapses every catalog node's SQL-structural
// ports to ONE synthetic `__scope` port (a bc `{obj:…}`), so bc evaluates that in ITS scope and
// hands the runtime a plain scope; the handler renders the pre-compiled op against it. No
// SQL-structural port is ever evaluated by bc.

package litedbmodel_runtime

import (
	"fmt"
	"regexp"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// Version is synced from package.json by scripts/sync-versions.mjs (Go = VCS tag, not a manifest
// field, so this constant is the in-source mirror the CI tag check compares against).
const Version = "1.2.10"

// scopePort is the synthetic port that carries a SQL node's render scope (runtime.ts SCOPE_PORT).
const scopePort = "__scope"

// entityRoot is the body-write RETURNING row exposed to later tx stages under `$.entity.*`
// (writes.ts ENTITY_ROOT).
const entityRoot = "__entity"

var returningRe = regexp.MustCompile(`(?i)\breturning\b`)

// SqlBundle is the parsed §8 published bundle. The heavy fields stay as raw bc JNodes so their
// Expression-IR param slots are evaluated by bc (not pre-decoded), byte-true to the TS SqlBundle.
type SqlBundle struct {
	IRVersion     int64
	ExprVersion   int64
	Dialect       string
	Component     *bc.JObj            // surrogate component (wiring/plan/output; catalog ports → __scope)
	Operations    map[string]*bc.JObj // nodeId → CompiledOperation
	OptionalHeads []string            // input heads normalized to present-as-null (absent-key SKIP)
	Relations     map[string]*bc.JObj // relation name → RelationOp (pure JSON)
	Transaction   *bc.JObj            // write-tx plan (nil for read/exec bundles)
}

// ParseBundle parses a §8 bundle from its pure-JSON bytes (the published artifact) into a
// SqlBundle whose IR/param slots stay as bc JNodes. This is the "executes from published JSON
// alone" entry point (#32 AC 3): no TS state, no re-derivation.
func ParseBundle(data []byte) (*SqlBundle, error) {
	root, err := bc.ParseJSONOrdered(data)
	if err != nil {
		return nil, fmt.Errorf("scp runtime: bundle parse: %w", err)
	}
	obj, ok := root.(*bc.JObj)
	if !ok {
		return nil, fmt.Errorf("scp runtime: bundle root is not an object")
	}
	return BundleFromJObj(obj)
}

// BundleFromJObj builds a SqlBundle from a parsed bundle object (shared by ParseBundle + the
// vector runner, which parses whole suites at once and hands the per-vector `bundle` object here).
func BundleFromJObj(obj *bc.JObj) (*SqlBundle, error) {
	b := &SqlBundle{Operations: map[string]*bc.JObj{}, Relations: map[string]*bc.JObj{}}
	b.IRVersion = jintField(obj, "irVersion")
	b.ExprVersion = jintField(obj, "exprVersion")
	if d, ok := obj.Get("dialect"); ok {
		b.Dialect, _ = d.(string)
	}
	if c, ok := obj.Get("component"); ok {
		b.Component, _ = c.(*bc.JObj)
	}
	if b.Component == nil {
		return nil, fmt.Errorf("scp runtime: bundle has no component")
	}
	if ops, ok := obj.Get("operations"); ok {
		if o, ok := ops.(*bc.JObj); ok {
			for _, id := range o.Keys {
				op, _ := o.Vals[id].(*bc.JObj)
				b.Operations[id] = op
			}
		}
	}
	if oh, ok := obj.Get("optionalHeads"); ok {
		if arr, ok := oh.([]bc.JNode); ok {
			for _, h := range arr {
				if s, ok := h.(string); ok {
					b.OptionalHeads = append(b.OptionalHeads, s)
				}
			}
		}
	}
	if rel, ok := obj.Get("relations"); ok {
		if r, ok := rel.(*bc.JObj); ok {
			for _, name := range r.Keys {
				ro, _ := r.Vals[name].(*bc.JObj)
				b.Relations[name] = ro
			}
		}
	}
	if tx, ok := obj.Get("transaction"); ok {
		b.Transaction, _ = tx.(*bc.JObj)
	}
	return b, nil
}

// jintField reads an integer field from a parsed object (json.Number → int64).
func jintField(o *bc.JObj, key string) int64 {
	v, ok := o.Get(key)
	if !ok {
		return 0
	}
	if dv, err := bc.DecodeValue(v); err == nil {
		if i, ok := dv.(int64); ok {
			return i
		}
	}
	return 0
}

// ── Handlers (render → execute → assembly) ────────────────────────────────────

// sqlHandlers is the bc ComponentCtxExec handler registry: one render→execute handler shared by
// every SQL Catalog name; the pre-compiled op keyed by nodeId encodes the per-node operation
// (runtime.ts buildHandlers). It implements ComponentCtxExec so bc passes the node identity.
type sqlHandlers struct {
	db      SQLDB
	ops     map[string]*bc.JObj
	dialect Dialect
}

// Exec satisfies bc.ComponentExec (used when the node identity is not available — not our path,
// but required by the interface). It fails closed since the op is keyed by nodeId.
func (h *sqlHandlers) Exec(component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	return bc.ErrOutcome("scp runtime: SQL handler requires the node identity (ExecCtx)"), true
}

// ExecCtx satisfies bc.ComponentCtxExec: render the node's pre-compiled op against the surrogate
// `__scope` port and execute it (runtime.ts handle).
func (h *sqlHandlers) ExecCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	op, ok := h.ops[nodeID]
	if !ok || op == nil {
		return bc.ErrOutcome(fmt.Sprintf("scp runtime: no compiled operation for node '%s' (%s)", nodeID, component)), true
	}
	scopeV, _ := ports.Get(scopePort)
	scope, ok := scopeV.(*bc.Obj)
	if !ok {
		return bc.ErrOutcome(fmt.Sprintf("scp runtime: node '%s' surrogate scope did not evaluate to an object", nodeID)), true
	}
	return executeRendered(h.db, op, scope, h.dialect), true
}

// executeRendered renders one SQL op and runs it, returning a bc ExecOutcome (runtime.ts
// executeRendered). A SELECT/RETURNING returns its rows; a non-returning write returns the
// single-row summary `[{changes, lastInsertRowid}]`. A driver error becomes bc `{error}` so the
// node's Policy Kind governs propagation.
func executeRendered(db SQLDB, op *bc.JObj, scope *bc.Obj, dialect Dialect) bc.ExecOutcome {
	rendered, err := RenderOperation(op, scope, dialect)
	if err != nil {
		return bc.ErrOutcome(err.Error())
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	hasReturn := operationComponent(op) == "Select" || returningRe.MatchString(rendered.SQL)
	if hasReturn {
		rows, err := queryRows(db, rendered.SQL, args)
		if err != nil {
			return bc.ErrOutcome(err.Error())
		}
		return bc.OkOutcome(rows)
	}
	changes, lastInsert, err := execWrite(db, rendered.SQL, args)
	if err != nil {
		return bc.ErrOutcome(err.Error())
	}
	summary := bc.NewObj()
	summary.Set("changes", changes)
	summary.Set("lastInsertRowid", lastInsert)
	return bc.OkOutcome([]bc.Value{summary})
}

// ── Input normalization (schema-driven — SSoT) ────────────────────────────────

// normalizeInput normalizes the caller input to null (present-as-null) for every OPTIONAL binding
// the caller omitted (runtime.ts normalizeInput). "Optional" is the SSoT, not an ad-hoc default: a
// head is optional iff (a) the component's Input Port schema marks it `required !== true`, OR (b)
// it is in the bundle's optionalHeads (a SKIP-guarded / refOpt head). A required, non-optional
// missing head is left absent so a real wiring bug surfaces as bc's UNKNOWN_BINDING.
func normalizeInput(component *bc.JObj, optionalHeads []string, input *bc.Obj) *bc.Obj {
	out := bc.NewObj()
	for _, k := range input.Keys {
		out.Set(k, input.Vals[k])
	}
	if ip, ok := component.Get("inputPorts"); ok {
		if ports, ok := ip.(*bc.JObj); ok {
			for _, port := range ports.Keys {
				schema, _ := ports.Vals[port].(*bc.JObj)
				required := false
				if schema != nil {
					if r, ok := schema.Get("required"); ok {
						if b, ok := r.(bool); ok {
							required = b
						}
					}
				}
				if !required {
					if _, present := out.Get(port); !present {
						out.Set(port, nil)
					}
				}
			}
		}
	}
	for _, head := range optionalHeads {
		if _, present := out.Get(head); !present {
			out.Set(head, nil)
		}
	}
	return out
}

// componentName reads the surrogate component's name.
func componentName(component *bc.JObj) string {
	if n, ok := component.Get("name"); ok {
		s, _ := n.(string)
		return s
	}
	return ""
}

// ── Public runtime entrypoint ─────────────────────────────────────────────────

// ExecuteBundle executes a §8 read/exec SqlBundle end-to-end (runtime.ts executeBundle): feed bc
// RunBehavior the bundle's surrogate component (plan / map / wire / output orchestration) with SQL
// handlers that render the bundle's compiled operations and run REAL SQL. This is the SAME code
// path a thin per-language runtime follows — it consumes ONLY the serialized bundle + bc
// runtime-core, never re-running litedbmodel's Backend-Compile.
func ExecuteBundle(bundle *SqlBundle, input *bc.Obj, db SQLDB) (bc.Value, error) {
	dialect, err := DialectFor(bundle.Dialect)
	if err != nil {
		return nil, err
	}
	// bc.RunBehavior takes a JNode IR; build the {components:[…]} wrapper as an ordered JObj.
	irNode := jobjOf("components", []bc.JNode{bundle.Component})

	handlers := &sqlHandlers{db: db, ops: bundle.Operations, dialect: dialect}
	normalized := normalizeInput(bundle.Component, bundle.OptionalHeads, input)

	out, err := bc.RunBehavior(irNode, handlers, normalized, componentName(bundle.Component))
	if err != nil {
		return nil, reErrorToSqlFailure(err)
	}
	return out, nil
}

// jobjOf builds a *bc.JObj from alternating key/value pairs (an ordered IR wrapper for RunBehavior).
func jobjOf(pairs ...bc.JNode) *bc.JObj {
	return bc.JObjOf(pairs...)
}

// reErrorToSqlFailure re-surfaces a structured SqlFailure from a bc OP_FAILED whose message embeds
// a `SQLITE_*` code (runtime.ts reErrorToSqlFailure). Non-driver errors are returned verbatim.
func reErrorToSqlFailure(err error) error {
	msg := err.Error()
	if i := strings.Index(msg, "SQLITE_"); i >= 0 {
		rest := msg[i:]
		end := 0
		for end < len(rest) && (rest[end] == '_' || (rest[end] >= 'A' && rest[end] <= 'Z')) {
			end++
		}
		code := rest[:end]
		switch {
		case code == "SQLITE_CONSTRAINT_FOREIGNKEY":
			return &SqlFailure{Kind: KindForeignKeyViolation, Policy: "fail", SqliteCode: code, Msg: msg}
		case strings.HasPrefix(code, "SQLITE_CONSTRAINT"):
			return &SqlFailure{Kind: KindConstraintViolation, Policy: "fail", SqliteCode: code, Msg: msg}
		case code == "SQLITE_BUSY" || code == "SQLITE_LOCKED":
			return &SqlFailure{Kind: KindRetryable, Policy: "retry", SqliteCode: code, Msg: msg}
		default:
			return &SqlFailure{Kind: KindDriverError, Policy: "fail", SqliteCode: code, Msg: msg}
		}
	}
	return err
}
