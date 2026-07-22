// litedbmodel v2 SCP — read-relation batch EXECUTION (Go port of src/scp/relation.ts, #43).
//
// Byte-for-byte port of the TS reference relation runtime: the STATIC pre-compiled batch op
// (bundle.relations[name] — pure JSON) is EXECUTED, never regenerated. A RelationOp carries the
// batched child SELECT text with ONE `?` for the deduped-key array param; the runtime dedupes the
// parent keys, resolves the deferred PG array cast from the REAL keys, renders `?`→`$N`, short-
// circuits an empty key set (NO query), runs the batch, groups the child rows by target key, and
// distributes them onto the parents per cardinality (hasMany → list, belongsTo/hasOne → single or
// nil). The SAME RunRelationOp / DistributeToParent / dedupeKeys the TS eager path uses.
//
// #40 parallel-safe: the batch is grouped-then-distributed by key, so the hydrated result is
// deterministic regardless of query-completion order. Independent sibling relations may run in any
// order (bc RunPlan fan-out) and still produce the identical hydrated shape.

package litedbmodel_runtime

import (
	"context"
	"fmt"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

func errReadNotList(out bc.Value) error {
	kind := "non-list"
	if out == nil {
		kind = "null"
	}
	return fmt.Errorf("scp read: the read behavior output is not a row list (got %s); the typed-object read surface expects a Select-shaped output", kind)
}

func errRelationNotDeclared(name string) error {
	return fmt.Errorf("declarative select: relation '%s' is not declared on this model", name)
}

// RelationOp is the pre-compiled STATIC batch op read out of bundle.relations[name] (pure JSON).
// Single-key relations carry ParentKey/TargetKey; composite (#47 item 1) carry ParentKeys/TargetKeys.
type RelationOp struct {
	Name        string
	Kind        string // "belongsTo" | "hasMany" | "hasOne"
	ParentKey   string
	TargetKey   string
	ParentKeys  []string // composite: ordered parent key columns (nil for single-key)
	TargetKeys  []string // composite: ordered child key columns (nil for single-key)
	Dialect     string
	Connection  string // CROSS-DB (V0 R1): the target model's connection tag (empty for same-DB)
	SQL         string
	TargetTable string // the child (target) table name — the LimitExceededError.Model for a relation
	// HardLimit is the baked per-batch runaway cap (Phase E-2, epic #74; v1 _selectForRelation
	// hasManyHardLimit): when the batch fetches MORE than this TOTAL, runRelationOp throws
	// LimitExceededError (context "relation", EXACT count). nil ⇒ NO check (disabled, or an
	// intrinsic per-parent LIMIT-window relation whose fanout is already bounded — the artifact omits
	// the field). Caps come from the ARTIFACT ONLY (no config surface).
	HardLimit *int
}

// relationOpFromJObj reads one bundle.relations entry into a RelationOp.
func relationOpFromJObj(o *bc.JObj) RelationOp {
	return RelationOp{
		Name:        getStrJ(o, "name"),
		Kind:        getStrJ(o, "kind"),
		ParentKey:   getStrJ(o, "parentKey"),
		TargetKey:   getStrJ(o, "targetKey"),
		ParentKeys:  getStrArrJ(o, "parentKeys"),
		TargetKeys:  getStrArrJ(o, "targetKeys"),
		Dialect:     getStrJ(o, "dialect"),
		Connection:  getStrJ(o, "connection"),
		SQL:         getStrJ(o, "sql"),
		TargetTable: getStrJ(o, "targetTable"),
		HardLimit:   optIntJ(o, "hardLimit"),
	}
}

// optIntJ reads an OPTIONAL integer field off a parsed JObj, returning nil when the key is absent (so
// a present cap of 0 stays distinct from "no cap"). Used for the baked relation hard-limit (Phase E-2).
func optIntJ(o *bc.JObj, k string) *int {
	v, ok := o.Get(k)
	if !ok {
		return nil
	}
	if dv, err := bc.DecodeValue(v); err == nil {
		if i, ok := dv.(int64); ok {
			n := int(i)
			return &n
		}
	}
	return nil
}

func getStrJ(o *bc.JObj, k string) string {
	if v, ok := o.Get(k); ok {
		s, _ := v.(string)
		return s
	}
	return ""
}

// getStrArrJ reads an optional string[] field (nil if absent) — the composite key column lists.
func getStrArrJ(o *bc.JObj, k string) []string {
	v, ok := o.Get(k)
	if !ok {
		return nil
	}
	arr, ok := v.([]bc.JNode)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, _ := e.(string)
		out = append(out, s)
	}
	return out
}

// parentKeyCols / targetKeyCols return the ordered key columns (single-key → 1-element).
func (op RelationOp) parentKeyCols() []string {
	if op.ParentKeys != nil {
		return op.ParentKeys
	}
	return []string{op.ParentKey}
}

func (op RelationOp) targetKeyCols() []string {
	if op.TargetKeys != nil {
		return op.TargetKeys
	}
	return []string{op.TargetKey}
}

// The key-identity + dedupe + group + attach primitives are the SHARED grouping CORE (grouping.go) —
// the SINGLE source of truth this lazy/declarative path and the native leaf transport (leaf_transport.go)
// both consume: KeyIdentity / DedupeKeyTuples / GroupByKey / AttachToParent (no duplicated grouping).

// bindKeys binds the deduped keys to the op's params per dialect + arity (TS bindKeys). Single-key:
// PG → ONE scalar array param; MySQL/SQLite → ONE JSON scalar-array string. Composite: PG → ONE
// array param PER key column (transposed tuples); MySQL/SQLite → ONE JSON array-of-tuples string.
// Returns the positional args list.
func bindKeys(op RelationOp, tuples [][]bc.Value) []any {
	composite := op.ParentKeys != nil
	if op.Dialect == "postgres" {
		nCols := 1
		if composite {
			nCols = len(op.parentKeyCols())
		}
		args := make([]any, nCols)
		for col := 0; col < nCols; col++ {
			colArr := make([]any, len(tuples))
			for i, t := range tuples {
				colArr[i] = toDriverParam(t[col])
			}
			args[col] = colArr
		}
		return args
	}
	// MySQL/SQLite: ONE JSON param — a scalar array (single-key) or an array-of-tuples (composite).
	var payload []bc.Value
	if composite {
		payload = make([]bc.Value, len(tuples))
		for i, t := range tuples {
			payload[i] = bc.Value([]bc.Value(t))
		}
	} else {
		payload = make([]bc.Value, len(tuples))
		for i, t := range tuples {
			payload[i] = t[0]
		}
	}
	return []any{jsStringify(bc.Value(payload))}
}

// RelationBatch is the child rows grouped for a batch: stringified target-key identity → child rows.
type RelationBatch map[string][]bc.Value

// RunRelationOp runs ONE relation batch op for a set of parent rows (port of TS runRelationOp).
// Dedup the parent-key tuples, resolve the deferred PG array cast(s) from the REAL keys (one per key
// column for composite) BEFORE the `?`→`$N` render; on a NON-empty key set execute binding the keys
// (single array / per-column arrays / JSON tuples) and group the child rows by target-key identity.
// An EMPTY key set issues NO query, matching TS.
//
// Backward-compat wrapper (§6): wraps `db` in a thin ExecutionContext and delegates to the
// ctx-threaded core, so an existing caller passing a raw db keeps its byte-identical behavior.
func RunRelationOp(op RelationOp, parents []bc.Value, db SQLDB) (RelationBatch, error) {
	return runRelationOpCtx(ContextForDB(db), op, parents)
}

// runRelationOpCtx runs ONE relation batch op through the CENTRAL SEAM (§2): the batched child SELECT
// funnels through Execute(ctx, …, ReadIntent) — the resolved connection is the tx-owned one when the
// relation runs inside a tx-scoped ctx, else the primary db. This is the ctx-threaded core.
func runRelationOpCtx(ctx *ExecutionContext, op RelationOp, parents []bc.Value) (RelationBatch, error) {
	pCols := op.parentKeyCols()
	keys := DedupeKeyTuples(parents, pCols)
	batch := RelationBatch{}
	sqlText := op.SQL
	if op.Dialect == "postgres" {
		for col := range pCols {
			colVals := make([]bc.Value, len(keys))
			for i, t := range keys {
				colVals[i] = t[col]
			}
			sqlText = resolvePgArrayCast(sqlText, colVals)
		}
	}
	sqlText = renderPlaceholders(sqlText, op.Dialect)
	if len(keys) == 0 {
		return batch, nil
	}
	tCols := op.targetKeyCols()
	rows, err := Execute(ctx, sqlText, bindKeys(op, keys), ReadIntent())
	if err != nil {
		return nil, err
	}
	// Phase E-2 (#74): post-fetch hard-limit runaway guard. When the batch TOTAL exceeds the baked
	// cap, throw with the EXACT count (the batch is fetched in full, no N+1) BEFORE grouping/hydration
	// so an over-cap read never assembles an unbounded result set. Field mapping: Model = the relation
	// TARGET TABLE, Relation = the relation name. Absent op.HardLimit ⇒ disabled / intrinsic-limit
	// relation ⇒ no check. One guard point → both eager (ReadBundle) and lazy (StitchRelation). The
	// SAME check the TS reference (runRelationOp) + the rust/py/php ports run off the same field.
	if op.HardLimit != nil {
		if err := checkHardLimit(*op.HardLimit, len(rows), LimitContextRelation, op.TargetTable, op.Name); err != nil {
			return nil, err
		}
	}
	// Group the fetched child rows by their target-key tuple identity via the shared CORE (GroupByKey).
	batch = GroupByKey(rows, tCols)
	return batch, nil
}

// DistributeToParent distributes a resolved batch onto ONE parent per cardinality (port of TS
// distributeToParent): hasMany → the child list ([] when none); belongsTo/hasOne → the single child
// (or nil), keyed by the parent's key-tuple identity. Delegates to the shared CORE (AttachToParent) —
// `single` = a non-hasMany cardinality (belongsTo/hasOne).
func DistributeToParent(op RelationOp, parent *bc.Obj, batch RelationBatch) bc.Value {
	return AttachToParent(parent, op.parentKeyCols(), batch, op.Kind != "hasMany")
}

// driverForOp returns the driver a relation runs against: its tagged cross-DB connection, else the
// primary db. CROSS-DB (V0 R1): a relation whose op carries a `Connection` tag (its target model
// lives in a DIFFERENT DB — v1 LazyRelation.ts:236) routes to connections[tag]. Loud failure when
// the tag has no registered driver (a real wiring bug — never a silent same-DB fallback that would
// run the target's query on the wrong DB). Untagged relations use the primary db.
func driverForOp(op RelationOp, db SQLDB, connections map[string]SQLDB) (SQLDB, error) {
	if op.Connection == "" {
		return db, nil
	}
	d, ok := connections[op.Connection]
	if !ok || d == nil {
		return nil, fmt.Errorf("cross-DB relation '%s': no driver registered for connection '%s' (pass it in ReadBundle connections)", op.Name, op.Connection)
	}
	return d, nil
}

// ReadBundle runs a READ bundle's primary row list, then batch-loads + hydrates the selected
// relations onto each parent (port of the TS readBundle typed-object surface, declarative-select
// path). The primary read output must be a bare row list; each named relation in withNames is
// batch-prefetched ONCE over the whole page (staged, no N+1) via the SAME RunRelationOp and
// attached onto each parent as an own key. `relations` is the bundle.relations JObj.
//
// CROSS-DB (V0 R1): a relation op carrying a `connection` tag is batched against connections[tag]
// (its target model's DB) instead of the primary db; untagged relations ignore connections. Pass a
// nil/empty map for a single-DB read.
// StitchRelation batch-loads + hydrates ONE declared relation onto an ALREADY-FETCHED parent row
// list, using the SAME RunRelationOp / DistributeToParent the runtime's own read path uses (no
// reimplemented grouping — the semantics stay single-sourced here). `opJObj` is the relation op as
// it appears under bundle.relations[name] (pure JSON, bc-ordered). The public seam the codegen bench
// cell uses: it runs the GENERATED de-interpreted module for the primary read (its own distinct code
// entry — NOT ExecuteBundle), then hydrates the related rows through this shared stitch so the
// hydrated result is byte-identical to ReadBundle's.
func StitchRelation(opJObj *bc.JObj, parents []bc.Value, db SQLDB) ([]bc.Value, error) {
	op := relationOpFromJObj(opJObj)
	batch, err := runRelationOpCtx(ContextForDB(db), op, parents)
	if err != nil {
		return nil, err
	}
	for _, r := range parents {
		if obj, ok := r.(*bc.Obj); ok {
			obj.Set(op.Name, DistributeToParent(op, obj, batch))
		}
	}
	return parents, nil
}

func ReadBundle(bundle *SqlBundle, relations *bc.JObj, input *bc.Obj, db SQLDB, withNames []string, connections map[string]SQLDB) (bc.Value, error) {
	return ReadBundleCtx(context.Background(), bundle, relations, input, db, withNames, connections)
}

// ReadBundleCtx is [ReadBundle] riding a caller-supplied (Phase D scoped) context.Context: the primary
// read AND every relation-batch SELECT funnel through an [ExecutionContext] whose middleware chain
// resolves THAT context's scope registry ([ContextForDBCtx]). A middleware registered inside a
// [WithMiddlewareScope] therefore observes BOTH the primary read and the relation-batch SQL (the
// end-to-end relation coverage the #92 reference asserts). A cross-DB relation derives a distinct ctx
// over its tagged connection but shares the SAME scoped Go context, so its batch is intercepted too.
// With no middleware registered the chain is empty ⇒ byte-identical to [ReadBundle].
func ReadBundleCtx(goCtx context.Context, bundle *SqlBundle, relations *bc.JObj, input *bc.Obj, db SQLDB, withNames []string, connections map[string]SQLDB) (bc.Value, error) {
	if goCtx == nil {
		goCtx = context.Background()
	}
	// One ExecutionContext for the primary read; a cross-DB relation derives a distinct ctx over its
	// tagged connection (§6), sharing the scoped Go context so its batch is intercepted too.
	primaryCtx := ContextForDBCtx(goCtx, db)
	out, err := executeBundleCtx(primaryCtx, bundle, input)
	if err != nil {
		return nil, err
	}
	rows, ok := out.([]bc.Value)
	if !ok {
		return nil, errReadNotList(out)
	}
	for _, name := range withNames {
		opN, present := relations.Get(name)
		if !present {
			return nil, errRelationNotDeclared(name)
		}
		opObj, ok := opN.(*bc.JObj)
		if !ok {
			return nil, errRelationNotDeclared(name)
		}
		op := relationOpFromJObj(opObj)
		relDB, err := driverForOp(op, db, connections)
		if err != nil {
			return nil, err
		}
		relCtx := primaryCtx
		if relDB != db {
			relCtx = ContextForDBCtx(goCtx, relDB)
		}
		batch, err := runRelationOpCtx(relCtx, op, rows)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if obj, ok := r.(*bc.Obj); ok {
				obj.Set(name, DistributeToParent(op, obj, batch))
			}
		}
	}
	return rows, nil
}
