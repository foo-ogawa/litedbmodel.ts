// litedbmodel v2 SCP — Relation ops (Read) + staged batch resolution (Go port of src/scp/relation.ts;
// spec §5).
//
// A relation is a pre-compiled batch op (spec §8) carried pure-JSON in the bundle. It renders
// through the SAME normative RenderOperation (fragment tree + IN-list `(?, ?, …)` expansion), so a
// thin per-language runtime gets the relation batch SQL for free. runRelationOp renders the batch
// SELECT ONCE for the deduped parent key set (structurally no N+1) and groups the child rows by
// their target key; distributeToParent attaches per cardinality. Ported byte-true to relation.ts.
//
// The vector corpus exercises relations indirectly (the exec bundles' Φ output is the base
// read + the map-node authors; the bundle's `relations` are the typed-object read surface, not the
// executeBundle path). This port keeps the runtime surface complete + gives the Go tests a real
// belongsTo/hasMany batch to assert against the TS reference behavior.

package litedbmodel_runtime

import (
	"fmt"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// relationKeysHead is the reserved input head the relation batch query binds its deduped key
// array to (relation.ts RELATION_KEYS_HEAD).
const relationKeysHead = "__keys"

// RelationOp is a parsed §8 relation batch op.
type RelationOp struct {
	Name      string
	Kind      string // belongsTo / hasMany / hasOne
	ParentKey string
	TargetKey string
	Query     *bc.JObj // the batched child SELECT (CompiledOperation)
}

// relationOpFromJObj parses a bundle relation op object.
func relationOpFromJObj(o *bc.JObj) (*RelationOp, error) {
	r := &RelationOp{}
	if v, ok := o.Get("name"); ok {
		r.Name, _ = v.(string)
	}
	if v, ok := o.Get("kind"); ok {
		r.Kind, _ = v.(string)
	}
	if v, ok := o.Get("parentKey"); ok {
		r.ParentKey, _ = v.(string)
	}
	if v, ok := o.Get("targetKey"); ok {
		r.TargetKey, _ = v.(string)
	}
	if v, ok := o.Get("query"); ok {
		r.Query, _ = v.(*bc.JObj)
	}
	if r.Query == nil {
		return nil, fmt.Errorf("scp relation '%s': missing query", r.Name)
	}
	return r, nil
}

// relationBatch groups child rows by parent-key value (stringified) — relation.ts RelationBatch.
type relationBatch map[string][]*bc.Obj

// runRelationOp runs ONE relation op for a set of parent rows: dedup the parent keys, render +
// execute the batched child SELECT ONCE, and group the child rows by target key (relation.ts
// runRelationOp). When there are no non-null keys the query is NOT issued (empty batch), matching
// v1 — the correct empty-set behavior, not a fallback default. Returns the rendered SQL, the
// deduped keys, and the grouping.
func runRelationOp(op *RelationOp, parents []*bc.Obj, db SQLDB, dialect Dialect) (sqlText string, keys []bc.Value, batch relationBatch, err error) {
	keys = dedupeKeys(parents, op.ParentKey)
	batch = relationBatch{}

	scope := bc.NewObj()
	scope.Set(relationKeysHead, keys)
	rendered, rerr := RenderOperation(op.Query, scope, dialect)
	if rerr != nil {
		return "", keys, batch, rerr
	}
	if len(keys) == 0 {
		// No parent keys → no batched query issued; still return the rendered SQL (the IN-list
		// `1 = 0` degeneration is observable) but do not touch the driver.
		return rendered.SQL, keys, batch, nil
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	rows, qerr := queryRows(db, rendered.SQL, args)
	if qerr != nil {
		return "", keys, batch, qerr
	}
	for _, rowV := range rows {
		row, ok := rowV.(*bc.Obj)
		if !ok {
			continue
		}
		k := keyString(getObj(row, op.TargetKey))
		batch[k] = append(batch[k], row)
	}
	return rendered.SQL, keys, batch, nil
}

// dedupeKeys returns the deduped, non-null parent-key values (insertion order preserved,
// deterministic) — relation.ts dedupeKeys.
func dedupeKeys(parents []*bc.Obj, parentKey string) []bc.Value {
	seen := map[string]bool{}
	var out []bc.Value
	for _, p := range parents {
		v := getObj(p, parentKey)
		if v == nil {
			continue
		}
		s := keyString(v)
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, v)
	}
	return out
}

// distributeToParent attaches a resolved batch onto ONE parent per cardinality (relation.ts
// distributeToParent): hasMany → the child list ([] when none); belongsTo/hasOne → the single
// child (or nil). This is the declared cardinality's empty representation, not an ad-hoc default.
func distributeToParent(op *RelationOp, parent *bc.Obj, batch relationBatch) bc.Value {
	key := getObj(parent, op.ParentKey)
	var rows []*bc.Obj
	if key != nil {
		rows = batch[keyString(key)]
	}
	if op.Kind == "hasMany" {
		out := make([]bc.Value, len(rows))
		for i, r := range rows {
			out[i] = r
		}
		return out
	}
	if len(rows) > 0 {
		return rows[0]
	}
	return nil
}

// getObj reads a key off a bc.Obj (nil if absent).
func getObj(o *bc.Obj, key string) bc.Value {
	if o == nil {
		return nil
	}
	v, _ := o.Get(key)
	return v
}

// keyString stringifies a key value deterministically (relation.ts String(v)).
func keyString(v bc.Value) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int64:
		return fmt.Sprintf("%d", t)
	case float64:
		return fmt.Sprintf("%v", t)
	case bool:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
