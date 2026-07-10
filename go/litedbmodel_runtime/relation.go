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
type RelationOp struct {
	Name      string
	Kind      string // "belongsTo" | "hasMany" | "hasOne"
	ParentKey string
	TargetKey string
	Dialect   string
	SQL       string
}

// relationOpFromJObj reads one bundle.relations entry into a RelationOp.
func relationOpFromJObj(o *bc.JObj) RelationOp {
	return RelationOp{
		Name:      getStrJ(o, "name"),
		Kind:      getStrJ(o, "kind"),
		ParentKey: getStrJ(o, "parentKey"),
		TargetKey: getStrJ(o, "targetKey"),
		Dialect:   getStrJ(o, "dialect"),
		SQL:       getStrJ(o, "sql"),
	}
}

func getStrJ(o *bc.JObj, k string) string {
	if v, ok := o.Get(k); ok {
		s, _ := v.(string)
		return s
	}
	return ""
}

// stringifyKey mirrors TS `String(v)` for the key-identity used by dedupe + grouping. A whole
// float prints as an integer (a scanned int column arrives as float64), bool → "true"/"false".
func stringifyKey(v bc.Value) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case string:
		return t
	case float64:
		return encodeFloat(t)
	case int64:
		return encodeFloat(float64(t))
	default:
		return jsStringify(v)
	}
}

// dedupeKeys returns the deduped, non-nil parent-key values (insertion order preserved) — a
// byte-for-byte port of the TS dedupeKeys (skip nil, dedupe on String(v), keep first-seen order).
func dedupeKeys(parents []bc.Value, parentKey string) []bc.Value {
	seen := map[string]struct{}{}
	out := []bc.Value{}
	for _, p := range parents {
		obj, ok := p.(*bc.Obj)
		if !ok {
			continue
		}
		v, present := obj.Get(parentKey)
		if !present || v == nil {
			continue
		}
		s := stringifyKey(v)
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, v)
	}
	return out
}

// bindKeys binds the deduped key set to the op's single array param per dialect (TS bindKeys):
// PG binds the array verbatim (a []bc.Value → pgx array); MySQL/SQLite bind the JSON-encoded array
// string (server-side json_each/JSON_TABLE expansion). Compact JSON matches TS JSON.stringify.
func bindKeys(op RelationOp, keys []bc.Value) any {
	if op.Dialect == "postgres" {
		args := make([]any, len(keys))
		for i, k := range keys {
			args[i] = toDriverParam(k)
		}
		return args
	}
	return jsStringify(bc.Value([]bc.Value(keys)))
}

// RelationBatch is the child rows grouped for a batch: stringified target-key → child rows.
type RelationBatch map[string][]bc.Value

// RunRelationOp runs ONE relation batch op for a set of parent rows (port of TS runRelationOp).
// Dedup the parent keys, resolve the deferred PG array cast from the REAL keys BEFORE the `?`→`$N`
// render (PG only), render placeholders; on a NON-empty key set execute the batch binding the keys
// as the SINGLE array param and group the child rows by target key. An EMPTY key set issues NO
// query (the correct empty-set behaviour), matching TS.
func RunRelationOp(op RelationOp, parents []bc.Value, db SQLDB) (RelationBatch, error) {
	keys := dedupeKeys(parents, op.ParentKey)
	batch := RelationBatch{}
	sqlText := op.SQL
	if op.Dialect == "postgres" {
		sqlText = resolvePgArrayCast(sqlText, keys)
	}
	sqlText = renderPlaceholders(sqlText, op.Dialect)
	if len(keys) == 0 {
		return batch, nil
	}
	rows, err := queryRows(db, sqlText, []any{bindKeys(op, keys)})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		obj, ok := r.(*bc.Obj)
		if !ok {
			continue
		}
		tv, _ := obj.Get(op.TargetKey)
		k := stringifyKey(tv)
		batch[k] = append(batch[k], r)
	}
	return batch, nil
}

// DistributeToParent distributes a resolved batch onto ONE parent per cardinality (port of TS
// distributeToParent): hasMany → the child list ([] when none); belongsTo/hasOne → the single child
// (or nil). Keyed by String(parent[parentKey]).
func DistributeToParent(op RelationOp, parent *bc.Obj, batch RelationBatch) bc.Value {
	var rows []bc.Value
	if key, ok := parent.Get(op.ParentKey); ok && key != nil {
		rows = batch[stringifyKey(key)]
	}
	if op.Kind == "hasMany" {
		if rows == nil {
			return []bc.Value{}
		}
		return rows
	}
	if len(rows) > 0 {
		return rows[0]
	}
	return nil
}

// ReadBundle runs a READ bundle's primary row list, then batch-loads + hydrates the selected
// relations onto each parent (port of the TS readBundle typed-object surface, declarative-select
// path). The primary read output must be a bare row list; each named relation in withNames is
// batch-prefetched ONCE over the whole page (staged, no N+1) via the SAME RunRelationOp and
// attached onto each parent as an own key. `relations` is the bundle.relations JObj.
func ReadBundle(bundle *SqlBundle, relations *bc.JObj, input *bc.Obj, db SQLDB, withNames []string) (bc.Value, error) {
	out, err := ExecuteBundle(bundle, input, db)
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
		batch, err := RunRelationOp(op, rows, db)
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
