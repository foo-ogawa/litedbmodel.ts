// litedbmodel v2 SCP — the SHARED relation-grouping CORE (#141), Go port.
//
// The ONE implementation of relation key-identity + dedupe + parent grouping over bc Value rows,
// behaviour-identical to the TS SSoT `src/scp/grouping.ts` (and the Rust twin
// `rust/litedbmodel_runtime/src/grouping.rs`). It is consumed by BOTH relation surfaces so there is a
// single source of truth (no duplicated grouping logic):
//
//   - the EAGER graph — the op-independent `Pluck` / `Group` leaves (leaves.go), which call THIS core;
//   - the RUNTIME lazy / declarative path (relation.go `runRelationOpCtx` / `DistributeToParent`),
//     which groups already-fetched rows over the SAME core.
//
// Nothing here touches SQL or a driver: it is pure in-memory grouping over already-fetched rows (bc
// `*Obj` records). Ordered TUPLE keys are supported (composite keys), matching TS.

package litedbmodel_runtime

import (
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// keySep is a separator no scalar stringifyKey rendering contains, so distinct tuples never collide
// (matches the TS `KEY_SEP`).
const keySep = " "

// KeyIdentity is the stringified key identity for dedupe/grouping. A single scalar → its stringifyKey
// rendering; a tuple → the renderings joined by keySep (matches TS `keyIdentity`).
func KeyIdentity(values []bc.Value) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = stringifyKey(v)
	}
	return strings.Join(parts, keySep)
}

// stringifyKey mirrors TS `String(v)` for the key-identity used by dedupe + grouping. A whole float
// prints as an integer (a scanned int column arrives as float64), bool → "true"/"false", null →
// "null" (a null key is dropped before it is ever stringified, so that arm never affects a grouping
// result — it exists only for totality).
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

// field returns a record's column value and whether it is PRESENT and non-nil (a bc `*Obj` is
// insertion-ordered). Absent OR nil → ok=false (the TS `v === undefined || v === null` drop). A
// non-`*Obj` record has no fields (ok=false), matching the record-is-object contract.
func field(row bc.Value, col string) (bc.Value, bool) {
	obj, ok := row.(*bc.Obj)
	if !ok {
		return nil, false
	}
	v, present := obj.Get(col)
	if !present || v == nil {
		return nil, false
	}
	return v, true
}

// keyTuple builds the ordered key tuple for cols from a record. ok=false (drop) if ANY column is
// absent/nil (no partial keys) — the shared null-drop rule for dedupe, grouping AND attach.
func keyTuple(row bc.Value, cols []string) ([]bc.Value, bool) {
	tuple := make([]bc.Value, len(cols))
	for i, c := range cols {
		v, ok := field(row, c)
		if !ok {
			return nil, false
		}
		tuple[i] = v
	}
	return tuple, true
}

// DedupeKeyTuples returns the deduped, non-null key TUPLES of rows over keyCols (insertion order
// preserved — deterministic). A tuple is dropped if ANY key column is absent/null (no partial keys);
// deduped on the stringified tuple identity (so `1` and `"1"` collapse exactly as stringifyKey). Port
// of TS `dedupeKeyTuples`.
func DedupeKeyTuples(rows []bc.Value, keyCols []string) [][]bc.Value {
	seen := map[string]struct{}{}
	out := [][]bc.Value{}
	for _, r := range rows {
		tuple, ok := keyTuple(r, keyCols)
		if !ok {
			continue
		}
		id := KeyIdentity(tuple)
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, tuple)
	}
	return out
}

// GroupByKey groups children by their fkCols tuple identity (a null/absent key drops the child). The
// child list order within a bucket is the input order (append order). Port of TS `groupByKey`.
func GroupByKey(children []bc.Value, fkCols []string) map[string][]bc.Value {
	byKey := map[string][]bc.Value{}
	for _, c := range children {
		tuple, ok := keyTuple(c, fkCols)
		if !ok {
			continue
		}
		k := KeyIdentity(tuple)
		byKey[k] = append(byKey[k], c)
	}
	return byKey
}

// AttachToParent distributes grouped children onto ONE parent per cardinality (port of TS
// `attachToParent`): single==false (hasMany) → the child list ([]bc.Value{} when none); single==true
// (belongsTo/hasOne) → the single child (or nil). Keyed by the parent's pkCols tuple identity; a
// null/absent parent key matches nothing ([] / nil).
func AttachToParent(parent *bc.Obj, pkCols []string, byKey map[string][]bc.Value, single bool) bc.Value {
	var rows []bc.Value
	if tuple, ok := keyTuple(parent, pkCols); ok {
		rows = byKey[KeyIdentity(tuple)]
	}
	if !single {
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
