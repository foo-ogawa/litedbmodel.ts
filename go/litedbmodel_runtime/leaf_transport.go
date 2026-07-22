// litedbmodel v2 SCP — the op-INDEPENDENT NATIVE leaf TRANSPORT (#141), Go.
//
// The bc `go-typed-native` covered module (RunNativeRawStruct_<comp>) calls ONE op-agnostic leaf
// transport DIRECTLY at each covered node: `ExecuteSQL` (a SQL node), `PluckKeys` (relation key
// extraction), `GroupChildren` (relation parent grouping). Post-bc#164 (wire-passthrough) each node's
// result rides as a BC-OWNED `wire.WireValue`, so these three transports are the ONLY boundary between
// the wire plane and the runtime: they convert `wire.WireValue` ↔ bc `Value`/`*Obj` and delegate the
// relation shaping to the SHARED grouping CORE (grouping.go `DedupeKeyTuples`/`GroupByKey`/
// `AttachToParent`). There is NO second grouping implementation here — that core is the single source
// of truth (the runtime lazy path in relation.go consumes the SAME functions); this file only bridges
// wire ↔ Value at the transport edge and issues SQL through the central [Execute]/[Run] seam. This is
// the Go twin of the rust `execute_sql`/`pluck_keys`/`group_children` leaf (same op-agnostic wire
// contract), NOT the py/php native-record method leaf.
//
// CONNECTION: the covered module calls these as free functions (bc's transport contract carries no db
// handle), so the consumer BINDS the target connection once via [BindLeafTransport] before driving the
// generated runners. This is the leaf transport's single bound connection — not a fallback path.

package litedbmodel_runtime

import (
	"fmt"
	"strconv"
	"sync/atomic"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	"github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime/wire"
)

// wireProbeGot mirrors the BC-OWNED wire package's probe-result Kind for "present and matching" (the
// wire package keeps its probe-kind consts unexported; 0 = got is the stable public contract the
// generated de-box also compares against).
const wireProbeGot uint8 = 0

// leaf transport bound state (set by BindLeafTransport). The bench/consumer drives the generated
// runners sequentially against ONE bound connection; ExecuteSQL funnels every SQL node through it.
var (
	leafExecCtx    *ExecutionContext
	leafDialect    = "sqlite"
	leafQueryCount int64
)

// BindLeafTransport binds the connection (+ dialect for placeholder rendering) the free-function leaf
// transport issues SQL against. Call ONCE before driving RunNativeRawStruct_<comp>.
func BindLeafTransport(db SQLDB, dialect string) {
	leafExecCtx = ContextForDB(db)
	leafDialect = dialect
}

// UnbindLeafTransport clears the bound connection (leaves ExecuteSQL fail-closed until re-bound).
func UnbindLeafTransport() { leafExecCtx = nil }

// ResetLeafQueryCount / LeafQueryCount expose the SQL-node count for the N+1-avoidance safety proof
// (a batched relation issues 1 parent + 1 batched child = 2, not 1+N — Pluck/Group are in-memory and
// do NOT count). ExecuteSQL is the ONE place a real query is issued on the native plane.
func ResetLeafQueryCount() { atomic.StoreInt64(&leafQueryCount, 0) }
func LeafQueryCount() int  { return int(atomic.LoadInt64(&leafQueryCount)) }

// ExecuteSQL runs ONE SQL node and returns its rows as a wire list of wire rows (empty list for a
// non-RETURNING write). Params ride as wire values: a scalar binds directly (toDriverParam); a wire
// LIST param binds as ONE JSON array string (the `json_each(?)` batch-key contract — SAME rendering as
// the runtime relation bindKeys). bigint is a render hint the native path does not need here.
func ExecuteSQL(bigint bool, params []wire.WireValue, returning bool, sql string, write bool) (wire.WireValue, error) {
	_ = bigint
	if leafExecCtx == nil {
		return wire.WireNull(), fmt.Errorf("leaf transport: no bound connection (call BindLeafTransport before running the native module)")
	}
	atomic.AddInt64(&leafQueryCount, 1)
	args := make([]any, len(params))
	for i, p := range params {
		args[i] = leafParam(p)
	}
	text := renderPlaceholders(sql, leafDialect)
	if write && !returning {
		info, err := Run(leafExecCtx, text, args, WriteIntent())
		if err != nil {
			return wire.WireNull(), err
		}
		// The affected-write summary row — a uniform one-row `[{changes,lastInsertRowid}]` list (the
		// TS `writeSummary` / rust `execute_sql` shape), so every leaf output is a List of Rows.
		summary := wire.WireRowOf([]wire.WireField{
			{Key: "changes", Val: wire.WireInt(info.Changes)},
			{Key: "lastInsertRowid", Val: wire.WireInt(info.LastInsertRowid)},
		})
		return wire.WireListOf([]wire.WireValue{summary}), nil
	}
	rows, err := Execute(leafExecCtx, text, args, ReadIntent())
	if err != nil {
		return wire.WireNull(), err
	}
	items := make([]wire.WireValue, len(rows))
	for i, r := range rows {
		items[i] = valueToWire(r)
	}
	return wire.WireListOf(items), nil
}

// WithAmbientTransaction runs `body` inside ONE transaction on `db`, threading the tx-owned connection
// as the AMBIENT the free-function [ExecuteSQL] resolves — so a bc-generated tx runner (which calls
// ExecuteSQL directly, taking no db handle) executes every statement ON the transaction. BEGIN →
// run body under the tx-pinned ambient → COMMIT on ok / ROLLBACK on a body error (atomicity). This is
// the CONSUMER's tx-boundary responsibility (NOT a bc feature, NOT emitted into the generated runner);
// it adds NO tx engine — it reuses the existing tx combinator ([WithTransaction], which owns BEGIN/
// COMMIT/ROLLBACK through the central seam) and only swaps the ambient leaf ctx for the body span.
// Go twin of the rust `with_ambient_transaction` leaf. Requires a bound transport ([BindLeafTransport]).
func WithAmbientTransaction(db TxDB, body func() error) error {
	base := leafExecCtx
	if base == nil {
		return fmt.Errorf("leaf transport: WithAmbientTransaction needs a bound transport (call BindLeafTransport first)")
	}
	prev := leafExecCtx
	_, err := WithTransaction(base, db, func(txCtx *ExecutionContext) (struct{}, error) {
		leafExecCtx = txCtx                   // the tx-owned ctx is the ambient the covered runner's ExecuteSQL resolves…
		defer func() { leafExecCtx = prev }() // …restored on COMMIT / ROLLBACK / panic (scopes restore)
		return struct{}{}, body()
	})
	return err
}

// PluckKeys extracts the deduped, non-null key array from `rows` over the ordered key-column TUPLE
// `col` — the batch key set the relation child fetch binds (`WHERE fk IN (SELECT value FROM
// json_each(?))` single-key, or the `$[i]` per-ordinal EXISTS form for a composite tuple). Delegates
// dedupe to the shared grouping CORE ([DedupeKeyTuples]) — the SAME SSoT the runtime relation path
// consumes (no duplicated dedupe); this transport only bridges wire ↔ Value at the edge. A single-key
// `col` emits a FLAT scalar key array; a composite `col` emits an array-of-tuples (each a wire list) —
// the SAME shape the child SQL's json_each param expects. Go twin of the rust `pluck_keys` leaf.
func PluckKeys(col []string, rows []wire.WireValue) (wire.WireValue, error) {
	valueRows := make([]bc.Value, len(rows))
	for i, r := range rows {
		valueRows[i] = wireToValue(r)
	}
	tuples := DedupeKeyTuples(valueRows, col)
	keys := make([]wire.WireValue, len(tuples))
	for i, t := range tuples {
		if len(col) == 1 {
			keys[i] = valueToWire(t[0]) // single key → flat scalar (json_each scalar `value`)
			continue
		}
		items := make([]wire.WireValue, len(t)) // composite → an array-of-tuples element
		for j, v := range t {
			items[j] = valueToWire(v)
		}
		keys[i] = wire.WireListOf(items)
	}
	return wire.WireListOf(keys), nil
}

// GroupChildren distributes the flat `children` onto `parents` by matching the child `fk` tuple to the
// parent `pk` tuple, nesting the result under `into`: single==true (belongsTo/hasOne) nests the one
// matching child (or nil); otherwise (hasMany) nests the child list ([] when none). Grouping is the
// shared CORE ([GroupByKey]/[AttachToParent]) — the SAME SSoT the runtime relation path uses (no
// duplicated grouping); `pk`/`fk` are the ordered key-column TUPLES, so a composite relation nests by
// the WHOLE tuple identity (no scalar-collapse cartesian). Each parent is shallow-copied before the
// own-key set (matching the TS `{...par, [into]: …}` spread — the input is not mutated). Go twin of the
// rust `group_children` leaf.
func GroupChildren(children []wire.WireValue, fk []string, into string, parents []wire.WireValue, pk []string, single bool) (wire.WireValue, error) {
	valueChildren := make([]bc.Value, len(children))
	for i, c := range children {
		valueChildren[i] = wireToValue(c)
	}
	byKey := GroupByKey(valueChildren, fk)
	out := make([]wire.WireValue, len(parents))
	for i, p := range parents {
		obj, ok := wireToValue(p).(*bc.Obj)
		if !ok {
			// Records are objects by contract (SQL rows); a non-object passes through untouched.
			out[i] = p
			continue
		}
		nested := AttachToParent(obj, pk, byKey, single)
		out[i] = valueToWire(withOwnKey(obj, into, nested))
	}
	return wire.WireListOf(out), nil
}

// withOwnKey returns a shallow copy of obj with key set to v (insertion order preserved; an existing
// key keeps its position, value overwritten). Mirrors the TS `{...par, [into]: v}` spread — the leaf
// output is a new record, the input parent is not mutated.
func withOwnKey(obj *bc.Obj, key string, v bc.Value) *bc.Obj {
	clone := bc.NewObj()
	for _, k := range obj.Keys {
		val, _ := obj.Get(k)
		clone.Set(k, val)
	}
	clone.Set(key, v)
	return clone
}

// leafParam converts ONE wire param to a driver-bindable arg. A wire LIST (the plucked batch keys)
// binds as ONE JSON array string (json_each(?) contract, same as relation bindKeys); a scalar binds
// via toDriverParam.
func leafParam(p wire.WireValue) any {
	v := wireToValue(p)
	if arr, ok := v.([]bc.Value); ok {
		return jsStringify(bc.Value(arr))
	}
	return toDriverParam(v)
}

// ── wire ↔ Value bridge (transport edge only) ──────────────────────────────────────────────────────

// valueToWire lowers a bc Value into the BC-OWNED wire representation (recursive for rows/lists). Row
// cells arrive from the DB scan as float64/string/bool/nil (value.go scanValue); grouped parents carry
// nested []Value / *Obj.
func valueToWire(v bc.Value) wire.WireValue {
	switch t := v.(type) {
	case nil:
		return wire.WireNull()
	case bool:
		return wire.WireBool(t)
	case float64:
		return wire.WireFloat(t)
	case int64:
		return wire.WireInt(t)
	case string:
		return wire.WireStr(t)
	case []bc.Value:
		items := make([]wire.WireValue, len(t))
		for i, e := range t {
			items[i] = valueToWire(e)
		}
		return wire.WireListOf(items)
	case *bc.Obj:
		fields := make([]wire.WireField, 0, t.Len())
		for _, k := range t.Keys {
			fields = append(fields, wire.WireField{Key: k, Val: valueToWire(t.Vals[k])})
		}
		return wire.WireRowOf(fields)
	default:
		return wire.WireNull()
	}
}

// wireToValue reverse-maps ONE wire value to a bc Value using the public probe API (the wire scalar
// payload is unexported; the probe classifiers are the sanctioned reader). Exactly one classifier
// matches a non-null value, so probe order is not ambiguous.
func wireToValue(w wire.WireValue) bc.Value {
	if p := w.AsNumber(); p.Kind == wireProbeGot {
		return parseWireNum(p.Got)
	}
	if p := w.AsString(); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := w.AsBool(); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := w.AsRow(); p.Kind == wireProbeGot {
		return wireRowToObj(p.Got)
	}
	if p := w.AsList(); p.Kind == wireProbeGot {
		out := make([]bc.Value, p.Got.Len())
		for i := 0; i < p.Got.Len(); i++ {
			out[i] = wireElemToValue(p.Got, i)
		}
		return out
	}
	return nil
}

// wireRowToObj rebuilds an insertion-ordered *Obj from a wire row (keys preserved).
func wireRowToObj(r wire.WireRow) *bc.Obj {
	o := bc.NewObj()
	for _, k := range r.Keys() {
		o.Set(k, wireFieldToValue(r, k))
	}
	return o
}

// wireFieldToValue classifies one wire row field via the probe API.
func wireFieldToValue(r wire.WireRow, k string) bc.Value {
	if p := r.ProbeNumber(k); p.Kind == wireProbeGot {
		return parseWireNum(p.Got)
	}
	if p := r.ProbeString(k); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := r.ProbeBool(k); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := r.ProbeRow(k); p.Kind == wireProbeGot {
		return wireRowToObj(p.Got)
	}
	if p := r.ProbeList(k); p.Kind == wireProbeGot {
		out := make([]bc.Value, p.Got.Len())
		for i := 0; i < p.Got.Len(); i++ {
			out[i] = wireElemToValue(p.Got, i)
		}
		return out
	}
	return nil
}

// wireElemToValue classifies one wire list element via the probe API.
func wireElemToValue(l wire.WireList, i int) bc.Value {
	if p := l.ElemNumber(i); p.Kind == wireProbeGot {
		return parseWireNum(p.Got)
	}
	if p := l.ElemString(i); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := l.ElemBool(i); p.Kind == wireProbeGot {
		return p.Got
	}
	if p := l.ElemRow(i); p.Kind == wireProbeGot {
		return wireRowToObj(p.Got)
	}
	if p := l.ElemList(i); p.Kind == wireProbeGot {
		out := make([]bc.Value, p.Got.Len())
		for j := 0; j < p.Got.Len(); j++ {
			out[j] = wireElemToValue(p.Got, j)
		}
		return out
	}
	return nil
}

// parseWireNum decodes the raw numeric text a wire number carries into a float64 (the row-scan
// convention — an integer column scans as a JS-number float64; grouping key identity handles it).
func parseWireNum(raw string) bc.Value {
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return raw
	}
	return f
}
