// litedbmodel v2 SCP — fragment-tree render + param assembly (Go port of src/scp/render.ts).
//
// The NORMATIVE render reproduced EXACTLY from the TS golden reference: given a §8
// CompiledOperation (parsed as bc JNode) and a bound scope, it deterministically produces the
// final SQL text (`?` placeholders, then the dialect's final placeholder pass) and the flat
// params slice. Every rule is byte-true to render.ts:
//
//	§2 SKIP → fragment existence (present iff `always` OR `when` evaluates to a present binding —
//	    null/false are absent).
//	§3 empty-WHERE degeneration (no present fragment ⇒ the whole `{where}` splice, WHERE keyword
//	    included, collapses to "").
//	§4 AND/OR structure + parenthesization for a nested tree.
//	§5 IN-list array expansion (`(?)` → `(?, ?, …)`; empty array → the `1 = 0` always-false
//	    degeneration, byte-identical to v1's empty-IN).
//	§6 param order = SQL text order (pre-WHERE statics, then fragment params, then post-WHERE).
//	§8 the `?`→`$N` conversion runs ONCE at the end (PG only), via the Dialect.
//
// The generic Expression-IR param/guard evaluation is delegated to the bc common core
// (EvaluateExpression) — this file re-implements NO evaluator, only the SQL-structural assembly.

package litedbmodel_runtime

import (
	"fmt"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// whereSlot is the literal `{where}` splice marker inside CompiledOperation.sql (ir.ts WHERE_SLOT).
const whereSlot = "{where}"

// Rendered is the output of rendering one §8 CompiledOperation: final SQL text + flat params
// (1:1 with `?`/`$N`, each a bc runtime Value).
type Rendered struct {
	SQL    string
	Params []bc.Value
}

// asJObj narrows a parsed JNode to a *bc.JObj (nil if not an object).
func asJObj(n bc.JNode) (*bc.JObj, bool) {
	o, ok := n.(*bc.JObj)
	return o, ok
}

// isTree reports whether a fragment node is a FragmentTree (carries a `connector`).
func isTree(node *bc.JObj) bool {
	_, ok := node.Get("connector")
	return ok
}

// fragmentPresent applies the SKIP existence rule (render.ts fragmentPresent): present iff
// `always === true`, or `when` evaluates to a present binding (not null, not false). Fail-closed
// when neither `always` nor `when` is set.
func fragmentPresent(f *bc.JObj, scope *bc.Obj) (bool, error) {
	if a, ok := f.Get("always"); ok {
		if b, isBool := a.(bool); isBool && b {
			return true, nil
		}
	}
	whenN, hasWhen := f.Get("when")
	if !hasWhen {
		return false, nil // fail-closed: neither always nor when
	}
	v, err := bc.EvaluateExpression(whenN, scope)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	if b, ok := v.(bool); ok && !b {
		return false, nil
	}
	return true, nil
}

// fragmentSQL returns the fragment's literal `sql` text.
func fragmentSQL(f *bc.JObj) (string, error) {
	s, ok := f.Get("sql")
	if !ok {
		return "", fmt.Errorf("scp render: fragment missing 'sql'")
	}
	str, ok := s.(string)
	if !ok {
		return "", fmt.Errorf("scp render: fragment 'sql' is not a string")
	}
	return str, nil
}

// fragmentParams returns the fragment's param-slot JNodes (1:1 with its `?`).
func fragmentParams(f *bc.JObj) []bc.JNode {
	p, ok := f.Get("params")
	if !ok {
		return nil
	}
	arr, ok := p.([]bc.JNode)
	if !ok {
		return nil
	}
	return arr
}

// fragmentExpand returns the IN-list expansion slot index (and whether it is set) (ir.ts `expand`).
func fragmentExpand(f *bc.JObj) (int, bool) {
	e, ok := f.Get("expand")
	if !ok {
		return 0, false
	}
	// The bundle encodes numbers as json.Number (bc ParseJSONOrdered UseNumber).
	switch n := e.(type) {
	case int64:
		return int(n), true
	default:
		// json.Number path: coerce via bc's own decode.
		if v, err := bc.DecodeValue(e); err == nil {
			if i, ok := v.(int64); ok {
				return int(i), true
			}
		}
	}
	return 0, false
}

// renderFragment renders one leaf fragment's SQL + params into the accumulator, handling IN-list
// expansion (render.ts renderFragment, byte-true incl. the `1 = 0` empty-array degeneration).
func renderFragment(f *bc.JObj, scope *bc.Obj, params *[]bc.Value) (string, error) {
	sql, err := fragmentSQL(f)
	if err != nil {
		return "", err
	}
	slots := fragmentParams(f)
	expandIdx, hasExpand := fragmentExpand(f)
	if !hasExpand {
		for _, slot := range slots {
			v, err := bc.EvaluateExpression(slot, scope)
			if err != nil {
				return "", err
			}
			*params = append(*params, v)
		}
		return sql, nil
	}
	// IN-list expansion. Evaluate all slots; the `expand` slot must be an array.
	for i, slot := range slots {
		v, err := bc.EvaluateExpression(slot, scope)
		if err != nil {
			return "", err
		}
		if i == expandIdx {
			arr, isArr := v.([]bc.Value)
			if !isArr {
				return "", fmt.Errorf("IN-list expansion slot %d did not bind to an array (got %s)", i, typeLabel(v))
			}
			if len(arr) == 0 {
				// Empty-array degeneration (spec §5): `col IN (?)` collapses to `1 = 0`; no params.
				sql = "1 = 0"
			} else {
				qs := make([]string, len(arr))
				for j := range arr {
					qs[j] = "?"
				}
				sql = strings.Replace(sql, "(?)", "("+strings.Join(qs, ", ")+")", 1)
				*params = append(*params, arr...)
			}
		} else {
			*params = append(*params, v)
		}
	}
	return sql, nil
}

// typeLabel mirrors the TS `null`/typeof message for the IN-list error text (best-effort parity).
func typeLabel(v bc.Value) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case bool:
		return "boolean"
	case int64, float64:
		return "number"
	case string:
		return "string"
	case *bc.Obj:
		return "object"
	default:
		return "object"
	}
}

// renderTree renders a fragment tree into a WHERE clause body (no leading ` WHERE `). Present
// fragments join by ` <connector> `; a nested tree is parenthesized. Empty when no fragment is
// present (render.ts renderTree).
func renderTree(tree *bc.JObj, scope *bc.Obj, params *[]bc.Value) (string, error) {
	connector, _ := tree.Get("connector")
	conn, _ := connector.(string)
	fragsN, _ := tree.Get("fragments")
	frags, _ := fragsN.([]bc.JNode)

	var parts []string
	for _, node := range frags {
		fo, ok := asJObj(node)
		if !ok {
			return "", fmt.Errorf("scp render: fragment node is not an object")
		}
		if isTree(fo) {
			inner, err := renderTree(fo, scope, params)
			if err != nil {
				return "", err
			}
			if inner != "" {
				parts = append(parts, "("+inner+")")
			}
			continue
		}
		present, err := fragmentPresent(fo, scope)
		if err != nil {
			return "", err
		}
		if present {
			frag, err := renderFragment(fo, scope, params)
			if err != nil {
				return "", err
			}
			parts = append(parts, frag)
		}
	}
	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " "+conn+" "), nil
}

// countPlaceholders counts `?` in a static SQL segment (render.ts countPlaceholders).
func countPlaceholders(sql string) int {
	return strings.Count(sql, "?")
}

// operationSQL / operationParams / operationWhere / operationComponent read the compiled-op fields.
func operationSQL(op *bc.JObj) (string, error) {
	s, ok := op.Get("sql")
	if !ok {
		return "", fmt.Errorf("scp render: operation missing 'sql'")
	}
	str, ok := s.(string)
	if !ok {
		return "", fmt.Errorf("scp render: operation 'sql' is not a string")
	}
	return str, nil
}

func operationParams(op *bc.JObj) []bc.JNode {
	p, ok := op.Get("params")
	if !ok {
		return nil
	}
	arr, _ := p.([]bc.JNode)
	return arr
}

func operationWhere(op *bc.JObj) *bc.JObj {
	w, ok := op.Get("where")
	if !ok {
		return nil
	}
	o, _ := w.(*bc.JObj)
	return o // nil when where is JSON null
}

func operationComponent(op *bc.JObj) string {
	c, ok := op.Get("component")
	if !ok {
		return ""
	}
	s, _ := c.(string)
	return s
}

// RenderOperation renders a §8 CompiledOperation (parsed JNode) to final SQL + params for a bound
// scope and dialect. Ported EXACTLY from render.ts renderOperation (pre/post-WHERE static
// partition, fragment splice, then the dialect's finalize pass).
func RenderOperation(op *bc.JObj, scope *bc.Obj, dialect Dialect) (Rendered, error) {
	var params []bc.Value
	sql, err := operationSQL(op)
	if err != nil {
		return Rendered{}, err
	}
	staticSlots := operationParams(op)

	markerIdx := strings.Index(sql, whereSlot)
	if markerIdx == -1 {
		// No dynamic WHERE: all params are static, in position order.
		for _, slot := range staticSlots {
			v, err := bc.EvaluateExpression(slot, scope)
			if err != nil {
				return Rendered{}, err
			}
			params = append(params, v)
		}
		return Rendered{SQL: dialect.FinalizePlaceholders(sql), Params: params}, nil
	}

	before := sql[:markerIdx]
	after := sql[markerIdx+len(whereSlot):]

	// Static params partitioned by whether their `?` sits before or after the marker.
	beforeQ := countPlaceholders(before)
	var preStatics, postStatics []bc.JNode
	if beforeQ <= len(staticSlots) {
		preStatics = staticSlots[:beforeQ]
		postStatics = staticSlots[beforeQ:]
	} else {
		preStatics = staticSlots
	}

	for _, slot := range preStatics {
		v, err := bc.EvaluateExpression(slot, scope)
		if err != nil {
			return Rendered{}, err
		}
		params = append(params, v)
	}

	whereSQL := ""
	if where := operationWhere(op); where != nil {
		body, err := renderTree(where, scope, &params)
		if err != nil {
			return Rendered{}, err
		}
		if body != "" {
			whereSQL = " WHERE " + body // degeneration §3: drop the keyword when empty
		}
	}

	for _, slot := range postStatics {
		v, err := bc.EvaluateExpression(slot, scope)
		if err != nil {
			return Rendered{}, err
		}
		params = append(params, v)
	}

	return Rendered{SQL: dialect.FinalizePlaceholders(before + whereSQL + after), Params: params}, nil
}
