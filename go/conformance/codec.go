// Package conformance is the RUNNER-side conformance value codec (bigint-safe JSON ⇄ bc Value).
//
// The go runtime LIB is NATIVE-ONLY (#8) — it carries NO `encoding/json` (it parses/renders SQL and
// binds params through its own native codec). The conformance corpus, however, is pure JSON with a
// canonical bigint-safe value encoding (`{"$bigint":"<dec>"}` for a JS-bigint-typed int; everything
// else structural JSON), and the RUNNERS (vectors_runner / livedb_runner / lm_bench) must decode +
// re-encode corpus VALUES to compare a runtime result against the frozen golden. That codec — the
// ONLY place a JSON-number TYPE (`json.Number`, bc.ParseJSONOrdered's number output) is inspected —
// lives HERE, in a runner-owned package, so it is out of the runtime lib entirely. Mirrors
// harness.ts encodeValue/decodeValue exactly:
//   - a bc `int` (a bigint at the render boundary) is `{"$bigint":…}` → int64;
//   - a bare JSON number (a JS `number`: a plain-int input threaded through ref/coalesce, or a DB row
//     column) → float64; encoded back as a plain integer JSON number when whole-valued.
package conformance

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// DecodeConformanceValue converts a parsed corpus JNode into a runtime bc Value, reproducing the TS
// decode: bare integral JSON number → float64 (JS number); `{"$bigint":"…"}` → int64 (bc int);
// fractional number → float64; recurse arrays/objects (order preserved).
func DecodeConformanceValue(x bc.JNode) (bc.Value, error) {
	switch v := x.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case string:
		return v, nil
	case json.Number:
		return decodeConformanceNumber(v)
	case []bc.JNode:
		out := make([]bc.Value, 0, len(v))
		for _, e := range v {
			dv, err := DecodeConformanceValue(e)
			if err != nil {
				return nil, err
			}
			out = append(out, dv)
		}
		return out, nil
	case *bc.JObj:
		if v.Len() == 1 && v.Keys[0] == "$bigint" {
			s, ok := v.Vals["$bigint"].(string)
			if !ok {
				return nil, fmt.Errorf("$bigint tag expects a string")
			}
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("$bigint %q not int64", s)
			}
			return i, nil
		}
		out := bc.NewObj()
		for _, k := range v.Keys {
			dv, err := DecodeConformanceValue(v.Vals[k])
			if err != nil {
				return nil, err
			}
			out.Set(k, dv)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot decode conformance value: %T", x)
	}
}

// decodeConformanceNumber classifies a bare JSON number: integral or fractional → float64 (both are
// JS numbers in TS; the distinction from a bc int is the `{"$bigint"}` tag, not a bare number).
func decodeConformanceNumber(n json.Number) (bc.Value, error) {
	f, err := n.Float64()
	if err != nil {
		return nil, err
	}
	return f, nil
}

// EncodeConformanceJSON renders a runtime bc Value to the canonical corpus JSON string (byte-true to
// JSON.stringify(encodeValue(v))): int64 → `{"$bigint":"…"}`; a whole-valued float64 → a plain
// integer JSON number; a fractional float64 → its shortest JSON; string/bool/null structural;
// arrays/objects recurse with key order preserved. String escaping is native (no json.Marshal).
func EncodeConformanceJSON(v bc.Value) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case string:
		return jsonEscapeString(t)
	case int64:
		return fmt.Sprintf("{\"$bigint\":%q}", strconv.FormatInt(t, 10))
	case float64:
		return encodeFloat(t)
	case []bc.Value:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = EncodeConformanceJSON(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *bc.Obj:
		parts := make([]string, 0, t.Len())
		for _, k := range t.Keys {
			parts = append(parts, jsonEscapeString(k)+":"+EncodeConformanceJSON(t.Vals[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "null"
	}
}

// encodeFloat renders a float64 as JSON matching JS: a whole value prints as an integer (no `.0`), a
// fractional value prints its shortest round-trip form.
func encodeFloat(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// jsonEscapeString writes a JSON string literal natively (JS JSON.stringify form) — no encoding/json.
func jsonEscapeString(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 2)
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			if r < 0x20 {
				sb.WriteString(`\u`)
				h := strconv.FormatInt(int64(r), 16)
				for len(h) < 4 {
					h = "0" + h
				}
				sb.WriteString(h)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}
