// litedbmodel v2 SCP — conformance value codec (Go).
//
// The conformance corpus is pure JSON with a canonical, bigint-safe value encoding (harness.ts
// encodeValue/decodeValue): a bc int (bigint) is `{"$bigint":"<dec>"}`; everything else is
// structural JSON. This mirrors the TS/JS type dichotomy the runtime must reproduce EXACTLY:
//
//   - A bc `int` (an Expression-IR integer literal, or an arithmetic result) is a JS bigint at the
//     render boundary → `{"$bigint":…}`. In Go that is an int64.
//   - A JS `number` — a bare-JSON-integer input value threaded through ref/coalesce (never widened
//     to bigint), or a DB row column read back by the driver — encodes as a plain JSON number. In
//     Go that is a float64.
//
// So the codec is: DecodeConformanceValue maps a bare JSON integer → float64 (a JS number) and
// `{"$bigint"}` → int64 (a bc int); EncodeConformanceValue maps int64 → `{"$bigint"}` and a
// whole-valued float64 → a plain integer JSON number. This is the byte-true inverse of harness.ts.

package litedbmodel_runtime

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// DecodeConformanceValue converts a parsed corpus JNode into a runtime bc Value, reproducing the
// TS decode: bare integral JSON number → float64 (JS number); `{"$bigint":"…"}` → int64 (bc int);
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

// decodeConformanceNumber classifies a bare JSON number: integral → float64 (a JS number),
// fractional → float64. (Both are JS numbers in TS; the distinction from a bc int is the
// `{"$bigint"}` tag, not a bare number.)
func decodeConformanceNumber(n json.Number) (bc.Value, error) {
	f, err := n.Float64()
	if err != nil {
		return nil, err
	}
	return f, nil
}

// EncodeConformanceJSON renders a runtime bc Value to the canonical corpus JSON string (byte-true
// to JSON.stringify(encodeValue(v))): int64 → `{"$bigint":"…"}`; a whole-valued float64 → a plain
// integer JSON number; a fractional float64 → its shortest JSON; string/bool/null structural;
// arrays/objects recurse with key order preserved.
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
		b, _ := json.Marshal(t)
		return string(b)
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
			kb, _ := json.Marshal(k)
			parts = append(parts, string(kb)+":"+EncodeConformanceJSON(t.Vals[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "null"
	}
}

// encodeFloat renders a float64 as JSON matching JS: a whole value prints as an integer (no `.0`),
// a fractional value prints its shortest round-trip form. (better-sqlite3 integer columns and
// plain-number inputs are whole-valued floats here → plain integers, matching the corpus.)
func encodeFloat(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
