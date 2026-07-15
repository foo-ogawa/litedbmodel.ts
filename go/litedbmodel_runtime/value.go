// litedbmodel v2 SCP — native numeric formatting for the runtime exec path (Go).
//
// The runtime is NATIVE-ONLY (#8): it carries NO `encoding/json`. The bigint-safe conformance value
// codec (`DecodeConformanceValue`/`EncodeConformanceJSON` — the only place a `json.Number` type is
// inspected) is RUNNER-side, in package `github.com/foo-ogawa/litedbmodel/go/conformance`. This file
// keeps only the JSON-library-free numeric formatter the exec path needs (IN-list JSON param + emit
// payload rendering via `jsStringify`, relation key identity) — no encoding/json.

package litedbmodel_runtime

import (
	"math"
	"strconv"
)

// encodeFloat renders a float64 as JSON matching JS: a whole value prints as an integer (no `.0`),
// a fractional value prints its shortest round-trip form. (better-sqlite3 integer columns and
// plain-number inputs are whole-valued floats here → plain integers, matching the corpus.)
func encodeFloat(f float64) string {
	if f == math.Trunc(f) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
