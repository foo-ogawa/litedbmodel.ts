package wire

import (
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// Relation batch-key encoding — the deduped parent keys bound to the ONE child query. sqlite/mysql bind
// a single JSON array (`json_each(?)`); pg binds native `int[]` (`= ANY(?::int[])` / `UNNEST(?::int[],…)`).
// Shared by every relation companion (single-key int64 + composite [2]int64).
func IntKeysJSON(ks []int64) string {
	p := make([]string, len(ks))
	for i, k := range ks {
		p[i] = strconv.FormatInt(k, 10)
	}
	return "[" + strings.Join(p, ",") + "]"
}
func IntKeysPg(ks []int64) []any { return []any{pq.Array(ks)} }

func PairKeysJSON(ks [][2]int64) string {
	p := make([]string, len(ks))
	for i, k := range ks {
		p[i] = "[" + strconv.FormatInt(k[0], 10) + "," + strconv.FormatInt(k[1], 10) + "]"
	}
	return "[" + strings.Join(p, ",") + "]"
}
func PairKeysPg(ks [][2]int64) []any {
	ts := make([]int64, len(ks))
	us := make([]int64, len(ks))
	for i, k := range ks {
		ts[i], us[i] = k[0], k[1]
	}
	return []any{pq.Array(ts), pq.Array(us)}
}
