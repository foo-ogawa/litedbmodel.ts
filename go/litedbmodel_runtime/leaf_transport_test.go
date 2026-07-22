// Unit tests for the op-agnostic NATIVE leaf transport (#141): the wire ↔ Value bridge over the shared
// grouping CORE. PluckKeys / GroupChildren are pure (no DB), so they are tested directly over BC-owned
// wire values; ExecuteSQL's live path is exercised end-to-end by the lm_orm_native bench cell.

package litedbmodel_runtime

import (
	"errors"
	"testing"

	"github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime/wire"
)

// wireUsers builds the wire row SLICE (the shape the covered runner hands the transport — a
// de-boxed []wire.WireValue, NOT a wire list value) for a set of {id:int} rows.
func wireUsers(ids ...int64) []wire.WireValue {
	items := make([]wire.WireValue, len(ids))
	for i, id := range ids {
		items[i] = wire.WireRowOf([]wire.WireField{{Key: "id", Val: wire.WireInt(id)}})
	}
	return items
}

func wirePosts(idAuthor ...[2]int64) []wire.WireValue {
	items := make([]wire.WireValue, len(idAuthor))
	for i, pa := range idAuthor {
		items[i] = wire.WireRowOf([]wire.WireField{
			{Key: "id", Val: wire.WireInt(pa[0])},
			{Key: "author_id", Val: wire.WireInt(pa[1])},
		})
	}
	return items
}

func TestPluckKeysDedupesNonNull(t *testing.T) {
	// duplicate + a null id → deduped, null dropped (CORE dedupe semantics over wire). A single-key
	// `col` emits FLAT scalar keys (not 1-tuples).
	rows := []wire.WireValue{
		wire.WireRowOf([]wire.WireField{{Key: "id", Val: wire.WireInt(1)}}),
		wire.WireRowOf([]wire.WireField{{Key: "id", Val: wire.WireInt(2)}}),
		wire.WireRowOf([]wire.WireField{{Key: "id", Val: wire.WireInt(1)}}),
		wire.WireRowOf([]wire.WireField{{Key: "id", Val: wire.WireNull()}}),
	}
	out, err := PluckKeys([]string{"id"}, rows)
	if err != nil {
		t.Fatalf("PluckKeys: %v", err)
	}
	lp := out.AsList()
	if lp.Kind != 0 {
		t.Fatalf("expected a wire list, kind=%d", lp.Kind)
	}
	if lp.Got.Len() != 2 {
		t.Fatalf("expected 2 deduped non-null keys, got %d", lp.Got.Len())
	}
	if k0 := lp.Got.ElemNumber(0); k0.Kind != 0 || k0.Got != "1" {
		t.Fatalf("single-key pluck must emit flat scalars; elem0 = %+v", k0)
	}
}

// A COMPOSITE `col` emits an array-of-TUPLES (each key a wire list) — deduped on the whole tuple.
func TestPluckKeysCompositeEmitsTuples(t *testing.T) {
	mk := func(tid, uid int64) wire.WireValue {
		return wire.WireRowOf([]wire.WireField{
			{Key: "tenant_id", Val: wire.WireInt(tid)},
			{Key: "user_id", Val: wire.WireInt(uid)},
		})
	}
	rows := []wire.WireValue{mk(1, 9), mk(1, 9), mk(1, 8)} // one dup tuple
	out, err := PluckKeys([]string{"tenant_id", "user_id"}, rows)
	if err != nil {
		t.Fatalf("PluckKeys composite: %v", err)
	}
	lp := out.AsList()
	if lp.Kind != 0 || lp.Got.Len() != 2 {
		t.Fatalf("expected 2 deduped tuples, got kind=%d len=%d", lp.Kind, lp.Got.Len())
	}
	if el := lp.Got.ElemList(0); el.Kind != 0 || el.Got.Len() != 2 {
		t.Fatalf("composite pluck must emit 2-element tuples; elem0 = %+v", el)
	}
}

func TestGroupChildrenDistributesPerParent(t *testing.T) {
	parents := wireUsers(1, 2, 3)
	children := wirePosts([2]int64{10, 1}, [2]int64{11, 1}, [2]int64{12, 2})
	out, err := GroupChildren(children, []string{"author_id"}, "posts", parents, []string{"id"}, false)
	if err != nil {
		t.Fatalf("GroupChildren: %v", err)
	}
	lp := out.AsList()
	if lp.Kind != 0 || lp.Got.Len() != 3 {
		t.Fatalf("expected 3 grouped parents, got kind=%d len=%d", lp.Kind, lp.Got.Len())
	}
	// parent id=1 must carry its 2 posts under "posts"; id=3 must carry an empty list.
	postsLen := func(i int) int {
		row := lp.Got.ElemRow(i)
		if row.Kind != 0 {
			t.Fatalf("parent %d not a row", i)
		}
		pl := row.Got.ProbeList("posts")
		if pl.Kind != 0 {
			t.Fatalf("parent %d has no posts list (kind=%d)", i, pl.Kind)
		}
		return pl.Got.Len()
	}
	if got := postsLen(0); got != 2 {
		t.Fatalf("parent id=1 expected 2 posts, got %d", got)
	}
	if got := postsLen(2); got != 0 {
		t.Fatalf("parent id=3 expected 0 posts, got %d", got)
	}
}

// A batch write's opaque `rows` array param (createMany/upsertMany/updateMany) rides as ONE JSON array
// string through leafParam — the `json_each(?)` batch payload the generated batch SQL binds (SAME
// encoding as the relation bindKeys JSON). Key order is preserved (jsStringify = insertion order).
// (#141 slice-2 piece 3 — the go param-encoding half of rust's baked 1-statement batch.)
func TestLeafParam_BatchRowsArrayEncoding(t *testing.T) {
	rows := wire.WireListOf([]wire.WireValue{
		wire.WireRowOf([]wire.WireField{
			{Key: "email", Val: wire.WireStr("a@x")},
			{Key: "name", Val: wire.WireStr("A")},
		}),
		wire.WireRowOf([]wire.WireField{
			{Key: "email", Val: wire.WireStr("b@x")},
			{Key: "name", Val: wire.WireStr("B")},
		}),
	})
	got := leafParam(rows)
	want := `[{"email":"a@x","name":"A"},{"email":"b@x","name":"B"}]`
	if s, ok := got.(string); !ok || s != want {
		t.Fatalf("batch rows leafParam = %#v, want JSON %q", got, want)
	}
}

// A scalar param binds directly (NOT JSON-wrapped) — only an array param (batch rows / plucked keys)
// serializes to a JSON string. Guards the leafParam array-vs-scalar branch.
func TestLeafParam_ScalarBindsDirect(t *testing.T) {
	if got := leafParam(wire.WireStr("user@x")); got != "user@x" {
		t.Fatalf("scalar string leafParam = %#v, want the bare value", got)
	}
	// an integral wire number binds as int64 (toDriverParam collapses a whole float to the integer slot).
	if got := leafParam(wire.WireInt(42)); got != int64(42) {
		t.Fatalf("scalar int leafParam = %#v, want int64(42)", got)
	}
}

// CheckFindHardLimit (#141 slice-2 piece 5): count > cap ⇒ a *LimitExceededError with the find-context
// message; count ≤ cap ⇒ nil. Delegates to the shared checkHardLimit SSoT (twin of rust
// check_find_hard_limit); NOT wired into the transport (the 19 ops bake explicit LIMITs).
func TestCheckFindHardLimit(t *testing.T) {
	if err := CheckFindHardLimit(100, 100, "benchmark_users"); err != nil {
		t.Fatalf("count == limit must pass, got %v", err)
	}
	err := CheckFindHardLimit(100, 101, "benchmark_users")
	if err == nil {
		t.Fatal("count > limit must error")
	}
	var lim *LimitExceededError
	if !errors.As(err, &lim) {
		t.Fatalf("want *LimitExceededError, got %T", err)
	}
	if lim.Context != LimitContextFind || lim.Model != "benchmark_users" || lim.Limit != 100 || lim.Count != 101 {
		t.Fatalf("unexpected limit error fields: %+v", lim)
	}
	want := "Query limit exceeded: find() on benchmark_users returned more than 100 records, " +
		"but limit is 100. This usually indicates a missing WHERE clause or an N+1 query pattern. " +
		"Set a higher limit or use pagination."
	if lim.Error() != want {
		t.Fatalf("find-context message mismatch:\n got: %s\nwant: %s", lim.Error(), want)
	}
}
