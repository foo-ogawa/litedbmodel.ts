// Unit tests for the SHARED relation-grouping core (grouping.go, #141) — the Go twin of the Rust
// tests in `rust/litedbmodel_runtime/src/grouping.rs` and the TS SSoT `src/scp/grouping.ts`.

package litedbmodel_runtime

import (
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// row builds an insertion-ordered *bc.Obj record from key/value pairs (reuses the package `scope`
// helper defined in runtime_test.go; a distinct name keeps the grouping tests self-describing).
func row(pairs ...any) bc.Value {
	return scope(pairs...)
}

func cols(cs ...string) []string { return cs }

func TestKeyIdentity_MatchesJSString(t *testing.T) {
	// whole float → integer text (a scanned INT column), int64 same, string/bool verbatim, tuple space-joined.
	cases := []struct {
		in   []bc.Value
		want string
	}{
		{[]bc.Value{float64(1)}, "1"},
		{[]bc.Value{int64(2)}, "2"},
		{[]bc.Value{"x"}, "x"},
		{[]bc.Value{true}, "true"},
		{[]bc.Value{false}, "false"},
		{[]bc.Value{float64(1.5)}, "1.5"},
		{[]bc.Value{int64(1), "a"}, "1 a"},
	}
	for _, c := range cases {
		if got := KeyIdentity(c.in); got != c.want {
			t.Errorf("KeyIdentity(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDedupeKeyTuples_DropsNullAndDedupesPreservingOrder(t *testing.T) {
	rows := []bc.Value{
		row("id", int64(2)),
		row("id", int64(1)),
		row("id", int64(2)),    // dup
		row("id", nil),         // dropped (null)
		row("other", int64(9)), // dropped (absent id)
	}
	keys := DedupeKeyTuples(rows, cols("id"))
	if len(keys) != 2 {
		t.Fatalf("got %d tuples, want 2", len(keys))
	}
	if KeyIdentity(keys[0]) != "2" || KeyIdentity(keys[1]) != "1" {
		t.Errorf("insertion order/dedupe wrong: got [%q %q], want [2 1]", KeyIdentity(keys[0]), KeyIdentity(keys[1]))
	}
}

func TestDedupeKeyTuples_CompositeTuple(t *testing.T) {
	rows := []bc.Value{
		row("t", int64(1), "u", int64(9)),
		row("t", int64(1), "u", int64(9)), // dup tuple
		row("t", int64(1), "u", int64(8)),
		row("t", int64(1), "u", nil), // dropped (partial null)
	}
	keys := DedupeKeyTuples(rows, cols("t", "u"))
	if len(keys) != 2 {
		t.Fatalf("got %d tuples, want 2", len(keys))
	}
	if KeyIdentity(keys[0]) != "1 9" || KeyIdentity(keys[1]) != "1 8" {
		t.Errorf("composite tuples wrong: got [%q %q]", KeyIdentity(keys[0]), KeyIdentity(keys[1]))
	}
}

func TestGroupByKey_And_AttachToParent_HasMany(t *testing.T) {
	children := []bc.Value{
		row("author_id", int64(1), "t", "a"),
		row("author_id", int64(1), "t", "b"),
		row("author_id", int64(2), "t", "c"),
		row("author_id", nil, "t", "x"), // dropped (null fk)
	}
	byKey := GroupByKey(children, cols("author_id"))

	// parent 1 → two children in input order
	a1 := AttachToParent(scope("id", int64(1)), cols("id"), byKey, false)
	list1, ok := a1.([]bc.Value)
	if !ok || len(list1) != 2 {
		t.Fatalf("hasMany parent 1: want 2-elem list, got %#v", a1)
	}
	if got, _ := list1[0].(*bc.Obj).Get("t"); got != "a" {
		t.Errorf("child order wrong: first t = %v, want a", got)
	}

	// parent 2 → one child
	a2 := AttachToParent(scope("id", int64(2)), cols("id"), byKey, false)
	if list2, ok := a2.([]bc.Value); !ok || len(list2) != 1 {
		t.Errorf("hasMany parent 2: want 1-elem list, got %#v", a2)
	}

	// a parent with no matches → empty list (NOT nil)
	a3 := AttachToParent(scope("id", int64(3)), cols("id"), byKey, false)
	list3, ok := a3.([]bc.Value)
	if !ok || len(list3) != 0 {
		t.Errorf("hasMany no-match: want empty list, got %#v", a3)
	}

	// null-fk child was dropped → never in any bucket
	if len(byKey["null"]) != 0 {
		t.Errorf("null-fk child must be dropped, got bucket %#v", byKey["null"])
	}
}

func TestAttachToParent_SingleReturnsFirstOrNil(t *testing.T) {
	children := []bc.Value{
		row("post_id", int64(5), "b", "first"),
		row("post_id", int64(5), "b", "second"),
	}
	byKey := GroupByKey(children, cols("post_id"))

	// single → the FIRST matching child (input order)
	one := AttachToParent(scope("id", int64(5)), cols("id"), byKey, true)
	obj, ok := one.(*bc.Obj)
	if !ok {
		t.Fatalf("single match: want *bc.Obj, got %#v", one)
	}
	if got, _ := obj.Get("b"); got != "first" {
		t.Errorf("single match: b = %v, want first", got)
	}

	// single, no match → nil
	if none := AttachToParent(scope("id", int64(6)), cols("id"), byKey, true); none != nil {
		t.Errorf("single no-match: want nil, got %#v", none)
	}

	// single, parent key null → nil (null parent key matches nothing)
	if none := AttachToParent(scope("id", nil), cols("id"), byKey, true); none != nil {
		t.Errorf("single null-key: want nil, got %#v", none)
	}
}
