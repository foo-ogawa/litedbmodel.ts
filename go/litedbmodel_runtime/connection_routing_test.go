// Phase C (#89, go) — UNIT tests for connection routing + config (no live DB).
//
// These pin the PURE routing/config logic (the SAME contract the live tests exercise end-to-end):
//   - SessionStatements / SessionResetStatements per-dialect mapping + defaults (mirrors the TS
//     `sessionStatements maps queryTimeout/searchPath/charset` unit assertions).
//   - resolvePool's reader/writer split + writer-sticky + withWriter + named-DB routing, each with a
//     faithful-mutation RED proof: reader→writer / never-mark-sticky / ignore-intent.DB / drop
//     session-config each go RED when broken.
//   - the WriterStickyClock injectable-clock expiry.
//   - the unknown-name LOUD failure.
//   - configuredPool's reset-on-release (via a fake recording pool).
//
// A fake in-memory [Pool] (recordPool) records a label per Acquire + captures the session/reset
// statements a configuredPool runs, so the routing SELECTION + the session-config effect are provable
// without a real database — the live counterpart (connection_routing_livedb_test.go) proves the SAME
// against real PG/MySQL.

package litedbmodel_runtime

import (
	"reflect"
	"strings"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// ── A fake recording pool (the go analogue of the TS recordingPool) ────────────

// recordConn is an in-memory owned connection: Execute returns a fixed row, Run is a no-op; it
// records every statement run on it (so a configuredPool's session/reset statements are observable).
type recordConn struct {
	stmts *[]string
}

func (c *recordConn) Execute(sql string, _ []any) ([]bc.Value, error) {
	*c.stmts = append(*c.stmts, sql)
	obj := bc.NewObj()
	obj.Set("one", float64(1))
	return []bc.Value{obj}, nil
}

func (c *recordConn) Run(sql string, _ []any) (RunInfo, error) {
	*c.stmts = append(*c.stmts, sql)
	return RunInfo{}, nil
}

// recordPool records the label on every Acquire (so a test asserts WHICH pool served each statement)
// and the ordered statement stream (so session/reset statements are observable). It is a genuine
// [Pool] — no real DB.
type recordPool struct {
	label     string
	acquires  *[]string // ordered label stream across all pools sharing this slice
	stmts     []string  // statements run on acquired connections (session/reset + the query)
	acquireN  int
	releaseN  int
	destroyed int
}

func newRecordPool(label string, acquires *[]string) *recordPool {
	return &recordPool{label: label, acquires: acquires}
}

func (p *recordPool) Acquire() (PooledConn, error) {
	p.acquireN++
	*p.acquires = append(*p.acquires, p.label)
	return &recordConn{stmts: &p.stmts}, nil
}

func (p *recordPool) Release(_ PooledConn, destroy bool) error {
	p.releaseN++
	if destroy {
		p.destroyed++
	}
	return nil
}

func intPtr(i int) *int    { return &i }
func boolPtr(b bool) *bool { return &b }

// ── SessionStatements / SessionResetStatements (C3) ────────────────────────────

func TestSessionStatementsPerDialect(t *testing.T) {
	cases := []struct {
		name string
		cfg  ConnectionConfig
		want []string
	}{
		{"pg queryTimeout", ConnectionConfig{Driver: "postgres", QueryTimeout: intPtr(250)}, []string{"SET statement_timeout = 250"}},
		{"pg searchPath", ConnectionConfig{Driver: "postgres", SearchPath: "app,public"}, []string{"SET search_path TO app,public"}},
		{"pg charset", ConnectionConfig{Driver: "postgres", Charset: "UTF8"}, []string{"SET client_encoding TO UTF8"}},
		{"mysql queryTimeout", ConnectionConfig{Driver: "mysql", QueryTimeout: intPtr(250)}, []string{"SET SESSION max_execution_time = 250"}},
		{"mysql charset", ConnectionConfig{Driver: "mysql", Charset: "utf8mb4"}, []string{"SET NAMES utf8mb4"}},
		{"mysql searchPath ignored", ConnectionConfig{Driver: "mysql", SearchPath: "x"}, []string{}},
		{"all-default empty", ConnectionConfig{}, []string{}},
		{"sqlite empty", ConnectionConfig{Driver: "sqlite", QueryTimeout: intPtr(9), SearchPath: "x", Charset: "y"}, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SessionStatements(ResolveConnectionConfig(tc.cfg))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SessionStatements(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestSessionResetStatementsPerDialect(t *testing.T) {
	cases := []struct {
		name string
		cfg  ConnectionConfig
		want []string
	}{
		{"pg queryTimeout", ConnectionConfig{Driver: "postgres", QueryTimeout: intPtr(250)}, []string{"RESET statement_timeout"}},
		{"pg searchPath", ConnectionConfig{Driver: "postgres", SearchPath: "app"}, []string{"RESET search_path"}},
		{"pg charset", ConnectionConfig{Driver: "postgres", Charset: "UTF8"}, []string{"RESET client_encoding"}},
		{"mysql queryTimeout", ConnectionConfig{Driver: "mysql", QueryTimeout: intPtr(250)}, []string{"SET SESSION max_execution_time = DEFAULT"}},
		{"mysql charset", ConnectionConfig{Driver: "mysql", Charset: "utf8mb4"}, []string{"SET NAMES DEFAULT"}},
		{"all-default empty", ConnectionConfig{}, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SessionResetStatements(ResolveConnectionConfig(tc.cfg))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SessionResetStatements(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestResolveConnectionConfigDefaults(t *testing.T) {
	r := ResolveConnectionConfig(ConnectionConfig{})
	if r.Driver != "postgres" || r.QueryTimeout != 0 || r.KeepAlive != false || r.MinPool != 0 || r.MaxPool != 10 || r.KeepAliveInitialDelayMillis != 10000 {
		t.Fatalf("defaults wrong: %+v", r)
	}
	// Explicit 0 maxPool is honored (a *int of &0 is not "unset").
	if got := ResolveConnectionConfig(ConnectionConfig{MaxPool: intPtr(0)}).MaxPool; got != 0 {
		t.Fatalf("explicit maxPool=0 not honored: %d", got)
	}
}

// ── C1: reader/writer split (+ RED: reader→writer) ─────────────────────────────

func routingWithPools(reader, writer Pool, sticky *WriterStickyClock) RoutingConfig {
	reg, _ := RegistryFromDefault(ReaderWriterPools{Reader: reader, Writer: writer}).Build()
	return RoutingConfig{Registry: reg, Sticky: sticky}
}

func TestReaderWriterSplitRouting(t *testing.T) {
	var log []string
	reader := newRecordPool("reader", &log)
	writer := newRecordPool("writer", &log)
	ctx := ContextForRouting(routingWithPools(reader, writer, NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})), nil)

	if _, err := Execute(ctx, "SELECT 1", nil, ReadIntent()); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, "INSERT ...", nil, WriteIntent()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(log, []string{"reader", "writer"}) {
		t.Fatalf("split routing = %v, want [reader writer]", log)
	}

	// MUTATION (RED): collapse the split to ONE pool ⇒ both statements land on 'solo' ⇒ the read is
	// no longer distinguishable from the write. Proves the split is load-bearing.
	var mlog []string
	solo := newRecordPool("solo", &mlog)
	mctx := ContextForRouting(routingWithPools(solo, solo, NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})), nil)
	_, _ = Execute(mctx, "SELECT 1", nil, ReadIntent())
	_, _ = Run(mctx, "INSERT ...", nil, WriteIntent())
	if !reflect.DeepEqual(mlog, []string{"solo", "solo"}) {
		t.Fatalf("mutation log = %v, want [solo solo]", mlog)
	}
}

// ── C1: writer-sticky via injectable clock (+ RED: never-mark-sticky) ──────────

func TestWriterStickyClockExpiry(t *testing.T) {
	clock := int64(1_000_000)
	sticky := NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(true), WriterStickyDuration: intPtr(5000), Now: func() int64 { return clock }})
	if sticky.IsSticky() {
		t.Fatal("sticky before any mark should be false")
	}
	sticky.Mark()
	clock += 100
	if !sticky.IsSticky() {
		t.Fatal("in-window read should be sticky")
	}
	clock += 6000
	if sticky.IsSticky() {
		t.Fatal("after-window read should NOT be sticky")
	}

	// MUTATION (RED): a DISABLED sticky clock never sticks even in-window.
	off := NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false), Now: func() int64 { return clock }})
	off.Mark()
	if off.IsSticky() {
		t.Fatal("disabled sticky must never stick (never-mark-sticky RED)")
	}
}

func TestWriterStickyRoutesReadToWriter(t *testing.T) {
	var log []string
	clock := int64(1_000_000)
	reader := newRecordPool("reader", &log)
	writer := newRecordPool("writer", &log)
	sticky := NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(true), WriterStickyDuration: intPtr(5000), Now: func() int64 { return clock }})
	ctx := ContextForRouting(routingWithPools(reader, writer, sticky), nil)

	// Before any write → reader.
	_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
	if log[len(log)-1] != "reader" {
		t.Fatalf("pre-write read = %v, want reader", log[len(log)-1])
	}
	// Arm the clock (a commit would call this in the real tx path).
	sticky.Mark()
	clock += 100
	_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
	if log[len(log)-1] != "writer" {
		t.Fatalf("in-window read = %v, want writer (read-your-writes)", log[len(log)-1])
	}
	clock += 6000
	_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
	if log[len(log)-1] != "reader" {
		t.Fatalf("after-window read = %v, want reader", log[len(log)-1])
	}
}

// ── C1: withWriter (+ RED: outside-scope read hits reader) ─────────────────────

func TestWithWriterScope(t *testing.T) {
	var log []string
	reader := newRecordPool("reader", &log)
	writer := newRecordPool("writer", &log)
	ctx := ContextForRouting(routingWithPools(reader, writer, NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})), nil)

	// A read in the writer scope → writer.
	wctx := ctx.WithWriter()
	_, _ = Execute(wctx, "SELECT 1", nil, ReadIntent())
	if log[len(log)-1] != "writer" {
		t.Fatalf("withWriter read = %v, want writer", log[len(log)-1])
	}
	// A write inside withWriter is REJECTED (read-only) — v1 parity.
	if _, err := RunGuarded(wctx, "INSERT ...", nil, "INSERT", "X"); err == nil {
		t.Fatal("write in withWriter scope must be rejected (read-only)")
	} else if f, ok := err.(*SqlFailure); !ok || f.Kind != "write_in_read_only_context" {
		t.Fatalf("withWriter write rejection = %v, want write_in_read_only_context", err)
	}

	// MUTATION (RED): the SAME read OUTSIDE the withWriter scope → reader (proves withWriter diverts).
	// Fresh pools so the log is unambiguous; the ONLY difference from the green path is the missing
	// WithWriter() derivation.
	var mlog []string
	mreader := newRecordPool("reader", &mlog)
	mwriter := newRecordPool("writer", &mlog)
	mctx := ContextForRouting(routingWithPools(mreader, mwriter, NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})), nil)
	_, _ = Execute(mctx, "SELECT 1", nil, ReadIntent()) // mctx, NOT mctx.WithWriter()
	if !reflect.DeepEqual(mlog, []string{"reader"}) {
		t.Fatalf("outside-scope read = %v, want [reader] (withWriter divert was load-bearing)", mlog)
	}
}

// ── C2: named-DB routing (+ RED: ignore-intent.DB) ─────────────────────────────

func TestNamedDBRouting(t *testing.T) {
	var log []string
	aPool := newRecordPool("A", &log)
	bPool := newRecordPool("B", &log)
	reg := NewConnectionRegistry(map[string]ReaderWriterPools{
		"default": SinglePoolPair(aPool),
		"B":       SinglePoolPair(bPool),
	})
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	_, _ = Execute(ctx, "SELECT 42", nil, StatementIntent{Write: false})         // untagged → A (default)
	_, _ = Execute(ctx, "SELECT 7", nil, StatementIntent{Write: false, DB: "B"}) // tagged → B
	if !reflect.DeepEqual(log, []string{"A", "B"}) {
		t.Fatalf("named routing = %v, want [A B]", log)
	}

	// MUTATION (RED): if routing IGNORED intent.DB, the "B"-tagged read would land on the default (A)
	// pool — the log would read [A A], not [A B]. Model the mutation by resolving with DB:"" (the
	// "tag ignored" case) and confirm it hits A, not B.
	var mlog []string
	mA := newRecordPool("A", &mlog)
	mB := newRecordPool("B", &mlog)
	mreg := NewConnectionRegistry(map[string]ReaderWriterPools{"default": SinglePoolPair(mA), "B": SinglePoolPair(mB)})
	mctx := ContextForRouting(RoutingConfig{Registry: mreg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	_, _ = Execute(mctx, "SELECT 7", nil, StatementIntent{Write: false}) // DB:"" ⇒ tag ignored ⇒ A
	if !reflect.DeepEqual(mlog, []string{"A"}) {
		t.Fatalf("ignore-tag mutation = %v, want [A] (routing IS reading intent.DB)", mlog)
	}
}

func TestUnknownNameLoudFailure(t *testing.T) {
	var log []string
	aPool := newRecordPool("A", &log)
	ctx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(aPool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	_, err := Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false, DB: "ghost"})
	if err == nil {
		t.Fatal("unknown connection name must LOUD-fail, not silently fall back")
	}
	if got := err.Error(); !strings.Contains(got, "no connection registered under name 'ghost'") {
		t.Fatalf("loud-fail message = %q, want the unknown-name error", got)
	}
	if len(log) != 0 {
		t.Fatalf("a loud fail must NOT have acquired any pool, got %v", log)
	}
}

// ── C3: configuredPool reset-on-release (+ RED: drop session-config) ───────────

func TestConfiguredPoolSessionAndReset(t *testing.T) {
	var log []string
	base := newRecordPool("base", &log)
	cfg := ResolveConnectionConfig(ConnectionConfig{Driver: "postgres", QueryTimeout: intPtr(200), SearchPath: "app"})
	cp := ConfiguredPool(base, cfg)

	conn, err := cp.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	// The session statements ran on the acquired connection at checkout, in order.
	if !reflect.DeepEqual(base.stmts, []string{"SET statement_timeout = 200", "SET search_path TO app"}) {
		t.Fatalf("session-on-checkout = %v", base.stmts)
	}
	if err := cp.Release(conn, false); err != nil {
		t.Fatal(err)
	}
	// The reset statements ran on release (so the config does not leak to the next caller).
	want := []string{"SET statement_timeout = 200", "SET search_path TO app", "RESET statement_timeout", "RESET search_path"}
	if !reflect.DeepEqual(base.stmts, want) {
		t.Fatalf("session+reset = %v, want %v", base.stmts, want)
	}

	// A destroyed release STILL runs the reset (best-effort) AND drops the connection: go's
	// database/sql has no force-discard for a pooled *sql.Conn, so a statement-timeout-canceled (but
	// still alive) connection must be reset before it can re-enter the pool, else it leaks the session
	// knobs. The inner Release is called with destroy=true.
	base.stmts = nil
	base.destroyed = 0
	conn2, _ := cp.Acquire()
	_ = cp.Release(conn2, true) // destroy
	want2 := []string{"SET statement_timeout = 200", "SET search_path TO app", "RESET statement_timeout", "RESET search_path"}
	if !reflect.DeepEqual(base.stmts, want2) {
		t.Fatalf("destroyed release should still reset (no go force-discard), got %v", base.stmts)
	}
	if base.destroyed != 1 {
		t.Fatalf("destroyed release should drop the connection (inner destroy=true), destroyed=%d", base.destroyed)
	}

	// MUTATION (RED): an ALL-DEFAULT config ⇒ ConfiguredPool is a transparent passthrough (ZERO
	// session/reset statements) ⇒ dropping the session-config leaves the connection untouched.
	var plog []string
	plain := newRecordPool("plain", &plog)
	pass := ConfiguredPool(plain, ResolveConnectionConfig(ConnectionConfig{}))
	if pass != Pool(plain) {
		t.Fatal("all-default ConfiguredPool must be the SAME pool (transparent passthrough)")
	}
	c3, _ := pass.Acquire()
	_ = pass.Release(c3, false)
	if len(plain.stmts) != 0 {
		t.Fatalf("all-default config ran session statements = %v, want none (drop-session-config RED)", plain.stmts)
	}
}

// ── Backward-compat: a routed single-default ctx is byte-identical to the primary-db path ──

func TestRoutedSingleDefaultBackwardCompat(t *testing.T) {
	var log []string
	solo := newRecordPool("solo", &log)
	// A single-default registry with reader==writer + disabled sticky ⇒ every intent routes to the
	// one pool (the Phase A/B single-pool behavior).
	ctx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(solo), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
	_, _ = Run(ctx, "INSERT ...", nil, WriteIntent())
	_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
	if !reflect.DeepEqual(log, []string{"solo", "solo", "solo"}) {
		t.Fatalf("single-default routing = %v, want all 'solo' (reader===writer)", log)
	}
	// A ctx with NO routing (ContextForDB) never consults the registry at all — InTransaction/
	// routing nil ⇒ the primary-db path.
	if ContextForDB(nil).Routing() != nil {
		t.Fatal("ContextForDB must carry NO routing (byte-identical Phase A path)")
	}
}
