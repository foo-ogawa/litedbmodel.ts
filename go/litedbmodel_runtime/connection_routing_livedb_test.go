// Phase C (#89, go) — LIVE-DB integration tests for connection routing + config on REAL Postgres
// (:5433) + MySQL (:3307). The go mirror of the TS test/integration/ConnectionRouting.test.ts,
// exercising the completion of ConnectionFor's resolution (design §3 steps 2-4) end-to-end against
// real engines, all on the UNMODIFIED Phase A/B exec-context seam:
//
//	C1 reader/writer separation + writer-sticky (injectable clock) + withWriter (write-reject)
//	C2 multi-DB connection registry + name→connection routing (PG = A, MySQL = B) + tx-pin precedence
//	C3 setConfig — queryTimeout FIRES a server statement timeout; maxPool is the SOLE cap (RED when
//	   deleted); searchPath/charset reset-on-release (no session leak); closeAllPools closes the pools
//
// Every live assertion has a faithful-mutation RED proof (break the routing/sticky/timeout/cap ⇒ the
// assertion goes RED), mirroring the TS `MUTATION:` blocks.
//
// GO-SPECIFIC NAMESPACING (parallel-port isolation): all tables live in a go-only PG SCHEMA
// `phase_c_routing_go` and a go-only MySQL table name prefix `phase_c_routing_go_*`, so this leg does
// not collide with the rust/py/php ports running against the SAME docker PG:5433 + MySQL:3307.
//
// Gated behind LITEDBMODEL_TX_ISOLATION=1 (the same gate as the other live tests). Requires:
//
//	docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//	LITEDBMODEL_TX_ISOLATION=1 go test ./litedbmodel_runtime/ -run PhaseCRouting -v

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

const (
	goPGSchema   = "phase_c_routing_go" // go-only PG schema (isolates from parallel ports)
	goRouteTable = "phase_c_routing_go_route"
)

func phaseCGated(t *testing.T) {
	t.Helper()
	if os.Getenv("LITEDBMODEL_TX_ISOLATION") != "1" {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up: PG:5433 + MySQL:3307)")
	}
}

func openPhaseCPG(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		envOr("TEST_DB_USER", "testuser"), envOr("TEST_DB_PASSWORD", "testpass"),
		envOr("TEST_DB_HOST", "localhost"), envOr("TEST_DB_PORT", "5433"), envOr("TEST_DB_NAME", "testdb"))
	db, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatalf("pg connect (docker up? PG:5433): %v", err)
	}
	return db
}

func openPhaseCMysql(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		envOr("TEST_MYSQL_USER", "testuser"), envOr("TEST_MYSQL_PASSWORD", "testpass"),
		envOr("TEST_MYSQL_HOST", "127.0.0.1"), envOr("TEST_MYSQL_PORT", "3307"), envOr("TEST_MYSQL_DB", "testdb"))
	db, err := OpenMysql(dsn)
	if err != nil {
		t.Fatalf("mysql connect (docker up? MySQL:3307): %v", err)
	}
	return db
}

// setupPhaseCPGSchema creates the go-only PG schema + route table (dropped + recreated each run).
func setupPhaseCPGSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	exec := func(q string) {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("pg setup %q: %v", q, err)
		}
	}
	exec("DROP SCHEMA IF EXISTS " + goPGSchema + " CASCADE")
	exec("CREATE SCHEMA " + goPGSchema)
	exec("CREATE TABLE " + goPGSchema + "." + goRouteTable + " (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
}

func setupPhaseCMysqlTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	exec := func(q string) {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("mysql setup %q: %v", q, err)
		}
	}
	exec("DROP TABLE IF EXISTS " + goRouteTable)
	exec("CREATE TABLE " + goRouteTable + " (id INT PRIMARY KEY, val TEXT NOT NULL)")
}

// ── A recording [Pool] over a REAL *sql.DB (records WHICH pool served each acquire) ──

// livePool wraps a real *sql.DB pool, recording a label per Acquire into a shared log — the go
// analogue of the TS recordingPool. It delegates to a genuine [SQLDBPool] so the SQL actually runs.
type livePool struct {
	label string
	inner *SQLDBPool
	log   *[]string
	mu    *sync.Mutex
}

func newLivePool(label string, db *sql.DB, log *[]string, mu *sync.Mutex) *livePool {
	return &livePool{label: label, inner: NewSQLDBPool(db), log: log, mu: mu}
}

func (p *livePool) Acquire() (PooledConn, error) {
	p.mu.Lock()
	*p.log = append(*p.log, p.label)
	p.mu.Unlock()
	return p.inner.Acquire()
}

func (p *livePool) Release(conn PooledConn, destroy bool) error {
	return p.inner.Release(conn, destroy)
}

// ════════════════════════════════════════════════════════════════════════════════
// C1 — reader/writer separation + writer-sticky + withWriter (live PG)
// ════════════════════════════════════════════════════════════════════════════════

func TestPhaseCRoutingReaderWriterSplitLive(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCPG(t)
	defer db.Close()
	setupPhaseCPGSchema(t, db)
	tbl := goPGSchema + "." + goRouteTable

	var mu sync.Mutex
	var log []string
	reader := newLivePool("reader", db, &log, &mu)
	writer := newLivePool("writer", db, &log, &mu)
	reg, _ := RegistryFromDefault(ReaderWriterPools{Reader: reader, Writer: writer}).Build()
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	rows, err := Execute(ctx, "SELECT 1 AS one", nil, StatementIntent{Write: false})
	if err != nil {
		t.Fatal(err)
	}
	if bcObjInt(t, rows[0], "one") != 1 {
		t.Fatalf("SELECT 1 returned %v", rows[0])
	}
	if _, err := Run(ctx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING", tbl), []any{int64(1), "a"}, StatementIntent{Write: true}); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(log, []string{"reader", "writer"}) {
		t.Fatalf("split routing = %v, want [reader writer]", log)
	}

	// MUTATION (RED) — PRODUCTION PATH: collapse the split to ONE pool; the SAME read+write through
	// the SAME seam now both land on 'solo' ⇒ ['solo','solo'], NOT ['reader','writer'].
	var mlog []string
	solo := newLivePool("solo", db, &mlog, &mu)
	msreg, _ := RegistryFromDefault(ReaderWriterPools{Reader: solo, Writer: solo}).Build()
	mctx := ContextForRouting(RoutingConfig{Registry: msreg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	_, _ = Execute(mctx, "SELECT 1 AS one", nil, StatementIntent{Write: false})
	_, _ = Run(mctx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING", tbl), []any{int64(1), "a"}, StatementIntent{Write: true})
	if !equalStrings(mlog, []string{"solo", "solo"}) {
		t.Fatalf("mutation log = %v, want [solo solo] (split was load-bearing)", mlog)
	}
}

func TestPhaseCRoutingWriterStickyLive(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCPG(t)
	defer db.Close()
	setupPhaseCPGSchema(t, db)
	tbl := goPGSchema + "." + goRouteTable

	var mu sync.Mutex
	var log []string
	clock := int64(1_000_000)
	reader := newLivePool("reader", db, &log, &mu)
	writer := newLivePool("writer", db, &log, &mu)
	sticky := NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(true), WriterStickyDuration: intPtr(5000), Now: func() int64 { return clock }})
	reg, _ := RegistryFromDefault(ReaderWriterPools{Reader: reader, Writer: writer}).Build()
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: sticky}, nil)

	// Read BEFORE any tx → reader.
	_, _ = Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false})
	if log[len(log)-1] != "reader" {
		t.Fatalf("pre-tx read = %v, want reader", log[len(log)-1])
	}

	// A committed transaction on the writer db ARMS the sticky clock (WithTransactionDecidedIsolated
	// .Mark()s on a successful commit because the ctx carries routing).
	_, err := Transaction(ctx, db, "postgres", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		_, e := RunGuarded(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING", tbl), []any{int64(2), "b"}, "INSERT", "M")
		return 0, e
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read 100ms later (within the 5s window) → WRITER (read-your-writes).
	clock += 100
	_, _ = Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false})
	if log[len(log)-1] != "writer" {
		t.Fatalf("in-window read = %v, want writer (read-your-writes)", log[len(log)-1])
	}
	// Read after the window elapses → back to READER.
	clock += 6000
	_, _ = Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false})
	if log[len(log)-1] != "reader" {
		t.Fatalf("after-window read = %v, want reader", log[len(log)-1])
	}

	// MUTATION (RED) — PRODUCTION PATH: disable writer-sticky; the SAME commit-then-read now lands the
	// in-window read on the READER (no read-your-writes).
	var mlog []string
	mclock := int64(2_000_000)
	mreader := newLivePool("reader", db, &mlog, &mu)
	mwriter := newLivePool("writer", db, &mlog, &mu)
	mreg, _ := RegistryFromDefault(ReaderWriterPools{Reader: mreader, Writer: mwriter}).Build()
	mctx := ContextForRouting(RoutingConfig{Registry: mreg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false), Now: func() int64 { return mclock }})}, nil)
	_, err = Transaction(mctx, db, "postgres", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		_, e := RunGuarded(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING", tbl), []any{int64(3), "c"}, "INSERT", "M")
		return 0, e
	})
	if err != nil {
		t.Fatal(err)
	}
	before := len(mlog) // ignore the tx's own acquisitions; observe only the post-commit read tail
	mclock += 100
	_, _ = Execute(mctx, "SELECT 1", nil, StatementIntent{Write: false})
	tail := mlog[before:]
	if !equalStrings(tail, []string{"reader"}) {
		t.Fatalf("sticky-off in-window read = %v, want [reader] (sticky was load-bearing)", tail)
	}
}

func TestPhaseCRoutingWithWriterLive(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCPG(t)
	defer db.Close()
	setupPhaseCPGSchema(t, db)

	var mu sync.Mutex
	var log []string
	reader := newLivePool("reader", db, &log, &mu)
	writer := newLivePool("writer", db, &log, &mu)
	reg, _ := RegistryFromDefault(ReaderWriterPools{Reader: reader, Writer: writer}).Build()
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// A read inside the writer scope → WRITER.
	wctx := ctx.WithWriter()
	rows, err := Execute(wctx, "SELECT 1 AS one", nil, StatementIntent{Write: false})
	if err != nil {
		t.Fatal(err)
	}
	if bcObjInt(t, rows[0], "one") != 1 || log[len(log)-1] != "writer" {
		t.Fatalf("withWriter read routed to %v (rows %v), want writer", log[len(log)-1], rows[0])
	}
	// A write inside withWriter is REJECTED (read-only) — v1 parity.
	if _, err := RunGuarded(wctx, "INSERT INTO x VALUES (1)", nil, "INSERT", "X"); err == nil {
		t.Fatal("write in withWriter scope must be rejected (read-only)")
	} else if f, ok := err.(*SqlFailure); !ok || f.Kind != "write_in_read_only_context" {
		t.Fatalf("withWriter write rejection = %v, want write_in_read_only_context", err)
	}

	// MUTATION (RED) — the SAME read OUTSIDE the scope → reader.
	var mlog []string
	mreader := newLivePool("reader", db, &mlog, &mu)
	mwriter := newLivePool("writer", db, &mlog, &mu)
	mreg, _ := RegistryFromDefault(ReaderWriterPools{Reader: mreader, Writer: mwriter}).Build()
	mctx := ContextForRouting(RoutingConfig{Registry: mreg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	_, _ = Execute(mctx, "SELECT 1 AS one", nil, StatementIntent{Write: false}) // no WithWriter()
	if !equalStrings(mlog, []string{"reader"}) {
		t.Fatalf("outside-scope read = %v, want [reader] (withWriter divert was load-bearing)", mlog)
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// C2 — multi-DB name routing (PG = A, MySQL = B) + tx-pin precedence
// ════════════════════════════════════════════════════════════════════════════════

func TestPhaseCRoutingMultiDBLive(t *testing.T) {
	phaseCGated(t)
	pg := openPhaseCPG(t)
	defer pg.Close()
	my := openPhaseCMysql(t)
	defer my.Close()
	setupPhaseCPGSchema(t, pg)
	setupPhaseCMysqlTable(t, my)
	pgTbl := goPGSchema + "." + goRouteTable

	var mu sync.Mutex
	var log []string
	aPool := newLivePool("A", pg, &log, &mu)
	bPool := newLivePool("B", my, &log, &mu)
	reg := NewConnectionRegistry(map[string]ReaderWriterPools{
		"default": SinglePoolPair(aPool),
		"B":       SinglePoolPair(bPool),
	})
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// Untagged read → default (DB A = PG); "B"-tagged read → DB B = MySQL.
	ra, err := Execute(ctx, "SELECT 42 AS n", nil, StatementIntent{Write: false})
	if err != nil {
		t.Fatal(err)
	}
	rb, err := Execute(ctx, "SELECT 7 AS n", nil, StatementIntent{Write: false, DB: "B"})
	if err != nil {
		t.Fatal(err)
	}
	if bcObjInt(t, ra[0], "n") != 42 || bcObjInt(t, rb[0], "n") != 7 {
		t.Fatalf("untagged=%v tagged=%v", ra[0], rb[0])
	}
	if !equalStrings(log, []string{"A", "B"}) {
		t.Fatalf("named routing = %v, want [A B]", log)
	}

	// Real cross-DB: write a distinct row into each DB via its tagged pool; read it back only from
	// that DB (the other DB does NOT have it).
	if _, err := Run(ctx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING", pgTbl), []any{int64(100), "in-A"}, StatementIntent{Write: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=val", goRouteTable), []any{int64(200), "in-B"}, StatementIntent{Write: true, DB: "B"}); err != nil {
		t.Fatal(err)
	}
	inA, _ := Execute(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id=$1", pgTbl), []any{int64(100)}, StatementIntent{Write: false})
	inB, _ := Execute(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id=?", goRouteTable), []any{int64(200)}, StatementIntent{Write: false, DB: "B"})
	if bcObjStr(t, inA[0], "val") != "in-A" || bcObjStr(t, inB[0], "val") != "in-B" {
		t.Fatalf("cross-DB read A=%v B=%v", inA[0], inB[0])
	}
	missInB, _ := Execute(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id=?", goRouteTable), []any{int64(100)}, StatementIntent{Write: false, DB: "B"})
	if len(missInB) != 0 {
		t.Fatalf("A-only row must NOT be in B, got %v", missInB)
	}

	// MUTATION (RED): if routing IGNORED intent.DB, the "B"-tagged MySQL query (a `?` placeholder)
	// would be sent to PG (which uses `$N` and REJECTS a bare `?`) → THROW. Model by forcing the
	// "B"-tagged read onto the default pool (A = PG) and confirm it fails.
	forced := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(aPool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	if _, err := Execute(forced, fmt.Sprintf("SELECT val FROM %s WHERE id=?", goRouteTable), []any{int64(200)}, StatementIntent{Write: false}); err == nil {
		t.Fatal("a MySQL-placeholder query on the PG default pool must FAIL (routing-ignored mutation)")
	}

	// A missing connection name is a LOUD error (never a silent default fallback).
	if _, err := Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false, DB: "ghost"}); err == nil || !strings.Contains(err.Error(), "no connection registered under name 'ghost'") {
		t.Fatalf("unknown name must loud-fail, got %v", err)
	}
}

// tx-pin precedence: a named-DB transaction runs ENTIRELY on ONE pinned writer connection — the
// routing steps 2-4 do NOT re-resolve mid-tx (Phase B ownership is preserved). Proven by running a
// multi-statement tx on DB B (MySQL) and confirming every statement hit the SAME pinned *sql.Tx (the
// routing recording pool is NOT touched for in-tx statements — the pin wins in ConnectionFor).
func TestPhaseCRoutingTxPinPrecedenceLive(t *testing.T) {
	phaseCGated(t)
	pg := openPhaseCPG(t)
	defer pg.Close()
	my := openPhaseCMysql(t)
	defer my.Close()
	setupPhaseCPGSchema(t, pg)
	setupPhaseCMysqlTable(t, my)

	var mu sync.Mutex
	var log []string
	aPool := newLivePool("A", pg, &log, &mu)
	bPool := newLivePool("B", my, &log, &mu)
	reg := NewConnectionRegistry(map[string]ReaderWriterPools{"default": SinglePoolPair(aPool), "B": SinglePoolPair(bPool)})
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// Run a tx on DB B (MySQL): the tx is opened on `my` (the writer of DB B) and pins ONE *sql.Tx.
	// Every statement inside resolves the PINNED connection (step 1 wins) — the recording pools are
	// NOT re-acquired mid-tx.
	log = log[:0]
	_, err := Transaction(ctx, my, "mysql", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		if _, e := RunGuarded(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (?,?)", goRouteTable), []any{int64(300), "tx1"}, "INSERT", "M"); e != nil {
			return 0, e
		}
		if _, e := RunGuarded(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES (?,?)", goRouteTable), []any{int64(301), "tx2"}, "INSERT", "M"); e != nil {
			return 0, e
		}
		// A read INSIDE the tx also resolves the pinned conn (NOT the routing reader pool).
		rows, e := Execute(txCtx, fmt.Sprintf("SELECT COUNT(*) AS c FROM %s WHERE id IN (300,301)", goRouteTable), nil, StatementIntent{Write: false})
		if e != nil {
			return 0, e
		}
		if bcObjInt(t, rows[0], "c") != 2 {
			t.Fatalf("in-tx count = %v, want 2", rows[0])
		}
		return 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The routing recording pools saw ZERO acquisitions for the in-tx statements (the pin won every
	// time). This is the tx-pin-precedence proof: Phase C routing did NOT break Phase B ownership.
	if len(log) != 0 {
		t.Fatalf("in-tx statements must resolve the PINNED conn, not the routing pool; got acquisitions %v", log)
	}
	// Both rows committed on the ONE pinned connection.
	var c int
	if err := my.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id IN (300,301)", goRouteTable)).Scan(&c); err != nil || c != 2 {
		t.Fatalf("post-tx count = %d (err %v), want 2", c, err)
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// C3 — setConfig: queryTimeout / maxPool sole-cap / searchPath reset-on-release / closeAllPools
// ════════════════════════════════════════════════════════════════════════════════

func TestPhaseCConfigQueryTimeoutLivePG(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCPG(t)
	defer db.Close()

	// A configured pool over live PG with a 200ms statement_timeout.
	cfg := ResolveConnectionConfig(ConnectionConfig{Driver: "postgres", QueryTimeout: intPtr(200)})
	pool := ConfiguredPool(NewSQLDBPool(db), cfg)
	reg := SingleDefaultRegistry(pool)
	ctx := ContextForRouting(RoutingConfig{Registry: reg, Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// pg_sleep(2) (2s) exceeds the 200ms server statement_timeout → the SERVER aborts it.
	_, err := Execute(ctx, "SELECT pg_sleep(2)", nil, StatementIntent{Write: false})
	if err == nil || !(strings.Contains(strings.ToLower(err.Error()), "statement timeout") || strings.Contains(strings.ToLower(err.Error()), "canceling statement")) {
		t.Fatalf("queryTimeout must fire a server statement timeout, got %v", err)
	}

	// MUTATION (RED): the SAME slow query on an UNCONFIGURED pool (no statement_timeout) does NOT time
	// out at 200ms — it completes — so the timeout is the config's doing.
	plain := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(NewSQLDBPool(db)), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	ok, err := Execute(plain, "SELECT pg_sleep(0.3) AS done", nil, StatementIntent{Write: false}) // 300ms > 200ms; no timeout set ⇒ succeeds
	if err != nil || len(ok) != 1 {
		t.Fatalf("unconfigured 300ms query should succeed, got rows=%d err=%v", len(ok), err)
	}
}

func TestPhaseCConfigQueryTimeoutLiveMySQL(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCMysql(t)
	defer db.Close()

	cfg := ResolveConnectionConfig(ConnectionConfig{Driver: "mysql", QueryTimeout: intPtr(200)})
	pool := ConfiguredPool(NewSQLDBPool(db), cfg)
	ctx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(pool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// A CPU-heavy SELECT (cross-join + SHA2 per row) burns past 200ms; max_execution_time aborts it
	// (max_execution_time does NOT apply to SLEEP(), so a heavy read is required). SELECT-only ⇒ read.
	heavy := `SELECT COUNT(*) AS n FROM
		information_schema.COLLATIONS a,
		information_schema.COLLATIONS b,
		information_schema.COLLATIONS c
		WHERE SHA2(CONCAT(a.ID, b.ID, c.ID, RAND()), 256) > ''`
	_, err := Execute(ctx, heavy, nil, StatementIntent{Write: false})
	if err == nil {
		t.Fatal("mysql queryTimeout must abort the heavy query (max_execution_time)")
	}
	le := strings.ToLower(err.Error())
	if !(strings.Contains(le, "max_execution_time") || strings.Contains(le, "execution was interrupted") || strings.Contains(le, "3024") || strings.Contains(le, "query execution")) {
		t.Fatalf("mysql abort error = %v, want a max_execution_time abort", err)
	}

	// MUTATION (RED): a smaller heavy query on an UNCONFIGURED pool COMPLETES.
	plain := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(NewSQLDBPool(db)), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	small := `SELECT COUNT(*) AS n FROM
		information_schema.COLLATIONS a,
		information_schema.COLLATIONS b
		WHERE SHA2(CONCAT(a.ID, b.ID), 256) > ''`
	ok, err := Execute(plain, small, nil, StatementIntent{Write: false})
	if err != nil || bcObjInt(t, ok[0], "n") <= 0 {
		t.Fatalf("unconfigured small heavy query should complete, got rows=%v err=%v", ok, err)
	}
}

// maxPool is the SOLE cap: BuildRoutingConfig CONSTRUCTS the *sql.DB via the factory from the resolved
// config, so SetMaxOpenConns = maxPool at construction. 5 concurrent slow queries against a maxPool=2
// pool ⇒ at most 2 connections live mid-flight. Deleting maxPool (⇒ default 10) lets all 5 open.
func TestPhaseCConfigMaxPoolSoleCapLivePG(t *testing.T) {
	phaseCGated(t)
	// Ensure the go schema exists so the connection is valid; queries here are just pg_sleep (no table).
	prep := openPhaseCPG(t)
	prep.Close()

	pgConn := ConnectionConfig{Driver: "postgres",
		Host: envOr("TEST_DB_HOST", "localhost"), Port: int(atoiOr(envOr("TEST_DB_PORT", "5433"), 5433)),
		Database: envOr("TEST_DB_NAME", "testdb"), User: envOr("TEST_DB_USER", "testuser"), Password: envOr("TEST_DB_PASSWORD", "testpass")}

	// Capture the constructed *sql.DB so we can read its live Stats().OpenConnections. The factory's
	// `max` comes ONLY from cfg.MaxPool — there is no other cap.
	var captured *sql.DB
	capturingFactory := func(cfg ResolvedConnectionConfig, role string) (BuiltPool, error) {
		bp, err := PgPoolFactory()(cfg, role)
		if err != nil {
			return BuiltPool{}, err
		}
		captured = bp.Pool.(*SQLDBPool).db
		return bp, nil
	}

	cfg := pgConn
	cfg.MaxPool = intPtr(2)
	cfg.KeepAlive = boolPtr(true)
	cfg.KeepAliveInitialDelayMillis = intPtr(3000)
	built, err := BuildRoutingConfig([]ConnectionSetup{{Config: cfg, PoolFactory: capturingFactory}}, StickyOptions{UseWriterAfterTransaction: boolPtr(false)})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ContextForRouting(built.Routing, nil)
	if captured == nil {
		t.Fatal("factory did not capture the *sql.DB")
	}
	// maxPool reached the *sql.DB at CONSTRUCTION (Stats().MaxOpenConnections).
	if captured.Stats().MaxOpenConnections != 2 {
		t.Fatalf("MaxOpenConnections = %d, want 2 (maxPool applied at construction)", captured.Stats().MaxOpenConnections)
	}

	// Fire 5 concurrent slow queries; the pool CANNOT exceed maxPool=2 mid-flight.
	var wg sync.WaitGroup
	fire := func(c *ExecutionContext, n int) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = Execute(c, "SELECT pg_sleep(0.25) AS d", nil, StatementIntent{Write: false})
			}()
		}
	}
	fire(ctx, 5)
	time.Sleep(120 * time.Millisecond)
	mid := captured.Stats().OpenConnections
	if mid > 2 {
		t.Fatalf("mid-flight OpenConnections = %d, want <= 2 (maxPool cap)", mid)
	}
	wg.Wait()

	// MUTATION (RED): DELETE maxPool (⇒ default 10); the SAME 5 concurrent queries open > 2
	// connections at once. If maxPool were dead surface (always 2), this would ALSO cap at 2 and
	// falsely pass — it does NOT, proving maxPool is the load-bearing sole cap.
	var capturedM *sql.DB
	capturingM := func(cfg ResolvedConnectionConfig, role string) (BuiltPool, error) {
		bp, err := PgPoolFactory()(cfg, role)
		if err != nil {
			return BuiltPool{}, err
		}
		capturedM = bp.Pool.(*SQLDBPool).db
		return bp, nil
	}
	builtM, err := BuildRoutingConfig([]ConnectionSetup{{Config: pgConn /* NO MaxPool ⇒ default 10 */, PoolFactory: capturingM}}, StickyOptions{UseWriterAfterTransaction: boolPtr(false)})
	if err != nil {
		t.Fatal(err)
	}
	mctx := ContextForRouting(builtM.Routing, nil)
	fire(mctx, 5)
	time.Sleep(120 * time.Millisecond)
	midM := capturedM.Stats().OpenConnections
	if midM <= 2 {
		t.Fatalf("uncapped mid-flight OpenConnections = %d, want > 2 (RED if maxPool were 'always 2')", midM)
	}
	wg.Wait()
	_ = builtM.Close()

	// closeAllPools closed the first pool: a query after close fails (proves the close is real).
	if err := built.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := Execute(ctx, "SELECT 1", nil, StatementIntent{Write: false}); err == nil {
		t.Fatal("query after closeAllPools must fail (pool closed)")
	}
}

// searchPath reset-on-release: a configured pool with searchPath sets it on checkout and RESETs it on
// release, so a pooled connection does NOT leak the search_path to the next caller. Proven by: with
// searchPath=go schema, an UNQUALIFIED table name resolves to the go schema's table; after the
// configured connection is released, a RAW query on the same *sql.DB pool sees the DEFAULT search_path
// (public) — the go-schema table is NOT visible unqualified (no leak).
func TestPhaseCConfigSearchPathResetOnReleaseLivePG(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCPG(t)
	defer db.Close()
	setupPhaseCPGSchema(t, db)
	// Seed a row in the go-schema table.
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (1, 'in-go-schema') ON CONFLICT (id) DO NOTHING", goPGSchema, goRouteTable)); err != nil {
		t.Fatal(err)
	}
	// Use a small pool (maxPool=1) so the SAME physical connection is reused → a leak WOULD be visible.
	singleDB := openPhaseCPG(t)
	defer singleDB.Close()
	singleDB.SetMaxOpenConns(1)

	cfg := ResolveConnectionConfig(ConnectionConfig{Driver: "postgres", SearchPath: goPGSchema})
	pool := ConfiguredPool(NewSQLDBPool(singleDB), cfg)
	ctx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(pool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	// With searchPath = go schema, the UNQUALIFIED table name resolves to the go-schema table.
	rows, err := Execute(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id=1", goRouteTable), nil, StatementIntent{Write: false})
	if err != nil {
		t.Fatalf("unqualified read with searchPath should resolve go schema: %v", err)
	}
	if bcObjStr(t, rows[0], "val") != "in-go-schema" {
		t.Fatalf("searchPath read = %v, want in-go-schema", rows[0])
	}

	// After the configured connection was released (reset ran), a RAW query on the SAME single-conn
	// *sql.DB pool sees the DEFAULT search_path — the unqualified go-schema table is NOT visible.
	var leaked string
	rawErr := singleDB.QueryRow(fmt.Sprintf("SELECT val FROM %s WHERE id=1", goRouteTable)).Scan(&leaked)
	if rawErr == nil {
		t.Fatalf("search_path LEAKED: unqualified %s resolved after release (got %q) — reset-on-release failed", goRouteTable, leaked)
	}
	if !strings.Contains(strings.ToLower(rawErr.Error()), "does not exist") && !strings.Contains(strings.ToLower(rawErr.Error()), "undefined") && !strings.Contains(strings.ToLower(rawErr.Error()), "relation") {
		t.Fatalf("expected an unqualified-table-not-found error after reset, got %v", rawErr)
	}

	// MUTATION (RED): a pool WITHOUT the searchPath config (all-default) does NOT set search_path, so
	// the unqualified read fails immediately (proving the searchPath session statement was load-bearing
	// for the green read above).
	plainPool := ConfiguredPool(NewSQLDBPool(db), ResolveConnectionConfig(ConnectionConfig{Driver: "postgres"}))
	plainCtx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(plainPool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)
	if _, err := Execute(plainCtx, fmt.Sprintf("SELECT val FROM %s WHERE id=1", goRouteTable), nil, StatementIntent{Write: false}); err == nil {
		t.Fatal("without searchPath config, the unqualified go-schema read must FAIL (searchPath was load-bearing)")
	}
}

// charset reset-on-release (MySQL): a configured pool with charset sets SET NAMES on checkout and
// resets on release. Proven by reading @@character_set_connection through the configured pool (= the
// configured charset) vs. a raw connection after release (= the server default).
func TestPhaseCConfigCharsetResetOnReleaseLiveMySQL(t *testing.T) {
	phaseCGated(t)
	db := openPhaseCMysql(t)
	defer db.Close()
	db.SetMaxOpenConns(1) // reuse the SAME physical conn so a leak would be visible

	cfg := ResolveConnectionConfig(ConnectionConfig{Driver: "mysql", Charset: "latin1"})
	pool := ConfiguredPool(NewSQLDBPool(db), cfg)
	ctx := ContextForRouting(RoutingConfig{Registry: SingleDefaultRegistry(pool), Sticky: NewWriterStickyClock(StickyOptions{UseWriterAfterTransaction: boolPtr(false)})}, nil)

	rows, err := Execute(ctx, "SELECT @@character_set_connection AS cs", nil, StatementIntent{Write: false})
	if err != nil {
		t.Fatal(err)
	}
	if bcObjStr(t, rows[0], "cs") != "latin1" {
		t.Fatalf("configured charset = %v, want latin1", rows[0])
	}

	// After release (reset ran), a RAW query on the SAME single-conn pool sees the server default
	// charset (NOT latin1) — no leak.
	var raw string
	if err := db.QueryRow("SELECT @@character_set_connection").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw == "latin1" {
		t.Fatalf("charset LEAKED: raw conn still latin1 after release — reset-on-release failed")
	}
}

// ── small helpers (bc row extraction) ──────────────────────────────────────────

func bcObjInt(t *testing.T, v bc.Value, key string) int64 {
	t.Helper()
	obj, ok := v.(*bc.Obj)
	if !ok {
		t.Fatalf("row is not an object: %T", v)
	}
	cell := obj.Vals[key]
	switch n := cell.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case string:
		return atoiOr(n, -1)
	default:
		t.Fatalf("cell %q is not numeric: %T (%v)", key, cell, cell)
		return 0
	}
}

func bcObjStr(t *testing.T, v bc.Value, key string) string {
	t.Helper()
	obj, ok := v.(*bc.Obj)
	if !ok {
		t.Fatalf("row is not an object: %T", v)
	}
	s, _ := obj.Vals[key].(string)
	return s
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func atoiOr(s string, def int64) int64 {
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	return n
}
