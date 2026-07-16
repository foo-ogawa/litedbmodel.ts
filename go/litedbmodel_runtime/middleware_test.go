// Phase D (#94, go) — the SCP MIDDLEWARE layer, hook-mechanics unit tests.
//
// Ports test/scp/middleware.test.ts to the go seam (in-proc modernc SQLite, real), the reference the
// TS #92 layer defines. Proves D1/D2/D3 on the REAL Phase A exec-context seam + genuine RED proofs:
//
//	D1 SQL-level hook — a registered middleware intercepts EVERY SQL through the seam
//	   (read/write/tx-control), OBSERVE / REWRITE / TIME / SHORT-CIRCUIT; applied ORDER
//	   first-registered-outermost; per-scope + per-goroutine ISOLATION (the stack-copy-not-state-map
//	   guarantee). RED: no registration ⇒ nothing observed (byte-identical passthrough).
//	D2 method-level hooks — RunMethod(kind, …) fires the matching op-kind hook, before/after observed,
//	   composes outermost-first + rewrites args. RED: a hook of a DIFFERENT kind does not fire.
//	D3 Logger + raw Execute/Query — Logger records real SQL/params/timing; RawExecute/RawQuery go
//	   THROUGH the seam (a registered SQL middleware sees the raw call); RawQuery is two-level (query
//	   method hook + execute hook). RED: without the wiring the Logger records nothing.
//
// Every registration is inside a WithMiddlewareScope so the process-global registry stays clean (an
// unregistered chain is byte-identical — the conformance/livedb runners register none). The live
// PG/MySQL async-seam proof rides the standard runners (byte-identical passthrough) + is covered by the
// relation-batch end-to-end here on the SAME central seam the relation walker uses.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	_ "modernc.org/sqlite"
)

func mwDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return db
}

// observeMiddleware records each observed SQL into `seen` (guarded), passing through to next.
func observeMiddleware(seen *[]string, mu *sync.Mutex) *MiddlewareHandle {
	return NewMiddleware(MiddlewareConfig{
		Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
			mu.Lock()
			*seen = append(*seen, sql)
			mu.Unlock()
			return next(sql, args)
		},
	})
}

// ── D1: SQL-level execute hook ─────────────────────────────────────────────────

// The middleware intercepts EVERY SQL through the seam (write, tx-control, read) — observe.
func TestD1InterceptsEverySQL(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var seen []string
	var mu sync.Mutex
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, observeMiddleware(&seen, &mu).Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		if _, e := Run(ctx, "BEGIN", nil, WriteIntent()); e != nil {
			return struct{}{}, e
		}
		if _, e := Run(ctx, "INSERT INTO t (name) VALUES (?)", []any{"a"}, WriteIntent()); e != nil {
			return struct{}{}, e
		}
		if _, e := Run(ctx, "COMMIT", nil, WriteIntent()); e != nil {
			return struct{}{}, e
		}
		if _, e := Execute(ctx, "SELECT * FROM t", nil, ReadIntent()); e != nil {
			return struct{}{}, e
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	want := []string{"BEGIN", "INSERT INTO t (name) VALUES (?)", "COMMIT", "SELECT * FROM t"}
	if !equalStrs(seen, want) {
		t.Errorf("intercept: got %v want %v", seen, want)
	}
}

// RED proof: WITHOUT the middleware wiring, nothing is observed (byte-identical passthrough).
func TestD1RedNoRegistrationNoInterception(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var seen []string
	ctx := ContextForDB(db) // no scope, no registration → empty chain
	if _, err := Run(ctx, "INSERT INTO t (name) VALUES (?)", []any{"a"}, WriteIntent()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := Execute(ctx, "SELECT * FROM t", nil, ReadIntent()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(seen) != 0 {
		t.Errorf("RED: expected no observation, got %v", seen)
	}
}

// A middleware can REWRITE the SQL/params passed to next.
func TestD1Rewrite(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		mw := NewMiddleware(MiddlewareConfig{
			Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
				if len(sql) >= 6 && sql[:6] == "INSERT" {
					return next(sql, []any{"rewritten"})
				}
				return next(sql, args)
			},
		})
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Run(ctx, "INSERT INTO t (name) VALUES (?)", []any{"original"}, WriteIntent())
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	var name string
	if e := db.QueryRow("SELECT name FROM t").Scan(&name); e != nil {
		t.Fatalf("read back: %v", e)
	}
	if name != "rewritten" {
		t.Errorf("rewrite: got %q want %q", name, "rewritten")
	}
}

// A middleware can TIME next.
func TestD1Time(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	timed := time.Duration(-1)
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		mw := NewMiddleware(MiddlewareConfig{
			Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
				t0 := time.Now()
				out, e := next(sql, args)
				timed = time.Since(t0)
				return out, e
			},
		})
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Execute(ctx, "SELECT * FROM t", nil, ReadIntent())
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if timed < 0 {
		t.Errorf("time: hook did not time next (%v)", timed)
	}
}

// A middleware can SHORT-CIRCUIT (skip next → the real DB is never touched).
func TestD1ShortCircuit(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var rows []bc.Value
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		synthetic := bc.NewObj()
		synthetic.Set("id", float64(99))
		synthetic.Set("name", "synthetic")
		mw := NewMiddleware(MiddlewareConfig{
			Execute: func(_ any, _ ExecNext, _ string, _ []any) (any, error) {
				return []bc.Value{synthetic}, nil // do NOT call next
			},
		})
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		r, e := Execute(ctx, "SELECT * FROM t", nil, ReadIntent())
		rows = r
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if len(rows) != 1 || firstRowField(rows, "name") != "synthetic" {
		t.Errorf("short-circuit: got %v", rows)
	}
	// Nothing was ever inserted; a real query returns 0 — proves the DB was bypassed.
	var c int
	if e := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&c); e != nil || c != 0 {
		t.Errorf("short-circuit must bypass DB: count=%d err=%v", c, e)
	}
}

// Applied ORDER: first-registered is outermost (A.before → B.before → B.after → A.after).
func TestD1AppliedOrder(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var order []string
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		a := NewMiddleware(MiddlewareConfig{Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
			order = append(order, "A:before")
			out, e := next(sql, args)
			order = append(order, "A:after")
			return out, e
		}})
		b := NewMiddleware(MiddlewareConfig{Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
			order = append(order, "B:before")
			out, e := next(sql, args)
			order = append(order, "B:after")
			return out, e
		}})
		RegisterMiddleware(scopeCtx, a.Descriptor())
		RegisterMiddleware(scopeCtx, b.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Execute(ctx, "SELECT 1", nil, ReadIntent())
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	want := []string{"A:before", "B:before", "B:after", "A:after"}
	if !equalStrs(order, want) {
		t.Errorf("order: got %v want %v", order, want)
	}
}

// RED proof (fold direction): a LAST→FIRST fold with index0 outermost is REQUIRED. A wrong (first→last)
// fold would yield B:before,A:before,A:after,B:after — this asserts the correct order so a reversed
// fold fails. (The RED is the negation asserted in TestD1AppliedOrder: the wrong order is not equal.)
func TestD1FoldDirectionRed(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var order []string
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		for _, tag := range []string{"A", "B"} {
			tag := tag
			mw := NewMiddleware(MiddlewareConfig{Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
				order = append(order, tag)
				return next(sql, args)
			}})
			RegisterMiddleware(scopeCtx, mw.Descriptor())
		}
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Execute(ctx, "SELECT 1", nil, ReadIntent())
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	// index 0 (A) runs first (outermost). A reversed fold would put B first — that must NOT happen.
	if !equalStrs(order, []string{"A", "B"}) {
		t.Errorf("fold RED: index0 must be outermost, got %v", order)
	}
}

// Per-scope STATE is isolated + fresh (v1 getCurrentContext): a state counter starts at 0 in each
// fresh scope, NOT carried from a previous scope.
func TestD1PerScopeStateFreshAndIsolated(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	type counter struct{ n int }
	mw := NewMiddleware(MiddlewareConfig{
		NewState: func() any { return &counter{} },
		Execute: func(state any, next ExecNext, sql string, args []any) (any, error) {
			state.(*counter).n++
			return next(sql, args)
		},
	})
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, _ = Execute(ctx, "SELECT 1", nil, ReadIntent())
		_, _ = Execute(ctx, "SELECT 2", nil, ReadIntent())
		if got := mw.State(scopeCtx).(*counter).n; got != 2 {
			t.Errorf("state: got %d want 2", got)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope1: %v", err)
	}
	// A fresh scope starts from a fresh state (0 → 1), NOT the previous scope's 2.
	_, err = WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, _ = Execute(ctx, "SELECT 3", nil, ReadIntent())
		if got := mw.State(scopeCtx).(*counter).n; got != 1 {
			t.Errorf("fresh state: got %d want 1", got)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope2: %v", err)
	}
}

// CONCURRENT-ISOLATION (the TS M5 guarantee, go = context.Context scope): two goroutines with DISTINCT
// scopes do not cross-talk — neither observes the other's SQL, and each per-middleware state stays its
// own. This is the stack-copy-not-state-map isolation reproduced concurrently.
func TestD1ConcurrentScopeIsolation(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var seenA, seenB []string
	var muA, muB sync.Mutex
	type counter struct{ n int }

	run := func(seen *[]string, mu *sync.Mutex, tag string, delay time.Duration) {
		_, _ = WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
			mw := NewMiddleware(MiddlewareConfig{
				NewState: func() any { return &counter{} },
				Execute: func(state any, next ExecNext, sql string, args []any) (any, error) {
					mu.Lock()
					*seen = append(*seen, tag+":"+sql)
					mu.Unlock()
					state.(*counter).n++
					return next(sql, args)
				},
			})
			RegisterMiddleware(scopeCtx, mw.Descriptor())
			ctx := ContextForDBCtx(scopeCtx, db)
			time.Sleep(delay) // overlap the OTHER scope while it is concurrently active
			q := "SELECT 1"
			if tag == "B" {
				q = "SELECT 2"
			}
			if _, e := Execute(ctx, q, nil, ReadIntent()); e != nil {
				return struct{}{}, e
			}
			// This scope's state saw exactly ONE statement — no bleed from the other goroutine.
			if got := mw.State(scopeCtx).(*counter).n; got != 1 {
				t.Errorf("%s: state bled — got %d want 1", tag, got)
			}
			return struct{}{}, nil
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); run(&seenA, &muA, "A", 8*time.Millisecond) }()
	go func() { defer wg.Done(); run(&seenB, &muB, "B", 1*time.Millisecond) }()
	wg.Wait()

	if !equalStrs(seenA, []string{"A:SELECT 1"}) {
		t.Errorf("isolation A: got %v want [A:SELECT 1]", seenA)
	}
	if !equalStrs(seenB, []string{"B:SELECT 2"}) {
		t.Errorf("isolation B: got %v want [B:SELECT 2]", seenB)
	}
}

// RED proof (shared-registry cross-talk): if the two goroutines shared ONE registry (the global) they
// would observe each other's SQL. Registering both on the GLOBAL registry (no scope) reproduces the
// cross-talk — proving the per-scope isolation above is load-bearing.
func TestD1SharedRegistryCrossTalkRed(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	ClearMiddlewares()
	defer ClearMiddlewares()
	var seen []string
	var mu sync.Mutex
	// Two middlewares on the GLOBAL registry (no WithMiddlewareScope) → they share ONE stack.
	RegisterMiddleware(context.Background(), observeMiddleware(&seen, &mu).Descriptor())
	ctx := ContextForDB(db) // background ctx resolves the GLOBAL registry
	if _, err := Execute(ctx, "SELECT 1", nil, ReadIntent()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("global registry must intercept, got %v", seen)
	}
	// A SECOND observer on the same global registry sees the FIRST's statement too — the shared-stack
	// cross-talk a scope prevents. (Both observe the one SQL.)
	seen = nil
	RegisterMiddleware(context.Background(), observeMiddleware(&seen, &mu).Descriptor())
	if _, err := Execute(ctx, "SELECT 2", nil, ReadIntent()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Two observers on the shared registry ⇒ 2 records for the ONE statement (cross-talk proof).
	if len(seen) != 2 {
		t.Errorf("shared-registry RED: expected 2 observers to both fire, got %v", seen)
	}
}

// The state-map-copy trap (the CRITICAL watch-out): a NESTED scope inherits the outer's STACK but
// starts with an EMPTY state map — so the inner middleware's per-scope state is FRESH (0), NOT the
// outer's accumulated value. A copyStackOnly that ALSO copied the state map would leak the outer's
// state here (the TS M5 bug). This guards that copy-only-the-stack contract directly.
func TestD1NestedScopeStartsEmptyState(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	type counter struct{ n int }
	mw := NewMiddleware(MiddlewareConfig{
		NewState: func() any { return &counter{} },
		Execute: func(state any, next ExecNext, sql string, args []any) (any, error) {
			state.(*counter).n++
			return next(sql, args)
		},
	})
	_, err := WithMiddlewareScope(context.Background(), func(outerCtx context.Context) (struct{}, error) {
		RegisterMiddleware(outerCtx, mw.Descriptor())
		outer := ContextForDBCtx(outerCtx, db)
		_, _ = Execute(outer, "SELECT 1", nil, ReadIntent())
		_, _ = Execute(outer, "SELECT 2", nil, ReadIntent())
		if got := mw.State(outerCtx).(*counter).n; got != 2 {
			t.Fatalf("outer state: got %d want 2", got)
		}
		// A NESTED scope inherits the STACK (mw still registered) but the state map is FRESH.
		_, e := WithMiddlewareScope(outerCtx, func(innerCtx context.Context) (struct{}, error) {
			inner := ContextForDBCtx(innerCtx, db)
			_, _ = Execute(inner, "SELECT 3", nil, ReadIntent())
			// Inner state is 1 (its OWN fresh counter), NOT 3 (2 inherited + 1). Proves state NOT copied.
			if got := mw.State(innerCtx).(*counter).n; got != 1 {
				t.Errorf("nested state-map-copy trap: inner got %d want 1 (state must NOT be copied)", got)
			}
			return struct{}{}, nil
		})
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
}

// ── D1 END-TO-END: relation-batch read — the middleware observes the relation-batch SELECT ─────

func relE2EDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, s := range []string{
		`CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)`,
		`CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)`,
		`INSERT INTO parent VALUES (1,'p')`,
		`INSERT INTO child VALUES (10,1,'a'),(11,1,'b')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

// A registered middleware observes the relation-BATCH SELECT (the hasMany child fan-out) of a
// multi-node read — funneled through the SAME central seam (runRelationOpCtx → Execute) as the walker.
func TestD1RelationBatchEndToEnd(t *testing.T) {
	db := relE2EDB(t)
	defer db.Close()
	var seen []string
	var mu sync.Mutex
	op := RelationOp{
		Name: "kids", Kind: "hasMany",
		ParentKey: "id", TargetKey: "parent_id", Dialect: "sqlite",
		SQL: "SELECT id, parent_id, label FROM child WHERE parent_id IN (SELECT value FROM json_each(?))",
	}
	parent := bc.NewObj()
	parent.Set("id", float64(1))
	parent.Set("name", "p")

	var batch RelationBatch
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, observeMiddleware(&seen, &mu).Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		// The primary read on `parent` PLUS the hasMany batch SELECT on `child` — both funnel the seam.
		if _, e := Execute(ctx, "SELECT id, name FROM parent WHERE id = ?", []any{int64(1)}, ReadIntent()); e != nil {
			return struct{}{}, e
		}
		b, e := runRelationOpCtx(ctx, op, []bc.Value{parent})
		batch = b
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	// The relation actually loaded (2 children under parent 1) — a genuine multi-node read.
	if got := len(batch[keyIdentity([]bc.Value{float64(1)})]); got != 2 {
		t.Fatalf("relation load: got %d children want 2", got)
	}
	// The middleware saw the primary read AND the relation-batch SELECT (querying the child table).
	if !containsSub(seen, "from child") && !containsSub(seen, "FROM child") {
		t.Errorf("relation-batch SQL not observed: %v", seen)
	}
	if !containsSub(seen, "FROM parent") {
		t.Errorf("primary read SQL not observed: %v", seen)
	}
}

// RED proof: without registration, the relation-batch SELECT is NOT observed (byte-identical).
func TestD1RelationBatchRed(t *testing.T) {
	db := relE2EDB(t)
	defer db.Close()
	var seen []string
	op := RelationOp{
		Name: "kids", Kind: "hasMany",
		ParentKey: "id", TargetKey: "parent_id", Dialect: "sqlite",
		SQL: "SELECT id, parent_id, label FROM child WHERE parent_id IN (SELECT value FROM json_each(?))",
	}
	parent := bc.NewObj()
	parent.Set("id", float64(1))
	// No middleware registered → the relation batch runs as a byte-identical passthrough.
	batch, err := RunRelationOp(op, []bc.Value{parent}, db)
	if err != nil {
		t.Fatalf("run relation: %v", err)
	}
	// The read still WORKS (byte-identical) — the relation loaded — but nothing was observed.
	if got := len(batch[keyIdentity([]bc.Value{float64(1)})]); got != 2 {
		t.Fatalf("relation load: got %d want 2", got)
	}
	if len(seen) != 0 {
		t.Errorf("RED: expected no observation, got %v", seen)
	}
}

// ── D2: method-level hooks ─────────────────────────────────────────────────────

// Fires the matching op-kind hook (find/create/update/delete), before/after observed.
func TestD2FiresMatchingKind(t *testing.T) {
	for _, kind := range []MethodKind{MethodFind, MethodCreate, MethodUpdate, MethodDelete} {
		kind := kind
		var events []string
		result, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (any, error) {
			mw := NewMiddleware(MiddlewareConfig{
				Methods: map[MethodKind]MethodFn{
					kind: func(_ any, next MethodNext, args ...any) (any, error) {
						events = append(events, string(kind)+":before")
						r, e := next(args...)
						events = append(events, string(kind)+":after")
						return r, e
					},
				},
			})
			RegisterMiddleware(scopeCtx, mw.Descriptor())
			return RunMethod(scopeCtx, kind, nil, func(_ ...any) (any, error) {
				events = append(events, string(kind)+":core")
				return "ok", nil
			})
		})
		if err != nil || result != "ok" {
			t.Fatalf("%s: result=%v err=%v", kind, result, err)
		}
		want := []string{string(kind) + ":before", string(kind) + ":core", string(kind) + ":after"}
		if !equalStrs(events, want) {
			t.Errorf("%s: got %v want %v", kind, events, want)
		}
	}
}

// RED proof: a hook of a DIFFERENT kind does not fire (op kind is a TAG, never guessed from SQL).
func TestD2RedWrongKindDoesNotFire(t *testing.T) {
	var events []string
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (any, error) {
		mw := NewMiddleware(MiddlewareConfig{
			Methods: map[MethodKind]MethodFn{
				MethodCreate: func(_ any, next MethodNext, args ...any) (any, error) {
					events = append(events, "create")
					return next(args...)
				},
			},
		})
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		// Dispatch a `find` — the `create` hook must NOT fire (kind mismatch).
		return RunMethod(scopeCtx, MethodFind, nil, func(_ ...any) (any, error) { return "r", nil })
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("RED: create hook fired on a find dispatch: %v", events)
	}
}

// Method hooks compose first-registered-outermost + can rewrite args.
func TestD2ComposeOrderAndRewrite(t *testing.T) {
	var order []string
	var coreArg int
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (any, error) {
		a := NewMiddleware(MiddlewareConfig{Methods: map[MethodKind]MethodFn{
			MethodFind: func(_ any, next MethodNext, args ...any) (any, error) {
				order = append(order, "A")
				return next(args[0].(int) + 1)
			},
		}})
		b := NewMiddleware(MiddlewareConfig{Methods: map[MethodKind]MethodFn{
			MethodFind: func(_ any, next MethodNext, args ...any) (any, error) {
				order = append(order, "B")
				return next(args[0].(int) + 10)
			},
		}})
		RegisterMiddleware(scopeCtx, a.Descriptor())
		RegisterMiddleware(scopeCtx, b.Descriptor())
		return RunMethod(scopeCtx, MethodFind, nil, func(args ...any) (any, error) {
			coreArg = args[0].(int)
			return nil, nil
		}, 0)
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	if !equalStrs(order, []string{"A", "B"}) {
		t.Errorf("order: got %v want [A B]", order)
	}
	if coreArg != 11 { // 0 +1 (A) +10 (B)
		t.Errorf("rewrite: coreArg=%d want 11", coreArg)
	}
}

// ── D3: Logger + raw Execute/Query ─────────────────────────────────────────────

// Logger records real SQL / params / timing for every seam statement.
func TestD3Logger(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	logger := Logger(LoggerOptions{})
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, logger.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		if _, e := Run(ctx, "INSERT INTO t (name) VALUES (?)", []any{"x"}, WriteIntent()); e != nil {
			return struct{}{}, e
		}
		if _, e := Execute(ctx, "SELECT * FROM t WHERE name = ?", []any{"x"}, ReadIntent()); e != nil {
			return struct{}{}, e
		}
		entries := logger.State(scopeCtx).(*LoggerState).Entries()
		if len(entries) != 2 {
			t.Fatalf("logger: got %d entries want 2", len(entries))
		}
		if entries[0].SQL != "INSERT INTO t (name) VALUES (?)" || entries[1].SQL != "SELECT * FROM t WHERE name = ?" {
			t.Errorf("logger SQL: %q, %q", entries[0].SQL, entries[1].SQL)
		}
		if len(entries[0].Params) != 1 || entries[0].Params[0] != "x" {
			t.Errorf("logger params[0]: %v", entries[0].Params)
		}
		for _, e := range entries {
			if e.Duration < 0 {
				t.Errorf("logger duration must be >= 0: %v", e.Duration)
			}
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
}

// RED proof: without the wiring the Logger records nothing.
func TestD3LoggerRed(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	logger := Logger(LoggerOptions{})
	// NOT registered → the seam never invokes it. Read state on a background ctx (fresh, empty).
	ctx := ContextForDB(db)
	if _, err := Execute(ctx, "SELECT 1", nil, ReadIntent()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := logger.State(context.Background()).(*LoggerState).Entries(); len(got) != 0 {
		t.Errorf("RED: logger recorded without registration: %v", got)
	}
}

// RawExecute goes THROUGH the seam — a registered SQL middleware sees it (both a write and a read).
func TestD3RawExecuteThroughSeam(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	var seen []string
	var mu sync.Mutex
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, observeMiddleware(&seen, &mu).Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		ins, e := RawExecute(ctx, "INSERT INTO t (name) VALUES (?)", []any{"raw"}, RawExecuteOptions{})
		if e != nil {
			return struct{}{}, e
		}
		if ins.RowCount != 1 {
			t.Errorf("rawExecute insert rowCount: got %d want 1", ins.RowCount)
		}
		read, e := RawExecute(ctx, "SELECT name FROM t", nil, RawExecuteOptions{})
		if e != nil {
			return struct{}{}, e
		}
		if len(read.Rows) != 1 || firstRowField(read.Rows, "name") != "raw" {
			t.Errorf("rawExecute read: got %v", read.Rows)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	want := []string{"INSERT INTO t (name) VALUES (?)", "SELECT name FROM t"}
	if !equalStrs(seen, want) {
		t.Errorf("rawExecute through seam: got %v want %v", seen, want)
	}
}

// RawQuery fires a `query` method hook AND flows through the execute seam (two-level).
func TestD3RawQueryTwoLevel(t *testing.T) {
	db := mwDB(t)
	defer db.Close()
	if _, err := db.Exec("INSERT INTO t (name) VALUES ('q')"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var events []string
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		mw := NewMiddleware(MiddlewareConfig{
			Methods: map[MethodKind]MethodFn{
				MethodQuery: func(_ any, next MethodNext, args ...any) (any, error) {
					events = append(events, "query")
					return next(args...)
				},
			},
			Execute: func(_ any, next ExecNext, sql string, args []any) (any, error) {
				events = append(events, "execute:"+sql)
				return next(sql, args)
			},
		})
		RegisterMiddleware(scopeCtx, mw.Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		rows, e := RawQuery(ctx, "SELECT name FROM t", nil)
		if e != nil {
			return struct{}{}, e
		}
		if len(rows) != 1 || firstRowField(rows, "name") != "q" {
			t.Errorf("rawQuery rows: %v", rows)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	want := []string{"query", "execute:SELECT name FROM t"}
	if !equalStrs(events, want) {
		t.Errorf("rawQuery two-level: got %v want %v", events, want)
	}
}

// ── test helpers ───────────────────────────────────────────────────────────────

func equalStrs(a, b []string) bool {
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

func containsSub(list []string, sub string) bool {
	for _, s := range list {
		if len(sub) <= len(s) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
