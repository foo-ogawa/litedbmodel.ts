// Command lm_orm_native — the NATIVE-codegen ORM-bench cell (#141), Go twin of rust/orm_bench.
//
// A litedbmodel-CONSUMER: it opens sqlite, seeds the canonical fixture (generated_setup STATEMENTS +
// SEED, both from the orm-domain SSoT), BINDS the op-agnostic leaf transport to that connection, and
// drives the bc-GENERATED covered readers (behaviors.RunNativeRawStruct_<op>) directly. Every SQL node
// funnels through litedbmodel_runtime.ExecuteSQL; PluckKeys/GroupChildren shape relations over the
// shared grouping CORE. The consumer holds NO SQL, NO hand-written exec seam, NO node handlers.
//
// The RETURNING-chained TRANSACTIONS run THROUGH the runtime tx boundary (WithAmbientTransaction:
// BEGIN → the .map runner's 2 body statements via the leaf → COMMIT on ok / ROLLBACK on error) — the
// consumer's tx-boundary responsibility (NOT a bc feature, NOT emitted into the generated runner).
//
// Modes:
//
//	lm_orm_native            — run all 19 covered ops once; print per-op statement-count + row-count;
//	                           assert the N+1-free relation counts + the atomic tx statement counts.
//	lm_orm_native bench      — additionally time each op over reps iterations and print a flat CSV.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"
	behaviors "github.com/foo-ogawa/litedbmodel/go/lm_bench/lm_orm_native/gen"
	"github.com/foo-ogawa/litedbmodel/go/lm_bench/setup"

	_ "modernc.org/sqlite" // PURE-GO sqlite driver (registered as "sqlite")
)

// openSeeded opens a fresh in-memory sqlite and applies the ONE seed SSoT (.setup/sqlite.json, from
// orm-domain.ts) — schema then the canonical 110-user fixture. No hand-written schema/seed here.
func openSeeded() (*sql.DB, error) {
	doc, err := setup.Load("sqlite")
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one in-memory connection so schema + seed + ops share the same DB
	db.SetMaxIdleConns(1)
	for _, group := range [][]string{doc.Schema, doc.Delete, doc.Insert} {
		for _, s := range group {
			if _, err := db.Exec(s); err != nil {
				return nil, fmt.Errorf("setup %q: %w", s, err)
			}
		}
	}
	return db, nil
}

// userRows builds the 10-row batch record set for createMany/upsertMany as ONE opaque `rows` wire array
// (the json_each/JSON_TABLE batch param). `stable` reuses fixed emails (upsertMany — conflict-updates);
// else the email varies by iteration so a plain INSERT stays insertable under the UNIQUE(email)
// constraint. Mirrors the rust `user_rows` / python `_user_rows` batch shape.
func userRows(it int, stable bool) []behaviors.T4 {
	rows := make([]behaviors.T4, 10)
	for i := 0; i < 10; i++ {
		email := fmt.Sprintf("many%d_%d@bench.com", it, i)
		if stable {
			email = fmt.Sprintf("many%d@bench.com", i)
		}
		rows[i] = behaviors.T4{Email: email, Name: fmt.Sprintf("Many %d", i)}
	}
	return rows
}

// updateManyRows builds the id-keyed 10-row batch set for updateMany (updates the seeded users 1..10).
func updateManyRows() []behaviors.T5 {
	rows := make([]behaviors.T5, 10)
	for i := 1; i <= 10; i++ {
		rows[i-1] = behaviors.T5{Id: int64(i), Name: fmt.Sprintf("Many %d", i)}
	}
	return rows
}

// op runs ONE covered op for iteration it and returns its row count (writes report the terminal row
// count, which for a bare write is 0). Fixed inputs mirror the SCP ops SSoT; mutating ops vary their
// UNIQUE column by it so a timed loop does not collide. A RETURNING-chained tx op runs THROUGH the
// runtime tx boundary (WithAmbientTransaction over the bound db) so BEGIN/COMMIT bracket the leaf's 2
// body statements on the tx-owned connection; the generated runner emits no BEGIN/COMMIT.
func op(db *sql.DB, name string, it int) (int, error) {
	switch name {
	case "findAll":
		r, err := behaviors.RunNativeRawStruct_findAll(behaviors.In_findAll{})
		return len(r), err
	case "filterPaginateSort":
		r, err := behaviors.RunNativeRawStruct_filterPaginateSort(behaviors.In_filterPaginateSort{Published: 1})
		return len(r), err
	case "findFirst":
		r, err := behaviors.RunNativeRawStruct_findFirst(behaviors.In_findFirst{Name: "User%"})
		return len(r), err
	case "findUnique":
		r, err := behaviors.RunNativeRawStruct_findUnique(behaviors.In_findUnique{Email: "user500@example.com"})
		return len(r), err
	case "nestedFindAll":
		r, err := behaviors.RunNativeRawStruct_nestedFindAll(behaviors.In_nestedFindAll{})
		return len(r), err
	case "nestedFindFirst":
		r, err := behaviors.RunNativeRawStruct_nestedFindFirst(behaviors.In_nestedFindFirst{Name: "User%"})
		return len(r), err
	case "nestedFindUnique":
		r, err := behaviors.RunNativeRawStruct_nestedFindUnique(behaviors.In_nestedFindUnique{Email: "user1@example.com"})
		return len(r), err
	case "nestedRelations":
		r, err := behaviors.RunNativeRawStruct_nestedRelations(behaviors.In_nestedRelations{})
		return len(r), err
	case "compositeRelations":
		r, err := behaviors.RunNativeRawStruct_compositeRelations(behaviors.In_compositeRelations{})
		return len(r), err
	case "create":
		r, err := behaviors.RunNativeRawStruct_create(behaviors.In_create{Email: fmt.Sprintf("new%d@bench.com", it), Name: "New"})
		return len(r), err
	case "update":
		r, err := behaviors.RunNativeRawStruct_update(behaviors.In_update{Id: 1, Name: "Updated 1"})
		return len(r), err
	case "upsert":
		r, err := behaviors.RunNativeRawStruct_upsert(behaviors.In_upsert{Email: "user1@example.com", Name: "Upserted One"})
		return len(r), err
	case "createMany":
		// 10 fresh rows — email is UNIQUE NOT NULL, so vary per iteration to stay insertable.
		r, err := behaviors.RunNativeRawStruct_createMany(behaviors.In_createMany{Rows: userRows(it, false)})
		return len(r), err
	case "upsertMany":
		// 10 rows keyed on email (ON CONFLICT DO UPDATE) — idempotent across iterations.
		r, err := behaviors.RunNativeRawStruct_upsertMany(behaviors.In_upsertMany{Rows: userRows(it, true)})
		return len(r), err
	case "updateMany":
		// 10 rows keyed on id (1..10) — updates the seeded users.
		r, err := behaviors.RunNativeRawStruct_updateMany(behaviors.In_updateMany{Rows: updateManyRows()})
		return len(r), err
	case "nestedCreate":
		// Fresh user per iteration (email is UNIQUE) → INSERT user RETURNING id → INSERT post (author_id).
		err := rt.WithAmbientTransaction(db, func() error {
			_, e := behaviors.RunNativeRawStruct_nestedCreate(behaviors.In_nestedCreate{Email: fmt.Sprintf("nc%d@bench.com", it), Name: "NC", Title: "NC Post"})
			return e
		})
		return 0, err
	case "nestedUpsert":
		// Existing email (ON CONFLICT DO UPDATE) → INSERT post keyed on the upserted user's id.
		err := rt.WithAmbientTransaction(db, func() error {
			_, e := behaviors.RunNativeRawStruct_nestedUpsert(behaviors.In_nestedUpsert{Email: "user1@example.com", Name: "NUp", Title: "NUp Post"})
			return e
		})
		return 0, err
	case "nestedUpdate":
		// UPDATE seeded user 1 RETURNING id → UPDATE that user's posts.
		err := rt.WithAmbientTransaction(db, func() error {
			_, e := behaviors.RunNativeRawStruct_nestedUpdate(behaviors.In_nestedUpdate{Id: 1, Name: "NU", Title: "NU Post"})
			return e
		})
		return 0, err
	case "delete":
		// Create-then-delete: INSERT a fresh user RETURNING id → DELETE the exact created row by id.
		err := rt.WithAmbientTransaction(db, func() error {
			_, e := behaviors.RunNativeRawStruct_delete(behaviors.In_delete{Email: fmt.Sprintf("del%d@bench.com", it), Name: "Del"})
			return e
		})
		return 0, err
	default:
		return 0, fmt.Errorf("unknown op %q", name)
	}
}

var ops = []string{
	"findAll", "filterPaginateSort", "findFirst", "findUnique",
	"nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations",
	"create", "update", "upsert",
	"createMany", "upsertMany", "updateMany",
	"nestedCreate", "nestedUpsert", "nestedUpdate", "delete",
}

// expectedStatements is the per-op statement count observed at the runtime seam (every read / write /
// tx-control BEGIN/COMMIT funnels through Execute/Run → middleware; Pluck/Group are in-memory and do
// NOT count). Relations prove 1 parent + 1 batched child per level (N+1-free) regardless of parent
// fan-out; batch writes are ONE statement; a RETURNING-chained tx is BEGIN + 2 body + COMMIT = 4.
var expectedStatements = map[string]int{
	"findAll": 1, "filterPaginateSort": 1, "findFirst": 1, "findUnique": 1,
	"nestedFindAll": 2, "nestedFindFirst": 2, "nestedFindUnique": 2, "nestedRelations": 3, "compositeRelations": 3,
	"create": 1, "update": 1, "upsert": 1,
	"createMany": 1, "upsertMany": 1, "updateMany": 1,
	"nestedCreate": 4, "nestedUpsert": 4, "nestedUpdate": 4, "delete": 4,
}

// txOps names the RETURNING-chained transactions (their count is BEGIN + 2 body + COMMIT statements,
// not plain queries) — used only to label the safety print.
var txOps = map[string]bool{"nestedCreate": true, "nestedUpsert": true, "nestedUpdate": true, "delete": true}

func main() {
	doBench := len(os.Args) > 1 && os.Args[1] == "bench"

	db, err := openSeeded()
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: seed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	rt.BindLeafTransport(db, "sqlite")
	defer rt.UnbindLeafTransport()

	// The N+1-avoidance / atomic-tx safety proof: a seam middleware counts EVERY statement that funnels
	// through Execute/Run (reads + writes + tx-control BEGIN/COMMIT) — the SAME lens the python/php cells
	// use. The bound leaf ctx resolves the process-global registry, so a global registration is seen.
	var stmtCount int64
	counter := rt.NewMiddleware(rt.MiddlewareConfig{
		Execute: func(_ any, next rt.ExecNext, sqlText string, args []any) (any, error) {
			atomic.AddInt64(&stmtCount, 1)
			return next(sqlText, args)
		},
	})
	unregister := rt.RegisterMiddleware(context.Background(), counter.Descriptor())
	defer unregister()

	fmt.Println("op                    statements  rows")
	fail := 0
	for _, name := range ops {
		atomic.StoreInt64(&stmtCount, 0)
		rows, err := op(db, name, 0)
		if err != nil {
			fmt.Printf("%-20s  ERR: %v\n", name, err)
			fail++
			continue
		}
		q := int(atomic.LoadInt64(&stmtCount))
		mark := "ok"
		if exp, ok := expectedStatements[name]; ok && exp != q {
			mark = fmt.Sprintf("STATEMENT-COUNT MISMATCH (want %d)", exp)
			fail++
		}
		kind := ""
		if txOps[name] {
			kind = " (BEGIN + 2 body + COMMIT)"
		}
		fmt.Printf("%-20s  %-10d  %-5d %s%s\n", name, q, rows, mark, kind)
	}

	if doBench {
		reps := 200
		if len(os.Args) > 2 {
			if n, e := strconv.Atoi(os.Args[2]); e == nil {
				reps = n
			}
		}
		fmt.Println("\ncell,op,iter,us")
		for _, name := range ops {
			for it := 0; it < reps; it++ {
				t := time.Now()
				if _, err := op(db, name, it+1); err != nil {
					fmt.Fprintf(os.Stderr, "bench %s: %v\n", name, err)
					os.Exit(1)
				}
				fmt.Printf("native,%s,%d,%d\n", name, it, time.Since(t).Microseconds())
			}
		}
	}

	if fail > 0 {
		fmt.Fprintf(os.Stderr, "\nFAILED: %d op(s) errored or mismatched.\n", fail)
		os.Exit(1)
	}
	fmt.Println("\nOK: all 19 covered ops ran green; relation counts are N+1-free; tx counts are atomic (BEGIN+2 body+COMMIT).")
}
