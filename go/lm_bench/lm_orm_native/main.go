// Command lm_orm_native — the NATIVE-codegen ORM-bench cell (#141), Go twin of rust/orm_bench.
//
// A litedbmodel-CONSUMER: it opens sqlite, seeds the canonical fixture (generated_setup STATEMENTS +
// SEED, both from the orm-domain SSoT), BINDS the op-agnostic leaf transport to that connection, and
// drives the bc-GENERATED covered readers (behaviors.RunNativeRawStruct_<op>) directly. Every SQL node
// funnels through litedbmodel_runtime.ExecuteSQL; PluckKeys/GroupChildren shape relations over the
// shared grouping CORE. The consumer holds NO SQL, NO hand-written exec seam, NO node handlers.
//
// Modes:
//
//	lm_orm_native            — run all 12 covered ops once; print per-op query-count + row-count; assert
//	                           the N+1-free relation query counts (nestedFindAll=2, nestedRelations=3, …).
//	lm_orm_native bench      — additionally time each op over reps iterations and print a flat CSV.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"
	"github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime/wire"
	behaviors "github.com/foo-ogawa/litedbmodel/go/lm_bench/lm_orm_native/gen"

	_ "modernc.org/sqlite" // PURE-GO sqlite driver (registered as "sqlite")
)

// openSeeded opens a fresh in-memory sqlite, applies the generated schema + seed, and returns it.
func openSeeded() (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one in-memory connection so schema + seed + ops share the same DB
	db.SetMaxIdleConns(1)
	for _, s := range behaviors.STATEMENTS {
		if _, err := db.Exec(s); err != nil {
			return nil, fmt.Errorf("ddl %q: %w", s, err)
		}
	}
	for _, s := range behaviors.SEED {
		if _, err := db.Exec(s.SQL, s.Params...); err != nil {
			return nil, fmt.Errorf("seed %q: %w", s.SQL, err)
		}
	}
	return db, nil
}

// op runs ONE covered op for iteration it and returns its row count (writes report the terminal row
// count, which for a bare write is 0). Fixed inputs mirror the SCP ops SSoT; mutating ops vary their
// UNIQUE column by it so a timed loop does not collide.
func op(name string, it int) (int, error) {
	switch name {
	case "findAll":
		r, err := behaviors.RunNativeRawStruct_findAll(behaviors.In_findAll{})
		return len(r), err
	case "filterPaginateSort":
		r, err := behaviors.RunNativeRawStruct_filterPaginateSort(behaviors.In_filterPaginateSort{Published: wire.WireInt(1)})
		return len(r), err
	case "findFirst":
		r, err := behaviors.RunNativeRawStruct_findFirst(behaviors.In_findFirst{Name: wire.WireStr("User%")})
		return len(r), err
	case "findUnique":
		r, err := behaviors.RunNativeRawStruct_findUnique(behaviors.In_findUnique{Email: wire.WireStr("user500@example.com")})
		return len(r), err
	case "nestedFindAll":
		r, err := behaviors.RunNativeRawStruct_nestedFindAll(behaviors.In_nestedFindAll{})
		return len(r), err
	case "nestedFindFirst":
		r, err := behaviors.RunNativeRawStruct_nestedFindFirst(behaviors.In_nestedFindFirst{Name: wire.WireStr("User%")})
		return len(r), err
	case "nestedFindUnique":
		r, err := behaviors.RunNativeRawStruct_nestedFindUnique(behaviors.In_nestedFindUnique{Email: wire.WireStr("user1@example.com")})
		return len(r), err
	case "nestedRelations":
		r, err := behaviors.RunNativeRawStruct_nestedRelations(behaviors.In_nestedRelations{})
		return len(r), err
	case "compositeRelations":
		r, err := behaviors.RunNativeRawStruct_compositeRelations(behaviors.In_compositeRelations{})
		return len(r), err
	case "create":
		r, err := behaviors.RunNativeRawStruct_create(behaviors.In_create{Email: wire.WireStr(fmt.Sprintf("new%d@bench.com", it)), Name: wire.WireStr("New")})
		return len(r), err
	case "update":
		r, err := behaviors.RunNativeRawStruct_update(behaviors.In_update{Id: wire.WireInt(100), Name: wire.WireStr("Updated 100")})
		return len(r), err
	case "upsert":
		r, err := behaviors.RunNativeRawStruct_upsert(behaviors.In_upsert{Email: wire.WireStr("user1@example.com"), Name: wire.WireStr("Upserted One")})
		return len(r), err
	default:
		return 0, fmt.Errorf("unknown op %q", name)
	}
}

var ops = []string{
	"findAll", "filterPaginateSort", "findFirst", "findUnique",
	"nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations",
	"create", "update", "upsert",
}

// expectedQueries is the N+1-free SQL-node count per op (# executeSQL leaves; pluck/group are
// in-memory). Relations prove 1 parent + 1 batched child per level regardless of parent fan-out.
var expectedQueries = map[string]int{
	"findAll": 1, "filterPaginateSort": 1, "findFirst": 1, "findUnique": 1,
	"nestedFindAll": 2, "nestedFindFirst": 2, "nestedFindUnique": 2, "nestedRelations": 3, "compositeRelations": 3,
	"create": 1, "update": 1, "upsert": 1,
}

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

	fmt.Println("op                    queries  rows")
	fail := 0
	for _, name := range ops {
		rt.ResetLeafQueryCount()
		rows, err := op(name, 0)
		if err != nil {
			fmt.Printf("%-20s  ERR: %v\n", name, err)
			fail++
			continue
		}
		q := rt.LeafQueryCount()
		mark := "ok"
		if exp, ok := expectedQueries[name]; ok && exp != q {
			mark = fmt.Sprintf("QUERY-COUNT MISMATCH (want %d)", exp)
			fail++
		}
		fmt.Printf("%-20s  %-7d  %-5d %s\n", name, q, rows, mark)
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
				if _, err := op(name, it+1); err != nil {
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
	fmt.Println("\nOK: all 12 covered ops ran green; relation query counts are N+1-free.")
}
