// Main-bench GO cell — the CLI-generated go-typed-native modules + the generic seam + leaf handlers, and
// the SDK baseline. Modes:
//   orm_bench_go run <op> <db> <native|sdk>             → print the canonical result (node driver compares)
//   orm_bench_go bench <seed_db> <warmup> <iters> <csv> → latency CSV (native + sdk per op)
package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func errOf(m string) error { return errors.New(m) }
func batchEmails() []string {
	v := make([]string, 10)
	for i := range v {
		v[i] = "many" + strconv.Itoa(i) + "@bench.com"
	}
	return v
}
func batchNames() []string {
	v := make([]string, 10)
	for i := range v {
		v[i] = "Many " + strconv.Itoa(i)
	}
	return v
}
func upsertManyEmails() []string {
	v := []string{"user1@example.com", "user2@example.com"}
	for i := 0; i < 8; i++ {
		v = append(v, "many"+strconv.Itoa(i)+"@bench.com")
	}
	return v
}

func openDB(path string) *seamDB {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		panic(err)
	}
	return newSeamDB(db)
}

func runNative(op string, db *seamDB) string {
	switch op {
	case "findAll":
		return nativeFindAll(db)
	case "filterPaginateSort":
		return nativeFilterPaginateSort(db)
	case "findFirst":
		return nativeFindFirst(db)
	case "findUnique":
		return nativeFindUnique(db)
	case "create":
		return nativeCreate(db)
	case "update":
		return nativeUpdate(db)
	case "upsert":
		return nativeUpsert(db)
	case "createMany":
		return nativeCreateMany(db)
	case "upsertMany":
		return nativeUpsertMany(db)
	case "updateMany":
		return nativeUpdateMany(db)
	case "nestedFindAll":
		return nativeNestedFindAll(db)
	case "nestedFindFirst":
		return nativeNestedFindFirst(db)
	case "nestedFindUnique":
		return nativeNestedFindUnique(db)
	case "nestedRelations":
		return nativeNestedRelations(db)
	case "compositeRelations":
		return nativeCompositeRelations(db)
	case "delete":
		return nativeDelete(db)
	case "nestedCreate":
		return nativeNestedCreate(db)
	case "nestedUpdate":
		return nativeNestedUpdate(db)
	case "nestedUpsert":
		return nativeNestedUpsert(db)
	}
	panic("native: unknown op " + op)
}
func runSdk(op string, db *seamDB) string {
	switch op {
	case "findAll":
		return sdkFindAll(db)
	case "filterPaginateSort":
		return sdkFilterPaginateSort(db)
	case "findFirst":
		return sdkFindFirst(db)
	case "findUnique":
		return sdkFindUnique(db)
	case "create":
		return sdkCreate(db)
	case "update":
		return sdkUpdate(db)
	case "upsert":
		return sdkUpsert(db)
	case "createMany":
		return sdkCreateMany(db)
	case "upsertMany":
		return sdkUpsertMany(db)
	case "updateMany":
		return sdkUpdateMany(db)
	case "nestedFindAll":
		return sdkNestedFindAll(db)
	case "nestedFindFirst":
		return sdkNestedFindFirst(db)
	case "nestedFindUnique":
		return sdkNestedFindUnique(db)
	case "nestedRelations":
		return sdkNestedRelations(db)
	case "compositeRelations":
		return sdkCompositeRelations(db)
	case "delete":
		return sdkDelete(db)
	case "nestedCreate":
		return sdkNestedCreate(db)
	case "nestedUpdate":
		return sdkNestedUpdate(db)
	case "nestedUpsert":
		return sdkNestedUpsert(db)
	}
	panic("sdk: unknown op " + op)
}
func runCell(cell, op string, db *seamDB) string {
	if cell == "native" {
		return runNative(op, db)
	}
	return runSdk(op, db)
}

var readOps = map[string]bool{"findAll": true, "filterPaginateSort": true, "findFirst": true, "findUnique": true, "nestedFindAll": true, "nestedFindFirst": true, "nestedFindUnique": true, "nestedRelations": true, "compositeRelations": true}
var allOps = []string{"findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations", "create", "update", "upsert", "createMany", "upsertMany", "updateMany", "delete", "nestedCreate", "nestedUpdate", "nestedUpsert"}

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: orm_bench_go run <op> <db> <native|sdk> | bench <seed_db> <w> <n> <csv>")
		os.Exit(2)
	}
	switch args[1] {
	case "run":
		op, dbPath, cell := args[2], args[3], args[4]
		db := openDB(dbPath)
		fmt.Println(runCell(cell, op, db))
		db.db.Close()
	case "bench":
		seed := args[2]
		warmup, _ := strconv.Atoi(args[3])
		iters, _ := strconv.Atoi(args[4])
		f, _ := os.Create(args[5])
		w := bufio.NewWriter(f)
		fmt.Fprintln(w, "op,cell,us")
		for _, op := range allOps {
			mutating := !readOps[op]
			n := iters
			if mutating && n > 500 {
				n = 500
			}
			for _, cell := range []string{"native", "sdk"} {
				if !mutating {
					db := openDB(seed)
					for i := 0; i < warmup; i++ {
						_ = runCell(cell, op, db)
					}
					for i := 0; i < n; i++ {
						t0 := time.Now()
						r := runCell(cell, op, db)
						us := float64(time.Since(t0).Nanoseconds()) / 1000.0
						_ = r
						fmt.Fprintf(w, "%s,%s,%.3f\n", op, cell, us)
					}
					db.db.Close()
				} else {
					wu := warmup
					if wu > 50 {
						wu = 50
					}
					for i := 0; i < wu+n; i++ {
						tmp := seed + "." + op + "." + cell + ".work"
						copyFile(seed, tmp) // UNTIMED reset
						db := openDB(tmp)
						t0 := time.Now()
						r := runCell(cell, op, db)
						us := float64(time.Since(t0).Nanoseconds()) / 1000.0
						_ = r
						db.db.Close()
						os.Remove(tmp)
						if i >= wu {
							fmt.Fprintf(w, "%s,%s,%.3f\n", op, cell, us)
						}
					}
				}
			}
		}
		w.Flush()
		f.Close()
		fmt.Fprintf(os.Stderr, "go bench done: %d ops × (native, sdk)\n", len(allOps))
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(2)
	}
}

func copyFile(src, dst string) {
	in, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		panic(err)
	}
}
