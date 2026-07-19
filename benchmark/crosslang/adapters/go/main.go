// Main-bench GO cell — the CLI-generated go-typed-native modules (bc 0.8.10 WireValue) + the consumer
// wire seam + machine-generated leaf companions, and the SDK baseline. ONE dialect per binary, selected
// by a build tag (`-tags sqlite|postgres|mysql`) — the go twin of the rust cell's cargo feature. The
// tagged cells_<dialect>.go provides dialectName() (drives the driver) + nativeCell() (the dispatch).
//
// Modes:
//   orm_bench_go run <op> <target> <native|sdk>          → print the canonical result (node driver compares)
//   orm_bench_go bench <seed_db> <warmup> <iters> <csv>  → latency CSV (sqlite only)
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"orm_bench_go/wire"
)

func errOf(m string) error { return errors.New(m) }

func openDB(target string) *wire.DB { return wire.Open(dialectName(), target) }

func runCell(cell, op string, db *wire.DB) string {
	if cell == "native" {
		return nativeCell(op, db)
	}
	return sdkCell(op, db)
}

var readOps = map[string]bool{"findAll": true, "filterPaginateSort": true, "findFirst": true, "findUnique": true, "nestedFindAll": true, "nestedFindFirst": true, "nestedFindUnique": true, "nestedRelations": true, "compositeRelations": true}
var allOps = []string{"findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations", "create", "update", "upsert", "createMany", "upsertMany", "updateMany", "delete", "nestedCreate", "nestedUpdate", "nestedUpsert"}

func main() {
	args := os.Args
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: orm_bench_go run <op> <target> <native|sdk> | bench <seed_db> <w> <n> <csv>")
		os.Exit(2)
	}
	switch args[1] {
	case "run":
		op, target, cell := args[2], args[3], args[4]
		db := openDB(target)
		fmt.Println(runCell(cell, op, db))
		db.Close()
	case "bench":
		benchSqlite(args)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(2)
	}
}

// Latency CSV (sqlite in-proc only): reads time on the seed; mutating ops reset (copy seed) per iter.
func benchSqlite(args []string) {
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
				db.Close()
			} else {
				wu := warmup
				if wu > 50 {
					wu = 50
				}
				for i := 0; i < wu+n; i++ {
					tmp := seed + "." + op + "." + cell + ".work"
					copyFile(seed, tmp)
					db := openDB(tmp)
					t0 := time.Now()
					r := runCell(cell, op, db)
					us := float64(time.Since(t0).Nanoseconds()) / 1000.0
					_ = r
					db.Close()
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
