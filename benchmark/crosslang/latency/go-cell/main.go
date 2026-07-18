// The go-native BENCH CELL. Same 4 ops as the rust + ts-IR cells, same seed, same iteration count.
// Times the WHOLE hot path (build input → RunNativeRawStruct_<Comp> = bind + exec + decode into the
// typed struct) and writes RAW per-iteration samples (µs) as flat CSV `op,us`. Reads run on a read-only
// seed; writes run on a fresh mutable copy with a UNIQUE input per iteration.
//
// Usage: go-cell <read_db> <write_db> <warmup> <iters> <out_csv>
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"litedbmodel_latency_bench/behaviors/createmany"
	"litedbmodel_latency_bench/behaviors/createuser"
	"litedbmodel_latency_bench/behaviors/findunique"
	"litedbmodel_latency_bench/behaviors/relsingle"
)

func openDB(path string) *sql.DB {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1) // sqlite: one writer; keeps the whole bench on one connection
	if err := db.Ping(); err != nil {
		panic(err)
	}
	return db
}

var sink int64 // prevents the compiler from eliminating the "unused" typed result

// relScale — (csv op id, author, iters). Matches behaviors.ts REL_SCALES; rel.db defines each author's
// child count (10 / 100 / 1000 / 10000).
type relScale struct {
	op     string
	author int64
	iters  int
}

var relScales = []relScale{{"rel10", 101, 5000}, {"rel100", 102, 5000}, {"rel1000", 103, 2000}, {"rel10000", 104, 300}}

func main() {
	args := os.Args
	if len(args) < 7 {
		panic("usage: go-cell <read_db> <write_db> <rel_db> <warmup> <iters> <out_csv>")
	}
	readDB, writeDB, relDB := args[1], args[2], args[3]
	warmup, _ := strconv.Atoi(args[4])
	iters, _ := strconv.Atoi(args[5])
	outCsv := args[6]

	f, err := os.Create(outCsv)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "op,us")

	// ── findunique (point read) — read-only ──
	{
		db := openDB(readDB)
		h := findUniqueH{db: newSeamDB(db)}
		for i := 0; i < warmup+iters; i++ {
			email := fmt.Sprintf("user%d@example.com", (i%100)+1)
			t0 := time.Now()
			out, err := findunique.RunNativeRawStruct_FindUnique(h, findunique.In_FindUnique{Email: email})
			us := float64(time.Since(t0).Nanoseconds()) / 1000.0
			if err != nil {
				panic(err)
			}
			sink += int64(len(out))
			if i >= warmup {
				fmt.Fprintf(w, "findunique,%.3f\n", us)
			}
		}
		db.Close()
	}
	// ── relsingle (batched relation) — read-only ──
	{
		db := openDB(readDB)
		h := relSingleH{db: newSeamDB(db)}
		for i := 0; i < warmup+iters; i++ {
			t0 := time.Now()
			out, err := relsingle.RunNativeRawStruct_ByAuthor(h, relsingle.In_ByAuthor{Author_id: 7})
			us := float64(time.Since(t0).Nanoseconds()) / 1000.0
			if err != nil {
				panic(err)
			}
			sink += int64(len(out.Rows) + len(out.Comments))
			if i >= warmup {
				fmt.Fprintf(w, "relsingle,%.3f\n", us)
			}
		}
		db.Close()
	}
	// ── createuser (single write, RETURNING) — mutable, UNIQUE email per iteration ──
	{
		db := openDB(writeDB)
		h := createUserH{db: newSeamDB(db)}
		for i := 0; i < warmup+iters; i++ {
			email := fmt.Sprintf("cu_%d_%d@example.com", i, time.Now().UnixNano())
			t0 := time.Now()
			out, err := createuser.RunNativeRawStruct_CreateUser(h, createuser.In_CreateUser{Email: email, Name: "Bench"})
			us := float64(time.Since(t0).Nanoseconds()) / 1000.0
			if err != nil {
				panic(err)
			}
			sink += int64(len(out))
			if i >= warmup {
				fmt.Fprintf(w, "createuser,%.3f\n", us)
			}
		}
		db.Close()
	}
	// ── createmany (batch write: ONE json_each INSERT for 10 records) — mutable, UNIQUE rows per iter ──
	{
		db := openDB(writeDB)
		h := createManyH{db: newSeamDB(db)}
		for i := 0; i < warmup+iters; i++ {
			ts := time.Now().UnixNano()
			emails := make([]string, 10)
			names := make([]string, 10)
			for k := 0; k < 10; k++ {
				emails[k] = fmt.Sprintf("cm_%d_%d_%d@example.com", i, k, ts)
				names[k] = fmt.Sprintf("BM%d_%d", i, k)
			}
			t0 := time.Now()
			out, err := createmany.RunNativeRawStruct_CreateMany(h, createmany.In_CreateMany{Emails: emails, Names: names})
			us := float64(time.Since(t0).Nanoseconds()) / 1000.0
			if err != nil {
				panic(err)
			}
			sink += int64(len(out))
			if i >= warmup {
				fmt.Fprintf(w, "createmany,%.3f\n", us)
			}
		}
		db.Close()
	}
	// ── SCALED relation sweep — the SAME relsingle at growing child counts (10 → 10000) ──
	{
		db := openDB(relDB)
		h := relSingleH{db: newSeamDB(db)}
		for _, sc := range relScales {
			wu := warmup
			if sc.iters < wu {
				wu = sc.iters
			}
			for i := 0; i < wu; i++ {
				_, _ = relsingle.RunNativeRawStruct_ByAuthor(h, relsingle.In_ByAuthor{Author_id: sc.author})
			}
			for i := 0; i < sc.iters; i++ {
				t0 := time.Now()
				out, err := relsingle.RunNativeRawStruct_ByAuthor(h, relsingle.In_ByAuthor{Author_id: sc.author})
				us := float64(time.Since(t0).Nanoseconds()) / 1000.0
				if err != nil {
					panic(err)
				}
				sink += int64(len(out.Rows) + len(out.Comments))
				fmt.Fprintf(w, "%s,%.3f\n", sc.op, us)
			}
		}
		db.Close()
	}
	fmt.Fprintf(os.Stderr, "go-native bench done: 4 base ops + %d rel scales → %s (sink=%d)\n", len(relScales), outCsv, sink)
}
