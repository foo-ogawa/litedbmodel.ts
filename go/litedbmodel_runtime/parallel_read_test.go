// #40 — proof that independent sibling-relation read nodes are dispatched CONCURRENTLY (Go).
//
// Unlike Rust (whose published bc crate has no parallel RunBehavior), the Go bc RunBehavior drives
// RunPlan, and RunPlan ITSELF dispatches the INDEPENDENT members of a plan stage on a bounded
// goroutine pool when plan.Concurrency > 1 (bc#23). The litedbmodel Go read handler (ExecCtx) reads
// only immutable graph/dialect state and queries through the SQLDB, and *sql.DB is the goroutine-
// safe built-in connection pool — so with a multi-sibling read graph carrying Concurrency: 16, the
// sibling relation queries fan out across pooled connections with NO litedbmodel-side change.
//
// This test proves it with a LATENCY-INJECTING SQLDB that sleeps before delegating to a real
// in-memory sqlite *sql.DB, counting simultaneous in-flight queries: N siblings @ D each overlap
// (wall ≈ D, not N·D), peak in-flight reaches N, and the Φ-merged result is deterministic.

package litedbmodel_runtime

import (
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	_ "modernc.org/sqlite"
)

// latencyDB wraps a real *sql.DB, sleeping `latency` before each Query and tracking the peak count
// of simultaneously in-flight queries. It satisfies SQLDB.
type latencyDB struct {
	inner    *sql.DB
	latency  time.Duration
	inFlight int64
	peak     int64
	calls    int64
}

func (d *latencyDB) Query(query string, args ...any) (*sql.Rows, error) {
	atomic.AddInt64(&d.calls, 1)
	n := atomic.AddInt64(&d.inFlight, 1)
	for {
		p := atomic.LoadInt64(&d.peak)
		if n <= p || atomic.CompareAndSwapInt64(&d.peak, p, n) {
			break
		}
	}
	time.Sleep(d.latency)
	atomic.AddInt64(&d.inFlight, -1)
	return d.inner.Query(query, args...)
}

func (d *latencyDB) Exec(query string, args ...any) (sql.Result, error) {
	return d.inner.Exec(query, args...)
}

// siblingGraphJSON builds a readGraph of n independent sibling nodes in ONE plan stage, each a
// trivial `SELECT <i> AS v` static statement, with the default plan concurrency (16).
func siblingGraphJSON(n int) string {
	var body, output, stmts []string
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("rel%d", i)
		body = append(body, fmt.Sprintf(
			`{"id":"%s","component":"__makeSqlNode","ports":{"__scope":{"obj":{}}}}`, id))
		output = append(output, fmt.Sprintf(`"%s":{"ref":["%s"]}`, id, id))
		stmts = append(stmts, fmt.Sprintf(`"%s":[{"sql":"SELECT %d AS v","params":[]}]`, id, i))
	}
	group := make([]string, n)
	for i := range group {
		group[i] = fmt.Sprintf("%d", i)
	}
	return fmt.Sprintf(`{
	  "dialect":"sqlite",
	  "name":"Siblings",
	  "ir":{"irVersion":1,"exprVersion":2,"components":[{
	    "name":"Siblings",
	    "inputPorts":{},
	    "body":[%s],
	    "output":{"obj":{%s}},
	    "plan":{"concurrency":16,"groups":[[%s]]}
	  }]},
	  "statementsById":{%s},
	  "optionalHeads":[]
	}`, strings.Join(body, ","), strings.Join(output, ","), strings.Join(group, ","), strings.Join(stmts, ","))
}

func mustReadGraph(t *testing.T, jsonStr string) *ReadGraph {
	t.Helper()
	n, err := bc.ParseJSONOrdered([]byte(jsonStr))
	if err != nil {
		t.Fatalf("parse graph: %v", err)
	}
	obj, ok := n.(*bc.JObj)
	if !ok {
		t.Fatalf("graph is not an object")
	}
	g, err := ReadGraphFromJObj(obj)
	if err != nil {
		t.Fatalf("ReadGraphFromJObj: %v", err)
	}
	return g
}

func TestSiblingRelationsDispatchConcurrently(t *testing.T) {
	const N = 8
	const latency = 60 * time.Millisecond

	inner, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer inner.Close()
	// A pool big enough that all N sibling queries can hold a connection at once.
	inner.SetMaxOpenConns(N)
	db := &latencyDB{inner: inner, latency: latency}

	g := mustReadGraph(t, siblingGraphJSON(N))

	start := time.Now()
	out, err := ExecuteReadGraph(g, bc.NewObj(), db)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteReadGraph: %v", err)
	}

	// 1. Overlap: N=8 × 60ms serial = 480ms; concurrent ≈ 60ms. Well under half proves overlap.
	if elapsed >= latency*N/2 {
		t.Fatalf("expected concurrent (<%v), took %v (serial would be %v)", latency*N/2, elapsed, latency*N)
	}
	// 2. All N ran and all N were simultaneously in flight.
	if c := atomic.LoadInt64(&db.calls); c != N {
		t.Fatalf("expected %d calls, got %d", N, c)
	}
	if peak := atomic.LoadInt64(&db.peak); peak != N {
		t.Fatalf("expected all %d siblings in flight at once, peak=%d", N, peak)
	}

	// 3. Determinism: the Φ-merged result carries each sibling's rows keyed by node id, in order.
	obj, ok := out.(*bc.Obj)
	if !ok {
		t.Fatalf("output is not an object: %T", out)
	}
	if len(obj.Keys) != N {
		t.Fatalf("expected %d sibling keys, got %d", N, len(obj.Keys))
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("rel%d", i)
		if obj.Keys[i] != id {
			t.Fatalf("sibling out of order at %d: got %q want %q", i, obj.Keys[i], id)
		}
	}

	t.Logf("GO PARALLEL PROOF: %d sibling queries @ %v each → wall %v (serial would be %v), peak in-flight = %d",
		N, latency, elapsed, latency*N, atomic.LoadInt64(&db.peak))
}

// A counterpart sanity check that a real serial dispatch (concurrency 1) does NOT overlap, so the
// test above is measuring the parallel path, not an artifact.
func TestConcurrencyOneStaysSerial(t *testing.T) {
	const N = 4
	const latency = 30 * time.Millisecond
	inner, _ := sql.Open("sqlite", ":memory:")
	defer inner.Close()
	db := &latencyDB{inner: inner, latency: latency}

	// Same graph but concurrency 1.
	j := strings.Replace(siblingGraphJSON(N), `"concurrency":16`, `"concurrency":1`, 1)
	g := mustReadGraph(t, j)
	if _, err := ExecuteReadGraph(g, bc.NewObj(), db); err != nil {
		t.Fatalf("ExecuteReadGraph: %v", err)
	}
	if peak := atomic.LoadInt64(&db.peak); peak != 1 {
		t.Fatalf("concurrency=1 must be serial, peak=%d", peak)
	}
}
