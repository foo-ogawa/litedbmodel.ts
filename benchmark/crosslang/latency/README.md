# Latency bench — is the codegen genuinely native, and actually faster?

Three cells run the **same 4 ops**, over the **same seed sqlite**, for the **same iteration count**,
and self-measure the **whole hot path** (build input → bind + exec SQL + decode into the typed result):

All three run as **plain standalone processes** (a rust binary, a go binary, a `tsx` script) that
self-measure and write a flat CSV; a separate collector aggregates. No stdio-protocol coupling.

| cell | what it is | runtime-free? |
|---|---|---|
| **ts-IR** | litedbmodel's shipping boxed **interpreter** (`executeBundle`/`readBundle` → `executeReadGraph`): walks the compiled read-graph IR, boxes values, assembles each node's SQL, executes, materializes rows. The path native codegen replaces. Run standalone via `tsx` against the built `dist/scp` bundle. | n/a (interpreter) |
| **rust-native** | the `rust-typed-native` generated module + the generic `exec` seam (`rust/e1_native_proof`): baked-SQL native literals, direct bind/exec/decode. | yes — `rustc --emit metadata` with **no** `--extern behavior_contracts`; purity greps 0 |
| **go-native** | the `go-typed-native` generated module + a generic `exec` seam (`go-cell/`, the go twin of the rust seam), driven by `mattn/go-sqlite3` (cgo, the same C sqlite engine as rust/ts). | yes — `go list -deps` shows **no** behavior-contracts runtime |

**Scaled relation sweep.** Beyond the fixed ops, the sweep runs the SAME batched relation at growing
child counts (`relScale` 10 / 100 / 1000 / 10000, one indexed author per scale in `rel.db`) so per-row
decode/clone/alloc cost — invisible at the 4-child scale — becomes measurable. This exposed a real
**O(N)** inefficiency in the rust seam's `query_batched_relation`: the alignment step cloned every
parent's child Vec (`groups.get(k).cloned()`). Fixed by **moving** the Vec out (`groups.remove(k)`, valid
since a relation's parent keys are distinct) — zero child clones — plus capacity reservations and dropping
the `Child: Clone` bound. `query_batch_write` now clones the records-JSON `n_params-1` times (0 for
createMany's single `?`). The go seam was already clone-free (slice-header assignment); it got capacity
reservations for parity. Measured effect: ~2% at 10 children → ~13% at 10000. Generated modules untouched.

**Prepared-statement reuse (applied to all three cells).** The baked SQL is static, so a real native
runtime prepares each op's statement once and reuses it. This is applied **symmetrically**: rust uses
`prepare_cached`, go a per-SQL `*sql.Stmt` cache, and the ts-IR cell wraps the driver in a
prepared-statement cache (litedbmodel's default runtime re-prepares each call — the wrapper isolates the
shared SQL-parse cost so the comparison measures codegen-vs-interpretation, not re-parsing). The rust
`QUERY_COUNT` proof-atomic is feature-gated OFF in the bench build (`--no-default-features`) so it never
runs in the timed hot path. The **generated modules are never hand-edited** — only the seams/harness.

The three cells run **byte-identical SQL** (`gen.test.ts` asserts the go module, the rust module, AND the
committed proof-crate rust module all bake the same SQL from the same bundle) and produce **equivalent
results** — only the execution surface differs.

## Fairness guardrails

- Identical workload per cell (same ops, inputs, seed, iteration count) and the **same C sqlite engine**
  in every cell (better-sqlite3 / rusqlite-bundled / mattn-go-sqlite3) — so the comparison isolates the
  execution surface, not the DB engine.
- Warmup then N timed iterations; **raw** per-iteration samples written to `.results/<cell>.csv`
  (`op,us`); the collector aggregates p50/p99/ops-sec identically for every cell (measurement vs
  aggregation stay separate). Writes use a **unique** input per iteration on a fresh copy of the seed.
- The native cells are the **real** runtime-free generated code — no hand-tuned fast path, no precomputed
  results. The ts-IR cell is a genuine interpreter run (no artificial slowdown).
- Numbers are reported **verbatim**. Where native is not faster, the number says so.

## Run

```bash
benchmark/crosslang/latency/run.sh [warmup] [iters]     # defaults: 1000 10000
```

Builds all three cells, runs them, and writes the table to `LATENCY.md` (+ `.results/summary.csv`).

## Note on the interpreter baseline

bc's *generic* `runBehavior` interpreter is **not wired to SQL** anywhere in litedbmodel (it is never
called in `src/`), so the ts-IR cell uses litedbmodel's own shipping boxed IR-walking runtime
(`executeReadGraph`) — the honest, real interpreter that native codegen replaces.
