# litedbmodel/go

The Go leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a `database/sql` driver,
semantics-identical to the TS reference (`src/scp`).

**Status: WS7c (#32) — implemented.** The runtime renders every §8 CompiledOperation byte-for-byte
with the TS golden (all 3 dialects), executes read/exec bundles + write-time-relations transaction
bundles against real in-proc SQLite, and passes the full frozen conformance corpus (49/49 vectors).

## What it does

- **`litedbmodel_runtime/`** — the thin runtime:
  - `render.go` — the normative fragment-tree render (SKIP existence, IN-list `(?, ?, …)`
    expansion, empty-WHERE degeneration, pre/post-WHERE param order), ported EXACTLY from
    `src/scp/render.ts`.
  - `dialect.go` — the closed dialect strategy (SSoT): `?`→`$N` PG final one-pass, `orderByNulls`
    (native NULLS for PG/SQLite, `IS NULL` emulation for MySQL), INSERT-conflict / guard-insert
    renderings — ported from `src/scp/dialect.ts`.
  - `runtime.go` — `ExecuteBundle`: feeds the surrogate component to bc `RunBehavior` (plan / map /
    wire / output orchestration) with SQL handlers that render + execute; schema/`optionalHeads`
    input normalization (present-as-null, SSoT); error mapping.
  - `write.go` — `ExecuteTransactionBundle`: the gate-first transaction envelope (BEGIN → gates →
    body → derive/edges/emits → COMMIT, short-circuit + ROLLBACK on a failing gate), ported from
    `src/scp/write-runtime.ts`.
  - `relation.go` — the staged-batch relation ops (belongsTo/hasMany, IN-list dedup, no N+1),
    ported from `src/scp/relation.ts`.
  - `errors.go` / `sqldb.go` / `value.go` — SQLite→SqlFailure mapping, the `database/sql` seam +
    value marshalling, and the bigint-safe conformance value codec.
- **`vectors_runner/`** — the conformance runner the cross-language orchestrator launches for the
  Go leg: loads `conformance/vectors/*.json`, runs each vector through the Go runtime, asserts the
  reproduced SQL text (all dialects) + execution results (in-proc SQLite) == expected, and emits
  the machine JSON summary. Real assertions; no hardcoded pass/skip.

## bc runtime-core is CONSUMED, not reimplemented

All generic Expression-IR evaluation (`EvaluateExpression`) AND the plan/map/wire/output
orchestration (`RunBehavior`) are delegated to the shared common core
`github.com/foo-ogawa/behavior-contracts/go`, **consumed via the published VCS tag `go/v0.2.0`**
(mirroring graphddb). No local `replace` onto a sibling checkout — the `check-no-local-deps` gate
forbids `../`-escaping deps.

behavior-contracts is a **private** repo: `go build` needs `GOPRIVATE=github.com/foo-ogawa/*` plus
authenticated git access (wired in `.github/workflows/conformance.yml`, mirroring graphddb's go
leg). The in-proc SQLite is the pure-Go `modernc.org/sqlite` driver (no cgo).

## Run the conformance runner locally

```bash
export GOPRIVATE=github.com/foo-ogawa/*
LITEDBMODEL_VECTORS=$PWD/../conformance/vectors go run ./vectors_runner
```

Live PG/MySQL (docker) is DEFERRED to a coordinated cross-language pass; the `database/sql`
SQL-handler seam is structured so a pgx/mysql driver plugs in there later.

## Layout

```
go/
  go.mod                          # module github.com/foo-ogawa/litedbmodel/go (Go = VCS tag release)
  litedbmodel_runtime/            # WS7c: the §8 bundle interpreter (render/dialect/runtime/write/relation)
  vectors_runner/main.go          # conformance runner entry (WS7c body)
```

## Versioning

Go publishes by VCS tag, not a manifest version field. `scripts/sync-versions.mjs` mirrors
`package.json`'s version into the `Version` constant in `runtime.go`; the release tag is
`go/v<version>`.
