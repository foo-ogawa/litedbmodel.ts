# cross-lang bench — REBUILD IN PROGRESS

This directory is mid-rebuild. The previous measurement pipeline was **removed** because it
bypassed BC entirely and measured the wrong thing. Tracking: **epic #107** (sub-issues #108–#114).

## Why the old pipeline was removed

The old bench did not measure litedbmodel's core value (static native codegen). It:

1. Never invoked litedbmodel's real codegen (`generateCodegenArtifact`). Instead it built a
   hand-rolled `generated/orm-plan.json` (660 KB of baked SQL+params) and had every language
   **parse it at runtime** and walk it with a generic executor (`run_plan`). That is a forbidden
   custom IR interpreted at runtime — not codegen.
2. Shipped a "purity gate" (`purity-gate.sh`) that **exempted the measured cells** from the
   serde/json check, so the impure path stayed green.
3. Labelled a generic plan-walker column as litedbmodel's "runtime", conflating it with the real
   `executeBundle`/`runBehavior` path.
4. Left even the SDK "baseline" fed by the same `orm-plan.json` (no independent hand-written cell).
5. Claimed go/rust "typed-native" while shipping no Go cell at all.

## Target architecture (mirrors graphddb; see litedbmodel native-codegen model)

Behaviors declared **once** on the SCP surface → **BC compile → BC-standard IR → BC codegen per
language** emits compiled source with every static datum (SQL, params, keys) baked as a **native
literal**. read and write are the **same** flow (make SQL → execute via BC) — never split. The only
runtime is a thin, op-agnostic `exec(sql, params)` seam per driver (no IR walk, no JSON, no dispatch).

Hard rules (deviation = re-rejection):

- BC-standard IR only. No hand-rolled plan artifact.
- No IR at runtime in rust/go/ts — baked native literals.
- serde_json / encoding-json / JSON out of the codegen cells' **dependency graph** (verified at the
  dependency level with a compiler-backed gate).
- No half-implementations / faked codegen / generic-runtime fallback. A real bc capability gap is
  escalated to bc, never papered over.
- py/php native codegen is a known bc capability gap → they run **sdk + ir (interpreter)** cells,
  honestly labelled — never presented as codegen.

## What was kept (reusable, decoupled)

| File | Role |
|---|---|
| `contract.ts` | Axis SSoT — the 19 ORM ops (`ORM_OPS`, ids/labels/write-set) × 3 dialects. Now self-contained (no plan import). |
| `orm-domain.ts` | Domain DDL + deterministic seed (matches `test/parity/v1-sql-golden`). Every cell creates/seeds these tables. |
| `domain.ts` | Domain model, fixed inputs, and the **hand-written raw-SQL baseline** (`SQL_BASELINE`) — the SDK cells' source. |
| `metrics.ts` | Pure stats (p50, relative overhead). |
| `report.ts` / `collect.ts` | CSV collection + report rendering skeleton. Re-pointed to native/sdk/ir surfaces in P6 (labels currently reflect the old model). |
| `coverage-roundtrip.ts` | Typed de-box round-trip verifier over the **real** read path (`executeBundle`) on 3 live DBs. Unrelated to the old fraud. |

## What was removed

`generated/orm-plan.json`, `orm-plan.ts`, `gen-orm-plan.ts`, `generate.ts`, `orm-exec-ts.ts`,
`orm-smoke.ts`, `selfcheck.ts`, `run.ts`, `run-bench.sh`, `purity-gate.sh`, and all
`adapters/{rust,python,php,ts}` executor/runner cells (the generic plan-walkers).

## Rebuild phases

- P0 (this commit) — demolition + keep reusable + establish independent SDK baseline cells — #108
- P1 — declare the 19 ops as SCP behaviors (SSoT, replaces orm-plan) — #109
- P2 — unified native codegen: bake SQL as native literals, retire the JSON companion, drop the
  write-throw, wire tx — #110
- P3 — rust/go/ts native codegen cells on the graphddb template (codegen-build + drift gate) — #111
- P4 — py/php as sdk + ir (interpreter) cells — #112
- P5 — dependency-level purity gate (compiler-backed, CI-required) — #113
- P6 — report honesty + 3-real-DB re-measure + verify — #114
