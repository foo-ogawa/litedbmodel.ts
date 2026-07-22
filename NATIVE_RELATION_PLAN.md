# litedbmodel #141 — native relation codegen: implementation plan (SSoT)

**This file is the single source of truth for the native relation codegen design.**
Do NOT re-litigate the locked design below. If the "bc needed / not needed" question
comes up again, the answer is here (verified, grounded), not re-derived.

---

## Locked design (do not change)

- **declare-via-BC → bc typed-native codegen.** litedbmodel declares read/relation/write
  behaviors; `bc generateModule({language:'rust-typed-native' | 'go-typed-native'})` emits
  the typed native module. py/php are **literal** (ir-exec over the native record).
- **ONE op-agnostic transport that touches the DB:** `execute_sql(sql, params)`. The fk IN-clause
  is embedded in the **generated SQL**; `params` = the deduped keys. Nothing else does I/O.
- **pluck / group are op-agnostic WIRE leaves** (`pluck_keys`, `group_children`) — NOT the DB
  transport, NOT per-op hand-written code. They take/return the BC-owned `WireValue`, convert
  `WireValue <-> bc Value` at their edge, and call the **grouping SSoT core**.
- **Relation flow:**
  `execute_sql(parent) -> wire -> pluck(keys) -> execute_sql(child, WHERE fk IN(keys)) -> wire
   -> group(nest by fk) -> TERMINAL de-box -> typed`.
- **Intermediate nodes stay WIRE** (no de-box). **Only the terminal node de-boxes** to the read's
  typed outType. (This is exactly what bc **#164** provides.)
- **Grouping logic = ONE SSoT core per language** (`grouping.{ts,rs,go,py,php}`) over bc `Value`.
  The wire leaf is a thin wrapper. **No typed->Value re-box on the covered plane** (that is the
  rejected "(A) cancer"). Wire exists ONLY at the transport boundary.
- **Leaves live in the RUNTIME** (`litedbmodel_runtime`), NOT the benchmark (`orm_bench`). Responsibility.
- **rust and go are SYMMETRIC** (both typed-native, both WireValue wire leaves). py/php are literal
  (the record IS the value; leaf = core over Value directly, no WireValue).

## Invariants (never violate)
- No boxing / no `vec![(` (key-value container) on the covered/generated plane. Wire only at the boundary.
- op-agnostic transports only. No per-op hand-written code. No per-op typed struct crossing the boundary.
- grouping = one SSoT core per language (consume it; never duplicate).
- Leaves in runtime, not the bench.
- litedbmodel never edits bc; bc shortfalls become bc issues (handled in a separate thread).

---

## bc gaps — VERIFIED COMPLETE SET = { #164, #165 } (both filed). Nothing else.

Audited by compiling BOTH rust+go targets to exhaustion and categorizing every error, plus a
grounded type check of bc 0.8.15's actual rust output.

- **#164 — wire-passthrough node output.** Admit a node whose output is opaque wire and store the
  raw `WireValue` (no de-box); de-box deferred to the terminal node.
  **Needed by BOTH rust AND go** (grounded: bc 0.8.15 emits `cell_n0: RefCell<Vec<T0>>`,
  `f_rows: Vec<T0>`, `f_children: Vec<T2>` — i.e. it de-boxes every intermediate node in both
  languages). The one-off audit note "rust doesn't need #164" was WRONG; corrected here by direct
  type check.
- **#165 — rust `--shared-types-import` parity.** Emit the BC-owned `WireValue` (+ error/probe types)
  into a shared crate so the covered module, the runtime leaf, and the consumer share ONE `WireValue`.
  **rust ONLY** (go already has `--shared-types-import`; rust's emitter has no such branch).
- (#160 value[] body-node spread = DONE/works. #162 = closed, not a gap.)

## litedbmodel-side work — NO bc change required

- **B-1. sentinel-where phantom `@`-input ports (the WHOLE family:** `whereLike/ILike/In/Between/
  Cast/Dynamic/Immediate/TupleIn/InSubquery/Exists/RawPredicate`**).** `authoring-sql.ts` builds
  `$[@x]` sentinel refs; bc faithfully derives an input port for each. `authoring.ts`
  `lowerRecordedWhere` strips the transient `where` port but LEAVES the derived `inputPorts["@x"]`
  (typed `unknown`) -> breaks native compile (the `_like: Value` leak). **Fix:** prune the
  `@`-prefixed inputPorts in the same post-compile pass (or stop encoding structural metadata as `$`-refs).
- **B-2. composite (multi-column) key relations — 3 litedbmodel layers.** `decorator-adapter.ts:424`
  `attachRelation` does `parentKey = op.parentKey ?? ''` — a composite op yields `parentKeys`/
  `targetKeys` ARRAYS, so scalar keys fall to `''`. **Fix:** (1) pass the key TUPLES; (2) widen the
  pluck/group leaf ports from `'string'` to a tuple shape `{arr:'string'}` (bc already emits that
  shape); (3) extend `leaves.ts encodeParams` for array-of-tuples (tuple IN). The grouping SSoT core
  already accepts column tuples. NOT a bc gap.
- **B-3a. batch ops (createMany/upsertMany/updateMany) — DONE.** Authored via `emitBatchWrite`
  (`decorator-adapter.ts`): the json_each/JSON_TABLE batch form (SSoT `compileWriteNode`) lowers to ONE
  `executeSQL` node whose `?`(s) all bind the ONE opaque `rows` array value (bc#156) — no `__batchRows`
  marker, no per-column parallel arrays. 1 query each (safety-proven). NOT a bc gap.
- **B-3b. RETURNING-tx ops (delete/nestedCreate/nestedUpdate/nestedUpsert) — litedbmodel-side, NOT a bc gap.**
  The DB transaction boundary (BEGIN/COMMIT/ROLLBACK + atomicity) is the CONSUMER's responsibility, not a
  BC feature (BC is a generic behavior framework; DB tx is domain-specific). The runtime already owns tx
  scope (`litedbmodel_runtime` `begin_tx`/`acquire_tx`/`begin_tx_isolated`/`TxConnection`). The correct
  model: the runtime brackets the runner call in a tx (Ok→commit / Err→rollback) and the generated runner
  runs the body statements via `execute_sql` on the tx connection, returning `Result` — NO BEGIN/COMMIT
  emitted into the generated code, so no bc transaction construct is needed. The remaining work is entirely
  litedbmodel-side:
  1. **runtime**: a `with_transaction`-style wrapper that begins a tx, runs the runner, commits on Ok /
     rolls back on Err, threading the `TxConnection` as the ambient driver so `execute_sql` runs on it.
  2. **authoring**: express the body (INSERT…RETURNING id → INSERT using id) as a COVERED map/chain — the
     RETURNING-write source node must carry a typed outType so the `.map` is covered (the reproduced
     `non-covered map shape` error is because it was NOT authored as a covered typed-source node, not a bc
     gap). bc#169 (which framed DB-tx as a bc feature) was closed as a responsibility violation.
  Until authored, these 4 ops still run via the TS interpreter (`deriveTransactionPlan`/`executeTransactionBundle`),
  outside `generateModule`. NOT faked/stubbed on the native plane.

---

## Status (2026-07)
- **rust native = PROVEN** via HAND-AUTHORED post-#164/#165 stand-in files (commit `b236bfc`):
  `cargo build` green, query-count measured **nestedFindAll=2 / nestedFindFirst=2 / nestedFindUnique=2
  / nestedRelations=3 / compositeRelations=2** (N+1 would be 6). The hand-authored `behaviors_generated.rs`
  + `wire.rs` are STAND-INS that bc #164/#165 regen will replace (aim: no-op diff). `leaves.rs`/`main.rs`/
  runtime re-exports are permanent (not regen targets).
- **grouping SSoT cores done in all 5 languages.** py/php native-record leaves done. go typed-native
  (symmetric with rust) in progress.

## Sequencing
1. **bc (separate thread):** #164 (wire-passthrough, rust+go), #165 (rust shared-types-import).
2. **litedbmodel (now, no bc dependency):** B-1 (prune `@`-ports), B-2 (composite keys, 3 layers),
   B-3 (re-author batch/tx as leaf graphs).
3. **after #164/#165 land:** regen (replaces the rust/go hand-authored stand-ins), leaves in runtime,
   cargo/go build green, query-count over ALL ops.
