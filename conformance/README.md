# litedbmodel SCP conformance harness (WS7a, #30)

The multi-language conformance corpus + runners for the litedbmodel v2 SCP §8 artifact. It is
the machine-verified half of the §10 promise: **同一 IR + 入力 → 同一 SQL + 同一結果** across
languages. The dialect axis (SQLite / Postgres / MySQL SQL text) is compiled ONCE, TS-side, into
the published bundle; the language axis (TS / Python / PHP / Go / Rust runtimes) is proven here.

## Layout

```
conformance/
  harness.ts            SSoT: vector types + the reference generator + the TS asserter
  gen-vectors.ts        writeCorpus()/checkCorpus() — capture the corpus from the reference
  gen-vectors.test.ts   vitest wrapper that (re)writes conformance/vectors/*.json
  vitest.config.ts      config for the generator (kept out of the main test/** include)
  vectors/*.json        the FROZEN corpus (one file per suite) — pure JSON, byte-true
  vectors-runner.ts     TS runner: runs the corpus through the BUILT dist/scp artifact,
                        emits a machine JSON summary (consumer-path leg)
  vectors-run.ts        cross-language orchestrator (TS now; py/php/go/rust join as WS7b-e land)
  frozen/               additive-refreeze golden (ts-golden.frozen.json)
```

The **assertion baseline** lives in the main test suite: `test/scp/conformance-vectors.test.ts`
loads the frozen corpus, asserts it is byte-true to the current reference (drift gate), and runs
the TS reference against every vector (green). That test is part of `vitest run` (the 740+ suite).

## How vectors are generated (byte-true, never hand-authored)

`generateCorpus()` builds every vector by running the REAL TS SCP reference and CAPTURING its
output:

- **render** — `compileSelect`/`compileInsertFor`/`compileUpdate`/`compileDelete` + the normative
  `renderOperation` for every dialect. `expectedSql`/`expectedParams` are the reference's own
  output (PG `?`→`$N` applied after IN-list expansion, canonical alphabetical column order).
- **exec / tx** — `compileBundle`/`compileWriteBundle` produce the §8 `SqlBundle`; `executeBundle`/
  `executeTransactionBundle` run it against a fresh in-memory `better-sqlite3`. `expectedResult`
  (+ post-tx DB state) is the captured runtime output.
- **dialect** — the reference `Dialect.orderByNulls` output per dialect/dir/nulls.

Because the expected fields ARE the reference's output, they cannot silently diverge; the drift
gate (`checkCorpus()`) re-derives and byte-compares.

## Running

```bash
# assertion baseline (part of the normal suite)
npx vitest run test/scp/conformance-vectors.test.ts

# regenerate the corpus after a reviewed reference change
npx vitest run --config conformance/vitest.config.ts

# cross-language orchestrator (TS reference leg; WS7b-e languages auto-join)
npm run build:scp && npx tsx conformance/vectors-run.ts
```

## Adding a language runtime (WS7b-e)

Each language ships a `vectors_runner` that (1) loads `conformance/vectors/*.json`, (2) runs each
vector through ITS runtime (render/exec/tx/dialect) — consuming behavior-contracts' port for the
Expression-IR evaluation, NOT re-implementing it — and (3) prints the SAME machine JSON summary
`{"lang","suites",{...},"total_pass","total_fail","version_mismatch"}` as its last stdout line.
`vectors-run.ts` discovers the runner and adds it to the cross-language agreement check. The stub
runner files under `python/ php/ go/ rust/` carry the `WS7B_E_RUNTIME_STUB` marker so the
orchestrator reports them PENDING (not FAIL) until their body lands.
