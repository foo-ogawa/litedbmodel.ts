# Frozen conformance golden (WS7a, #30)

`ts-golden.frozen.json` is the machine-summary the **TS reference runner** must reproduce:
the per-suite pass counts of the whole vector corpus. It is the *additive-refreeze* anchor
(mirrors graphddb's `conformance/frozen/`):

- A vector is only ever **added** or an existing expectation **re-frozen** after a reviewed
  reference change — never silently mutated.
- CI runs `conformance/vectors-runner.ts` and diffs its summary against this file. A change in
  totals (a dropped/failing vector, or an un-refrozen count) fails the gate.

## Refreeze procedure

1. Change the reference (or add vectors in `conformance/harness.ts`).
2. Regenerate the corpus: `npx vitest run --config conformance/vitest.config.ts`.
3. Review the `conformance/vectors/*.json` diff.
4. Rebuild + re-freeze: `npm run build:scp && npx tsx conformance/vectors-runner.ts > conformance/frozen/ts-golden.frozen.json`.
5. Commit corpus + frozen golden together.

## Corpus (current freeze)

| suite   | vectors | coverage |
|---------|---------|----------|
| render  | 30      | CRUD (Select/Insert/Update/Delete) × 3 dialects × SKIP present/null, empty-WHERE, IN-list N/empty |
| exec    | 3       | Read bundles vs seeded SQLite: SKIP-present, absent-key SKIP (normalizeInput), belongsTo + hasMany(limit) relations |
| tx      | 4       | Write-time-relations gate-first transaction: single-write commit (requires/idempotency/unique/body/derive/emit) + gate short-circuit ROLLBACK; **WS8a composite (multi-write) tx-DAG** (#28): nested-write commit (child.post_id = `$.ref.post.id`, parent→child topological order) + gate-first-across-the-DAG ROLLBACK |
| dialect | 12      | `orderByNulls` (WS6-flagged untested) × 3 dialects × {ASC,DESC} × {FIRST,LAST} — PG/SQLite native NULLS, MySQL `IS NULL` emulation |

Total: **49 vectors** (47 baseline + 2 WS8a composite tx-DAG, #28).
