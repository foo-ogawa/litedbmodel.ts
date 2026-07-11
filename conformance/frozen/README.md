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
| render  | 18      | Read/Create/Rename/Remove bundles × 3 dialects: Feed (status SKIP present/null + coalesce-default LIMIT + ORDER), ByIds (single-JSON-param IN-list), INSERT/UPDATE/DELETE + RETURNING (v1-DBConditions-sourced text) |
| exec    | 2       | Read bundles vs seeded SQLite: SKIP-present + belongsTo/hasMany relations, absent-key SKIP (normalizeInput) + relations |
| tx      | 4       | Write-time-relations gate-first transaction: single-write commit (requires/idempotency/unique/body/derive/emit) + gate short-circuit ROLLBACK; **WS8a composite (multi-write) tx-DAG** (#28): nested-write commit (child.post_id = `$.ref.post.id`, parent→child topological order) + gate-first-across-the-DAG ROLLBACK |
| dialect | 12      | `orderByNulls` (WS6-flagged untested) × 3 dialects × {ASC,DESC} × {FIRST,LAST} — PG/SQLite native NULLS, MySQL `IS NULL` emulation |

Total: **36 vectors**.

## Provenance — why the count changed 49 → 36 (#43 makeSQL repair)

The original WS7a scaffold (commit `5bc2e23`) froze **49** vectors in the *reduced-IR era*: the
SCP renderer was a hand-rolled reduced spine, so the mock corpus carried many byte-shape vectors
that duplicated construct coverage. The **#43 makeSQL repair** (Phase-B flip `867dc84`, then
WHERE/predicate routed through v1 `DBConditions` in `5a16d12`) made `makeSQL` the SOLE SCP path,
sourcing all SQL text from the v1 builders. During that repair the mock corpus was **re-shaped, not
degraded** — it shrank to **36** (render 30→18, exec 3→2) and the retired reduced-IR byte vectors'
coverage MIGRATED to two stronger homes:

- **`test/scp/makesql-golden.test.ts`** — 187 v1-byte-asserts proving `makeSQL` byte-matches the
  original v1 builders (`DBConditions` / `_buildSelectSQL` / `_insert` / `LazyRelation`) for every
  WHERE construct × dialect, incl. IN-list N, empty-IN → `1 = 0`, empty-WHERE drop, subquery/EXISTS.
- **`conformance/vectors-livedb/livedb.json`** — 57 vectors executing on real PG + MySQL, incl. the
  retired `Feed: hasMany limit=2 caps children` exec vector verbatim.

Every removed vector's SQL-construct coverage was verified present in one or both homes before this
re-freeze (removed-vector → coverage table below). No construct was covered nowhere; nothing was
lost. This is a reviewed, non-additive re-freeze — the frozen check correctly caught the stale 49.

### Removed-vector → surviving-coverage map

**render −12** (5 retired constructs × dialects; `select eq+IN` folded into Feed + primitive asserts):

| removed construct (per dialect)      | coverage now lives in |
|--------------------------------------|-----------------------|
| `select eq+IN` (eq AND IN-list)      | golden A `equality` + `IN list` + `AND grouping`; live `ByIds:* IN-list` + Feed |
| `empty-WHERE degeneration` (→ no WHERE) | golden A `empty AND → drop`; golden B `compileSelect({}, …)` (SELECT-tail negative test) |
| `empty-WHERE present` (`WHERE status = ?`) | golden A `equality`; live `Feed: status present` |
| `IN-list N=3` (`id IN (?, ?, ?)`)     | golden A `IN list` (PG byte-match, MySQL/SQLite JSON form); live `ByIds: INT IN-list` |
| `IN-list empty → 1 = 0`               | golden A `empty IN → 1 = 0`; live `ByIds: EMPTY INT IN-list → zero rows` |

**exec −1:**

| removed vector                        | coverage now lives in |
|---------------------------------------|-----------------------|
| `Feed: hasMany limit=2 caps children` | golden C `single-key hasMany + per-parent LIMIT` (byte); live `Feed: hasMany limit=2 caps children` (verbatim, real PG+MySQL) + `Posts: hasMany-limit tags` |

## Other-language frozen files

There are none. The cross-language runners (py/go/rust/php) consume THIS SAME corpus and are
cross-checked for byte-identical per-suite pass/fail against the TS leg by
`conformance/vectors-run.ts` — there is no separate per-language frozen summary to keep in sync.
