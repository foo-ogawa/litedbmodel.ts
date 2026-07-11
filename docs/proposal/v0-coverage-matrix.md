# V0 — litedbmodel v2 completeness coverage matrix

> Step 1 of `v2-completeness-plan.md` §5. The definitive op × dialect × {byte-match?, live-exec on PG+MySQL?}
> matrix for the whole read+write makeSQL surface, plus the confirmed/adjusted R1–R8 gap list against reality.
>
> Definitions (plan §1/§3):
> - **byte** = a `makesql-golden.test.ts` assert that the compiled makeSQL reproduces the v1 builder's direct
>   output byte-for-byte, on every dialect where the dialect text differs. `(P/M/S)` = postgres/mysql/sqlite.
> - **live** = the op is exercised on real dockerized PG **and** MySQL through the `conformance/vectors-livedb`
>   corpus (all 5 language runtimes replay the same bundle). SQLite is the in-proc reference.
> - **done** = byte (all differing dialects) **AND** live (PG+MySQL). Byte-only / PG-only / SQLite-mock-only ≠ done.

## Reachability model (the load-bearing V0 finding)

There are TWO distinct compile entry points, and they do **not** cover the same construct set:

1. **`compileWhere` / `compileSelect` / `compile*` (direct v1-parity functions)** — take a v1-shaped
   `ConditionObject` / `SelectDesc` and reproduce the v1 builder byte-for-byte. `makesql-golden.test.ts` drives
   these directly (hand-building `DBSubquery`/`DBExists`/`dbCast`/`__raw__`/`SelectDesc.join|cte|append`). This
   is where BYTE coverage lives.

2. **The authoring→bundle path (`L.Select(...)` → `compileBundle` → `compileSelectNode`)** — the ONLY path that
   produces a live-executable bundle the language runtimes replay. Its WHERE comes from the bc closed-set
   fragment IR (`whereEq/whereNe/whereLt/whereLe/whereGt/whereGe/whereIsNull/whereIn` — `authoring-sql.ts`), and
   `compileSelectNode` wires only the `SELECT_PORTS` catalog surface: `table, select, where, order, limit,
   offset, group` (`catalog.ts` `SELECT_PORTS`). This is where LIVE coverage lives.

**Consequence:** a construct is live-reachable only if it can be expressed through the fixed §2 authoring surface
(the bc where-primitives + the `SELECT_PORTS`). Constructs that exist ONLY in the v1 `DBConditions`/`DBValues`
layer or in `SelectDesc` fields with no catalog port are byte-provable but **cannot be placed in a live bundle**
without adding a new authoring primitive / catalog port — a §2 interface change (invariant #2 → escalate, do not
hand-roll).

## Matrix — READ surface

| Op / construct | byte (v1) | live PG+MySQL | Reachable via §2 authoring? | Status |
|---|---|---|---|---|
| Select head `SELECT <cols> FROM <t>` | ✅ P/M/S | ✅ (Feed/Posts/ByIds…) | yes | **done** |
| WHERE eq / ne / lt / le / gt / ge | ✅ P/M/S | ✅ (Feed whereEq/whereGe; when/ne SKIP) | yes (`whereEq`…) | **done** |
| WHERE IS NULL / IS NOT NULL | ✅ P/M/S | ⚠️ IS NULL via `whereIsNull` reachable; not in a live vector | partial (isNull yes; NOT NULL = `dbNotNull`, not authorable) | byte done; live gap is cosmetic (eq-null path already live via SKIP) |
| IN-list (single col, non-empty) | ✅ P/M/S (M/S = JSON deviation) | ✅ (ByIds/ByUuids/ByBig/ByTxt/ByFlag/ByTs/ByAmt) | yes (`whereIn`+`inColumn`) | **done** |
| IN-list empty → `1 = 0` | ✅ P/M/S | ✅ (ByIds empty) | yes | **done** |
| COUNT(*) [+ WHERE] | ✅ P/M/S | ✅ (CountAll/CountByAuthor/empty) | yes (`L.Count`) | **done** |
| GROUP BY | ✅ P/M/S (SELECT-tail golden) | ❌ | **port exists** (`group`) but **no live vector** | byte done; **live gap (R3)** — addable additively |
| ORDER BY / LIMIT / OFFSET | ✅ P/M/S | ✅ (Feed order/limit) | yes | **done** |
| FOR UPDATE | ✅ P/M/S (SELECT-tail golden) | ❌ | **NO catalog port** (`forUpdate` in `SelectDesc` only) | byte done; **live blocked — §2 gap (R3)** |
| BETWEEN | ✅ P/M/S (custom-op `__raw__`) | ❌ | **NO authoring primitive** | byte done; **live blocked — §2 gap (R3)** |
| ILIKE / LIKE | ✅ P/M/S (`__raw__`) | ❌ | **NO authoring primitive** | byte done; **live blocked — §2 gap (R3)** |
| tuple / composite IN (`__tuple__`) | ✅ P/M/S | ❌ | **NO authoring primitive** | byte done; **live blocked — §2 gap (R3)** |
| dbCast `::type` / dbCastIn | ✅ P/M/S (dialect-gated) | ❌ | **NO authoring primitive** | byte done; **live blocked — §2 gap (R3)** |
| dbDynamic `fn(?)` | ✅ P/M/S | ❌ | **NO authoring primitive** | byte done; **live blocked — §2 gap (R3)** |
| dbImmediate / dbRaw | ✅ P/M/S | ⚠️ (dbRaw NOW() live in createMany fallback tx) | write-side only | byte done; read-side live blocked — §2 gap (R3) |
| IN(subquery) single key | ✅ P only | ❌ | **NO authoring primitive** | byte(P) done; **live blocked — §2 gap (R2)** |
| NOT IN(subquery) | ✅ P only | ❌ | **NO authoring primitive** | byte(P) done; **live blocked — §2 gap (R2)** |
| composite (a,b) IN(subquery) | ✅ P only | ❌ | **NO authoring primitive** | byte(P) done; **live blocked — §2 gap (R2)** |
| EXISTS / NOT EXISTS (correlated) | ✅ P only | ❌ | **NO authoring primitive** | byte(P) done; **live blocked — §2 gap (R2)** |
| `= ANY(?::type[])` scalar-array (PG) | ✅ P (raw) | ✅ (relation batches = ANY($1::int[])) | via relation compile | **done** (as relation batch) |
| CTE / WITH (`withQuery`) | ❌ | ❌ | **`cte` in `SelectDesc` only, NO catalog port** | **byte + live gap — §2 gap (R4)** |
| JOIN (`join`/`joinParams`) | ❌ | ❌ | **`join` in `SelectDesc` only, NO catalog port** | **byte + live gap — §2 gap (R5)** |
| append / HAVING | ❌ | ❌ | **`append` in `SelectDesc` only, NO catalog port** | **byte + live gap — §2 gap (R6)** |
| findByPkeys / findById (`compileFindByPkeys`) | ❌ | ❌ | dead code; NO catalog port | **R7 — decide adopt/delete (see below)** |
| FIND_FILTER (per-model soft filter) | ❌ | ❌ | v1 `DBModel` concern; not in SCP surface | **R8 — implement or waive (see below)** |

## Matrix — RELATIONS

| Relation shape | byte (v1 LazyRelation) | live PG+MySQL | Status |
|---|---|---|---|
| single-key belongsTo unlimited | ✅ P/M/S | ✅ (Posts author) | **done** |
| single-key hasMany unlimited (+order+where) | ✅ P/M/S | ✅ (Posts comments) | **done** |
| single-key hasMany + per-parent LIMIT (LATERAL / ROW_NUMBER) | ✅ P/M/S | ✅ (Posts tags limit=2; Feed) | **done** |
| composite-key hasMany unlimited | ✅ P/M/S | ✅ (Docs revisions) | **done** |
| composite-key hasMany + LIMIT | ✅ P/M/S | ⚠️ live via Docs (unlimited); limited-composite live not in a vector | byte done; composite-limited live is cosmetic gap |
| composite-key belongsTo | ✅ (composite unnest) | ✅ (Docs owner) | **done** |
| composite STATIC unnest form (PG) | ✅ P (byte + negative) | ✅ | **done** |
| **cross-DB relations (target driver/connection)** | n/a (SQL same) | ❌ | **R1 — needs per-statement connection-tag bundle shape** |

## Matrix — WRITE surface

| Op | byte (v1) | live PG+MySQL | Status |
|---|---|---|---|
| INSERT single | ✅ P/M/S (M/S JSON deviation) | ✅ (tx Create) | **done** |
| createMany homogeneous | ✅ P/M/S | ✅ | **done** |
| createMany heterogeneous (column-set groups) | ✅ P/M/S | ✅ | **done** |
| createMany DBToken(NOW()) → v1 fallback | ✅ M/S (byte-pinned) | ✅ (dbRaw NOW()) | **done** |
| upsert ON CONFLICT DO UPDATE / IGNORE | ✅ P/M/S | ✅ (tx idempotency/unique ON CONFLICT/IGNORE) | **done** |
| updateMany (per-row) | ✅ P/M/S | ✅ | **done** |
| **deleteMany (single + composite PK)** | ✅ P/M/S **(ADDED in V0)** | ✅ (deleteMany {1,3}) | **done (this increment)** |
| single UPDATE (+per-col PG cast) | ✅ P/M/S | ✅ (bare UPDATE) | **done** |
| single DELETE (IN-list) | ✅ P/M/S | ✅ (bare DELETE) | **done** |
| RETURNING forms (bare / t.col / table.col / MySQL none) | ✅ P/M/S | ✅ (across tx) | **done** |
| DELETE…RETURNING (MySQL emul → []) | ✅ | ✅ (per-dialect vector) | **done** |
| INSERT RETURNING uuid-PK / composite-PK (MySQL re-select) | ✅ | ✅ | **done** |
| tx gate spine (requires/idempotency/unique/derive/emits) | ✅ (mock corpus) | ✅ (commit + ROLLBACK) | **done** |
| tx edge (m2m link + fk claim) | ✅ | ✅ | **done** |
| tx composite multi-write DAG | ✅ | ✅ | **done** |

## Confirmed/adjusted gap list (R1–R8)

The plan assumed R2/R3 were "byte-only, live-zero" additive-only fills. V0 confirms the byte side but finds the
LIVE side is **blocked by the fixed §2 authoring surface** for most read constructs — they have no bc where-
primitive / no catalog port, so a live vector cannot be authored without a §2 interface change. Same for
R4/R5/R6 (fields exist on `SelectDesc` but have no `SELECT_PORTS` port). This is the invariant-#2 escalation
condition, not an additive fill.

| Item | Reality after V0 | Additive within §2? |
|---|---|---|
| **R1** cross-DB relations | SQL is v1-identical; needs a per-statement `connection`/`dialect` tag in the bundle shape + runtime routing. Plan pre-pins this shape. | **NO — bundle-shape change → escalate** (plan already anticipates) |
| **R2** subquery/EXISTS/NOT IN/composite-IN | byte(PG) present; live blocked — no authoring primitive for subquery/exists | **NO — needs new where-primitive + catalog wiring → escalate** |
| **R3** GROUP BY / FOR UPDATE / BETWEEN / ILIKE / tuple-IN / dbCast / dbDynamic / dbImmediate | byte present all dialects; GROUP BY has a port (live addable); the rest have NO authoring primitive / port | GROUP BY live = **additive**; FOR UPDATE + BETWEEN/ILIKE/tuple-IN/dbCast/dbDynamic/dbImmediate live = **NO → escalate** |
| **R4** CTE / query-based reads | `cte` field on `SelectDesc`, no port; no byte, no live | **NO — needs catalog port → escalate** |
| **R5** JOIN | `join`/`joinParams` on `SelectDesc`, no port; no byte, no live | **NO — needs catalog port → escalate** |
| **R6** append / HAVING | `append` on `SelectDesc`, no port; no byte, no live | **NO — needs catalog port → escalate** |
| **R7** findById/findByPkeys | `compileFindByPkeys` is **dead** (no non-def/non-export references). v1 has a public `findById` API. | Decision below |
| **R8** FIND_FILTER | v1 `DBModel` per-model global soft filter (`DBModel.ts:597/859`). Not represented in SCP behaviors. | Decision below |

### R7 decision — DELETE `compileFindByPkeys` (do not adopt via live)

`compileFindByPkeys` is dead in the SCP surface. Adopting it "byte+live" would require a NEW authoring/catalog
entry (a `FindByPkeys` read leaf) to reach a live bundle — a §2 change. But it is **semantically redundant**: a
findByPkeys is a PK-set membership read, which the existing live-covered surface already expresses —
single-key = the `whereIn`+`inColumn` IN-list (ByIds/ByUuids, live on PG+MySQL), composite-key = the composite
relation batch (Docs owner/revisions, live). The v1 public `findById` API is a v1-runtime convenience, not a
distinct makeSQL construct. **Recommendation: DELETE `compileFindByPkeys` (+ its `builderFor().buildFindByPkeys`
plumbing if unused elsewhere) as dead code** rather than manufacture a redundant catalog entry. (Left in place
pending sign-off; flagged as the R7 resolution — see escalation.)

### R8 decision — FIND_FILTER (WAIVE with rationale)

FIND_FILTER is a v1 `DBModel`-level implicit predicate injected into `find`/`count`. In SCP the WHERE is authored
explicitly per behavior; there is no per-model implicit-filter concept in the bundle shape, and adding one would
be a semantic + interface change (every read leaf would need to merge an out-of-band model filter — a new bundle
field + runtime merge in all 5 languages). It is **not a SQL construct gap** — any filter FIND_FILTER expresses is
already authorable as an explicit `whereEq`/`when(...)` in the behavior, and that path is live-covered. **Waive:
FIND_FILTER is a v1 ergonomic convenience, not a makeSQL completeness gap; explicit authored WHERE is the SCP
equivalent and is already byte+live done.**

## Net V0 outcome

- **Additive, done this increment:** `deleteMany` byte-assert (single + composite PK, all dialects) — was
  live-only; now byte+live done. Golden 130→136; golden-from-originals negative confirmed (perturbing the v1
  DELETE text moves all 6 asserts).
- **Additive, still open within §2:** GROUP BY live vector (has a catalog port) — the ONE R3 sub-item that is a
  genuine additive fill.
- **Escalation (cannot do within §2 without an interface change):** R1 (bundle connection-tag), R2 (subquery/
  EXISTS authoring primitive), R3-minus-GROUP-BY (FOR UPDATE port + BETWEEN/ILIKE/tuple-IN/dbCast/dbDynamic/
  dbImmediate authoring primitives), R4 (CTE port), R5 (JOIN port), R6 (append/HAVING port). All are byte-
  provable today (or trivially so) but **not live-reachable through the fixed §2 authoring/catalog surface**.
- **R7:** delete dead `compileFindByPkeys` (redundant with live IN-list/composite-relation reads).
- **R8:** waive FIND_FILTER (v1 ergonomic, not a makeSQL construct; explicit WHERE is the byte+live-done SCP form).
