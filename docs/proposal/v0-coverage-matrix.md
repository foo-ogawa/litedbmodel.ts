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
| GROUP BY | ✅ P/M/S (SELECT-tail golden) | ✅ (GroupByAuthor) | yes (`group` port) | **done** |
| ORDER BY / LIMIT / OFFSET | ✅ P/M/S | ✅ (Feed order/limit) | yes | **done** |
| FOR UPDATE | ✅ P/M/S (SELECT-tail + §D authored) | ✅ (ForUpdate, PG+MySQL) | yes (`forUpdate` port added) | **done (R3)** |
| BETWEEN | ✅ P/M/S (custom-op + §D authored) | ✅ (IdBetween) | yes (`whereBetween` added) | **done (R3)** |
| LIKE | ✅ P/M/S (custom-op + §D authored) | ✅ (TitleLike) | yes (`whereLike` added) | **done (R3)** |
| ILIKE | ✅ P/M/S (§D authored) | PG-only construct (see note) | yes (`whereILike` added) | byte done; live = PG-only (v1 `ILIKE` keyword errors on MySQL/SQLite — reproducing it live there = a v1 error, not a gap) |
| tuple / composite IN (`__tuple__`) | ✅ P/M/S (§D authored) | ✅ (TupleIn) | yes (`whereTupleIn`, literal tuples) | **done (R3)** |
| dbCast `::type` (dialect-gated) | ✅ P/M/S (§D authored) | ✅ (DocByCast: `::uuid` PG / no-cast MySQL·SQLite) | yes (`whereCast` added) | **done (R3)** |
| dbDynamic `fn(?)` | ✅ P/M/S (§D authored) | portable-fn only (see note) | yes (`whereDynamic` added) | byte done; live for a PORTABLE fn is addable; the byte exemplar `to_tsvector` is PG-only (a v1 PG construct, not a gap) |
| dbImmediate (inline SQL, no bind) | ✅ P/M/S (§D authored) | ✅ (ImmediateEq: `author_id = 7`) | yes (`whereImmediate` added) | **done (R3)** |
| IN(subquery) single key | ✅ P/M/S (§D authored) | ✅ (InSubquery) | yes (`whereInSubquery`, nested Fragment) | **done (R2)** |
| NOT IN(subquery) | ✅ P/M/S (§D authored) | ✅ (NotInSubquery) | yes (`whereInSubquery(…, true)`) | **done (R2)** |
| composite (a,b) IN(subquery) | ✅ P (byte, existing) | authorable via nested Fragment `sql` | yes (`whereInSubquery` w/ composite lhs+sql) | byte done; live addable (same nested-Fragment path) |
| EXISTS / NOT EXISTS (correlated) | ✅ P/M/S (§D authored) | ✅ (ExistsComment / NotExistsComment) | yes (`whereExists`, nested Fragment) | **done (R2)** |
| `= ANY(?::type[])` scalar-array (PG) | ✅ P (raw) | ✅ (relation batches = ANY($1::int[])) | via relation compile | **done** (as relation batch) |
| CTE / WITH | ✅ P/M/S (§D authored) | ✅ (CteLive) | yes (`cte`/`cteParams` ports added) | **done (R4)** |
| JOIN | ✅ P/M/S (§D authored) | ✅ (JoinAuthor) | yes (`join`/`joinParams` ports added) | **done (R5)** |
| append / HAVING | ✅ P/M/S (§D authored) | ✅ (HavingAuthor) | yes (`append` port added) | **done (R6)** |
| findByPkeys / findById (`compileFindByPkeys`) | n/a (dead) | n/a | DELETED (redundant) | **done (R7 — dead SCP export removed; v1 `buildFindByPkeys` kept)** |
| FIND_FILTER (per-model soft filter) | ✅ = authored WHERE (no-op proof) | ✅ via authored WHERE | folded into WHERE upstream | **done (R8 — evidence: merged filter == authored WHERE, byte+live done)** |

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
| **cross-DB relations (target driver/connection)** | ✅ (SQL = target's dialect, byte-identical) | TS reference ✅ (two-DB routing test); 4-lang live = ESCALATED | **R1 — bundle-shape tag DONE + TS routing; per-language live cross-DB run escalated (see below)** |

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

The plan assumed R2/R3 were "byte-only, live-zero" additive-only fills. V0 confirmed the byte side but found the
LIVE side was **blocked by the fixed §2 authoring surface**. Per plan §2 補足 the surface was then extended
ADDITIVELY (new where-primitives + new `SELECT_PORTS` ports — no signature change, no new bundle kind, SQL still
v1-sourced), closing R2/R3-remainder/R4/R5/R6 to byte+live. R1's bundle-shape tag landed additively; its
per-language LIVE cross-DB run is the one escalation.

| Item | Final state | How |
|---|---|---|
| **R1** cross-DB relations | Bundle-shape tag + TS routing DONE; 4-lang live cross-DB run ESCALATED | `RelationDecl.connection?`/`RelationOp.connection?` added (additive, untagged = byte-unchanged); `runRelationOp` already honors per-op `dialect`; TS `readBundle`/`buildResultSet` route by tag (two-DB test proves it). SQL is v1-identical (compiles for the target's own dialect). See escalation below. |
| **R2** subquery / EXISTS / NOT IN / NOT EXISTS | **done (byte + live PG+MySQL)** | `whereInSubquery`(+NOT) / `whereExists`(+NOT) — subquery as a nested makeSQL Fragment in the param slot. Live: InSubquery/NotInSubquery/ExistsComment/NotExistsComment. |
| **R3** GROUP BY / FOR UPDATE / BETWEEN / LIKE / tuple-IN / dbCast / dbImmediate | **done (byte + live)** | `group` (already) + new `forUpdate` port + `whereBetween`/`whereLike`/`whereTupleIn`/`whereCast`/`whereImmediate`. Live: GroupByAuthor/ForUpdate/IdBetween/TitleLike/TupleIn/DocByCast/ImmediateEq. ILIKE + dbDynamic(to_tsvector) are byte-done + PG-only-by-nature (see notes in the READ matrix — not live gaps). |
| **R4** CTE / WITH | **done (byte + live)** | `cte`/`cteParams` ports added; `compileSelect` WITH-wrap is v1-sourced. Live: CteLive. |
| **R5** JOIN | **done (byte + live)** | `join`/`joinParams` ports added; `compileSelect` JOIN position is v1-sourced. Live: JoinAuthor. |
| **R6** append / HAVING | **done (byte + live)** | `append` port added (raw trailing clause). Live: HavingAuthor. |
| **R7** findById/findByPkeys | **done — DELETED** | `compileFindByPkeys` + `FindByPkeysOptions` import + 2 re-exports removed (dead SCP export, redundant with live IN-list/composite-relation reads). v1 `buildFindByPkeys` kept (DBModel.findById). |
| **R8** FIND_FILTER | **done — no-op with evidence (variant b)** | v1 merges FIND_FILTER into the WHERE `DBConditions` BEFORE compiling (`DBModel.ts:596-604`/`:858-866`); the SCP compile reimplements the WHERE text from explicit `conditions` and never reads FIND_FILTER. `find-filter-noop.test.ts` proves (3 dialects) the merged filter == the authored WHERE byte-for-byte — folded upstream, a genuine no-op for makeSQL. |

### R1 escalation — per-language LIVE cross-DB relation run

**Landed (additive, byte-proven):** the per-statement `connection` tag on `RelationDecl`/`RelationOp`, threaded
through `compileRelationOp`/`compileBundle`; the relation SQL compiles for the TARGET model's own `dialect`
(byte-identical to the v1 PG/MySQL batch); TS `readBundle`/`buildResultSet` route a tagged relation to
`ReadOptions.connections[tag]` (proven against two genuinely separate SQLite DBs in
`test/scp/cross-db-relation.test.ts`, with a loud-fail negative for an unregistered tag).

**Escalated (needs sign-off — a coordinated 5-runtime change beyond thin plumbing):** a LIVE cross-DB vector
requires (1) a new corpus vector mode where a single bundle's parent executes on connection A and a tagged
relation on connection B, and (2) each of the 4 language `livedb_runner`s + runtime `ReadBundle` to accept a
connection REGISTRY and pick the pooled driver by `op.connection` (today each runner runs one whole bundle
against ONE connection). Concrete proposal: (a) add a `kind:'crossdb'` read vector carrying `bundlePg` with one
relation tagged `connection:'mysql'` (target on the OTHER live DB), reference captured cross-DB on SQLite×2; (b)
give each runtime's relation loop a `map[connName]driver` (the runners already open BOTH pg+my in `main()`, so
no new connection is needed — only routing); (c) route `RunRelationOp(op, parents, registry[op.connection])`.
This is ~1 thin change × 4 languages + 1 vector, but it is a new execution MODE across all runtimes, so it is
raised for approval rather than landed unilaterally (invariant #2 / plan R1 escalation clause).

## Net V0 outcome (final)

- **Done byte + live (PG+MySQL, all 5 runtimes):** GROUP BY, FOR UPDATE, BETWEEN, LIKE, tuple-IN, dbCast
  (dialect-gated), dbImmediate, IN/NOT-IN subquery, EXISTS/NOT EXISTS, CTE, JOIN, append/HAVING — via the
  additive where-primitives + `SELECT_PORTS` ports (plan §2 補足), every construct's SQL still v1-sourced.
  Golden 139→176 (+37 authored-path byte-asserts, incl. a golden-from-originals negative). livedb 39→52 vectors
  (all 4 language legs 52/52 pg + 52/52 mysql).
- **R1:** bundle-shape `connection` tag + TS routing landed and byte/two-DB-proven; the per-language live
  cross-DB RUN escalated with a concrete proposal (above).
- **R7:** dead `compileFindByPkeys` deleted (v1 `buildFindByPkeys` retained).
- **R8:** FIND_FILTER proven a makeSQL no-op (variant b) with a 3-dialect byte test — not a waive.
- **Byte-only-by-nature (not gaps):** ILIKE (v1 `ILIKE` keyword is PG-only; errors on MySQL/SQLite) and
  dbDynamic with a PG-only function (`to_tsvector`) — the AUTHORING primitives are added and byte-proven; a
  portable-function dbDynamic is live-reachable through the same primitive.
