# v2 SQL パリティ チェックリスト（epic #43 — PostgreSQL アンカー）

> 目的: v2 SCP の生成 SQL を、旧 `litedbmodel`(.ts v1) と旧 `litedbmodel.rs` の **tuned SQL** と一致させる。v2 の conformance を縮小基準（.ts v1 SqlBuilder の単一行 CRUD / SQLite-first 形）にアンカーしたため、このライブラリの核心である tuned SQL を取りこぼし、**旧 .ts・.rs の両方より SQL が退行**した（超デグレ）。本書はその全差分の作業チェックリスト。

## 原則
- **アンカー = PostgreSQL。** PG の最も tuned な SQL を正とし、MySQL/SQLite は PG 意味論に各 DB の能力範囲で導出する（PG を lowest-common-denominator に落とさない）。
- **conformance golden を旧 tuned 実装の PG SQL に対して作り直す**（縮小基準は破棄）。
- **原実装のバグは踏襲しない**（後述の「原実装バグ」参照）。3実装で挙動が割れる箇所は PG アンカーを選ぶ。
- 凡例 `差分`: ✅同一 / ⚠️差あり / ❌v2欠落。`優先度`: 高(correctness) / 中(perf/parity) / 低(cosmetic/edge)。

---

## 0. 構造的所見（最重要 — 単なる runtime 修正でなく §8 IR の破壊的変更が要る）

1. **§8 可搬 bundle が batch write を表現できない。** `compileInsertFor`/UPDATE 経路は単一行のみ。batch INSERT(UNNEST)/UPDATE(UNNEST/JOIN-VALUES/CASE-WHEN)/SKIP-column が bundle に無い → **Python/Go/Rust/PHP の thin runtime は batch write を実行不可**。多言語の約束の穴。
2. **§8 IR が複合キー relation を表現できない。** `RelationOp` が `parentKey`/`targetKey` を単一 `string` でハードコード（`src/scp/relation.ts:64-69,90-92`）。複合 FK の relation が構造的に不可能。
3. **relation IN に dialect 別 fragment 種が無い。** `= ANY(?::type[])` / UNNEST / `::type` cast / `CROSS JOIN LATERAL` を表す fragment kind が無く、`Dialect` strategy(`dialect.ts`) は INSERT-conflict/placeholder/NULLS しか扱わない（relation に未関与）。
4. **WHERE 語彙が閉集合に縮小。** v2 は eq/ne/lt/le/gt/ge/isNull/in/and/or/cond のみ（`bridge.ts:208` が他を loud-reject）。subquery/EXISTS/cast/LIKE/raw/BETWEEN 等が全欠落。
5. **hardLimit / relation where-filter / FOR UPDATE / HAVING / APPEND** が §8/runtime から欠落。

→ #43 は「IR 契約の作り直し（relation 複合キー・dialect relation fragment・batch write・条件語彙拡張）+ 全言語 render + conformance 再構築」を含む。**リリース済み 2.0.0 はこの点で超デグレ。**

---

## 1. 原実装バグ（PG アンカー統一時に *踏襲しない*）
- **.rs cast が dialect 非対応**: `condition.rs:97-116` が `?::type` をハードコードし SQLite/MySQL にも `::uuid` を漏らす。v1 は `SqlCastFormatter` で dialect gate。→ **v1 の dialect-aware を採用**。
- **.rs `Immediate`(`NOW()`) が条件で inline されず文字列を param bind**（`condition.rs` catch-all、test :613-622）。v1 は inline。→ **v1 の inline を採用**。
- **.rs 空 `IntArray/TextArray/FloatArray/BoolArray` → 不正な `IN ()`**（空ガードが `ValueArray` のみ）。v1/v2 は `1 = 0`。→ **`1 = 0` を採用**。
- **.rs per-parent-limit が PG でも `ROW_NUMBER()`**（LATERAL でない）。v1 は `CROSS JOIN LATERAL`。→ **v1 LATERAL を PG アンカーに採用**。
- **.rs PG 型推論が粗い**（uuid/timestamp/date/json を text に畳む、`model.rs:730`）。v1 は sqlCast 駆動で精緻。→ **v1 の sqlCast 駆動を採用**。

## 2. 3実装割れ（PG アンカーで確定すべき判断）
- **OR / 空OR / 単一要素の括弧規則**が v1/`.rs`/v2 で不一致（.rs は top-level OR を `(…)` 包む・単一要素 unwrap、v2 は top-level OR を包まず nested 単一 fragment を `(x)` 包む、v1 `__or__` は二重包み）。空OR は .rs=`1=0` / v1・v2=空文字（AND 合流時に TRUE 相当＝意味が違う）。→ **PG アンカー形を1つ決めて pin**。
- **boolean を WHERE で literal(`= TRUE`) か bound(`= ?`) か**（v1=literal / .rs・v2=bound）。→ 要決定（tuned PG は literal `TRUE`）。
- **LIMIT/OFFSET を parameterize(`LIMIT ?`) か inline(`LIMIT 10`) か**（v2=param / v1・.rs=inline）。→ **byte-golden が v1 と不一致になる**ため要決定。

---

## A. WHERE / 条件 / 値

| 構文 | dialect | .ts v1 | .rs | v2現状 | PGアンカー目標 | 差分 | 優先度 | ☐ |
|---|---|---|---|---|---|---|---|---|
| equality `col = ?` | all | `col = ?`/`$1` | `col = ?` | `col = ?` | `col = $1` | ✅同一 | — | |
| `!= / <>` | PG | custom-op key のみ | `Raw` のみ | `col <> ?`(first-class) | `col <> $1` | ⚠️差あり | 中 | [ ] |
| `< <= > >=` | PG | custom-op key のみ | `Raw` のみ | first-class `cmp` | `col > $1` | ⚠️差あり(出力は一致) | 中 | [ ] |
| IN-list `col IN (?,?)` | all | array→IN | array→IN | `IN (?)`→展開 | `col IN ($1,$2)` | ✅同一 | — | |
| empty-IN | all | `1 = 0` no param | ⚠️IntArray等は `IN ()`(不正) / ValueArrayのみ`1=0` | `1 = 0` | `1 = 0` | ⚠️.rsバグ | 高 | [ ] |
| NOT IN (list) | PG | custom-op のみ | 無し | ❌欠落 | `col NOT IN ($1,$2)` | ❌ | 中 | [ ] |
| IS NULL | all | `col IS NULL` | `col IS NULL` | `isNull` | `col IS NULL` | ✅同一 | — | |
| **IS NOT NULL** | all | `dbNotNull` | `Value::NotNull` | ❌欠落(isNullのみ) | `col IS NOT NULL` | ❌ | 高 | [ ] |
| boolean | PG | `= TRUE`literal | `= ?`bound | `= ?`bound | 要決定(PG tuned=`= TRUE`) | ⚠️差あり | 中 | [ ] |
| LIKE/NOT LIKE | PG | custom-op/raw | `Raw` | ❌欠落 | `col LIKE $1` | ❌ | 中 | [ ] |
| ILIKE | PG | custom-op/raw | `Raw` | ❌欠落 | `col ILIKE $1`(MySQL/SQLiteは`LIKE`・blindに落とさない) | ❌ | 中 | [ ] |
| BETWEEN | PG | custom-op | `Raw` | ❌欠落 | `col BETWEEN $1 AND $2` | ❌ | 中 | [ ] |
| **IN(subquery)** | PG | `DBSubquery` | `InSubquery` | ❌欠落 | `col IN (SELECT …)`(複合`(a,b) IN (…)`) | ❌ | 高 | [ ] |
| **NOT IN(subquery)** | PG | `DBSubquery` | `NotInSubquery` | ❌欠落 | `col NOT IN (SELECT …)` | ❌ | 高 | [ ] |
| **EXISTS** | PG | `DBExists` | `Exists` | ❌欠落 | `EXISTS (SELECT 1 …)` | ❌ | 高 | [ ] |
| **NOT EXISTS** | PG | `DBExists` | `NotExists` | ❌欠落 | `NOT EXISTS (…)` | ❌ | 高 | [ ] |
| 相関 parentRef | PG | `DBParentRef`→`t.col`inline | `parent_ref`inline | ❌欠落 | inlined `outer.col` | ❌ | 高 | [ ] |
| **raw / `__raw__`** | PG | verbatim/tuple | `Condition::Raw` | ❌欠落(WHERE未配線) | raw predicate + params(§13) | ❌ | 高 | [ ] |
| custom-op (key-with-?) | PG | verbatim | 無し(Raw) | ❌欠落 | author operator | ❌ | 中 | [ ] |
| AND grouping | all | `join(' AND ')`,nested`(…)` | 同 | tree AND,nested`(inner)` | `a=$1 AND b=$2` | ✅同一 | — | |
| **OR / 括弧** | PG | `__or__`二重包み | top-OR`(…)`,単一unwrap | top-OR包まず/nested単一`(x)` | 要確定(§2) | ⚠️差あり | 高 | [ ] |
| **空AND** | all | `''`(節ドロップ) | `1=1` | `''`(節ドロップ) | 節ドロップ | ⚠️.rs差 | 中 | [ ] |
| **空OR** | all | `''` | `1=0` | `''` | 要確定(意味差) | ⚠️差あり | 高 | [ ] |
| SKIP/optional | all | 暗黙(key無) | caller filter | `when`/`skipWhen`明示 | 不在fragment→SQL/param無 | ✅同一 | — | |
| SKIP on GROUP | all | 暗黙可 | 可 | ❌loud-reject(`compile-sqlite.ts:103`) | group条件droppable | ⚠️差あり | 中 | [ ] |
| **cast `::type`単体** | PG | `dbCast`→`?::uuid`(formatter, dialect-gate) | ⚠️`?::uuid`hardcode(dialect非対応) | ❌欠落 | `$1::uuid`(MySQL/SQLite=`?`) | ❌+.rsバグ | 高 | [ ] |
| **cast array `IN(?::t,…)`** | PG | `dbCastIn`(formatter) | ⚠️hardcode | ❌欠落 | `IN ($1::uuid,…)`(他=`IN(?,…)`) | ❌ | 高 | [ ] |
| cast array empty | PG | `1 = 0` | `1 = 0` | ❌欠落 | `1 = 0` | ❌ | 中 | [ ] |
| dynamic `col = fn(?)` | PG | `DBDynamicValue` | `Value::Dynamic` | ❌欠落 | `col = fn($1)` | ❌ | 中 | [ ] |
| **immediate `col = NOW()`** | PG | inline no-param | ⚠️`?`にstring bind(バグ) | ❌欠落 | `col = NOW()`inline | ❌+.rsバグ | 高 | [ ] |
| **tuple/composite IN** | PG | `dbTupleIn`→`(a,b) IN ((?,?),…)` | `db_tuple_in_pg`→UNNEST | ❌欠落 | `(a,b) IN (SELECT * FROM UNNEST($1::t[],…))`(他=value-list) | ❌ | 高 | [ ] |
| `= ANY($1::t[])`(条件) | PG | PK-helperのみ | UNNEST-helperのみ | ❌欠落 | `col = ANY($1::t[])`(他=IN展開) | ❌ | 中 | [ ] |
| value: date/json | PG | driver formatter/`::jsonb` | typed(chrono/serde) | ⚠️bc Value untyped | typed bind | ⚠️差あり | 中 | [ ] |

## B. INSERT / UPDATE / DELETE / placeholder / tail

| 構文 | dialect | .ts v1 | .rs | v2現状 | PGアンカー目標 | 差分 | 優先度 | ☐ |
|---|---|---|---|---|---|---|---|---|
| single INSERT | PG | `VALUES(?::cast…)` | `VALUES($1…)`(cast無) | `VALUES(?…)`(cast無) | 単一行+per-col`?::sqlCast`保持 | ⚠️差あり | 中 | [ ] |
| column order | all | caller順(明示list) | caller順 | JS object挿入順(`compile-dialect.ts:46`) | caller宣言順 | ⚠️差あり | 中 | [ ] |
| **batch INSERT** | PG | `UNNEST($i::type[])`+型推論(array/json/DBToken) | `UNNEST`(型粗い) | ❌欠落(単一行のみ) | PG UNNEST batch(型推論)+SQLite/MySQL=multi VALUES | ❌(bundle欠落→多言語batch不可) | 高 | [ ] |
| ON CONFLICT DO NOTHING/UPDATE | all | `EXCLUDED`/`excluded`/`VALUES(c)` | 同 | 同(`dialect.ts`) | 各方言verb | ✅同一 | — | |
| empty DO UPDATE cols→all | all | (暗黙) | 明示fallback(`model.rs:524`) | ❌fallback無→`DO UPDATE SET `破損 | empty→全列 | ⚠️差あり | 中 | [ ] |
| RETURNING bare | PG | `RETURNING id` | 同 | `RETURNING cols` | bare | ✅同一 | — | |
| RETURNING `table.col`(SQLite/batch) | SQLite/PG | `table.col`/`t.col` | 同 | ❌bare のみ | SQLite=`table.col`/PG batch=`t.col`alias | ⚠️差あり | 中 | [ ] |
| RETURNING MySQL simulate | MySQL | append+driver sim | 同 | ❌無条件`RETURNING`(sim戦略無) | append+sim | ⚠️差あり | 中 | [ ] |
| single UPDATE | PG | `SET c=?::cast` | `SET c=?` | `SET c=?`(cast無) | per-col`?::sqlCast`保持 | ⚠️差あり | 中 | [ ] |
| **batch UPDATE** | PG | `SET c=v.c FROM UNNEST(?::t[],…) AS v(cols) WHERE t.k=v.k` | 同(型粗い) | ❌欠落 | PG UNNEST batch+MySQL JOIN-VALUES+SQLite CASE-WHEN | ❌(bundle欠落) | 高 | [ ] |
| **SKIP-column batch** | PG | `CASE WHEN v._skip_c…`+`?::bool[]` | 同 | ❌欠落 | PG CASE/MySQL IF/SQLite WHEN | ❌ | 高 | [ ] |
| batch RETURNING alias | PG | `RETURNING t.col` | 同 | ❌欠落 | `RETURNING t.col` | ❌ | 中 | [ ] |
| single DELETE | all | WHERE必須(throw) | WHERE任意(全消し可) | WHERE必須(throw) | WHERE必須(v1=PGアンカー) | ⚠️.rs外れ値 | 低 | [ ] |
| IN-list DELETE / empty | all | `IN(?,…)`,空`1=0` | 同 | 同 | 同 | ✅同一 | — | |
| `?`→`$N` | PG | naive replace | quote-aware(literalスキップ) | naive(`dialect.ts:129`) | quote-aware(#42) | ⚠️差あり(.rsが安全) | 中 | [ ] |
| **LIMIT/OFFSET** | all | inline literal `LIMIT 10` | inline | **param `LIMIT ?`**(`compile-sqlite.ts:158`) | 要確定(v1 goldenと text不一致) | ⚠️差あり | 高 | [ ] |
| NULLS FIRST/LAST | PG | 無(raw order) | 無 | `orderByNulls`定義済だが**未呼出(dormant)** | PG native/MySQL emulation | ⚠️差あり | 低 | [ ] |
| **FOR UPDATE** | all | `FOR UPDATE` | `FOR UPDATE` | ❌欠落 | `FOR UPDATE` | ❌ | 中 | [ ] |
| FOR SHARE | PG | 無 | `FOR SHARE`(.rsのみ) | ❌欠落 | `FOR SHARE` | ❌(.rsのみ) | 低 | [ ] |
| GROUP BY | all | 有 | 有 | 有 | 有 | ✅同一 | — | |
| HAVING | all | 無(v1core) | `HAVING`(.rsのみ) | ❌欠落 | `HAVING` | ⚠️差あり | 低 | [ ] |
| APPEND raw tail | all | 有 | 有 | ❌欠落 | raw append | ⚠️差あり | 低 | [ ] |
| identifier quoting | all | 無 | 無 | 無 | 無 | ✅同一 | — | |

## C. Relation batch-load / 型 binding

| 構文 | dialect | .ts v1 | .rs | v2現状 | PGアンカー目標 | 差分 | 優先度 | ☐ |
|---|---|---|---|---|---|---|---|---|
| single-key batch | PG | `= ANY(?::type[])`(1配列param) | `= ANY(?::t[])` | `IN (?,?,…)`(N param,cast無) | `= ANY($1::type[])` | ❌欠落 | 高 | [ ] |
| single-key batch | MySQL/SQLite | `IN (?,…)` | `IN (?,…)` | `IN (?,…)` | `IN (?,…)` | ✅同一 | — | |
| PG配列型推論 | PG | sqlCast駆動(uuid/ts/date/json) | ⚠️text畳み(粗) | 無(cast無) | sqlCast駆動 | ❌+.rsバグ | 高 | [ ] |
| per-parent-limit | PG | `CROSS JOIN LATERAL(…LIMIT n)` | ⚠️`ROW_NUMBER()`(LATERAL非) | `ROW_NUMBER()` | `CROSS JOIN LATERAL`(v1アンカー) | ❌+.rs退行 | 高 | [ ] |
| per-parent-limit | MySQL/SQLite | `ROW_NUMBER()` | 同 | 同 | `ROW_NUMBER()` | ✅同一 | — | |
| **複合キー(無limit)** | PG | `JOIN unnest(?::t[],…)`/`(cols) IN (SELECT * FROM UNNEST)` | `(cols) IN (SELECT * FROM UNNEST)` | ❌**表現不可**(単一key IR) | UNNEST tuple-IN | ❌(IR破壊的変更要) | 高 | [ ] |
| 複合キー(無limit) | MySQL/SQLite | `(a,b) IN ((?,?),…)` | 同 | ❌ | tuple-IN | ❌ | 高 | [ ] |
| 複合キー+limit | PG | LATERAL composite | ⚠️ROW_NUMBER composite | ❌ | LATERAL composite | ❌ | 高 | [ ] |
| 複合キー+limit | MySQL/SQLite | ROW_NUMBER composite | 同 | ❌ | ROW_NUMBER composite | ❌ | 高 | [ ] |
| 複合キー dedup | all | NUL-join tuple | 同 | ❌(複合経路無) | tuple dedup | ❌ | 中 | [ ] |
| 単一key dedup | all | Set/JSON | caller upstream | `dedupeKeys`Set | dedup | ✅同一 | — | |
| empty key set | all | query無 | query無 | query無(assert用`1=0`) | query無 | ✅同一 | — | |
| group by target key | hasMany | Map/NUL複合 | 同 | 単一keyのみ | 複合対応 | ⚠️差あり | 中 | [ ] |
| **hardLimit強制** | hasMany | 有(`DBModel.ts:1703`) | 有(`LIMIT n+1`+count) | ❌欠落 | hardLimit probe+throw | ❌ | 中 | [ ] |
| relation `where`filter | all | `config.conditions`merge | ⚠️decl有るが未配線 | ❌field無 | batch SELECTにmerge | ❌ | 中 | [ ] |
| ORDER BY(unlimited) | all | append | append | append | append | ✅同一 | — | |
| typed bind bool | PG | typed array | typed | plain`?` | typed | ⚠️差あり | 中 | [ ] |
| typed bind uuid | PG | `?::uuid`/`uuid[]` | `PgParam::Uuid` | text`?` | `?::uuid`/`=ANY(?::uuid[])` | ❌ | 高 | [ ] |
| typed bind timestamp/date | PG | typed | typed(chrono) | ISO string | typed | ⚠️差あり | 中 | [ ] |
| typed bind json | PG | `::jsonb` | `PgParam::Json` | 無 | `::jsonb` | ❌(PG) | 低 | [ ] |
| array param `::type[]` | PG | `= ANY`/unnest核 | `?::type[]` | 無(IN展開) | 1配列param`::type[]` | ❌ | 高 | [ ] |
| typed-IN helper `db_cast_in`/`db_uuid_in` | PG | (sqlCast) | `CastArray`→`?::type` | 無 | typed-cast IN | ❌ | 中 | [ ] |
| composite-IN helper `db_tuple_in_pg`(UNNEST) | PG | inline | `values.rs:439` | 無 | UNNEST composite-IN | ❌ | 高 | [ ] |

---

## 3. 高優先度 マスターロールアップ（実装の起点）

**IR 破壊的変更を伴う（§8 bundle + Dialect strategy + 全言語 render + conformance 再構築）:**
1. 複合キー relation（`RelationOp` を複数キー化 + PG UNNEST tuple-IN / 他 tuple-IN）— C
2. PG 単一キー relation `= ANY(?::type[])`（dialect 別 relation-IN fragment + 配列 param slot）— C
3. PG per-parent-limit `CROSS JOIN LATERAL`（v1 アンカー、.rs の ROW_NUMBER は非採用）— C
4. batch INSERT（PG UNNEST+型推論 / SQLite・MySQL multi-VALUES）を bundle で表現 — B
5. batch UPDATE（PG UNNEST / MySQL JOIN-VALUES / SQLite CASE-WHEN）+ SKIP-column を bundle で表現 — B
6. WHERE 条件語彙拡張: subquery(IN/NOT IN/EXISTS/NOT EXISTS/相関)、IS NOT NULL、cast(`::uuid`/array/empty)、raw/§13 escape、tuple/composite IN、immediate inline — A
7. typed binding（uuid/timestamp/date/json/array/bool）を PG で型付き — A/C

**text-breaking（v1 golden と不一致・要確定）:**
8. LIMIT/OFFSET を inline(v1) か param(v2) か — B
9. OR/空OR/単一要素の括弧規則、boolean literal vs bound を PG アンカーで pin — A

**中: per-col `?::sqlCast`、RETURNING `table.col`/MySQL sim、empty-DO-UPDATE→all fallback、`?`→`$N` quote-aware(#42)、FOR UPDATE、relation where-filter、hardLimit、NOT IN(list)/LIKE/BETWEEN/dynamic/custom-op、grouping 複合対応。**

**原実装バグは踏襲しない**（§1）。**3実装割れは PG アンカーで確定**（§2）。

---

## 進め方
1. 本チェックリストの各 ❌/⚠️ を issue のサブタスクに（#43 配下）。IR 破壊的変更（1-5）を先に設計確定。
2. **conformance golden を旧 tuned 実装（.ts LazyRelation/SqlBuilder + .rs、PG SQL）に対して再生成**（縮小基準を捨てる）。
3. PG から実装→MySQL/SQLite 導出→5言語 render→byte 一致・実 DB(docker) 検証。
4. #44 の 1.x 最終版 vs v2 ベンチで定量回帰確認。

*出所: 旧 `litedbmodel.rs/litedbmodel/src/{condition,values,model,relation,handler,driver/*}.rs`、旧 `litedbmodel/src/{DBConditions,DBValues,DBModel,LazyRelation,drivers/*}.ts`、v2 `litedbmodel/src/scp/{render,dialect,compile-sqlite,compile-dialect,bridge,relation,write-plan,runtime,ir}.ts`。file:line は各 issue に詳細。*
