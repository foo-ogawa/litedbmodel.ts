# litedbmodel v2 completeness — 実行計画（全サブエージェントの SSoT）

> 唯一の正典。全サブエージェントはこのファイルを渡され従う。個別プロンプトで「雰囲気」の指示を出さない。
> （このファイルは別エージェント監査でIF誤り・欠落 op・live 盲点を是正済み。）

## 0. モデル

```
DSL contract
  → compile（TS のみ。v1 builder を駆動して最適化 SQL を生成）
  → 可搬 makeSQL bundle（SQL文字 + params + skip。純JSON。方言別）
  → bc（全5言語 published）が bundle を汎用実行（runBehavior/runPlan）
  → 各言語 runtime は bundle を replay するだけ（SQL を生成しない）
```

- **最適化 SQL は TS で1回だけ作る**（v1 builder から verbatim コピー）。
- **多言語展開は bc の共通処理**。言語ごとに SQL/ロジックを作り直さない。
- **言語別 runtime = SQL 非依存の薄い plumbing だけ**。必要に応じてのみ。

## 1. タスクは足し算。掛け算のチェックは自動試験だけ。

- **作業は足し算**: 「op を1つ追加」= TS で SQL を1回作る（v1 コピー）+ bundle/catalog に載せる ＝ 1タスク。方言・言語の掛け算タスクは存在しない。
- **掛け算（op × 方言 × 言語）は自動試験が回す**:
  - `test/scp/makesql-golden.test.ts` — compile 出力 SQL を **v1 builder の直接出力に byte 一致** assert（`DBConditions.compile`/`LazyRelationContext`/`*SqlBuilder`/実 `DBModel._insert` capture を直接呼ぶ）。SQL 正しさの正典ゲート。
  - `conformance/vectors/*.json` + drift gate。
  - `conformance/vectors-livedb/livedb.json` — 実 PG+MySQL+SQLite 実行で結果一致（`::text[]` 級を捕まえる）。
  - cross-lang agreement — 全5言語が同一 bundle を同一 replay。
- **各 op は「byte-match 試験（全方言）」と「live 実行（PG+MySQL）」の両方**を満たして初めて done。byte だけ／PG だけ／SQLite mock だけ は不可（不変条件#3）。

## 2. モジュール IF（固定。逸脱禁止。拡張は「ケース追加」であって署名変更ではない）

- `src/scp/makesql/compile-crud.ts`: `builderFor` / `compileInsert` / `compileInsertMany` / `compileUpdateSingle` / `compileUpdateMany` / `compileDelete` / `compileDeleteMany` / `compileFindByPkeys`（+ `*BuildOptions`）。`(dialect, options) → MakeSQL|MakeSQL[]`、SQL は v1 builder 由来。
- `src/scp/makesql/compile-relation.ts`: `compileSingleKeyUnlimited` / `compileSingleKeyLimited` / `compileCompositeKeyStaticUnlimited` / `compileCompositeKeyUnlimited` / `compileCompositeKeyLimited` / `inferPgArrayType` / `resolvePgArrayCast` / `PG_ARRAY_CAST_TOKEN` / `RelationCompileBase`。
- `src/scp/relation.ts`: `RelationOp` / `RelationDecl` / `compileRelationOp`（compile 入口）/ `runRelationOp` / `distributeToParent`（single+composite 対応済）。
- `src/scp/makesql/compile-select.ts`: `compileSelect` / `SelectDesc`（head/GROUP BY/ORDER BY/LIMIT/OFFSET/FOR UPDATE/join/cte/append/HAVING を担う）。
- `src/scp/makesql/tx.ts`: `TxOp`/`TxStatement`/`TransactionPlan`/`GateRule`/`StatementRole`/`compileWriteNode`/`deriveTransactionPlan`/`deriveBatchPlan`/`executeTransaction`/`renderTxStatement`/`mysqlPkHint`/`stripMysqlPkHint`/`literalize`。
- `src/scp/catalog.ts`: `CatalogName = Select | Count | Insert | Update | Delete | Fragment | Tx`、`LITEDBMODEL_CATALOG`。（**relations は catalog entry でなく `compileRelationOp`。makeSQL は bundle 成果物で catalog 名ではない。**）
- `src/scp/makesql/static-bundle.ts`: `compileStaticBundle` / `compileReadGraph` / `compileSelectNode` / `executeReadGraph` / `executeReadGraphAsync` / `executeStaticBundle` / `executeStaticWrite` / `renderReadPrimary`。
- **bundle JSON shape**: `{ readGraph, statementsById, relations, transaction, optionalHeads }`（方言別）。新「kind」を勝手に増やさない（増やすならエスカレーション）。
- **runtime handler seam**（全言語）: bundle replay のみ。SQL 生成禁止。言語差は param bind のみ。

### §2 補足（監査 + V0 で判明・確定）
compile 入口は2つ: **byte 経路**（`compileWhere/compileSelect`、多構文が v1 一致検証可）と **live 経路**（`L.Select→compileBundle→compileSelectNode`、replay 可能 bundle を生む唯一路。WHERE=bc closed-set、SELECT=`SELECT_PORTS`）。**構文を live 到達可能にするには authoring 面を拡張する** — これは「足し算」として**承認済み**（署名を壊さず口/primitive を追加する。逸脱ではない）:
- **where-primitive 追加**（`authoring-sql.ts`）: `whereExists`/`whereInSubquery`(+NOT)/`whereBetween`/`whereLike`(ILIKE)/`whereTupleIn`/raw-cast 等。subquery は「param slot の入れ子 makeSQL/Fragment」として符号化（`src/scp/index.ts` header の想定通り）。
- **`SELECT_PORTS` 口追加**（`catalog.ts` + `compileSelectNode` 配線）: `forUpdate`/`cte`/`join`/`joinParams`/`append`（`SelectDesc` に既にフィールドあり、SQL は v1 `_buildSelectSQL` 由来）。
- **bundle shape 追加**（R1）: relation statement に per-statement の dialect/connection タグ。

## 3. 不変条件（非交渉）

1. SQL は v1 原本 verbatim コピー（golden-from-originals）。手書き・再発明禁止。
2. bc プリミティブで表現。できない構造は hand-roll せず**エスカレーション**。
3. **live-DB ゲート必須（実 PG+MySQL+SQLite）。byte 一致だけ・PG だけ・SQLite mock だけ は不可。**
4. 検証は bundle レベル（§1 試験群）。言語ごとに SQL 再実装・再検証しない。
5. 中途半端禁止（byte 全方言 + live 実行で初めて done）。
6. eager 経路無改変。
7. ゴミなし。

## 4. 作業項目（足し算リスト）と状態

### DONE（commit 済・現行スイート全 green: golden 135/135・conformance 5×36/36・livedb 76/76・ScpDialect 35/35）
- Write: createMany/updateMany（golden byte-assert 済）; deleteMany（**live-assert のみ**＝ScpDialect+livedb corpus。golden byte-assert は V0 で追加）; tx variants（idempotency/unique/edge/composite-DAG）; MySQL RETURNING 実 PK 基準。
- Read: `count()`（golden §B COUNT）; composite-key relation（golden §C）; PG 配列全型 live; hand-roll 除去（golden §B）; relations belongsTo/hasOne/hasMany/hasMany-limit（golden §C, live）。

### REMAINING（足し算。各 = TS で v1 SQL 1回 + byte-assert 追加 + live vector 追加）
- **V0. coverage matrix（最初）**: 全 op × 全方言について `{byte-match?, live-exec on PG+MySQL?}` の網羅マトリクスを作り、欠けを列挙。これが以降の確定スコープ。byte だけ/PG だけ/SQLite だけ を全て gap 認定。
- **R1. cross-DB relations**: v1 は relation を target の driver/接続で実行（`LazyRelation.ts:236`）。**bundle-shape を先に確定**: 「per-statement に dialect/connection タグを付す」方式を採る（bundle 分割でなく relation statement に接続タグ）。SQL は TS で1回 v1 由来、言語側は接続ルーティング（SQL 非依存 plumbing）。
- **R2. subquery / EXISTS / NOT EXISTS / NOT IN(subquery) / composite IN(subquery)**: 現状 PG byte のみ・live ゼロ。MySQL+SQLite の byte-assert 追加 + PG+MySQL live 実行。
- **R3. GROUP BY / FOR UPDATE / BETWEEN / ILIKE / tuple-IN / dbCast / dbDynamic / dbImmediate**: render byte はあるが live 未実行。PG+MySQL live 実行を追加。
- **R4. CTE / query-based model reads**（`withQuery`/`cte` port, `_buildSelectSQL` WITH ラップ）: byte+live 追加。
- **R5. JOIN（`join`/`joinParams`）**: byte+live 追加。
- **R6. append / HAVING**: byte+live 追加。
- **R7.（決定済）** `compileFindByPkeys` は dead SCP export → **削除**（+ 2 re-export）。single-key は `whereIn`+`inColumn` IN-list、composite は composite relation batch で live 済＝機能重複。v1 `buildFindByPkeys` は `DBModel.findById` が使うので残す。
- **R8. FIND_FILTER（モデル毎グローバル soft filter, `DBModel.ts:597/859`）**: **silent waive 禁止**（filtered モデルで soft-delete/tenant scope が漏れる correctness 影響）。v1 の適用点を特定し、(a) SCP compile 経路で auto-適用して v1 再現、または (b) SCP contract の上流で既に WHERE に畳まれている（＝makeSQL では no-op）ことを**証拠付きで**確定。どちらかを実証してから done。
- **V1. comprehensive 監査**（別エージェント）: 全 op が byte（全方言）+ live（PG+MySQL）green、golden-from-originals negative test、hand-roll 残存ゼロ、cross-lang 一致、ゴミなし。
- **D1. bc 依存 → 最新 0.2.3**（0.2.2 でない）: TS `^0.2.0`→`^0.2.3`、go/rust/python `0.2.0`→`0.2.3`、PHP は vendored 再取り込み。conformance green 維持。最終ベンチ前必須。
- **REL. release**: `fix43-sql-parity` → main PR/merge、**2.0.0 → 2.0.1**、npm/PyPI/crates/go publish（承認済・都度確認不要）。
- **BENCH（#44）**: cross-lang 実行サーフェス（sql/codegen/ir/dynamic/prepared）+ v1 比較（TS 1.2.10・可能なら旧 .rs）。graphddb#307 同型。完走。

## 5. 実行順序

1. **V0**: coverage matrix 作成（byte × live × 全方言 × 全 op）。確定スコープを出す。
2. **R1–R8**: matrix の欠けを足し算で埋める（各 TS 1回 + byte-assert + live vector）。
3. **V1**: comprehensive 監査（FAIL→修正→再監査）。
4. **D1**: bc 0.2.3。
5. **REL**: 2.0.1。
6. **BENCH**: #44 完走。

## 6. サブエージェント運用

各エージェントに本ファイルを渡す。担当は「§4 の項目 X を足す」だけ指定。**§2 の IF に適合・§1 の試験に byte-assert と live vector を足す・bundle レベル検証・hand-roll せずエスカレーション・green 増分 commit・ゴミなし。**
