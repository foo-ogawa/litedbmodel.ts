# HANDOFF — Rust参照実装から全言語へ展開

更新日: 2026-07-20  
worktree: `litedbmodel-wt-phase-a`  
branch: `feat/bench-rebuild`  
Rust参照HEAD: `1c0307a`  
対象epic: #123  
再openして是正したissue: #131 / #138 / #140

## 0. 結論

全言語のproduction経路は次の2要素だけにする。

1. op固有の単一generated module
2. 言語ごとの固定native runtime

production generated module/runtimeに、IR、Node、JSON metadata、runtime parser/evaluator、op名lookup、別sidecar、`companion_*`を入れない。

```text
application / benchmark
  -> generated_<op>.run(...)
  -> <language>_runtime.exec / execute_relation_batch
  -> database driver
```

検証経路はproductionから物理分離する。

```text
oracle / conformance tool
  -> generated native op
  -> interpreter package
       -> native runtime
```

依存方向は `interpreter -> native runtime` の一方向。production generated moduleとbenchmark binaryはinterpreterへ依存してはならない。

## 1. Rustで確定した責務境界

### 1.1 generated module

Rust成果物は `rust/orm_bench/src/gen/generated_<op>.rs` の1ファイルだけ。

含めるもの:

- BC生成core（型、typed input/output、typed de-box、ports、runner）
- op固有のstatic SQL/params
- 最小static adapter
- consumer向け `run(...)`
- relation opの場合は `hydrate_<relation>(...)`
- child relationのBC coreとdirect call chain

含めないもの:

- `companion_<op>.rs`
- relation child sidecar
- manifest/setup/bundleのJSON file
- `relation_ops_json()` / `relation_op()`
- `Node` / readGraph / childRelations metadata / interpreter bundle
- stash handler (`RefCell<Option<Wire>>`等)
- portsを無視するdecoder専用handler
- `let _ = &...` dummy reference
- blanket `allow(dead_code)`

生成実装の中心:

- `src/scp/codegen.ts:1499` 付近 — static adapterから共通runtimeへ委譲
- `src/scp/codegen.ts:1685` 付近 — relation childを`compileEager(RelationBatch)`でSCP/BC宣言からcompile
- `src/scp/codegen.ts:1727` — nested relation hydratorをcodegen時にdirect-call展開
- `src/scp/codegen.ts:1799` — generated file内static adapter
- `src/scp/codegen.ts:1955` — executable generated moduleの公開生成API

### 1.2 native runtime

Rust production runtimeは `rust/litedbmodel_runtime`。

責務:

- Driver / connection / transaction
- placeholder render / bind
- single `exec`
- Wire / typed de-box境界
- relation batchのvalue-only共通処理
- structured error

relationのSSoT:

- `rust/litedbmodel_runtime/src/relation.rs:74` — `execute_relation_batch`
  - empty-key
  - key dedupe
  - dialect cast
  - bind
  - exec
  - hardLimit
- `rust/litedbmodel_runtime/src/relation.rs:127` — `hydrate_children`
  - group
  - matched children lookup
  - parentへのdistribute

`RuntimeError::Limit`をSQL errorへ変換してはならない。variant/context/limit/fetched/modelを保持する。

### 1.3 interpreter / oracle

- `rust/litedbmodel_interpreter`: 非公開のinterpreter専用crate
- `rust/litedbmodel_oracle`: nativeとinterpreterを直接比較する非公開test crate

`litedbmodel_runtime`からNode/parser/evaluator/bundle interpreterは物理削除済み。ORM/E1 benchmarkは`litedbmodel_runtime`だけに依存する。

oracle fixtureは `benchmark/crosslang/oracle-fixture-build.ts` がcanonical SCP/BC宣言からtest-only native Rust sourceを生成する。手書きIR、JSON file、JSON transportは禁止。

oracle比較対象:

- nestedRelations: posts/commentsの全field
- create: return + DB state
- nestedCreate: commit result + DB state
- relation hardLimit: error variant + 全field
- SQLite/PostgreSQL/MySQLの同一初期状態

## 2. consumer API

通常opは1 call。

```rust
let rows = generated_findAll::run(
    driver,
    generated_findAll::InNRFindAll,
)?;
```

relation opは2 call。consumer側でmetadata parse、key group、child query orchestrationをしない。

```rust
let users = generated_nestedRelations::run(
    driver,
    generated_nestedRelations::InNRFindAll,
)?;
let tree = generated_nestedRelations::hydrate_posts(users, driver)?;
```

nested relationの2段目はgenerated hydrator内のdirect call chain。取得したcommentsを捨てず、戻り値のtyped treeへ含める。

## 3. ベンチ実装

### 3.1 native

- op本体: `rust/orm_bench/src/main.rs:104` の `prepare_op`
- timed loop: `rust/orm_bench/src/main.rs:346-360`
- 19 opすべてgenerated `run`、relationだけgenerated `run + hydrate`
- benchmark binaryのdependency closureにinterpreter/oracle/serde_jsonなし

timed区間:

```rust
let t = Instant::now();
run(g);
let us = t.elapsed().as_micros();
```

reseed、closure構築、warmup、oracle、CSV整形はtimed外。

### 3.2 SDK baseline

- op本体: `rust/orm_bench_sdk/src/main.rs:368` の `run_op`
- timed loop: `rust/orm_bench_sdk/src/main.rs:707-719`
- rusqlite / postgres / mysqlのraw driverを直接利用
- litedbmodel依存なし
- setupはJSONではなくcodegen-owned `generated_setup.rs::STATEMENTS`
- prepared statement/cacheを再利用するcompetent baseline

### 3.3 再現条件

今回の正式測定:

- release build
- `REPS=500`
- `WARMUP=50`
- 19 ops
- SQLite / PostgreSQL / MySQL
- nativeとSDKで同じfixture、同じ入力、同じ回数
- native -> SDK -> collectを単独直列実行
- benchmark中に別codegen swap/docker作業を並行させない

```bash
node --import tsx benchmark/crosslang/codegen-build.ts

cd rust/orm_bench
REPS=500 WARMUP=50 bash run-pilot.sh

cd ../orm_bench_sdk
REPS=500 WARMUP=50 bash run-sdk.sh

cd ../..
node --import tsx benchmark/crosslang/pilot-collect.ts
```

raw data:

- `benchmark/crosslang/.results/native.csv` — 28,500 samples
- `benchmark/crosslang/.results/sdk.csv` — 28,500 samples
- 集計: `benchmark/ORM-PILOT.md`

## 4. Rust benchmark結果

p50、単位ms。`x = native / SDK`。

| op | SQLite native/sdk (x) | PostgreSQL native/sdk (x) | MySQL native/sdk (x) |
|---|---:|---:|---:|
| findAll | 0.117 / 0.039 (3.00x) | 0.386 / 0.263 (1.47x) | 0.584 / 0.263 (2.22x) |
| filterPaginateSort | 0.083 / 0.046 (1.80x) | 0.269 / 0.206 (1.31x) | 0.513 / 0.266 (1.93x) |
| findFirst | 0.013 / 0.016 (0.81x) | 0.207 / 0.181 (1.14x) | 0.374 / 0.209 (1.79x) |
| findUnique | 0.011 / 0.005 (2.20x) | 0.210 / 0.191 (1.10x) | 0.391 / 0.235 (1.66x) |
| nestedFindAll | 0.592 / 0.145 (4.08x) | 1.063 / 0.649 (1.64x) | 1.750 / 0.604 (2.90x) |
| nestedFindFirst | 0.048 / 0.028 (1.71x) | 0.392 / 0.393 (1.00x) | 0.816 / 0.416 (1.96x) |
| nestedFindUnique | 0.047 / 0.018 (2.61x) | 0.366 / 0.367 (1.00x) | 0.820 / 0.394 (2.08x) |
| create | 0.449 / 0.312 (1.44x) | 0.191 / 0.199 (0.96x) | 0.431 / 0.237 (1.82x) |
| nestedCreate | 0.506 / 0.375 (1.35x) | 0.779 / 0.725 (1.08x) | 1.150 / 0.771 (1.49x) |
| update | 0.011 / 0.007 (1.57x) | 0.214 / 0.195 (1.10x) | 0.368 / 0.170 (2.17x) |
| nestedUpdate | 0.025 / 0.016 (1.56x) | 0.765 / 0.717 (1.07x) | 0.930 / 0.824 (1.13x) |
| upsert | 0.410 / 0.309 (1.33x) | 0.207 / 0.192 (1.08x) | 0.619 / 0.252 (2.46x) |
| nestedUpsert | 0.486 / 0.314 (1.55x) | 0.760 / 0.892 (0.85x) | 1.127 / 1.021 (1.10x) |
| delete | 0.586 / 0.436 (1.34x) | 0.739 / 0.727 (1.02x) | 1.151 / 0.815 (1.41x) |
| createMany | 0.573 / 0.437 (1.31x) | 0.248 / 0.223 (1.11x) | 0.554 / 0.287 (1.93x) |
| upsertMany | 0.469 / 0.367 (1.28x) | 0.293 / 0.330 (0.89x) | 0.594 / 0.393 (1.51x) |
| updateMany | 0.071 / 0.019 (3.74x) | 0.234 / 0.215 (1.09x) | 0.420 / 0.191 (2.20x) |
| nestedRelations | 1.496 / 0.363 (4.12x) | 2.896 / 1.335 (2.17x) | 4.170 / 1.373 (3.04x) |
| compositeRelations | 0.431 / 0.043 (10.02x) | 0.671 / 0.639 (1.05x) | 1.333 / 0.584 (2.28x) |

19 opのp50比の幾何平均:

- SQLite: 2.01x
- PostgreSQL: 1.13x
- MySQL: 1.89x

PostgreSQLは19件中12件がSDK比1.10x以内。主な残性能課題はrelationとSQLite/MySQLのtyped conversion/batch distribution。正しさを崩す最適化、結果を捨てる最適化、SDK baselineを遅くする比較は禁止。

## 5. 全言語展開の不変条件

以下をTS/Go/Python/PHPすべてに適用する。

### 5.1 production artifact

- 1 op = 1 generated module
- consumer公開APIは`run`、relationは追加で`hydrate_<rel>`
- op固有static dataはgenerated module
- 実処理は固定runtimeへ集約
- runtime executorはop名やmodel名で分岐しない
- relation SQL/key/shapeはnative literal/typed ports
- childRelationsはcodegen時direct call
- productionにIR/Node/JSON/interpreterなし

### 5.2 禁止

- `companion_*` / adapter sidecarを別生成
- JSON manifest/setup/bundle/oracle fileを生成・parse
- metadata dictionary/mapを実行時walk
- generated SQLを無視して別loaderが別SQLを実行
- stash/decode-only handler
- consumer側relation orchestration
- benchmark timed内のsetup/parse/convert/CSV処理
- smoke実行をnative≡interpreter proofと呼ぶ
- fixed `PASS`出力
- interpreter fallbackをproduction実装として採用
- capability不足を別issueへ移してclose

### 5.3 Python/PHPについて

旧案の「IR literalを埋め込みinterpreter実行」は、このhandoffでは不採用。Rust参照実装のproduction invariantに反するため。

Python/PHPのnative/static emitterが不足している場合:

1. emitter capabilityを実装する
2. generated op + fixed runtimeを作る
3. oracleでinterpreterと比較する

IR/JSON埋め込みへ戻してAC clearしてはならない。

## 6. 言語別実装順

issue単位で1言語ずつ実装し、別担当が監査する。同時にcodegen outputやDocker DBをswapしない。

### Go (#125)

1. `generated_<op>.go` へtyped BC core + static adapterを統合
2. `go/runtime`へsingle exec/Wire/relation core
3. interpreter/conformance packageをproduction依存から分離
4. 19 op consumerを1 call / relation 2 call化
5. 3DB oracle
6. raw database/sql baselineと同条件bench

### TypeScript (#126)

1. `generated_<op>.ts`へtyped core + adapter
2. runtimeはdriver/exec/bind/relation/errorだけ
3. productionからIR evaluator/JSON metadataを除外
4. 19 op consumer
5. 3DB oracle
6. raw driver baseline bench

### Python / PHP (#128)

1. native/static emitter capabilityを先に作る
2. generated opとfixed runtimeの責務をRustと一致
3. interpreterはtest packageへ物理分離
4. 19 op consumer
5. 3DB oracle
6. raw driver baseline bench

### cross-language bench (#127)

各言語のAC clear後にのみ統合する。未完成言語をreference cell、fallback、skipで埋めない。

## 7. 言語ごとのAC

- [ ] generated artifactは1 op 1 module、sidecar 0
- [ ] generated/runtimeのJSON output/read/parse 0
- [ ] production dependency closureにinterpreter/IR/Node 0
- [ ] relation executor SSoT 1本
- [ ] child relationはSCP/BC declarationからcompile
- [ ] nested resultに全child dataが残る
- [ ] Limit error variant/fields parity
- [ ] read/relation/write/txのnative≡interpreter direct comparison
- [ ] SQLite/PostgreSQL/MySQL oracle PASS
- [ ] 19 op consumerが1 call、relationのみ2 call
- [ ] benchmark timed区間がmain op callのみ
- [ ] raw SDK baselineがprepared/cacheを適切に再利用
- [ ] generated driftはstale extra fileを含めnonzero
- [ ] 禁止pattern negative injectionがFAIL
- [ ] dead dummy/private codeなし
- [ ] 別担当監査PASS

## 8. 運用

- 子issueは実装commit後、独立監査PASS、push成功の順でclose
- #123はmain merge/publishまでopen
- main merge/publishはowner承認必須
- 旧closeに未達ACが見つかったら元issueをreopenする。新issueへ洗浄しない
- benchmark中は他のswap/docker/codegen作業を並行させない
- 既存dirtyを勝手にstage/commitしない

## 9. Rust参照commit

- `75f72b6` — relation batchをgenerated moduleへcompile
- `a164300` — 初回監査gap修正
- `26a93bc` — native runtimeとoracle/interpreterを物理分離
- `d2965f3` — self-contained 3DB oracle
- `1bcfacb` — SDK/setup/catalog/drift/dead-code監査gap修正
- `1c0307a` — generated generic dummy reference除去

最終的なRust判定: #140 AC clear、独立監査blocking findingなし。pushは未実施。
