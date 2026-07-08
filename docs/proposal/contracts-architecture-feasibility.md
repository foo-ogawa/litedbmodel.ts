# SQL Contracts Architecture — フィージビリティ分析（訂正第2版）

`contracts-architecture.md` の提案に対する実現可能性の調査・分析。
既存の兄弟プロジェクト（graphddb / dsl-contracts / litedbmodel.rs / litedbmodel-gen）と
現行 litedbmodel(ts) の実装を突き合わせて評価した。SCP（Semantic Contract Programming,
[dsl-contracts/semantic-contract-programming.md](../../../dsl-contracts/semantic-contract-programming.md)）を上位の配線層とする。

> **訂正履歴**
> - 初版: 「ORM API 非公開＝litedbmodel の公開 API 廃止」と誤読 →「公開IF反転で互換不能」と誤結論。
> - 訂正第1版: litedbmodel は CQRS 内側の振る舞い記述基盤と訂正。ただし「eager と capture の
>   二重意味論ゆえ別ライブラリ必須」「Dialect 非依存 SQL IR が最難関」「LazyLoading は IR と両立不能」
>   と判断していた。
> - **訂正第2版（本版）: 上記3判断をすべて訂正。**
>   ① graphddb 自身が TS では eager 利用（呼び出し時に動的コンパイル→同一 IR/plan 経路で実行）であり、
>   「二重意味論」は存在しない。in-place 進化は成立する。
>   ② IR は **Dialect 依存（dialect SQL + param バインディング + wire オペレーション）**が正。
>   原提案の「Runtime は SQL を生成しない / Static Flatten」に立ち返る。
>   ③ LazyLoading は「事前コンパイル済み relation IR の実行時起動」として **IR 化可能**。
>   graphddb の `options.hydrate` と整合させる。

---

## 結論（TL;DR）

1. **アーキテクチャは実現可能。当初想定よりさらに低リスク。**
   骨格（Contract → Planner → IR → 多言語 thin Runtime → Conformance）は graphddb が本番実装済み。
   IR を dialect 解決済みにすることで、残っていた最難関（Dialect 非依存 SQL IR の設計）も消滅する。

2. **単一意味論に統合できる: 「宣言 → IR → Runtime」が唯一の実行経路。**
   TS では呼び出し時に動的コンパイル（eager UX、キャッシュ可）、多言語へは同じコンパイル経路の
   AOT 出力（bundle）。graphddb と同じ構図。**eager/capture の対立は存在しない**。
   対立軸は「引数が宣言データか、制御フローcoded かつホスト言語埋め込みか」だけ。

3. **litedbmodel の in-place 進化（メジャーバージョン）が既定路線として成立する。**
   condition タプル・Values・SKIP は既に宣言データであり、`find()` の内部を
   「文字列即ビルド」→「IR コンパイル→runtime 実行」に差し替えられる。
   `await user.posts` も残せる（§9）。別プロダクト切り直しは意味論の要請ではなく、
   **API 破壊量が既存ユーザに対して大きすぎる場合の製品判断**（その場合もコア資産は移転）。

4. **LazyLoading は typed-object でも成立する（prototype + 事前コンパイル済み relation IR）。**
   不変条件は「Runtime は SQL を生成しない」であり、「Runtime は実行タイミングを決めない」ではない。

5. **残る本質的な制約は1つ: ホスト TS に埋め込まれた命令的な *オペレーション間配線* は移植できない。**
   多言語へ持ち出す配線は SCP（Wire/DAG）で宣言する。自由 TS から呼ぶか SCP で配線するかは
   プロジェクト方針（両者は同じ IR+Runtime を使うため、自由 TS → SCP への昇格はリファクタで済む）。

---

## 1. 位置づけ（前提の確認）

```
外部境界              = CQRS Contract（Query / Command）
  └ 振る舞いの記述     = litedbmodel API（find/create/update, conditions, SKIP, relations）
       ↓ コンパイル（TS 内で動的 or AOT）
     IR = dialect SQL + param binding + wire ops + assembly + relation ops（§5）
       ↓
     thin Runtime（TS / Go / Rust / Python / PHP）: validate → slot 評価 → bind → execute → assemble
```

- litedbmodel は CQRS の**内側**で Command/Query の振る舞いを書くオーサリング基盤（廃止しない）。
- SCP 準拠で書かれたロジック（宣言的引数 + SCP 配線）は IR 化されるので多言語へ移行可能。
- 通常の開発では litedbmodel の利用を SCP 内部に閉じるよう記述する。外の自由 TS から直接呼ぶことも
  でき（eager UX）、その場合も同じ IR+Runtime 経路を通る — ただし配線がホスト TS に残るため
  その部分は TS 専用。

---

## 2. 単一意味論: eager UX と IR コンパイルの統合（訂正）

**訂正第1版の誤り:** 「eager と capture の二重意味論の同居は保守コスト高 → 別層で切るべき」。

**事実:** graphddb は TS では eager に使える。呼び出し時に planner が走り、静的ジェネレータと
TS ホストランタイムが**同じ** `deriveExecutionPlan` を共有する（`src/relation/execution-plan.ts`）。
つまり意味論は常に1つ（宣言→plan/IR→実行）で、消費モードが2つあるだけ:

| 消費モード | コンパイル時点 | 用途 |
|---|---|---|
| 動的（eager UX） | 呼び出し時（結果はキャッシュ可） | TS アプリからの直接利用 |
| AOT（bundle 出力） | ビルド時 | 多言語 Runtime / SCP 配線 / conformance |

litedbmodel も同じ統合ができる。鍵は **`find(conds, opts)` の引数がすでに宣言データ**であること
（condition タプル、`sql` fragment、SKIP、Values、relation 宣言 — すべて構造を持つ値であり制御フローではない）。
変えるのは内部だけ: 「conditions → SQL 文字列即ビルド」を「conditions → IR → runtime 実行」へ。

**移植可能性の境界（唯一残るもの):**
オペレーション間の配線。`find` の結果を TS の if/for で加工して次の `find` を組む自由 TS は
ホスト専用。多言語に持ち出す配線は SCP の Wire/DAG（`{result.*}` 参照 + Expression IR）で宣言する。
1オペレーション単位は常に IR 化されているため、自由 TS で書いたロジックの SCP 昇格は
「配線部分を Wire に移す」リファクタで済み、別世界への書き直しにならない。

---

## 3. in-place か別プロダクトか（訂正）

**訂正:** 「別ライブラリ必須」の根拠（二重意味論）は撤回。graphddb 自身が contract DSL・planner・
runtime・codegen を**1パッケージ**に同居させ、eager TS 利用と bundle 出力を両立している。
in-place の前例はむしろ graphddb 側にある。

**既定路線: litedbmodel のメジャーバージョンとして in-place 進化。**
ユーザから見た継続性が高い:

| API 面 | 移行後 | 破壊度 |
|---|---|---|
| `find/findOne/count/create/update/delete` + condition タプル/SKIP | ほぼそのまま（内部が IR 経路になる） | 小 |
| `await user.posts`（lazy relation） | 残る（§9: prototype + 事前コンパイル IR） | 小 |
| 結果オブジェクト | DBModel インスタンス → **typed-object**（own props はデータのみ） | **中〜大** |
| インスタンス側カスタムメソッド | typed-object には無い → `options.hydrate` factory で載せ替え（graphddb 同形） | 中 |
| `sql` タグ / dbDynamic / dbRaw | Dynamic Slot 語彙として存続（lower 可能サブセット内） | 小 |
| 完全動的な Raw SQL 実行（`execute`/`query`） | TS 専用 escape hatch として存続（契約化すれば方言別 SQL を IR に同梱可） | 小 |
| Middleware / TypeCast | Runtime 関心事として存続 | 小 |

**別プロダクト切り直しの発動条件（製品判断):** typed-object 化（結果がクラスインスタンスでなくなる）
が既存ユーザに受け入れ不能な場合のみ。その場合も SqlBuilder・relation バッチ SQL・driver 層・
TypeCast はコア資産としてそのまま移転する。**意味論上はどちらでも成立する**ので、
これはアーキテクチャではなく npm パッケージ運用の判断。

---

## 4. IR の形: Dialect 依存（訂正・最重要）

**訂正第1版の誤り:** 「Dialect 非依存 SQL IR を設計し、各言語 Runtime が方言へコンパイル」。
これは原提案からの逸脱だった。原提案は `Dialect SQL → Runtime`（**Runtime は SQL を生成しない**、
Compile → Static Flatten → Runtime Expansion）と明記している。

**正しいフロー:**

```
litedbmodel API で記述
  → condition / values / relation を解析
  → dialect SQL（static flatten 済み）
    + param バインディング仕様
    + param のオペレーション（wire 接続で解決する部分 = {result.*} / Expression IR）
    を IR 化
  → Runtime で実行（SQL 生成なし）
```

**IR（1オペレーション）が持つもの:**

- **dialect SQL**: 静的に確定する部分は完全に flatten した SQL テキスト。動的部分を含む場合は
  **fragment tree**（事前コンパイル済み SQL 断片の順序付き木 + AND/OR 構造 + 各断片の存在規則）。
  **プレースホルダは全方言 `?` で統一**（Postgres も IR 内は `?`。現行 litedbmodel の内部表現 —
  `sql` タグ・各 SqlBuilder — と同じ規約であり、資産とそのまま整合）。
- **params 配列**: SQL テキストの `?` と1:1 対応。各要素は
  ①外部入力参照 `prop.x`（validate 対象） ②wire 参照 `{result.field}`（前段オペレーション結果）
  ③オペレータ IR `{add: [prop.y, 1]}`（閉じた語彙の純粋式・入れ子可）。

  ```
  "SELECT ... WHERE x = ? AND y < ?",  [prop.x, {add: [prop.y, 1]}]
  ```

- **動的断片の存在規則**: SKIP 評価（`input.x ?? SKIP` → 断片ごと落とす）、条件付き SET 句など。
- **order / pagination の変種**: 事前コンパイル済みの選択肢集合（自由文字列は不可）。
- **assembly 仕様**: 行 → 論理モデルへの組み立て（relation の items 付与位置を含む）。
- **relation ops**: モデル宣言から導出した全 relation バッチクエリ（§9）。

**Runtime の責務（全言語共通・SQL 知識不要):**

```
validate → 断片選択（SKIP/存在規則）→ 配列展開（IN (?) → (?,?,...)）
  → param オペレータ評価（wire 参照の解決を含む）
  → 【最終1パス】? → $N 変換（Postgres のみ。左から機械的に番号付け。MySQL/SQLite は恒等）
  → bind → execute → assemble
```

`$N` 変換を「param が完全にフラットになった最後」に置くことで、断片の採否・配列展開のたびに
番号を振り直す問題が**設計から消滅**する。変換は生成テキストの `?` を左から数えるだけの
機械処理であり、全言語で自明に一致する。

**この設計にする根拠（前例):**
- 原提案自身（Static Flatten / Runtime Expansion / Runtime は SQL を生成しない）。
- litedbmodel.rs: `#[derive(Model)]` が compile 時に静的 SQL を展開し、動的条件だけ実行時に合成
  （fast path は事前展開 SQL をそのまま返す）。
- graphddb: operations.json はバックエンド形（DynamoDB API 形）に**解決済み**のテンプレートを持ち、
  各言語 runtime はテンプレート bind と実行だけを行う。dialect SQL はその正確な RDB 類似物。

**リスクの縮小（conformance の分解):**
方言軸は **コンパイル時に TS 側で1回**検証する（既存 SqlBuilder + golden SQL テスト。方言差の
吸収資産 — RETURNING / ON CONFLICT / UNNEST / ANY / LATERAL — はここで再利用）。
言語軸は「同一 IR + 同一入力 → 同一の最終 SQL テキスト + 同一の assemble 結果」の機械的検証。
**3方言 × 5言語 = 15セルの行列が「3 + 5」に分解**され、訂正第1版で最難関としたリスクが解消する。

**残る本当の難所（正直な評価):**
- 動的断片の存在規則と AND/OR 構造木の仕様化（SKIP の合成順序、空 WHERE の縮退、括弧規則）。
  litedbmodel.rs の実装が参考になるが、多言語で決定的に一致させる仕様書が必要。
  （プレースホルダ番号振り直しは `?` 統一 + 最終1パス `$N` 変換の設計で解消済み。）
- **オペレータ IR の語彙と意味論**: → **[dsl-contracts/expression-ir.md](../../../dsl-contracts/expression-ir.md)
  で確定済み**。許可される TS 構文の閉集合とモノモーフィック IR、i64 checked / NaN・Inf は Failure /
  `%` は被除数符号 / 文字列比較は code point 順、等の規範。SCP Expression と同一語彙を共有する
  （conformance の対象は SQL テキストだけでなく bind 値も含む）。
- **`?` の純度保証**: コンパイラは生成 SQL テキストに placeholder 以外の `?` を出さないことを保証する。
  契約付き Raw SQL（ユーザ記述）にはエスケープ規約（例: `??`）を定める。
- 実行時にしか形が決まらない完全動的 SQL は事前コンパイル不能 → 契約付き Raw SQL
  （Input/Output/Effect を宣言し、方言別 SQL 文字列を不透明なまま IR に同梱）へ隔離。

---

## 5. 再利用マップ

### litedbmodel 資産 → 新アーキテクチャでの位置
- **方言別 SqlBuilder** → **コンパイラの方言バックエンド**（IR 生成時に1回走る。Runtime からは消える）
- **LazyRelation のバッチ SQL**（LATERAL / ROW_NUMBER / `= ANY` / 複合キー unnest）→ relation ops の
  事前コンパイル内容としてそのまま（§9）
- **driver 層**（プール・reader/writer・retry・keepAlive）→ TS Runtime
- **TypeCast / Middleware** → Runtime 関心事
- **SKIP / dbDynamic / dbCast / sql`` / Conditions** → Dynamic Slot 語彙（lower 可能サブセットの中核）

### graphddb から借りる骨格
- contract DSL（`publicQueryModel`/`publicCommandModel`）、2層 IR + JSON bundle、planner、
  relation 実行 DAG（`deriveExecutionPlan`）、codegen、golden conformance、
  `options.hydrate`（§9）、RetryPolicy の階層解決（scp-error の優先順位仕様の元）

### dsl-contracts / SCP
- Portability Guard（lower 可能サブセットの強制）、Contract Service IF、
  SCP Wire/DAG = オペレーション間配線の宣言層、Error Policy（Kind=IR / Tuning=Runtime）

### 新規に作るもの
- RDB 用 Access Pattern Contract（Query/Command）
- **Operation IR（dialect SQL + fragment tree + param slots + assembly + relation ops）**と
  その動的展開仕様（§4 の「残る難所」）
- litedbmodel API → IR のコンパイラ（condition/values/relation 解析。SqlBuilder を方言バックエンド化）
- 各言語 thin Runtime + codegen + conformance（graphddb 方式のクローン）
- Transaction DAG 導出（後段スコープ、graphddb の mutation-derivation が参考）

---

## 6. リスク（訂正第2版）

1. **動的展開仕様の決定性**（§4）— 最難関だった「Dialect 非依存 SQL IR」の後継。範囲は大幅に狭い
   （断片の存在規則・構造木・番号振り直し・配列展開）が、多言語 byte 一致が要る。
   → 最初の conformance 対象はここに集中させる。
2. **意味的等価の限界** — SQL テキストが一致しても、駆動結果（NULL 順序・照合・timezone・浮動小数）は
   DB 側の性質。conformance は「同一 SQL + 同一 assemble」を保証し、DB 挙動差は方言コンパイル時の
   規約（ORDER BY の NULLS 指定強制など）で潰せる範囲だけ潰す、と明確に線を引く。
3. **契約付き Raw SQL の境界** — 静的解析（Unknown Column 排除）の保証が Raw 部分では効かない。
   「不透明・契約付き・方言別同梱」の3点セットを仕様として固定する。
4. **Transaction DAG 導出は依然研究的** — RDB は対話的・read-after-write・ロックを伴う。
   初期は単文 Command + 明示 transaction 契約（宣言された固定列）に限定し、導出は後段。

---

## 7. 段階化

1. graphddb の Contract / planner / bundle / codegen / conformance 骨格を RDB 向けに fork。
2. **SQLite + TS だけで縦に1本**: litedbmodel API（find + relation select）→ IR（dialect SQL +
   fragment tree）→ thin Runtime 実行。golden = 「同一入力 → 同一 SQL テキスト + 同一結果」。
   動的展開仕様（SKIP・断片木）をここで確定。
3. Postgres / MySQL 方言バックエンド追加（SqlBuilder 資産の移植。Runtime は不変のはず — それ自体が検証）。
4. Go / Rust / Python thin Runtime + codegen。litedbmodel.rs の `Relation<T>`/static-flatten 資産を
   Rust Runtime に接続。conformance を言語軸へ拡張。
5. 単文 Command → 明示 transaction 契約 → （後段）Transaction DAG 導出・Gate First。

---

## 8. エコシステム上の位置づけ

SCP（Behavior/Component/Wire）がオペレーション間配線の宣言層、dsl-contracts が
メタ契約（Metadata / Portable IR / Contract Service / Portability Guard）、graphddb が KVS 参照実装、
本件がその RDB プロファイル。litedbmodel(.ts/.rs) は Runtime ターゲット兼オーサリング表面として存続。
整合チェックは [dsl-contracts/scp-graphddb-consistency.md](../../../dsl-contracts/scp-graphddb-consistency.md)。

---

## 9. typed-object 化と LazyLoading（訂正・設計確定）

### 訂正

訂正第1版は「lazy loading（アクセス時の暗黙 I/O）はホスト言語に埋まった命令的実行であり
IR に落とせない → 採用しない」とした。**これは誤り。**

守るべき不変条件は「**Runtime は SQL を生成しない**」であって、「Runtime は実行タイミングを
決めない」ではない。そして lazy relation の実行内容は完全に宣言的である:

- モデル定義された relation グラフは**閉じた語彙**。全 relation のバッチクエリ
  （limit 変種 = LATERAL / ROW_NUMBER、複合キー = unnest / tuple-IN を含む）を
  **方言別に事前コンパイルして IR（relation ops）に同梱**できる。
- lazy アクセス = **事前コンパイル済み relation op の、実行時タイミングでの起動**。
  batch context（結果セット内の兄弟から親キーを収集 → 1回のバッチ実行 → 各親へ分配）も
  Runtime の一般アルゴリズムであり、SQL 知識を要しない。
- **litedbmodel.rs の `Relation<T>.load()` + 自動 batch が、他言語で同じ意味論を実装できることの
  既存証明**になっている。
- conformance: 「結果セット + relation load 要求 → 同一 SQL テキスト + 同一の行/assemble」で
  言語横断に検証可能。

これにより「アプリケーションによっては何をロードするか事前に確定できない」という現実の課題
（一覧の条件付き描画、動的なグラフ探索など）にも答えが出る: **全エッジが事前コンパイル済み**
なので、ad-hoc な traversal も常に IR + Runtime 経由で実行される。

### eager（宣言 select）と lazy の関係

| | 宣言 select（推奨経路） | lazy アクセス（fallback） |
|---|---|---|
| ロード決定 | クエリ宣言時 | アクセス時（Runtime がタイミング決定） |
| 実行 | planner が stage 化・兄弟 dedup（先読み最適化が効く） | batch context で N+1 回避（先読みはできない） |
| SQL | 事前コンパイル済み relation op | **同じ** relation op |
| 結果 | own プロパティ（データ） | アクセス時に解決 |

planner の stage 最適化（兄弟をまとめて 1 stage で dedup 実行）は宣言 select でしか効かないため、
**宣言できるものは宣言するのが最適経路、lazy はそれを強制しないための安全な fallback**、と位置づける。

### TS の実装機構: prototype で良い（設計確定）

- **own プロパティ = 純データのみ**。`JSON.stringify` / `Object.keys` は汚れない。
- **prototype に lazy getter**（`await user.posts` → `Promise<Post[]>`）と、
  **非列挙 Symbol の batch context**（graphddb の `GRAPHDDB_KEY` と同じ手法。spread/JSON に漏れない）。
- **shadowing による統一**: select で宣言 hydrate された relation は own プロパティ（`Post[]` そのもの）
  として付与され、**prototype の lazy getter を自然に shadow** する。同じプロパティ名で
  「宣言済み → データ / 未宣言 → lazy Promise」が両立し、型は select から推論
  （graphddb の `QueryResult<T,S>` 型代数と同型）。
- spread / structuredClone は lazy と batch context を落とす（データは保持）。これは graphddb の
  hidden updatable key と同じ「設計された劣化」であり、ドキュメント化する。
- 他言語のトリガーはエルゴノミクスに合わせる: Rust `user.posts.load().await`（既存）、
  Go `user.Posts(ctx)`、Python property/メソッド。**観測されるデータ挙動と SQL は全言語同一**。

### hydrate（graphddb と整合）

「後からホストオブジェクトに載せ替えたい」要求は、独自 API ではなく **graphddb の `options.hydrate`
と同形**にする: read options bag の factory `(raw) => R`。relation 解決後に適用、`null` には適用しない、
**IR には入らないホストランタイム専有**（graphddb docs/class-hydration.md と同じ位置づけ）。
インスタンスメソッドが欲しいユーザは `hydrate: (raw) => new UserDomain(raw)` で従来の
クラス的エルゴノミクスを回復できる。graphddb Phase 3（per-relation hydrate）が入る場合も同じ形に追随する。

### 現行からの移行の実態

- `await user.posts` は**そのまま動く**（機構が getter→遅延バッチ から getter→relation op 起動 に変わるだけ）。
- 変わるのは: 結果が DBModel インスタンスでなく typed-object になること（メソッドは hydrate で回復）、
  relation の select 宣言（推奨経路）が新設されること。
- LazyRelation の中身（バッチ SQL）は relation ops のコンパイル内容として全量活きる。
  消えるのは ActiveRecord クラス機構だけ。
