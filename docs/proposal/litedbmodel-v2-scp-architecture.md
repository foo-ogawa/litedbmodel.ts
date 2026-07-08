# litedbmodel v2.0 アーキテクチャ仕様案 — SCP / 多言語 CQRS

> **Status:** Draft（breaking / v2.0 系）。
> 先行文書 [`contracts-architecture.md`](./contracts-architecture.md)（原案）と
> [`contracts-architecture-feasibility.md`](./contracts-architecture-feasibility.md)（訂正第2版・実現可能性分析）を前提とする。
> 本書はそれらを、**behavior-contracts の汎用 SCP レイヤを consume する SQL バックエンド**という形に確定させた v2.0 の仕様案。

---

## 0. 要約（TL;DR）

- litedbmodel v2 は **新しい DSL を作らない**。**behavior-contracts の汎用 SCP レイヤ（Behavior/Component/Port/Wire + Expression IR + runtime-core）を consume する "SQL バックエンド consumer"** になる。graphddb（DynamoDB backend）と同型で、**差分は Catalog（SQL 操作群）と Backend Compile（IR→dialect SQL）と Handler（SQL 実行）だけ**（behavior-contracts の原則 **C2: difference is catalog only**）。
- **コンパイル経路は1本、実行モードが3つ**（単一意味論、feasibility §2 / §9）。公開 API も SCP 宣言も**同一の Authoring Parse → 内部 IR** を通り、内部 IR 以降は全モード共通:
  1. **TS 直接利用（eager）** — 公開 API 呼び出しを**同一コンパイラで動的に内部 IR 化**（キャッシュ）→ 共通 Runtime で実行（別解釈系は持たない）。
  2. **SCP 宣言ブロック** — **SCP 語彙で Behavior を宣言**（Query/Command は litedbmodel 語彙でなく Behavior の Effect 分類）→ **ビルド時に事前コンパイル**して IR（dialect SQL + 動的 condition の fragment）/ 各言語コードへ変換。
  3. **多言語利用** — publish された IR を各言語 runtime から呼ぶ（同一 IR）。
- **Relation は Read 系だけでなく Write 系（write-time relations）**を持つ（graphddb 同型）。書込時に関連エンティティの整合・cascade・edge・counter・outbox を**1つの SQL トランザクションに導出**する。
- **多言語 CQRS 対応**: 公開境界は CQRS（Query/Command）契約のみ。TS/Python/Rust/Go/(PHP) の薄い runtime が**同一 IR から同一 SQL・同一結果**を出す（conformance）。
- リファクタ後は **v2.0 系**（破壊的変更）。v1.x は別ブランチ（`v1.x`）で保全済み。

---

## 1. 位置づけ — litedbmodel は SCP の SQL バックエンド consumer

behavior-contracts は SCP を**汎用化**し、consumer が差し込む拡張点を確定させている（bc `runtime-boundary.md` / `concept.md`）。litedbmodel v2 は graphddb と並ぶ **2番目の consumer**（bc issue #1 の RDB プロファイルの本体）。

```
behavior-contracts（汎用 SCP レイヤ・DSL 非依存）
  ├─ Behavior / Component / Port / Wire（意味の合成）
  ├─ Expression IR（閉じたオペレータ集合）
  ├─ Execution Plan（stage groups / skip 伝播 / Error Policy Kind）
  └─ runtime-core（validateEnvelope / evaluateExpression / renderTemplate / runPlan / canonicalValue / …）を TS/Python/Rust/Go で提供
        ▲ consume                         ▲ consume
   graphddb（DynamoDB backend）      litedbmodel v2（SQL backend）  ← 本書
   Catalog=GetItem/Query/…            Catalog=Select/Insert/Update/Delete/Fragment/Tx
   Backend=DynamoDB API              Backend=dialect SQL（PG/MySQL/SQLite）
```

**litedbmodel v2 が実装する拡張点（これ以外は behavior-contracts から得る）:**

| 拡張点 | litedbmodel v2 での中身 |
|---|---|
| **Catalog 定義** | `Select` / `Insert` / `Update` / `Delete` / `Fragment`（動的 WHERE/SET 断片木）/ `Tx`（多文トランザクション）。各に Port schema（table・where(ExprIR)・set・limit・order 等） |
| **Authoring Parse** | 公開 API（`User.find(...)`）と SCP 宣言（`behaviors/` の export 関数）を **Component-graph IR** へ落とす |
| **Backend Compile** | IR → **dialect SQL テキスト + `?` パラメータ + fragment 木**（動的部）。`?`→`$N`（PG のみ・最終1パス） |
| **Handler 実装** | 各 Catalog 名 → 実行関数（driver で SQL 実行 + 行→論理モデル assembly） |
| **Error Mapping** | driver エラー（UNIQUE 制約違反・FK 違反・deadlock 等）→ SCP Failure（`constraint_violation` / `retryable` 等） |

**帰結:** litedbmodel v2 は「SQL に特化した薄い層」であり、意味論・IR 構造・多言語 conformance・codegen は behavior-contracts と共有する。

---

## 2. 利用モード（本仕様の中核）

利用者が求める4つの姿を単一意味論（feasibility §2: 宣言→IR→Runtime が唯一の経路、消費モードが複数）で同居させる。

### 2.1 モデル定義（TS・schema.sql から生成可）

物理スキーマ（`schema.sql`）から **litedbmodel-gen** 等で TS のモデル定義（列 + 型 + 主キー）を生成する。手書き部（リレーション・メソッド・SCP ブロック）は保持（embedoc/gen のマーカー方式、v1 と同様）。

```ts
// gen 生成部（schema.sql 由来）
@model('users')
export class UserModel extends DBModel {
  @column({ primaryKey: true }) id!: number;
  @column() name!: string;
  @column() post_count!: number;   // derive 対象の counter
}
export const User = UserModel.asModel();
```

### 2.2 リレーション定義（Read 系 + Write 系）

Read 系（従来の関連取得）に加え、**Write 系（write-time relations）をオプションで**持つ（graphddb 同型・§6）。

```ts
@model('posts')
export class PostModel extends DBModel {
  @column({ primaryKey: true }) id!: number;
  @column() author_id!: number;
  @column() title!: string;

  // ── Read-side（宣言 select または lazy）──
  @belongsTo(() => [Post.author_id, User.id]) declare author: User | null;

  // ── Write-side（書込時リレーション・任意）──
  static readonly writes = entityWrites<PostModel>((w) => ({
    create: w.lifecycle({
      requires:    [ w.exists(() => User, { id: '$.input.author_id' }) ], // 参照整合
      unique:      [ w.unique({ name: 'title_per_author', scope: ['$.input.author_id'], fields: ['$.input.title'] }) ],
      derive:      [ w.increment(() => User, { id: '$.input.author_id' }, 'post_count', +1) ], // cascade counter
      emits:       [ w.event('PostCreated', { postId: '$.entity.id', userId: '$.input.author_id' }) ], // outbox
      idempotency: w.idempotentBy('$.input.request_id'),
    }),
    remove: w.lifecycle({
      derive: [ w.increment(() => User, { id: '$.entity.author_id' }, 'post_count', -1) ],
    }),
  }));
}
```

### 2.3 TS 直接利用（公開 API・eager）

従来どおり公開 API を**直接**呼ぶ。**経路は分岐させない**: 公開 API 呼び出しは
**必ず「Authoring Parse → 内部 IR」という単一のコンパイル経路**を通り（AOT と**同一ロジック**）、
生成された内部 IR を**共通 Runtime**で即時実行する（結果はキャッシュ可）。
「eager だけ別の解釈系（メタデータを直に解釈する経路）」は持たない — **内部 IR 以降は全モードで完全に共通**（§9）。
書込は write-time relations があれば自動で1トランザクションに束ねられる（§6）。

```ts
// Read（eager）
const post = await Post.findById(1, { with: { author: true } }); // 宣言 select → staged batch + assembly
const author = await post.author;                                // lazy（宣言しなければアクセス時解決・§9）

// Write（write-time relations が自動導出 → 1 tx）
await Post.create({ author_id: 7, title: 'Hello', request_id: 'r-123' });
//   → BEGIN; (author 存在チェック); (unique guard); INSERT post; UPDATE users.post_count+1; INSERT outbox; (idempotency); COMMIT
```

### 2.4 SCP 宣言ブロック（SCP 語彙で宣言 → ビルド時事前コンパイル → IR/TS コード）

**litedbmodel 独自の宣言語彙（`defineQuery` 等）は導入しない。** SCP の宣言語彙
（**Behavior = Component 合成に名前を与えたもの**）で書く。litedbmodel が供給するのは
**Catalog（leaf Component: `Select`/`Insert`/… §11）だけ**で、合成・配線・命名は SCP 共通の authoring surface に従う（C2: 差分は Catalog のみ）。
Query / Command は**別の語彙ではなく Behavior の Effect 分類**（read-only か 書込 effect か。CQRS 標準語で litedbmodel 固有ではない）。

> **authoring surface（`@behavior` を巡る指摘への回答）:** `@behavior export const X = ($)=>{}` の
> **デコレータ形は現行 TS では不可**（デコレータはクラス／クラスメンバ専用。トップレベルの `const`/関数には付かない。TC39 stage-3 も同様）。
> config オブジェクトを渡す builder（`behavior('name',{...})`）も**採らない**。
> litedbmodel は**ビルド時コンパイル**（AST/型を読める）なので、**デコレータも builder も wrapper 呼び出しも不要な「素の export 関数」**を既定とする。

```ts
// behaviors/posts.ts — このディレクトリの export 関数を、ビルド時に Behavior として発見する。
// 関数名 = Behavior 名（＝component化）。引数 $ の型 Query<In> / Command<In> が Effect と Input Port を与える。
export function PostSearch($: Query<{ authorId: number; status?: string; since: string; limit?: number }>) {
  const posts = Select(Post, {                         // Catalog leaf component（litedbmodel 供給）
    where: [
      Post.author_id.eq($.in.authorId),
      $.in.status ? Post.status.eq($.in.status) : SKIP, // SKIP → fragment の存在規則（§8）
      Post.created_at.gte($.in.since),
    ],
    order: Post.created_at.desc(),
    limit: $.in.limit ?? 20,
  });
  const authors = posts.map((p) =>                     // 構造化制御は SCP 共通（.map / ?: / &&）
    Select(User, { where: [User.id.eq(p.author_id)], select: ['id', 'name'] }));
  return { posts, authors };                           // ← Output Port（ルートの出力）
}

// Command（Effect=write）: $ が Command<In> なので write-Catalog に到達可。write-time relations（§6）を導出。
export function CreatePost($: Command<{ authorId: number; title: string; requestId: string }>) {
  return Insert(Post, {
    values: { author_id: $.in.authorId, title: $.in.title },
    onWrite: Post.writes.create,                       // → 順序付き1トランザクションへ導出（§6）
    returning: ['id', 'title'],
  });
}
```

**component化 / ルートノードの指定（疑問への直接回答）:**
- **ルートノードを個別にマークしない。export された関数そのものが Composite Component（＝ルート）**。「Component 群の合成全体に名前を与えた Behavior が新しい意味を持つ」（SCP）を、**関数名 = Behavior 名**で表す。
- **Input Port = `$: Query<In>` / `Command<In>` の型引数 `In`**（ビルド時に型抽出）。**Effect = Query か Command か**（型で強制。`Query<>` 文脈からは write-Catalog に到達不可＝read-only を型保証）。
- **Output Port = 返り値**。内部 DAG は `$` 参照・`.map`・`?:` の**配線から Compiler が導出**（`await`/`Promise.all`/実行順は書かない）。
- leaf（Specialty Component）は Catalog プリミティブ（`Select`/`Insert`/…）のみ。**litedbmodel 独自の宣言語彙は無い**。
- **前提**: コンパイラが `behaviors/`（設定可）配下の export 関数を走査し発見。名前・入力型の取得はビルド時 AST/型情報に依存（v2 の事前コンパイル方針と整合）。graphddb の `$`-rooted 束縛（`from("$.field")`）が同モデルの具体形。

**代替表記（好みに応じ、いずれも同一 IR に落ちる）:** ① HOF マーカー `export const PostSearch = query($ => {...})`（config オブジェクトではなく効果ラッパ1個）② クラス + メソッドデコレータ `@query PostSearch($){...}`（**有効な TS デコレータ**・graphddb 寄り）。既定は上記「素の export 関数」。

**注（エコシステム・§11/§15）:** この authoring surface（関数→Behavior の発見規則・`$` 配線・structured control）は**本来 behavior-contracts で共有すべき**もの。consumer は Catalog（`Select`/`Insert`…）だけ供給する（C2）。現状 bc 未実装・graphddb は独自 `publishQuery` のため、共有 authoring を別 issue 化する。

- **公開されるのは Behavior 名**（`PostSearch` / `CreatePost`）と Effect 分類のみ。SQL 種別（SELECT/INSERT）は runtime 不可視。
- 入力の arity（単数/配列）・cardinality（one/many）は Catalog/Key から**導出され型で強制**（N+1 型安全）。
- ビルド時に IR bundle（§8）へ落ち、`ir/` として publish される。

### 2.5 多言語利用（publish された宣言ブロックを各言語 runtime で）

publish された IR bundle を、言語別の薄い runtime（behavior-contracts runtime-core + litedbmodel SQL runtime）が読み、**同一 SQL・同一結果**を実行する。

```python
# Python（生成コード or IR dynamic）
posts = PostQueries.search(db, author_id=7, since="2026-01-01", limit=10)
```
```go
posts, _ := postQueries.Search(ctx, db, SearchInput{AuthorID: 7, Since: "2026-01-01"})
```

---

## 3. レイヤ構成

```
公開境界        = CQRS Contract（Query / Command）※ 多言語で共有
  ├─ TS 直接利用（eager）……… Authoring Parse → Component-graph IR → Handler（Native Interpret）
  └─ SCP 宣言ブロック（export 関数）……… Component-graph IR（AOT）
        ↓ Backend Compile（litedbmodel）
     SQL IR = dialect SQL テキスト + fragment 木 + param slots(Expression IR) + assembly + relation ops + transaction plan
        ↓
     薄い Runtime（TS/Python/Rust/Go）: validate → 断片選択(SKIP) → 配列展開 → Expression 評価 → bind → SQL 実行 → assembly
```

**generic（behavior-contracts）と SQL backend（litedbmodel）の分界:**

| 汎用（behavior-contracts） | SQL backend（litedbmodel v2） |
|---|---|
| Component-graph IR 構造 / Execution Plan（groups・concurrency・skip 伝播・Policy Kind） | Catalog（Select/Insert/…）と各 Port schema |
| Expression IR 評価（`evaluateExpression`）・閉じたオペレータ集合 | 条件/値の SQL 断片への **Backend Compile**（dialect SQL + `?` + fragment 木） |
| `renderTemplate` / `canonicalValue` / `pyFloatRepr` / `validateEnvelope` / `runPlan` | Handler（driver で SQL 実行）・行→論理モデル **assembly** |
| conformance runner 基盤・codegen 基盤（bc issue #13） | dialect 別 SQL 生成（PG/MySQL/SQLite）・型 hydration・接続/プール/tx |
| Portability Guard | Error Mapping（driver エラー → Failure）・Raw SQL escape hatch |

---

## 4. モデル定義

- `@model(table)` / `@column(opts)` で物理対応（v1 継承・gen 生成）。結果は **typed-object**（DBModel インスタンスではない・feasibility §9）。
- 論理モデル ↔ 物理配置（table/column/PK/index）はモデル定義が吸収（原案 contracts-architecture.md「モデル定義」）。
- litedbmodel-gen は `schema.sql` から列定義を生成し、**マーカー内は再生成・手書き（リレーション/writes/SCP ブロック）は保持**。

## 5. Relation — Read 系

- Relation は SQL JOIN を**既定にしない**。graphddb / v1 LazyRelation と同型の **staged batch query-composition + object assembly**（feasibility §5・§9）。
  - `hasMany` → 親キー集合で `IN (...)` / `= ANY(...)`、per-parent limit は `LATERAL` / `ROW_NUMBER()`。
  - `belongsTo`/`hasOne` → 親から子キー収集 → 1回のバッチ SELECT。
- Execution Plan は behavior-contracts の `deriveExecutionPlan`（result path → stage groups + concurrency）を流用。relation ops はモデル宣言から**方言別に事前コンパイル**して IR に同梱。
- **宣言 select（推奨）** vs **lazy（fallback）** の2段（feasibility §9）:
  - 宣言 select（`with:{...}`）… planner が兄弟をまとめて dedup 実行（先読み最適化）。
  - lazy（`await post.author`）… prototype getter + 非列挙 Symbol の batch context。事前コンパイル済み relation op を**実行時タイミングで起動**（IR 化可能）。
- ホストオブジェクト化は graphddb と同形の `hydrate` factory（feasibility §9）。
- JOIN は Backend Compile が特定形状に対して選ぶ**最適化**であって、意味論の既定ではない。

## 6. Relation — Write 系（write-time relations）

書込時に関連エンティティへ波及する宣言。graphddb の `entityWrites`/`edgeWrites` を **SQL 慣用へ翻訳**し、**1つの SQL トランザクションに導出**する。

| 語彙 | 意味 | SQL への導出 |
|---|---|---|
| `requires` | 参照整合（関連が存在すること） | 先行 `SELECT ... FOR SHARE` / 存在ガード（FK 制約があれば併用） |
| `unique` | フィールド一意性 | `UNIQUE` 制約 or ガード行 `INSERT ... ON CONFLICT DO NOTHING`＋affected 検査 |
| `edges` | 関連の書込側（多対多の中間表、1対多の FK 設定） | 中間表 `INSERT`/`DELETE`（M:N）/ FK 列 `UPDATE`（1:N） |
| `derive` | 関連の派生値更新（counter 等） | cascade `UPDATE ... SET c = c ± n WHERE ...` |
| `emits` | ドメインイベント | outbox テーブルへ `INSERT`（同一 tx） |
| `idempotency` | クライアントトークン重複防止 | idempotency テーブルへ `INSERT`（`UNIQUE` 違反で重複検出） |

**Command 導出（graphddb mutation-derivation 同型）:** `CreatePost($: Command<...>) { return Insert(Post, { onWrite: Post.writes.create, ... }) }`（§2.4）の宣言的 intent から、コンパイラが `Post.writes.create` を展開して**順序付き多文トランザクション**を導出:

```
BEGIN;
  -- Gate First（意味が変わらない限り早期に打ち切り。原案「Gate First」）
  requires:    SELECT 1 FROM users WHERE id = :author_id;          -- 無ければ即 fail・ROLLBACK
  idempotency: INSERT INTO idem(token) VALUES(:request_id);        -- 重複なら短絡
  unique:      INSERT INTO uniq(...) ON CONFLICT DO NOTHING; ...   -- 衝突検査
  -- 本体
  INSERT INTO posts(author_id,title) VALUES(:author_id,:title) RETURNING id;
  derive:  UPDATE users SET post_count = post_count + 1 WHERE id = :author_id;
  emits:   INSERT INTO outbox(type,payload) VALUES('PostCreated', :payload);
COMMIT;
```

- **Transaction は公開仕様にしない**（原案）。公開されるのは Access Pattern（Command）のみ。tx DAG / 実行順（gate-first）は**導出される**。
- 実行順・gate-first・依存は Execution Plan に反映。多言語 runtime は同一計画を honor（再導出しない）。
- 多対多や nested write（親作成と同時に子作成）は `edges`/追加 write で表現し、同一 tx にまとめる。

> **難所（正直な評価・§13）:** SQL の tx 導出は DynamoDB の `TransactWriteItems`（≤25・read-your-writes 無し）と違い、対話的・ロック・read-after-write・gate-first 短絡を伴う。初期は「単文 Command + 明示 write-time relations（固定順）」に限定し、複雑な DAG 導出は後段（feasibility §7 と整合）。

## 7. SCP 宣言ブロック → IR コンパイル

`behaviors/` の export 関数（`$: Query<In>` / `Command<In>`・§2.4）の本体は **SCP の Behavior（Composite Component）**として扱われ、bc のコンパイルパイプラインで Component-graph IR へ落ちる。関数名=Behavior 名（ルート）、`$` の型引数=Input Port、返り値=Output Port、内部 DAG は配線から導出（§2.4）:

```
TS 宣言（AST）
  → Authoring Parse（litedbmodel）: 操作・relation・条件・projection を抽出
  → Component 化（Catalog 名 = Select/Insert/... + ports）
  → Wire / 依存抽出 / Cycle Check
  → 条件・値を **Expression IR** へ lower（bc `expression-ir.md`・閉集合・exprVersion 2）
  → Execution Plan 導出（stage groups / concurrency）
  → IR emit（portable JSON）or codegen（TS/他言語ソース）
```

- 条件 `Post.author_id.eq($.authorId)` → `{eq:[{ref:["author_id"]},{ref:["input","authorId"]}]}`。
- **動的条件は Expression でなく「fragment の存在規則」へ**: `cond ? [...] : SKIP` は値ではなく**断片の採否**（feasibility §4 / bc の SKIP は Expression 語彙外）。
- lower 可能サブセット外（任意の自由 SQL）は **Raw SQL escape hatch**（契約付き・不透明・方言別同梱・§13）。
- 制御構造は bc 準拠のネイティブ TS（`?:` / `&&` / `.map`）を lower（新語彙を足さない）。

## 8. IR の形（dialect 依存・feasibility §4 を確定）

Component-graph IR（bc 汎用構造）の **litedbmodel Catalog** ノードが、Backend Compile で以下の SQL IR を持つ:

- **dialect SQL テキスト**: 静的部は完全 flatten。動的部は **fragment 木**（事前コンパイル済み断片の順序木 + AND/OR 構造 + 各断片の存在規則）。
- **placeholder は全方言 `?` 統一**（v1 内部表現と一致）。param 完全フラット化後に**最終1パスで `?`→`$N`**（PG のみ・左から機械変換）。番号振り直し問題は設計から消滅（feasibility §4）。
- **params 配列**（`?` と 1:1）: 各要素は ①入力参照 `prop.x` ②wire 参照 `{result.field}` ③**オペレータ IR** `{add:[prop.y,1]}`（bc Expression IR・閉集合）。
- **assembly 仕様**: 行 → 論理モデル（relation items 付与位置含む）。
- **relation ops**: モデル宣言から導出した全 relation バッチ SQL（方言別・§5）。
- **transaction plan**: write-time relations の順序付き文 + gate-first + 依存（§6）。

```jsonc
// 例: search クエリの Select ノード（Backend Compile 後）
{
  "component": "Select",
  "sql": "SELECT id,author_id,title,created_at FROM posts WHERE author_id = ?{fragments}ORDER BY created_at DESC LIMIT ?",
  "fragments": [                         // 動的 WHERE 断片（存在規則つき）
    { "when": {"present":["input","status"]}, "sql": " AND status = ?", "params": [{"ref":["input","status"]}] },
    { "always": true, "sql": " AND created_at >= ?", "params": [{"ref":["input","since"]}] }
  ],
  "params": [ {"ref":["input","authorId"]}, /* ...fragment 展開後... */ {"coalesce":[{"ref":["input","limit"]},20]} ],
  "assembly": { "shape": "items", "relations": { "author": { "op": "author__batch", "attach": "author" } } }
}
```

bundle 直列化は bc の envelope（`irVersion`/`exprVersion` + fail-closed）に従う（version skew は `validateEnvelope` で拒否）。

## 9. 実行経路 — コンパイル経路は1本、実行モードが3つ

**コンパイル経路は単一**: 公開 API 呼び出しも SCP 宣言も、**同一の Authoring Parse → 内部 IR** を通る（同一ロジック・分岐なし）。内部 IR 以降（Backend Compile → Runtime）は**全モードで完全に共通**。異なるのは「その同一 IR を**いつ・どう実行するか**」の3モードだけ:

1. **動的コンパイル実行（TS・eager）** — 呼び出し時に**同じコンパイラで内部 IR を生成**（結果はキャッシュ）→ 共通 Runtime で in-process 実行。※ メタデータを直に解釈する別経路ではない（IR を必ず経由）。
2. **IR 参照・動的（全言語）** — publish 済み IR(JSON) を薄い runtime が `runPlan`/`evaluateExpression` で実行。
3. **Codegen・静的（全言語）** — IR → 各言語ソース生成（**ビルド時事前コンパイル**、runtime≈0）。litedbmodel の「ビルド時に TS コード/IR へ変換」はこれ（bc issue #13 の共有 generator に SQL catalog を供給）。

3モードとも**同一コンパイラ・同一内部 IR**を共有するため、経路差による意味論のズレは構造上生じない。eager で書いたロジックの SCP 化・多言語化は「配線を宣言に移す」リファクタで済む（feasibility §2）。

## 10. 多言語 Runtime と Conformance

- 公開境界は CQRS のみ。各言語 runtime = **bc runtime-core（共有）+ litedbmodel SQL runtime（driver + assembly + dialect）**。
- Conformance（bc の golden 方式）: **同一 Contract + 同一入力 → 同一 SQL テキスト + 同一 assembly 結果**。
- 分解: **方言軸はコンパイル時に TS 側で1回検証**（既存 SqlBuilder 資産 + golden SQL）。**言語軸は「同一 IR+入力 → 同一 SQL + 同一結果」の機械検証**。→ conformance 行列が「方言 3 × 言語 N」から「3 + N」に分解（feasibility §4）。
- runtime が読むのは IR bundle（manifest 相当のスキーマ + operations 相当の契約/クエリ/コマンド/トランザクション + executionPlan）。

## 11. Consumer 実装点（litedbmodel が書くもの / bc から得るもの）

**litedbmodel v2 が実装（§1 表の具体化）:**
1. **Catalog**: `Select/Insert/Update/Delete/Fragment/Tx` の Port schema。
2. **Authoring Parse**: 公開 API・SCP 宣言（`behaviors/` の export 関数）→ Component-graph IR。
3. **Backend Compile**: IR → dialect SQL + fragment 木 + `?`→`$N`（既存 SqlBuilder を IR 消費型へ移植）。
4. **Handlers**: Catalog 名 → driver 実行 + assembly。
5. **Error Mapping**: driver エラー → SCP Failure（Policy Kind: fail/retry/continue）。

**bc から得る（実装しない）:** IR 構造・Expression 評価・Execution Plan 実行（skip 伝播/concurrency/Policy Kind）・`renderTemplate`/`canonicalValue`/`validateEnvelope`・Portability Guard・codegen 基盤・conformance runner。

## 12. TS 公開 API の v1 → v2 移行（破壊的）

feasibility §9 で確定済み。要点のみ:

| 面 | v1 | v2 | 破壊度 |
|---|---|---|---|
| CRUD + condition タプル + SKIP | Active Record | ほぼ不変（内部が IR 経路に） | 小 |
| `await post.author`（lazy） | prototype getter（Promise） | 残す（getter → 事前コンパイル relation op 起動・§9） | 小 |
| 結果オブジェクト | DBModel インスタンス | **typed-object**（own props はデータのみ） | **中〜大** |
| インスタンスメソッド | クラスメソッド | typed-object には無い → `hydrate: (raw)=>new Domain(raw)` で回復 | 中 |
| `sql`/dbDynamic/dbRaw | 実行時文字列 | Dynamic Slot 語彙（lower 可能サブセット内） | 小 |
| 完全動的 Raw SQL | `execute`/`query` | 契約付き Raw SQL（方言別 SQL 同梱・IR 不透明） | 小 |
| Middleware / TypeCast | 実行時 | Runtime 関心事として存続 | 小 |

破壊の中心は「結果がインスタンス → typed-object」。メソッドは `hydrate` で回復。**v1.x はメンテブランチ `v1.x` で保全**。

## 13. リスク / 難所

1. **SQL IR / lower 可能サブセットの線引き**（最重要・feasibility §4/§6）: SQL-first の自由度 と 多言語決定的 lower は部分対立。Raw SQL は「不透明だが契約付き（I/O・Effect 宣言、SQL 文字列は方言別同梱、コンパイルしない）」に隔離。
2. **オペレータ/断片の決定性**: SKIP 合成順序・AND/OR 構造木・空 WHERE 縮退・`?`→`$N`・配列展開を多言語 byte 一致で仕様化（bc `expression-ir.md` の語彙を共有）。
3. **write-time relations の tx 導出 + gate-first**（§6）: 研究的。初期は固定順・単文 Command 先行。
4. **意味的等価の限界**: SQL テキスト一致でも DB 挙動差（NULL 順序・照合・timezone・浮動小数）。conformance は「同一 SQL + 同一 assembly」を保証し、DB 差は方言コンパイル時規約（`ORDER BY ... NULLS` 強制等）で潰せる範囲に線引き。

## 14. 段階化 / ロードマップ

bc の migration（litedbmodel は Phase 4 = 統合 generator + C2 実証で参入）と bc issue #1（RDB PoC）に整合。

1. **v2 α**: **SQLite + TS**・Query 契約のみで縦1本（公開 API/export 関数 → Component-graph IR → Backend Compile → 薄い Runtime）。golden = 同一入力→同一 SQL + 同一結果。動的展開仕様（SKIP/fragment 木）を確定。← bc issue #1 の本体。
2. **v2 β**: Postgres/MySQL 方言追加（SqlBuilder 資産を IR 消費型へ移植）。write-side（単文 Command + write-time relations）。typed-object + hydrate + lazy 確定。
3. **v2 RC**: Python/Rust/Go runtime + conformance（言語軸）。codegen（bc 共有 generator に SQL catalog 供給）。
4. **v2.0 GA**: 多言語 CQRS 公開・4レジストリ publish 基盤（behavior-contracts 方式）。write-time tx DAG 導出・gate-first 最適化。

## 15. バージョニング / エコシステム位置づけ

- **v2.0 系（破壊的）**。v1.x はメンテブランチ `v1.x` で保全（別トラック）。
- litedbmodel v2 = behavior-contracts の **SQL バックエンド consumer**（graphddb=DynamoDB と対）。IR/Expression/runtime-core/codegen/conformance を共有し、Catalog + Backend Compile + Handler + Error Mapping のみを供給。
- litedbmodel-gen は SQL 側の Authoring 入口（`schema.sql` → モデル生成）として位置づけ。
- 関連: behavior-contracts issue #1（RDB PoC）/ #13（共有 codegen）、[`contracts-architecture-feasibility.md`](./contracts-architecture-feasibility.md)（設計判断の根拠）。
