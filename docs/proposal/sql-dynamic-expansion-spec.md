# SQL 動的展開仕様（Dynamic-Expansion Spec） — litedbmodel v2 SCP

> **Status:** Normative Draft（WS1 / #21）。
> 対象: `litedbmodel-v2-scp-architecture.md` §7/§8/§13 と `contracts-architecture-feasibility.md` §4 が要求する
> 「動的断片の存在規則・AND/OR 構造木・空 WHERE 縮退・括弧規則・IN-list 配列展開・`?` 番号付け」の**規範**定義。
> 本書は**多言語 byte 一致**（TS/Rust/Python/Go/PHP が同一 IR + 同一入力から同一 SQL テキストを出す）の基準文書である。
> TS 参照実装は `src/scp/render.ts`（`renderOperation`）。golden ベクタは `test/scp/golden.test.ts` が pin する。

本書は Backend Compile（`src/scp/compile-sqlite.ts`）が生成した **SQL IR**（`CompiledOperation`・§8）を、
薄い runtime が**実行時入力**（`input` scope）とともに最終 SQL テキストへ展開する規則を定義する。方言は SQLite。
placeholder は全て `?` に統一する（`?`→`$N` の PG パスは WS1 の範囲外）。

---

## 1. SQL IR の構成要素（`CompiledOperation`）

各 CRUD ノードは Backend Compile 後に以下を持つ（`src/scp/ir.ts`）:

- `sql`: 静的に確定する部分を完全 flatten した SQLite SQL テキスト。動的 WHERE を持つ操作は、
  WHERE が展開される 1 箇所に**スプライスマーカー `{where}`**（`WHERE_SLOT`）を含む。
- `where`: 動的 WHERE の **fragment tree**（`FragmentTree | null`）。`null` は「動的 WHERE なし」。
- `params`: **静的 param slot** 配列（fragment tree の外にある `?`。Insert values / Update SET / LIMIT / OFFSET）。
  各要素は **Expression IR ノード（bc 閉集合のみ）**: ①入力参照 `{ref:["x"]}` ②wire 参照 `{ref:["<nodeId>","field"]}`
  ③演算子ノード `{coalesce:[{ref:["limit"]},20]}`。litedbmodel 独自オペコードは**一切**生成しない（§13 Raw SQL 隔離）。
- `assembly`: 行→論理モデルの shape（WS1 は shape のみ）。

**fragment（`Fragment`）**:

| フィールド | 意味 |
|---|---|
| `always: true` | 無条件に存在する断片。`always` か `when` の**いずれか一方**を持つ。 |
| `when: <Expr>` | **存在ガード**（Expression IR）。§2 の存在規則で採否を決める。 |
| `sql` | 断片の SQL テキスト（`?` 込み。**先頭コネクタを含まない**）。 |
| `params` | 断片の param slot（この断片の `?` と 1:1）。**存在するときだけ**組立配列へ追加。 |
| `expand: <index>` | IN-list 展開スロット（§5）。`params[expand]` が配列で、`(?)` を `(?, …)` に開く。 |

**fragment tree（`FragmentTree`）**:

```
FragmentTree = { connector: "AND" | "OR", fragments: (Fragment | FragmentTree)[] }
```

---

## 2. SKIP → 断片の存在規則（fragment existence）

authoring の `cond ? [<condition>] : SKIP`（spec §2.4）は **値ではなく断片の採否**である
（feasibility §4 / bc `expression-ir.md` §4: **SKIP は Expression 語彙の外**）。Backend Compile はこれを
「`when` ガード付き fragment」に落とす。runtime の存在判定は:

- `always === true` → **常に存在**。
- `when` あり → `evaluateExpression(when, input)` を評価し、結果が **present** なら存在、**absent** なら**脱落**。
  - **absent** = `null` または `false`。
  - **present** = それ以外すべて（`0`・`""`・空配列 `[]` も present。空配列の IN-list 展開は §5 で別途縮退）。
- `always` も `when` も無い fragment は**不正 IR**（fail-closed で脱落）。

**脱落した fragment は SQL テキストにも params 配列にも一切寄与しない。** これが「SKIP された任意条件が
present のときだけ現れ、absent のとき完全に消える」規則。`when` は明示的な presence/bool Expression を想定する
（例: `{ne:[{refOpt:["status"]}, null]}`）。非 bool を返す `when` は evaluateExpression の strict 規律で
fail-closed になる（暗黙 truthiness は存在しない）。

**合成順序（決定性）:** fragment は tree の `fragments` **配列順**にのみ評価・連結される。挿入順に依存しない。
`when` の評価に副作用はない（純 Expression）。したがって同一 IR + 同一入力 → 同一採否集合 → 同一テキスト。

---

## 3. 空 WHERE の縮退（empty-WHERE degeneration）

fragment tree を展開した結果、**present な fragment が 0 個**なら WHERE 本体は空文字列になる。このとき:

- `SELECT` / `UPDATE` / `DELETE` いずれも **` WHERE ` キーワードごと脱落**する。
  すなわち `{where}` スプライスは**空文字列**に置換される（`SELECT … FROM t{where} ORDER BY …`
  → `SELECT … FROM t ORDER BY …`）。
- 逆に present が 1 個以上なら `{where}` は ` WHERE <body>`（先頭に半角スペース + `WHERE` + スペース）に置換される。

これは litedbmodel v1 `DBConditions.compile` の「`parts.length === 0` → `''`」および `_buildSelectSQL` の
「`if (whereClause) sql += ' WHERE ' + whereClause`」と byte 一致する。

> **注（UPDATE/DELETE）:** WS1 の compile は UPDATE/DELETE で WHERE が静的に空だと**コンパイル時に拒否**する
> （v1 の `throw 'UPDATE requires conditions'` と同じ）。ただし全条件が `when` ガードで実行時に脱落し得る場合、
> runtime 側の縮退規則は SELECT と同一（keyword ごと脱落）。安全な WHERE を保証するのは authoring/compile の責務。

---

## 4. AND/OR 構造木と括弧規則（parenthesization）

- 1 つの tree の present fragment は ` <connector> `（前後に半角スペース）で連結する。
  top-level tree の connector は `AND`（litedbmodel v1 の `DBConditions` 既定と一致）。
- **入れ子 tree は括弧で囲む**: 親 tree の要素が `FragmentTree` の場合、その展開結果が空でなければ
  `(<inner-body>)` として親へ寄与する（v1 の `(${nested.compile(...)})` と一致）。inner が空なら親へ寄与しない。
- top-level tree 自身は括弧で囲まない。fragment の `sql` は**先頭コネクタを含まない**（連結はこの規則が行う）。

例（top=AND、入れ子 OR）:

```
[ eq(a), group(OR, [ eq(b), eq(c) ]) ]  →  "a = ? AND (b = ? OR c = ?)"
```

入れ子 OR の 1 要素だけ present:

```
group(OR, [ present(b), skip(c) ])  →  "(b = ?)"
```

---

## 5. IN-list 配列展開（array expansion）

`col IN (?)` の fragment は `expand` スロットを 1 つ持つ。runtime は `params[expand]` を評価し:

- **配列（長さ N ≥ 1）**: `(?)` を `(?, ?, …)`（`, ` 区切り・要素数 N）に置換し、各要素を params 配列へ順に push する。
  → `col IN (?, ?, ?)`。
- **空配列（N = 0）**: fragment 全体（`col IN (?)`）を**常偽の番兵 `1 = 0`** に置換し、**params は push しない**。
  → litedbmodel v1 `DBConditions` の「`value.length === 0` → `1 = 0`」と byte 一致。
- **非配列**: fail-closed（IR/入力不整合。runtime エラー）。

`, ` 区切り・`( )` の付き方・空配列の `1 = 0` 番兵は全て v1 golden に固定する。

---

## 6. `?` 番号付け（placeholder numbering / param order）

全 placeholder は `?`。**番号は振らない**（`?`→`$N` は PG 専用の最終 1 パスで WS1 範囲外・feasibility §4）。
params 配列は最終 SQL テキストの `?` 出現順（左→右）と**厳密に 1:1**。順序規則:

1. `{where}` マーカーより**前**にある静的 `?`（Insert values / Update SET）→ `params` の先頭から順に。
2. `{where}` 本体の fragment params → tree の配列順（present 分のみ、IN 展開後の要素順）。
3. `{where}` マーカーより**後**にある静的 `?`（LIMIT / OFFSET）→ 残りの静的 params。

`renderOperation` は `{where}` 前の静的セグメントの `?` 個数で `params` を pre/post に分割し、この順で組み立てる。
静的部が全て WHERE の前後どちらかに寄る WS1 の CRUD 形状では、**spliced SQL の左→右走査**がそのまま canonical な
`?` 順序になる。

---

## 7. CRUD ごとの生成形（golden 基準）

placeholder `?`、`[ ]` は省略可能部。全て v1 SQLite builder / `_buildSelectSQL` / `DBConditions` と byte 一致。

| 操作 | テンプレート |
|---|---|
| `Select` | `SELECT <cols> FROM <t>{where}[ GROUP BY <g>][ ORDER BY <o>][ LIMIT ?][ OFFSET ?]` |
| `Insert` | `INSERT INTO <t> (<c1, c2, …>) VALUES (?, ?, …)[ ON CONFLICT (<k>) DO UPDATE SET <c = excluded.c, …>][ RETURNING <r>]` |
| `Insert`(ignore) | `INSERT OR IGNORE INTO <t> (<cols>) VALUES (?, …)[ RETURNING <r>]` |
| `Update` | `UPDATE <t> SET <c1 = ?, c2 = ?, …>{where}[ RETURNING <r>]` |
| `Delete` | `DELETE FROM <t>{where}[ RETURNING <r>]` |

条件断片の形（`sql`・先頭コネクタなし）:

| 条件 | fragment `sql` | params |
|---|---|---|
| 等値 | `<col> = ?` | `[value]` |
| 比較 | `<col> <op> ?`（`op ∈ {<,<=,>,>=,<>}`） | `[value]` |
| NULL | `<col> IS NULL` | `[]` |
| IN | `<col> IN (?)`（`expand:0`） | `[arrayValue]` |
| group | （§4 で連結・括弧） | — |

`<cols>` は `select` を `, ` で join（単一 `['*']` は `*`）。RETURNING columns も `, ` join。

---

## 8. 決定性チェックリスト（多言語実装の受け入れ基準）

- [ ] fragment 採否は `fragments` 配列順に評価し、`when` present/absent のみで決める（§2）。
- [ ] present 0 個 → ` WHERE ` キーワードごと脱落（§3）。
- [ ] 入れ子 tree は空でなければ `( )` で囲む。top は囲まない。連結子は ` AND `/` OR `（§4）。
- [ ] IN 空配列 → `1 = 0`（params push なし）。長さ N → `(?, …)` N 個（§5）。
- [ ] `?` は params と左→右で 1:1。番号なし（§6）。
- [ ] 全 param slot は bc 閉集合の Expression IR のみ（`src/scp/guard.ts` `assertOperationPortable` が fail-closed 検査）。
