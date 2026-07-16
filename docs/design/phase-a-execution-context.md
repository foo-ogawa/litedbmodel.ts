# Phase A 設計: ExecutionContext + 中央 execute/query seam + 接続コンテキスト + 並行 tx 修正

**対象**: litedbmodel v2 → beta（v1 parity）Epic #74 の基盤フェーズ。tx(#69)・middleware(#70)・reader/writer/複数DB(#72)・config(#72) の全てがこの上に載る。**5 native 言語（TS/rust/go/py/php）共通契約**。設計参照: v1 TS(`AsyncLocalStorage`)・v1 rust(`litedbmodel.rs` `PoolTransaction`)・graphddb。

---

## 1. 現状（seam の素地と欠落）

全 5 runtime が共通形：
- `executeBundle(bundle, input, driver/db)` … read（TS `executeBundle`→`executeReadGraph`、go `ExecuteBundle`、py `execute_bundle`、php `Runtime::executeBundle`、rust `execute_bundle`）
- `executeTransactionBundle(bundle, input, driver/db)` … write/tx
- SQL 実行は `driver.prepare(sql).all()/run()`（go は `db.Query/Exec`、php は `PDO`）

**欠落**:
- 全 SQL が通る**中央 seam が無い**（各所で driver を直接叩く）→ middleware 挿入点が無い。
- **接続コンテキストが無い**（driver を生で渡すだけ）→ tx スコープ・reader/writer・複数DB ルーティングを表現できない。
- **tx が driver グローバル単一スロット**（rust `writer: Mutex<Option>`、py `driver.py:369` 等）→ **並行 tx 汚染バグ**。

---

## 2. 中核設計: `ExecutionContext` を driver の代わりに通す

`executeBundle`/`executeTransactionBundle`/relation walker が受け取るのを **生 driver → `ExecutionContext`（以下 ctx）** に変える。ctx が保持するもの：

1. **接続プロバイダ**（`connectionFor(intent)`）: この文にどの接続を使うかを解決（tx所有 / reader / writer-sticky / named-DB）。
2. **middleware chain**: 全 SQL をラップするフック列。
3. **設定**（timeout/limits 等、Phase C/E で拡張）。

**中央 seam（5 言語共通の 2 関数）**:
```
execute(ctx, sql, params) -> Rows      // read（SELECT）
run(ctx, sql, params)     -> RunInfo   // write（INSERT/UPDATE/DELETE, BEGIN/COMMIT）
```
両関数の中身（共通契約）:
```
seam(ctx, sql, params, intent):
    return ctx.middleware.wrap(sql, params, (sql, params) =>   # ① middleware chain
        conn = ctx.connectionFor(intent)                       # ② 接続解決（tx/reader/writer/db）
        return conn.exec(sql, params)                          # ③ 実行（唯一の driver 接点）
    )
```
→ **全 SQL がこの 1 点を通る**。middleware も接続解決もここに集約。既存の `driver.prepare().all()` 直呼びを全て `execute/run(ctx, …)` に置換。

---

## 3. 接続コンテキストモデル（per-execution・言語別）

**原則: 接続は「実行スコープ」が所有する。driver グローバルスロットは廃止。並行実行（HTTP req/tx）は各自別接続。**

`connectionFor(intent)` の解決優先順位（v1 準拠、`DBModel.ts:313` の順）:
1. **アクティブ tx の接続**（tx スコープ内なら必ずその接続）
2. **withWriter スコープ** / **writer-sticky**（直近 tx 後 `writerStickyDuration` 内の read は writer へ）
3. **read=reader pool / write=writer pool**（reader/writer 分離時）
4. **named-DB ルーティング**（model→connection、複数DB時）→ 対象 DB の pool

言語別の「実行スコープ所有」実装:
| 言語 | ctx 伝播 | tx 接続所有 |
|---|---|---|
| **TS** | `AsyncLocalStorage<Ctx>`（v1 と同じ、非同期境界を跨ぐ） | tx が接続を ALS に束ねる（v1 `txContext.run`） |
| **rust** | ctx を**引数で明示 thread**（or `tokio::task_local`）。current-thread runtime（v1rs 準拠、block_on 軽量化） | `PoolTransaction`（接続所有ハンドル、v1 rust `handler.rs`）を ctx が保持 |
| **go** | `context.Context` に ctx を載せる | `*sql.Tx`（接続所有）を ctx が保持 |
| **py** | `contextvars.ContextVar<Ctx>` | tx 接続を contextvar に束ねる |
| **php** | ctx を**明示引数**（PHP は 1 req 1 プロセスで並行 tx 無し、但し接続所有は同契約） | tx が PDO 接続を ctx 内で所有 |

**tx の per-execution 所有（並行 tx バグ根治）**:
```
withTransaction(ctx, opts, fn) -> R:
    conn = pool(writer, targetDB).acquire()      # tx 専用に 1 接続取得
    txctx = ctx.withConnection(conn, tx=true)     # スコープに束ねる（ALS/task-local/owned）
    conn.exec(BEGIN [ISOLATION opts.isolation])
    try:  r = fn(txctx)                            # fn 内の全 SQL は connectionFor→この conn
          conn.exec(COMMIT); return r
    except: conn.exec(ROLLBACK); (retry? #69-B3); raise
    finally: pool.release(conn) [or destroy if bad #69-B4]
```
→ **並行 tx は各々別 conn を所有**。driver グローバル `writer` スロット（rust:431 等）を**全言語で撤去**。

---

## 4. Middleware hook 機構（5言語共通・TS定義主・native登録可）

**ctx が middleware chain を保持**、中央 seam（§2 ①）が全 SQL をラップ。2 レベル:
- **SQL-level `execute` フック**（全 SQL 横取り）= 中央 seam に組込。**5言語全部**に hook 点あり。
- **method-level フック**（find?/create?/update?…）= ORM メソッド入口でラップ（decorator model は TS、native は該当メソッド境界）。

**登録**:
- **TS が主**（v1 `DBModel.use(MiddlewareClass)` 相当）。middleware chain を構築。
- **native 側でも登録可**: rust/go/py/php 各 runtime に `register_middleware(mw)` API を設け、その言語の ctx chain に append。
- chain の**契約（`execute(next, sql, params)`）と適用順は 5 言語共通**。

middleware は ctx（実行スコープ）に紐づく → per-request/context 分離（v1 ALS と同じ）。

---

## 5. seam 契約（共通シェイプ・言語別実装の型）

疑似型（各言語に写す）:
```
interface ExecutionContext {
    connectionFor(intent: { write: bool, db?: string }): Connection
    readonly middleware: MiddlewareChain
    withConnection(conn, tx: bool): ExecutionContext   // tx スコープ派生
    // config（timeout/limit）は Phase C/E で追加
}
interface Connection { exec(sql, params): Rows | RunInfo }   // 唯一の driver 接点
interface MiddlewareChain { wrap(sql, params, next): result }
execute(ctx, sql, params): Rows
run(ctx, sql, params): RunInfo
withTransaction(ctx, opts, fn): R
```
- 既存 `executeBundle(bundle, input, driver)` は `executeBundle(bundle, input, ctx)` に。呼び出し側は `ctx = Context.forDriver(driver)`（単一DB・middleware無し時は薄いラッパ＝後方互換）。
- relation walker / tx walker は `driver.prepare().all()` を `execute/run(ctx, …)` に置換。

---

## 6. 移行と後方互換

- **既存の単純パス**（bench/conformance が driver 直渡し）は `Context.forDriver(driver)`（reader=writer=同 pool・middleware 空・単一DB）で**挙動不変**。conformance/livedb は byte 不変で通す。
- Phase A は「seam + ctx + tx所有 + 並行tx修正」まで。reader/writer(§3-2/3)・複数DB(§3-4)・middleware 登録 API・config は **B/C/D で ctx を拡張**（本設計の ctx がその受け皿）。
- **並行 tx 隔離テスト**（#69 AC）を Phase A の完了条件に含める（現行 fail→修正で green、全言語・実 DB）。

---

## 7. Phase A の完了条件（AC）

- [ ] 中央 `execute/run(ctx, …)` seam を 5 言語に実装、全 SQL（read/write/tx/relation）がそこを通る（直 driver 呼び出しゼロを grep で担保）。
- [ ] `ExecutionContext` + per-execution 接続所有（tx が接続をスコープ所有）。driver グローバル tx スロット全言語撤去。
- [ ] **並行 tx 隔離テスト green**（同一プロセス・共有 runtime で N tx 同時、混入なし・原子性保持、実 PG/MySQL、全言語）。現行コードで fail することも実証。
- [ ] middleware chain 挿入点を seam に用意（空 chain で挙動不変。登録 API 実体は #70/Phase D）。
- [ ] conformance/livedb/unit/bench 全 green・byte 不変（後方互換ラッパ経由）、全ゲート native ARM。
- [ ] 独立監査（並行 tx の隔離を mutation で実証）。

---

## 8. 要判断（sign-off 前に確認したい）

1. **rust の ctx 伝播**: 明示引数 thread か `tokio::task_local` か（前者は型安全・後者は v1 TS の ALS に近い）。→ 提案: **明示引数**（rust らしく静的・所有明確）。
2. **current-thread runtime 化**（rust、block_on 軽量化・v1rs 準拠）を Phase A に含めるか（perf にも効く）。→ 提案: 含める。
3. **seam の粒度**: statement 単位（各 SQL）で middleware/接続解決。read-graph の serial 連鎖は同一 tx/接続スコープを共有（§3）。並列 read fan-out は別接続（既存 bc#23 維持）。→ 確認。
