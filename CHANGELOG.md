# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [2.0.0] - 2026-07-10

**BREAKING — v2.0 系リリース。** litedbmodel は「独自 ORM」から
**behavior-contracts の汎用 SCP レイヤを consume する SQL バックエンド consumer** へ再構成された
（graphddb=DynamoDB backend と対）。公開境界は CQRS（Query/Command）契約で、TS / Python / Rust /
Go / PHP の薄い runtime が **同一 IR から同一 SQL・同一結果**を出す（5 言語 conformance）。

v1.x はメンテナンスブランチ `v1.x` で保全（別トラック）。移行は下の "Migration: v1 → v2" を参照。

### Added

- **SCP IR レイヤ**（`litedbmodel/scp`）: Authoring Parse → 内部 IR → Backend Compile（IR→dialect
  SQL）→ 薄い Runtime。コンパイル経路は1本、実行モードが3つ（TS 直接 eager / SCP 宣言ブロックの
  事前コンパイル / 多言語 runtime での実行）。
- **全 CRUD × 全方言**: Select / Insert / Update / Delete × PostgreSQL / MySQL / SQLite。
- **write-time relations + tx DAG**: 複合 write（複数 base write / nested write）を 1 トランザクション
  へ導出。各 write は名前を持ち、後続 write は先行 write の RETURNING 行を `$.ref.<name>.<field>` で
  参照する。データ依存グラフ + gate-first 制約をトポロジカルソートし、byte-identical な SQL 列を生成。
  依存サイクル / 宙ぶらりんの `$.ref` / RETURNING 欠落は loud-reject（暗黙フォールバック無し）。
- **5 言語 runtime**: Python (`litedbmodel-runtime` / PyPI)、Rust (`litedbmodel_runtime` / crates.io)、
  Go (`github.com/foo-ogawa/litedbmodel/go`、VCS タグ `go/vX.Y.Z`)、PHP (`litedbmodel/runtime` /
  Packagist)。いずれも Expression-IR 評価を behavior-contracts へ委譲。
- **モノレポ統合**: `src/`(TS SSoT) + `python/ go/ php/ rust/` を同居。単一 `conformance/` +
  単一 CI + 単一 `sync-versions.mjs`（package.json = version SSoT）。
- **codegen**: bc 共有ジェネレータに SQL catalog を供給して各言語コードを生成。
- **live-DB conformance**: 実 PostgreSQL + MySQL に対する 4 言語 runtime の live-DB 検証。

### Changed (BREAKING)

- **結果オブジェクト: DBModel インスタンス → typed-object。** クエリ結果は own props が
  データのみの typed-object になった。インスタンスメソッドは持たない。ドメインメソッドが必要なら
  `hydrate: (raw) => new Domain(raw)` で回復する（破壊度: 中〜大）。
- **カラム順の正規化**: 生成 SQL のカラム列は決定的な**アルファベット順（canonical order）**に固定
  （多言語 byte 一致のため）。SQL テキストに依存したスナップショットは影響を受ける。
- **単一コンパイル経路**: 公開 API 呼び出しも SCP 宣言も同一の Authoring Parse → 内部 IR を通る
  （別解釈系を持たない）。実行時文字列組み立ての内部経路は IR 経由に置き換わった。
- **`sql` / dbDynamic / dbRaw**: 実行時文字列から Dynamic Slot 語彙（lower 可能サブセット）へ。
- **完全動的 Raw SQL**: `execute` / `query` は「契約付き Raw SQL」（方言別 SQL 同梱・IR 不透明）に隔離。

### Preserved (v1 parity)

- CRUD + condition タプル + SKIP はほぼ不変（内部が IR 経路になっただけ）。
- `await post.author`（lazy relation）は getter として残置（事前コンパイル relation op 起動）。
- Middleware / TypeCast は Runtime 関心事として存続。

### Migration: v1 → v2

詳細は仕様書 [`docs/proposal/litedbmodel-v2-scp-architecture.md`](docs/proposal/litedbmodel-v2-scp-architecture.md)
§12（TS 公開 API の v1 → v2 移行）を参照。要点:

1. **結果はインスタンスではない。** `row instanceof MyModel` は成立しない。`row.someMethod()` は
   `TypeError`。ドメインメソッドは `model({ ..., hydrate: (raw) => new MyDomain(raw) })` で復元し、
   `hydrate` した戻り値に対して呼ぶ。own props はデータのみなので `{ ...row }` / `JSON` は安全。
2. **カラム順に依存しない。** SQL 文字列の完全一致を検証しているテストは、v2 の canonical
   アルファベット順に合わせて再スナップショットする。
3. **動的 SQL の見直し。** ランタイム文字列を組み立てていた箇所は Dynamic Slot 語彙へ移す。lower
   できない完全動的 SQL は契約付き Raw SQL（`execute`/`query`、方言別 SQL 同梱）へ隔離する。
4. **多言語で使う場合**は publish された §8 bundle を各言語 runtime（PyPI/crates.io/Go/Packagist）
   から呼ぶ。同一 IR → 同一 SQL・同一結果（conformance で保証）。
5. **v1 のまま留まる場合**は `v1.x` メンテナンスブランチを使う（`litedbmodel@^1`）。

[2.0.0]: https://github.com/foo-ogawa/litedbmodel/releases/tag/v2.0.0
