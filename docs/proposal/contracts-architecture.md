SQL Contracts Architecture Draft

概要

本仕様は、litedbmodel の SQL-first 設計と GraphDDB の Contract / Planner / Runtime アーキテクチャを統合し、RDB 向けの契約駆動ランタイムを定義する。

目的は ORM を作ることではなく、

* 論理モデルを契約化する
* CQRS を公開IFとする
* SQL生成・トランザクション・実行計画を導出する
* 多言語 Runtime を実現する

ことである。

⸻

設計思想

SQL-first

SQLを隠蔽しない。

複雑な検索は SQL をそのまま利用できる。

ただし SQL であっても

* 入力
* 出力
* Safety
* Effect
* Transaction

は契約化する。

⸻

論理モデル中心

外部に見えるものは論理モデルである。

Logical Model
↓
CQRS Contract
↓
Execution Plan
↓
Dialect SQL

DB物理構造はモデル定義が吸収する。

⸻

CQRS公開IF

公開されるAPIは CQRS のみ。

Query
Command

Repository や ORM API は外部公開しない。

⸻

レイヤ構成

Logical Model
↓
Model Mapping
↓
Access Pattern Contract
↓
Composition IR
↓
SQL IR
↓
Dialect SQL
↓
Runtime

⸻

モデル定義

モデル定義は論理モデルと物理配置を対応付ける。

Logical User
↓
users table
↓
columns
↓
relations
↓
indexes

ここでは

* belongsTo
* hasOne
* hasMany
* many-to-many

などの意味だけを定義する。

⸻

Access Pattern

アクセスパターンが公開契約になる。

例

GetUser
SearchUsers
PatchUser
ReplaceUserRoles

ここから

* SQL
* Transaction
* Object Assembly

が導出される。

⸻

Composition IR

最初のIR。

SQLではない。

責務は

* Query API 呼び出し
* データフロー
* バッチロード
* Object Assembly

である。

例

User.findOne
↓
Post.findByUserIds
↓
Attach posts

保持するもの

API Node
Data Flow
Object Assembly
Execution Dependency

⸻

SQL IR

Composition IR を SQL に変換したもの。

ここでは

* SELECT
* UPDATE
* INSERT
* DELETE
* CTE
* Parameter
* Placeholder

などが扱われる。

Dialect 非依存。

⸻

Dialect SQL

最後に

PostgreSQL
MySQL
SQLite

へ変換する。

Runtime は SQL を生成しない。

⸻

Runtime

Runtime の責務は最小限。

Input Validation
↓
Dynamic Slot Evaluation
↓
Placeholder Expansion
↓
SQL Execute
↓
Object Assembly

SQL生成ロジックは持たない。

⸻

SKIP

SKIP は契約言語。

単なる便利機能ではない。

対象

WHERE
SET
Execution Step

例

name = input.name ?? SKIP
status = input.status ?? SKIP

⸻

Dynamic Slot

値だけではなく構造も Placeholder にする。

種類

Value
Column
Condition
Fragment
Set
Order

例

sql`
WHERE
?
`

に

Condition を埋め込める。

⸻

SQL Template

SQL Template は

静的部分

と

動的部分

を持つ。

Compile
↓
Static Flatten
↓
Runtime Expansion

Rust版と同様、

静的に確定できるものはできるだけ展開する。

動的条件だけ Runtime に残す。

⸻

静的解析

Compile 時に

Model
Relation
Column
Type
Scope
Placeholder

をすべて解決する。

Runtime で

Unknown Column
Unknown Relation

などは発生しない。

⸻

Relation

Relation は SQL JOIN ではない。

まず

hasMany
↓
Query Composition

へ展開される。

例えば

User
↓
Post.findByUserIds
↓
Object Assembly

SQL JOIN ではなく

Query の組み合わせになる。

⸻

Object Assembly

取得した Row を

論理モデルへ組み立てる。

例

User
↓
posts[]
↓
comments[]

この Assembly も IR に含まれる。

⸻

Relation Filter

JOIN 構文そのものではなく

Relation Filter を定義する。

例

User
WHERE
status = active
AND
Membership.group = xxx

公開契約では

User.filterThrough(...)

のような意味になる。

内部では

JOIN
EXISTS
IN

などへ変換可能。

⸻

Raw SQL

Raw SQL は Escape Hatch。

ただし契約外ではない。

契約化するもの

Input
Output
Safety
Effect
Dialect

SQLだけ自由に書ける。

⸻

Transaction

Transaction DAG を公開仕様にはしない。

公開されるのは

Access Pattern

のみ。

Transaction は導出される。

Access Pattern
↓
Dependency Graph
↓
Transaction DAG
↓
Execution Plan

⸻

Execution Planning

Planner が

Execution Graph

を生成する。

ここで

* Dependency
* Safety
* Cardinality

を考慮する。

⸻

Gate First

意味が変わらない限り

Gate を最優先で実行する。

例

Input Validation
↓
SET Exists
↓
WHERE Exists
↓
UPDATE
↓
Audit

不要な SQL はできるだけ早く打ち切る。

⸻

Static vs Dynamic

Compile

Model
Relation
Column
Join
Projection

Runtime

SKIP
Condition
Parameter
Dynamic Fragment

Compile 時に可能な限り解決する。

⸻

Multi-language Runtime

生成対象

TypeScript
Go
Rust
Python
PHP

Runtime は

Contract
↓
SQL Execute
↓
Assembly

だけを担当する。

⸻

Conformance

全 Runtime は

同一 Contract

同一 Input

から

同一 SQL

同一 Result

を生成しなければならない。

⸻

Responsibility

Model

物理配置との対応

Contract

公開API

Planner

Execution Plan導出

Compiler

Dialect SQL生成

Runtime

SQL実行

⸻

最終アーキテクチャ

Logical Model
        │
        ▼
 Model Mapping
        │
        ▼
Access Pattern Contract
        │
        ▼
Composition IR
(API Graph + Assembly)
        │
        ▼
SQL IR
(Condition / Placeholder / SQL Template)
        │
        ▼
Dialect SQL
(PostgreSQL/MySQL/SQLite)
        │
        ▼
Runtime
(Bind / Execute / Assemble)

中心思想

* 論理モデルを公開契約とする
* SQLを隠蔽しない
* 実行計画は契約から導出する
* Transactionは公開せず導出する
* SKIP・Condition・Relationを契約化する
* Compile時に可能な限り静的解析し、Runtimeでは最小限の動的解決のみ行う
* 多言語 Runtime は同一 Contract を実行するだけの薄い実装とする