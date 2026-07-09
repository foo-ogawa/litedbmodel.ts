# litedbmodel_runtime (Rust)

The Rust leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a SQL driver, semantics-identical to the TS
reference (`src/scp`) and the Python/PHP sibling ports.

**Status: WS7e — implemented (#34).** The runtime consumes the published §8 bundle + the
`behavior-contracts` common core and executes it against an in-process SQLite driver, passing the
frozen 47-vector conformance corpus byte-for-byte across all three dialects and agreeing with the
TS/Python/PHP runtimes.

## behavior-contracts dependency

The runtime delegates the CLOSED Expression-IR evaluation and the plan/map/wire/output
orchestration to the shared common core
[`behavior-contracts`](https://crates.io/crates/behavior-contracts) crate (`run_behavior` /
`evaluate_expression`), **consumed from crates.io** (`behavior-contracts = "0.2.0"`), mirroring the
TS reference's npm dependency. No local `path = "../.."` escaping the repo — the
`check-no-local-deps` gate forbids it (the `vectors_runner → litedbmodel_runtime` path is an in-repo
workspace member, which is allowed). The runtime re-implements **no** generic evaluator and **no**
SQL generation: the SQL text comes wholly from the published bundle.

## Layout

```
rust/
  Cargo.toml                          # workspace (crates.io behavior-contracts + rusqlite bundled)
  litedbmodel_runtime/
    Cargo.toml                        # published crate (litedbmodel_runtime), version-synced
    src/lib.rs                        # public surface + module map
    src/dialect.rs                    # ?→$N finalize + orderByNulls (spec §4/§8/§10)
    src/render.rs                     # NORMATIVE fragment-tree render + param assembly
    src/driver.rs                     # SQL-driver seam (in-proc rusqlite; PG/MySQL plug in later)
    src/errors.rs                     # SQLite error → structured SqlFailure (kind + bc Policy)
    src/value.rs                      # JSON ⇄ bc Value + the $bigint conformance codec
    src/runtime.rs                    # render → execute → assembly; gate-first write transaction
    tests/conformance.rs              # crate-local cargo-test gate (render/exec/tx)
  vectors_runner/
    Cargo.toml
    src/main.rs                       # runs the frozen vector corpus, emits the lang="rust" summary
```

## Conformance

```
cargo run --quiet --bin vectors_runner   # 47/47 across render/exec/tx/dialect (real in-proc sqlite)
cargo test                                # crate-local integration tests
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The cross-language orchestrator (`conformance/vectors-run.ts`, `npm run conformance:run`) launches
this runner as the Rust leg and asserts per-suite agreement across ts/py/php/rust.

## SQL-handler seam (PG/MySQL)

The conformance bar executes against an **in-process `rusqlite`** connection — the sanctioned
in-proc substitute for a docker integration DB (#34 AC). Live PostgreSQL/MySQL execution is
**deferred to the coordinated cross-language docker pass**; a `tokio-postgres` / `mysql_async`
driver plugs into the same `Driver` trait (over the `$N` / `?` paramstyle the bundle's dialect
already emits) with no runtime change. PG/MySQL **SQL-text** conformance is already proven on the
render axis (all three dialects reproduced byte-for-byte).

## Migration note: `foo-ogawa/litedbmodel.rs` → archive (WS8)

This crate is the monorepo consolidation of the previously standalone `foo-ogawa/litedbmodel.rs`
repository (v0.4.5). That repo carried its **own** SQL generation; here it is fully **retired** —
the SQL comes wholly from the published §8 bundle rendered by `render.rs`, and Expression-IR
evaluation is delegated to `behavior-contracts`. The old `foo-ogawa/litedbmodel.rs` repository is
to be **archived (read-only) at the WS8 release**, once this consolidated crate is published to
crates.io. It is intentionally **not** archived now — the archive is a WS8 release-time step so the
old repo stays available as a reference until the v2 Rust crate ships.
