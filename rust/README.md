# litedbmodel_runtime (Rust)

The Rust leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a SQL driver, semantics-identical to the TS
reference (`src/scp`).

**Status: WS7a scaffold.** Buildable cargo workspace skeleton + conformance runner entry point; the
runtime body is **WS7d**.

## behavior-contracts dependency

The runtime delegates the CLOSED Expression-IR evaluation to the shared common core
[`behavior-contracts`](https://crates.io/crates/behavior-contracts) crate, **consumed from
crates.io** (`behavior-contracts = "0.2.0"`), mirroring the TS reference's npm dependency. No
local `path = "../.."` escaping the repo — the `check-no-local-deps` gate forbids it (the
`vectors_runner → litedbmodel_runtime` path is an in-repo workspace member, which is allowed).

## Layout

```
rust/
  Cargo.toml                          # workspace (crates.io behavior-contracts pinned)
  litedbmodel_runtime/
    Cargo.toml                        # published crate (litedbmodel_runtime), version-synced
    src/lib.rs                        # WS7d: the §8 bundle interpreter surface
  vectors_runner/
    Cargo.toml
    src/main.rs                       # conformance runner entry (WS7d body)
```
