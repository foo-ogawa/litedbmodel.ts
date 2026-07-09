# litedbmodel/go

The Go leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a `database/sql` driver,
semantics-identical to the TS reference (`src/scp`).

**Status: WS7a scaffold.** Buildable module skeleton + conformance runner entry point; the runtime
body is **WS7c**.

## behavior-contracts dependency

The runtime delegates the CLOSED Expression-IR evaluation to the shared common core
`github.com/foo-ogawa/behavior-contracts/go`, **consumed via the published VCS tag `go/v0.2.0`**
(mirroring graphddb). No local `replace` onto a sibling checkout — the `check-no-local-deps` gate
forbids `../`-escaping deps.

behavior-contracts is a **private** repo: `go build` needs `GOPRIVATE=github.com/foo-ogawa/*`
plus authenticated git access (wired in CI, mirroring graphddb's `go` workflow).

## Layout

```
go/
  go.mod                          # module github.com/foo-ogawa/litedbmodel/go (Go = VCS tag release)
  litedbmodel_runtime/runtime.go  # WS7c: the §8 bundle interpreter surface
  vectors_runner/main.go          # conformance runner entry (WS7c body)
```

## Versioning

Go publishes by VCS tag, not a manifest version field. `scripts/sync-versions.mjs` mirrors
`package.json`'s version into the `Version` constant in `runtime.go`; the release tag is
`go/v<version>`.
