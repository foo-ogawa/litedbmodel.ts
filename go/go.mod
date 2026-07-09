module github.com/foo-ogawa/litedbmodel/go

go 1.21

// behavior-contracts (Go module): the litedbmodel SCP runtime delegates the CLOSED
// Expression-IR evaluation to the shared common core, mirroring the TS reference's npm
// dependency. Pinned to the PUBLISHED VCS tag `go/v0.2.0` (NO local `replace` onto a sibling
// checkout — the no-local-deps gate forbids it).
//
// behavior-contracts is a PRIVATE repo: resolving it needs GOPRIVATE + authenticated git
// access (see .github/workflows/ci.yml, mirroring graphddb's go build).
require github.com/foo-ogawa/behavior-contracts/go v0.2.0
