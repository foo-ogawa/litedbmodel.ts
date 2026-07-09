# Releasing litedbmodel (5-registry, owner-gated)

litedbmodel v2 ships from **one monorepo** to **five registries**, all pinned to a single version
(the `package.json` `version` field is the **SSoT**; `scripts/sync-versions.mjs` propagates it):

| Registry   | Package                                   | Source of truth                          |
|------------|-------------------------------------------|------------------------------------------|
| npm        | `litedbmodel`                             | `package.json` (the SSoT)                |
| PyPI       | `litedbmodel-runtime`                     | `python/pyproject.toml` + `__init__.py`  |
| crates.io  | `litedbmodel_runtime`                     | `rust/litedbmodel_runtime/Cargo.toml` + `src/lib.rs` |
| Go         | `github.com/foo-ogawa/litedbmodel/go`     | VCS tag `go/vX.Y.Z` (no registry upload) |
| Packagist  | `litedbmodel/runtime`                     | git tag `vX.Y.Z` via repo webhook        |

> **⚠️ Publishing is irreversible.** npm / PyPI / crates.io do not allow re-publishing a version,
> and a pushed git tag is public. Everything below the "OWNER-GATED" line is the owner's call.

---

## How publish is triggered (the automation)

The chain is **merge-to-`main` → GitHub Release → publish workflows**, mirroring graphddb:

1. **`release.yml`** (`GitHub Release`) runs on push to `main` (or `workflow_dispatch`). It runs the
   release-discipline gates (`sync:versions:check`, `deps:check`), audit, build, tests, `npm pack
   --dry-run`, then — **only if the `vX.Y.Z` tag does not already exist** — creates the GitHub
   Release (`gh release create vX.Y.Z`) **and** pushes the Go submodule tag `go/vX.Y.Z` on the same
   commit. Idempotent: a re-run is a no-op if the tag already exists; the Go-tag step self-heals a
   transiently-missing tag.
2. The three registry publishers chain off `workflow_run: ["GitHub Release"] completed` (GitHub does
   **not** re-fire `release: published` for a token-created release, so they cannot rely on it):
   - **`publish.yml`** → npm (`litedbmodel`)
   - **`publish-pypi.yml`** → PyPI (`litedbmodel-runtime`)
   - **`publish-crates.yml`** → crates.io (`litedbmodel_runtime`)
   Each is **idempotent** (skips if that version already exists on the registry) and each runs its
   own dry-run/build/test before uploading.
3. **Packagist** (`litedbmodel/runtime`) needs no workflow — its repo webhook syncs the new
   `vX.Y.Z` tag automatically. (First release only: submit the repo once at packagist.org.)
4. **Go** needs no upload — `go get .../litedbmodel/go@vX.Y.Z` resolves the `go/vX.Y.Z` tag that
   `release.yml` pushed.

### One-time prerequisites — MUST be configured before the release sequence

Status verified **2026-07-10** against `foo-ogawa/litedbmodel`. None of the ❌ items are done yet;
each blocks the step noted.

**A. Repo secrets** (`gh secret set <NAME> --repo foo-ogawa/litedbmodel`)

| Secret | Purpose | Status |
|---|---|---|
| `NPM_TOKEN` | npm publish (`litedbmodel`) | ✅ present — **verify it is an *automation* token** with publish rights (bypasses 2FA) |
| `PYPI_API_TOKEN` | PyPI publish (`litedbmodel-runtime`) | ❌ missing — see PyPI note below |
| `CARGO_REGISTRY_TOKEN` | crates.io publish (`litedbmodel_runtime`) | ❌ missing — publishing account must have a verified email |
| `BEHAVIOR_CONTRACTS_PAT` | private `behavior-contracts` **Go** module fetch (CI + go build) | ❌ missing — fine-grained PAT, **Contents: Read on `foo-ogawa/behavior-contracts`** (same name graphddb uses). Currently blocks the 3 CI jobs `conformance-ts` / `conformance-livedb` / `scaffold-build (go)` |

**B. `release` GitHub Environment — NOT YET CREATED (currently 0 environments).** The three registry
publish jobs declare `environment: release`. If the environment does not exist, GitHub auto-creates
it **with no protection rules on first run → publish would proceed WITHOUT approval**. You MUST
create it first: repo Settings → Environments → `release` → **Required reviewers = the owner**. Then
every registry upload pauses for explicit owner approval in the Actions UI — the single sign-off
point for npm + PyPI + crates.io. Publish is never automatic on merge.

**C. Registry account setup**

| Registry | Name | Status / action |
|---|---|---|
| npm | `litedbmodel` | ✅ owned by foo-ogawa (v1.2.10) — v2.0.0 publishes over it |
| PyPI | `litedbmodel-runtime` | 🆓 unclaimed (404). First publish claims it. Project doesn't exist yet, so a project-scoped token can't be minted — use an **account-scoped** `PYPI_API_TOKEN` for the first release, **or (recommended) configure PyPI Trusted Publishing (OIDC)** and drop the token entirely |
| crates.io | `litedbmodel_runtime` | 🆓 likely free (novel name). Needs `CARGO_REGISTRY_TOKEN` + verified-email account |
| Packagist | `litedbmodel/runtime` | ❌ not submitted (404). **First release only: submit the repo once at packagist.org** and connect the GitHub→Packagist webhook (thereafter tag pushes auto-sync) |

**D. Downstream note (not a publish blocker):** the Go module fetches `behavior-contracts` from its
**private** repo, so anyone consuming `github.com/foo-ogawa/litedbmodel/go` needs read access to
`foo-ogawa/behavior-contracts`. If the Go module is meant for public consumption, make bc's Go module
public (or otherwise redistribute it) before advertising the Go package.

---

## OWNER-GATED release sequence (do this once, per version)

Prereq (already done for 2.0.0 on branch `ws8b-release`): version bumped to the target in the SSoT,
`sync:versions` run, all dry-runs + conformance green, CHANGELOG updated. Verify with the
"Pre-release checklist" below.

1. **Merge the release PR to `main`** with `gh pr merge --merge` (NOT squash / rebase — history is
   preserved). ← *this is the irreversible trigger.*
2. `release.yml` runs on `main`, creates the `vX.Y.Z` GitHub Release, and pushes `go/vX.Y.Z`.
3. The three publish workflows queue and **wait on the `release` Environment**. **Approve each** in
   the Actions UI. They publish npm + PyPI + crates.io (idempotent).
4. Confirm Packagist picked up `vX.Y.Z` (packagist.org/packages/litedbmodel/runtime). If the webhook
   is not configured yet, click "Update" / submit the repo once.
5. Smoke-verify each published artifact resolves from a clean environment:
   - `npm view litedbmodel@X.Y.Z version`
   - `pip install litedbmodel-runtime==X.Y.Z` (fresh venv)
   - `cargo add litedbmodel_runtime@X.Y.Z` (throwaway crate)
   - `go get github.com/foo-ogawa/litedbmodel/go@vX.Y.Z`
   - `composer require litedbmodel/runtime:^X`
6. **Archive `foo-ogawa/litedbmodel.rs`** — see below.

---

## litedbmodel.rs archive plan (owner action AT release)

Per spec §14, the Rust runtime has moved into this monorepo's `rust/` and the standalone
`foo-ogawa/litedbmodel.rs` repository is retired at GA. **Do this only after the crates.io publish
of `litedbmodel_runtime@X.Y.Z` from THIS monorepo has succeeded**, so there is no gap:

1. In the old repo's README, add a deprecation banner pointing to the monorepo + the crates.io
   package (`litedbmodel_runtime`).
2. Optionally keep it as a **crate-mirror only** (no further development) if any consumer still
   pins the old coordinates; otherwise proceed to archive.
3. GitHub → `foo-ogawa/litedbmodel.rs` → Settings → **Archive this repository** (read-only).

This is a manual owner action and is intentionally **not** automated.

---

## Pre-release checklist (all green before the merge in step 1)

Run from the repo root:

- [ ] `npm run sync:versions:check`  — every language target in lockstep at the SSoT version
- [ ] `npm run deps:check`           — no `../`-escaping local deps in any manifest
- [ ] `npm run build`                — TS build + SCP bundle
- [ ] `npm run lint`                 — eslint clean
- [ ] `npx vitest run test/scp test/unit`
- [ ] `npm run conformance:run`      — 5-lang 49/49 + cross-language agreement + codegen leg
- [ ] `npm publish --dry-run`        — tarball has `dist/`, no `src/`/`../` leaks
- [ ] `(cd python && python -m build && twine check dist/*)`
- [ ] fresh-venv wheel smoke — `pip install` the built wheel + run a real vector
- [ ] `(cd rust && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test && cargo publish -p litedbmodel_runtime --dry-run)`
- [ ] `(cd php && composer validate && vendor/bin/phpunit)`
- [ ] `(cd go && gofmt -l . ; go vet ./... ; go test ./...)` — module path `.../litedbmodel/go`
- [ ] live-DB (optional, needs docker): `npm run conformance:livedb:docker` — py/php/go/rust × PG+MySQL

---

## Repository name — RESOLVED (2026-07-10)

The GitHub repository has been renamed `litedbmodel.ts` → **`litedbmodel`** (owner decision; GitHub
301-redirects the old URL). All manifests now agree on `github.com/foo-ogawa/litedbmodel`:
`go/go.mod` (`.../litedbmodel/go`), `rust/litedbmodel_runtime/Cargo.toml` (`repository`), and
`package.json` (`repository`/`homepage`). Go's VCS-tag resolution and the Packagist webhook resolve
against the live repo path. No further action required here.
