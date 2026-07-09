// Package litedbmodel_runtime is the Go leg of the litedbmodel v2 SCP multi-language runtime
// (WS7c scaffold, #30).
//
// It interprets the language-neutral §8 published bundle (SqlBundle: sql + fragment tree +
// closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it
// against a database/sql driver, semantics-identical to the TS reference (src/scp). The generic
// Expression-IR evaluation is delegated to the shared common core behavior-contracts (Go module),
// mirroring the TS reference's npm dependency — this package re-implements no generic evaluator.
//
// WS7A_SCAFFOLD: the runtime surface is declared here; the bodies are WS7c. They return an error
// so a premature call fails loudly instead of returning a fake result.
package litedbmodel_runtime

import "errors"

// Version is synced from package.json by scripts/sync-versions.mjs (Go = VCS tag, not a manifest
// field, so this constant is the in-source mirror the CI tag check compares against).
const Version = "1.2.10"

// ErrNotImplemented marks the WS7c runtime bodies not yet implemented.
var ErrNotImplemented = errors.New("litedbmodel/go: runtime is WS7c (WS7a scaffold only)")

// Rendered is the output of rendering one §8 CompiledOperation.
type Rendered struct {
	SQL    string
	Params []any
}

// RenderOperation renders a §8 CompiledOperation against a scope for a dialect. WS7c.
func RenderOperation(operation map[string]any, scope map[string]any, dialect string) (Rendered, error) {
	return Rendered{}, ErrNotImplemented
}

// ExecuteBundle executes a §8 read/exec SqlBundle end-to-end. WS7c.
func ExecuteBundle(bundle map[string]any, input map[string]any, db any) (any, error) {
	return nil, ErrNotImplemented
}

// ExecuteTransactionBundle executes a §8 write-tx SqlBundle as one gate-first transaction. WS7c.
func ExecuteTransactionBundle(bundle map[string]any, input map[string]any, db any) (any, error) {
	return nil, ErrNotImplemented
}

// OrderByNulls is the dialect NULLS-ordering primitive. WS7c.
func OrderByNulls(expr, direction, nulls, dialect string) (string, error) {
	return "", ErrNotImplemented
}
