// Package litedbmodel_runtime is the Go leg of the litedbmodel v2 SCP multi-language runtime
// (WS7c, #32).
//
// It consumes the language-neutral §8 published bundle (SqlBundle: sql + fragment tree +
// closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it against
// a database/sql driver, semantics-identical to the TS reference (src/scp). It is NATIVE: the
// read-graph orchestration (which node runs, map iteration, wire binding, output assembly) is a
// CLOSED-SET native walker (executeReadGraphNative) — NOT bc.RunBehavior (the generic IR
// interpreter, retired). The only bc dependency on the exec path is the per-statement typed-param
// evaluation (bc.EvaluateExpression resolves each deferred `{ref:…}`/`coalesce`/`__jsonArray` slot);
// every statement's SQL is fixed text carried verbatim.
//
// Execution pipeline (spec §3):
//
//	validate → fragment select (SKIP) → array expand → param eval + bind → SQL execute → native assembly
//
// #12: the read graph carries compileBehaviors' REAL Select/Count/map nodes; the native walker
// renders each node's pre-compiled statementsById against the walk scope directly (no `__scope`).

package litedbmodel_runtime

import (
	"fmt"
	"regexp"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// Version is synced from package.json by scripts/sync-versions.mjs (Go = VCS tag, not a manifest
// field, so this constant is the in-source mirror the CI tag check compares against).
const Version = "2.0.2"

// entityRoot is the body-write RETURNING row exposed to later tx stages under `$.entity.*`
// (writes.ts ENTITY_ROOT).
const entityRoot = "__entity"

var returningRe = regexp.MustCompile(`(?i)\breturning\b`)

// SqlBundle is the parsed makeSQL published bundle. A READ bundle carries a `readGraph` (the
// REAL Select-node ComponentGraphIR + per-node static statement templates); a WRITE bundle carries a
// gate-first `transaction` plan. The heavy fields stay as raw bc JNodes so their Expression-IR slots
// are evaluated by bc (not pre-decoded), byte-true to the TS SqlBundle.
type SqlBundle struct {
	Dialect     string
	Name        string
	ReadGraph   *ReadGraph // read/exec bundle: the static makeSQL read graph (nil for write bundles)
	Transaction *bc.JObj   // write-tx plan (nil for read/exec bundles)
}

// ParseBundle parses a makeSQL bundle from its pure-JSON bytes (the published artifact) into a
// SqlBundle whose IR/param slots stay as bc JNodes. This is the "executes from published JSON
// alone" entry point: no TS state, no re-derivation.
func ParseBundle(data []byte) (*SqlBundle, error) {
	root, err := bc.ParseJSONOrdered(data)
	if err != nil {
		return nil, fmt.Errorf("scp runtime: bundle parse: %w", err)
	}
	obj, ok := root.(*bc.JObj)
	if !ok {
		return nil, fmt.Errorf("scp runtime: bundle root is not an object")
	}
	return BundleFromJObj(obj)
}

// BundleFromJObj builds a SqlBundle from a parsed bundle object (shared by ParseBundle + the
// vector runner, which parses whole suites at once and hands the per-vector `bundle` object here).
func BundleFromJObj(obj *bc.JObj) (*SqlBundle, error) {
	b := &SqlBundle{}
	if d, ok := obj.Get("dialect"); ok {
		b.Dialect, _ = d.(string)
	}
	if n, ok := obj.Get("name"); ok {
		b.Name, _ = n.(string)
	}
	if rgN, ok := obj.Get("readGraph"); ok {
		if rg, ok := rgN.(*bc.JObj); ok {
			g, err := ReadGraphFromJObj(rg)
			if err != nil {
				return nil, err
			}
			b.ReadGraph = g
		}
	}
	if tx, ok := obj.Get("transaction"); ok {
		b.Transaction, _ = tx.(*bc.JObj)
	}
	return b, nil
}

// ── Public runtime entrypoint ─────────────────────────────────────────────────

// ExecuteBundle executes a read/exec SqlBundle end-to-end (runtime.ts executeBundle): a read bundle
// carries a `readGraph` (the REAL Select-node ComponentGraphIR + static statements); a CLOSED-SET
// native walker drives map/Φ/wiring (never bc.RunBehavior) and renders + executes each node against
// REAL SQL. This is the SAME code path a consumer runtime follows — it consumes ONLY the serialized
// bundle + bc runtime-core, never re-running litedbmodel's Backend-Compile.
func ExecuteBundle(bundle *SqlBundle, input *bc.Obj, db SQLDB) (bc.Value, error) {
	if bundle.ReadGraph == nil {
		return nil, fmt.Errorf("scp runtime: bundle '%s' carries no read graph (single-statement writes ride the write path)", bundle.Name)
	}
	return ExecuteReadGraph(bundle.ReadGraph, input, db)
}

// reErrorToSqlFailure re-surfaces a structured SqlFailure from a bc OP_FAILED whose message embeds
// a `SQLITE_*` code (runtime.ts reErrorToSqlFailure). Non-driver errors are returned verbatim.
func reErrorToSqlFailure(err error) error {
	msg := err.Error()
	if i := strings.Index(msg, "SQLITE_"); i >= 0 {
		rest := msg[i:]
		end := 0
		for end < len(rest) && (rest[end] == '_' || (rest[end] >= 'A' && rest[end] <= 'Z')) {
			end++
		}
		code := rest[:end]
		switch {
		case code == "SQLITE_CONSTRAINT_FOREIGNKEY":
			return &SqlFailure{Kind: KindForeignKeyViolation, Policy: "fail", SqliteCode: code, Msg: msg}
		case strings.HasPrefix(code, "SQLITE_CONSTRAINT"):
			return &SqlFailure{Kind: KindConstraintViolation, Policy: "fail", SqliteCode: code, Msg: msg}
		case code == "SQLITE_BUSY" || code == "SQLITE_LOCKED":
			return &SqlFailure{Kind: KindRetryable, Policy: "retry", SqliteCode: code, Msg: msg}
		default:
			return &SqlFailure{Kind: KindDriverError, Policy: "fail", SqliteCode: code, Msg: msg}
		}
	}
	return err
}
