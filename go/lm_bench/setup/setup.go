// Package setup loads the ONE cross-lang ORM-bench seed SSoT — benchmark/crosslang/.setup/<dialect>.json,
// emitted from orm-domain.ts by emit-setup.ts — for BOTH go bench cells (lm_orm_native + lm_orm). No go
// cell hand-writes a schema or seed: each applies Doc.Schema once at open and Doc.Delete+Doc.Insert as
// the canonical fixture. This is the single go-side consumer of the JSON artifact.
package setup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Doc is one dialect's setup: Schema (drop+create, applied once) + Delete+Insert (the canonical
// 110-user fixture as literal SQL, re-applied per op). Every statement execs with no bound params.
type Doc struct {
	Dialect string   `json:"dialect"`
	Users   int      `json:"users"`
	Schema  []string `json:"schema"`
	Delete  []string `json:"delete"`
	Insert  []string `json:"insert"`
}

// Load reads .setup/<dialect>.json relative to this source file (repo-root-anchored, cwd-independent).
func Load(dialect string) (Doc, error) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return Doc{}, fmt.Errorf("cannot locate setup package source")
	}
	// self = <repo>/go/lm_bench/setup/setup.go → repo root is three dirs up from the package dir.
	root := filepath.Join(filepath.Dir(self), "..", "..", "..")
	path := filepath.Join(root, "benchmark", "crosslang", ".setup", dialect+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return Doc{}, fmt.Errorf("read seed SSoT %s: %w", path, err)
	}
	var doc Doc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Doc{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return doc, nil
}
