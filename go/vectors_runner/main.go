// Command vectors_runner is the Go leg of the litedbmodel SCP conformance harness (WS7c, #30).
//
// WS7B_E_RUNTIME_STUB — WS7a scaffold. This is the ENTRY POINT the cross-language orchestrator
// (conformance/vectors-run.ts) launches for the Go leg. WS7c fills the body: load
// conformance/vectors/*.json, run each vector through litedbmodel_runtime (consuming
// behavior-contracts for Expression-IR eval), and print the SAME machine JSON summary as the TS
// runner as the LAST stdout line:
//
//	{"lang":"go","suites":{<suite>:{"pass","fail"}},"total_pass","total_fail","version_mismatch"}
//
// exit 0 (all pass) / 1 (any fail) / 2 (corpus-version mismatch).
//
// The orchestrator detects the WS7B_E_RUNTIME_STUB marker above and reports this runner as
// PENDING (not FAIL) until the body lands, so WS7a is green.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "litedbmodel/go: vectors_runner is a WS7c scaffold stub (no runtime yet)")
	os.Exit(3) // not-implemented sentinel; orchestrator gates on the stub marker, not this code
}
