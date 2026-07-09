<?php

declare(strict_types=1);

/**
 * litedbmodel SCP conformance vectors — PHP runner (WS7e, #30).
 *
 * WS7B_E_RUNTIME_STUB — WS7a scaffold. This is the ENTRY POINT the cross-language orchestrator
 * (conformance/vectors-run.ts) launches for the PHP leg. WS7e fills the body: load
 * conformance/vectors/*.json, run each vector through LiteDbModel\Runtime\Runtime (consuming the
 * VENDORED behavior-contracts PHP port for Expression-IR eval), and print the SAME machine JSON
 * summary as the TS runner as the LAST stdout line:
 *
 *   {"lang":"php","suites":{"<suite>":{"pass":N,"fail":N}},"total_pass":N,"total_fail":N,"version_mismatch":false}
 *
 * exit 0 (all pass) / 1 (any fail) / 2 (corpus-version mismatch).
 *
 * The orchestrator detects the WS7B_E_RUNTIME_STUB marker above and reports this runner as
 * PENDING (not FAIL) until the body lands, so WS7a is green.
 */

fwrite(STDERR, "litedbmodel/runtime: php vectors_runner is a WS7e scaffold stub (no runtime yet)\n");
exit(3); // not-implemented sentinel; orchestrator gates on the stub marker, not this code
