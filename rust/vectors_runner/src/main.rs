//! litedbmodel SCP conformance vectors — Rust runner (WS7d, #30).
//!
//! WS7B_E_RUNTIME_STUB — WS7a scaffold. This is the ENTRY POINT the cross-language orchestrator
//! (conformance/vectors-run.ts) launches for the Rust leg. WS7d fills the body: load
//! conformance/vectors/*.json, run each vector through litedbmodel_runtime (consuming the
//! behavior-contracts crate for Expression-IR eval), and print the SAME machine JSON summary as
//! the TS runner as the LAST stdout line:
//!
//! ```json
//! {"lang":"rust","suites":{"<suite>":{"pass":N,"fail":N}},"total_pass":N,"total_fail":N,"version_mismatch":false}
//! ```
//!
//! exit 0 (all pass) / 1 (any fail) / 2 (corpus-version mismatch).
//!
//! The orchestrator detects the `WS7B_E_RUNTIME_STUB` marker above and reports this runner as
//! PENDING (not FAIL) until the body lands, so WS7a is green.

fn main() {
    eprintln!("litedbmodel_runtime: rust vectors_runner is a WS7d scaffold stub (no runtime yet)");
    std::process::exit(3); // not-implemented sentinel; orchestrator gates on the stub marker
}
