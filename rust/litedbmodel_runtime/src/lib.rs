//! litedbmodel v2 SCP — Rust runtime (WS7d scaffold, #30).
//!
//! Interprets the language-neutral §8 published bundle (`SqlBundle`: sql + fragment tree +
//! closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it
//! against a SQL driver, semantics-identical to the TS reference (`src/scp`). The generic
//! Expression-IR evaluation is delegated to the shared common core `behavior-contracts` crate,
//! mirroring the TS reference's npm dependency — this crate re-implements no generic evaluator.
//!
//! `WS7A_SCAFFOLD`: the runtime surface is declared here; the bodies are WS7d. They return an
//! error so a premature call fails loudly instead of returning a fake result.

use serde_json::Value;

/// Version mirrored from package.json by scripts/sync-versions.mjs (SSoT).
pub const VERSION: &str = "1.2.10";

/// A runtime error placeholder until the WS7d bodies land.
#[derive(Debug)]
pub struct NotImplemented(pub &'static str);

impl std::fmt::Display for NotImplemented {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "litedbmodel_runtime: {} is WS7d (WS7a scaffold only)", self.0)
    }
}
impl std::error::Error for NotImplemented {}

/// The rendered output of one §8 CompiledOperation.
pub struct Rendered {
    pub sql: String,
    pub params: Vec<Value>,
}

/// Render a §8 CompiledOperation against a scope for a dialect. WS7d.
pub fn render_operation(_operation: &Value, _scope: &Value, _dialect: &str) -> Result<Rendered, NotImplemented> {
    Err(NotImplemented("render_operation"))
}

/// Execute a §8 read/exec SqlBundle end-to-end. WS7d.
pub fn execute_bundle(_bundle: &Value, _input: &Value) -> Result<Value, NotImplemented> {
    Err(NotImplemented("execute_bundle"))
}

/// Execute a §8 write-tx SqlBundle as one gate-first transaction. WS7d.
pub fn execute_transaction_bundle(_bundle: &Value, _input: &Value) -> Result<Value, NotImplemented> {
    Err(NotImplemented("execute_transaction_bundle"))
}

/// The dialect NULLS-ordering primitive. WS7d.
pub fn order_by_nulls(_expr: &str, _dir: &str, _nulls: &str, _dialect: &str) -> Result<String, NotImplemented> {
    Err(NotImplemented("order_by_nulls"))
}
