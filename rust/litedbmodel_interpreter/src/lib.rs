//! Interpreter-only bundle model, parser, evaluator, graph walker, and relation orchestration.
//!
//! Dependency direction is strictly `litedbmodel_interpreter -> litedbmodel_runtime`.

pub use litedbmodel_runtime::*;

pub mod node;
pub mod relation;
pub mod runtime;
pub mod static_bundle;
pub mod value;

pub use node::{decode_value, encode_value, eval_expr, EvalError, Node};
pub use relation::{read_bundle, read_bundle_pooled, stitch_relation, stitch_relation_tree};
pub use runtime::{
    execute_bundle, execute_bundle_pooled, execute_transaction_bundle,
    execute_transaction_bundle_ctx, order_by_nulls, render_read_primary_bundle, ENTITY_ROOT,
};
pub use static_bundle::{
    dispatch_read_nodes_parallel, execute_read_graph, execute_read_graph_orchestrator_for_test,
    execute_read_graph_pooled, render_read_primary, render_statements, render_tx_op, RenderedSql,
    NODE_COMPONENT, SCOPE_PORT,
};
pub use value::{decode_scope, Scope};
