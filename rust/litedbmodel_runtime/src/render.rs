//! litedbmodel v2 SCP — fragment-tree render + param assembly (Rust port of `src/scp/render.ts`).
//!
//! A byte-for-byte port of the NORMATIVE dynamic-expansion reference
//! (`docs/proposal/sql-dynamic-expansion-spec.md`), mirroring the audited Python/PHP sibling
//! runners. Given a §8 CompiledOperation and a bound input scope, it deterministically produces
//! the final SQL text (`?` placeholders, or `$N` for Postgres via the dialect's final pass) + the
//! flat params list — reproducing the TS `renderOperation` exactly. The moving parts:
//!
//!   §2 SKIP → fragment existence: a fragment with `when` is present iff `when` evaluates to a
//!       present (non-null / non-false) binding; an absent fragment contributes NO SQL and NO params.
//!   §3 empty-WHERE degeneration: if no fragment is present the whole ` WHERE …` splice collapses.
//!   §4 AND/OR structure + parenthesization: a nested tree renders `(… <connector> …)`.
//!   §5 IN-list array expansion: an `expand` slot turns its `(?)` into `(?, ?, …)` per element (0
//!       elements → the `1 = 0` always-false degeneration).
//!   §6 param order = SQL text order: pre-`{where}` statics, then fragment params in tree order,
//!       then post-`{where}` statics.
//!
//! The CLOSED Expression-IR evaluation (ref/refOpt/coalesce/eq/…) is delegated to
//! behavior-contracts (`evaluate_expression`) — this module re-implements NO generic evaluator,
//! exactly like the TS reference imports `evaluateExpression` from `behavior-contracts`.

use behavior_contracts::{evaluate_expression, Value};
use serde_json::Value as J;

use crate::dialect::Dialect;
use crate::value::Scope;

/// The literal `{where}` splice marker inside `CompiledOperation.sql` (spec §8 / ir.ts WHERE_SLOT).
pub const WHERE_SLOT: &str = "{where}";

/// The result of rendering: final SQL text + flat params (1:1 with `?`).
pub struct RenderedSql {
    pub sql: String,
    pub params: Vec<Value>,
}

/// A fragment tree carries a `connector`; a leaf fragment carries `sql`.
fn is_tree(node: &J) -> bool {
    node.get("connector").is_some()
}

/// SKIP existence (spec §2): present iff `always`, or `when` evaluates truthy-present.
///
/// `null` and `false` are absent; everything else (including `0`, `""`) is present — mirroring
/// the TS `fragmentPresent`. `when` is an explicit presence/bool Expression evaluated fail-closed
/// by bc's `evaluate_expression`. Neither `always` nor `when` set ⇒ fail-closed absent.
fn fragment_present(fragment: &J, scope: &Scope) -> Result<bool, String> {
    if fragment.get("always") == Some(&J::Bool(true)) {
        return Ok(true);
    }
    match fragment.get("when") {
        None => Ok(false), // fail-closed: neither always nor when
        Some(when) => {
            let v = evaluate_expression(when, scope).map_err(|e| e.message)?;
            Ok(!matches!(v, Value::Null | Value::Bool(false)))
        }
    }
}

/// Render one leaf fragment's SQL + params into the accumulator (IN-list expansion, §5).
fn render_fragment(fragment: &J, scope: &Scope, params: &mut Vec<Value>) -> Result<String, String> {
    let frag_params = fragment
        .get("params")
        .and_then(|p| p.as_array())
        .ok_or_else(|| "scp render: fragment missing 'params' array".to_string())?;
    let sql = fragment
        .get("sql")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "scp render: fragment missing 'sql' string".to_string())?;

    let expand = fragment.get("expand").and_then(|e| e.as_u64());
    if expand.is_none() {
        for slot in frag_params {
            params.push(evaluate_expression(slot, scope).map_err(|e| e.message)?);
        }
        return Ok(sql.to_string());
    }

    // IN-list expansion. Evaluate all slots; the `expand` slot must be an array.
    let expand = expand.unwrap() as usize;
    let mut out_sql = sql.to_string();
    for (i, slot) in frag_params.iter().enumerate() {
        let v = evaluate_expression(slot, scope).map_err(|e| e.message)?;
        if i == expand {
            match v {
                Value::Arr(items) => {
                    if items.is_empty() {
                        // Empty-array degeneration (spec §5): `col IN (?)` collapses to the
                        // always-false sentinel `1 = 0`. No params pushed. Byte-identical to TS/v1.
                        out_sql = "1 = 0".to_string();
                    } else {
                        // Replace the single `(?)` with `(?, ?, …)`; push each element.
                        let placeholders = std::iter::repeat_n("?", items.len())
                            .collect::<Vec<_>>()
                            .join(", ");
                        out_sql = out_sql.replacen("(?)", &format!("({placeholders})"), 1);
                        for el in items {
                            params.push(el);
                        }
                    }
                }
                other => {
                    return Err(format!(
                        "IN-list expansion slot {i} did not bind to an array (got {})",
                        other.type_name()
                    ));
                }
            }
        } else {
            params.push(v);
        }
    }
    Ok(out_sql)
}

/// Render a fragment tree into a WHERE body (no leading ` WHERE `). Empty when none present (§3).
fn render_tree(tree: &J, scope: &Scope, params: &mut Vec<Value>) -> Result<String, String> {
    let fragments = tree
        .get("fragments")
        .and_then(|f| f.as_array())
        .ok_or_else(|| "scp render: tree missing 'fragments' array".to_string())?;
    let mut parts: Vec<String> = Vec::new();
    for node in fragments {
        if is_tree(node) {
            let inner = render_tree(node, scope, params)?;
            if !inner.is_empty() {
                parts.push(format!("({inner})"));
            }
        } else if fragment_present(node, scope)? {
            parts.push(render_fragment(node, scope, params)?);
        }
    }
    if parts.is_empty() {
        return Ok(String::new());
    }
    let connector = tree
        .get("connector")
        .and_then(|c| c.as_str())
        .ok_or_else(|| "scp render: tree missing 'connector'".to_string())?;
    Ok(parts.join(&format!(" {connector} ")))
}

/// Count `?` placeholders in a static SQL segment (no fragment markers present).
fn count_placeholders(sql: &str) -> usize {
    sql.matches('?').count()
}

/// Render a §8 CompiledOperation to final SQL + params for a bound input scope.
///
/// Byte-for-byte port of the TS `renderOperation`. Param order matches SQL text order (spec §6):
/// pre-WHERE statics, then fragment params in tree order, then post-WHERE statics. A single
/// left-to-right walk of the spliced SQL yields the canonical placeholder order; the dialect's
/// `finalize_placeholders` applies the `?`→`$N` pass ONCE over the fully-assembled text.
pub fn render_operation(
    operation: &J,
    input_scope: &Scope,
    dialect: Dialect,
) -> Result<RenderedSql, String> {
    let mut params: Vec<Value> = Vec::new();
    let op_sql = operation
        .get("sql")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "scp render: operation missing 'sql' string".to_string())?;
    let empty_params: Vec<J> = Vec::new();
    let op_params = operation
        .get("params")
        .and_then(|p| p.as_array())
        .unwrap_or(&empty_params);

    let marker_idx = op_sql.find(WHERE_SLOT);

    if marker_idx.is_none() {
        // No dynamic WHERE: all params are static, in position order.
        for slot in op_params {
            params.push(evaluate_expression(slot, input_scope).map_err(|e| e.message)?);
        }
        return Ok(RenderedSql {
            sql: dialect.finalize_placeholders(op_sql),
            params,
        });
    }

    let marker_idx = marker_idx.unwrap();
    let before = &op_sql[..marker_idx];
    let after = &op_sql[marker_idx + WHERE_SLOT.len()..];

    // Static params are partitioned by whether their `?` sits before or after the marker.
    let before_q = count_placeholders(before);
    let (pre_statics, post_statics) = op_params.split_at(before_q.min(op_params.len()));

    for slot in pre_statics {
        params.push(evaluate_expression(slot, input_scope).map_err(|e| e.message)?);
    }

    let mut where_sql = String::new();
    if let Some(where_tree) = operation.get("where") {
        if !where_tree.is_null() {
            let body = render_tree(where_tree, input_scope, &mut params)?;
            if !body.is_empty() {
                where_sql = format!(" WHERE {body}"); // degeneration §3: drop keyword when empty
            }
        }
    }

    for slot in post_statics {
        params.push(evaluate_expression(slot, input_scope).map_err(|e| e.message)?);
    }

    let assembled = format!("{before}{where_sql}{after}");
    Ok(RenderedSql {
        sql: dialect.finalize_placeholders(&assembled),
        params,
    })
}
