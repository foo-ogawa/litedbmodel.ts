//! litedbmodel v2 SCP — JSON ⇄ bc runtime `Value` conversion + `$bigint` codec.
//!
//! bc's `evaluate_expression` / `run_behavior` operate on the shared `behavior_contracts::Value`
//! model (Int = i64, Float = f64, Str, Bool, Null, Arr, Obj) with a flat scope `[(String, Value)]`.
//! This module converts between the pure-JSON bundle/scope representation and that runtime model,
//! and implements the `{"$bigint": "<dec>"}` conformance codec exactly as the audited Python/PHP
//! sibling runners do (`decode_value` / `encode_value`).
//!
//! IMPORTANT: this is NOT bc's own `codec` (which decodes the dsl-contracts `{int:"…"}`/`{float:n}`
//! golden-vector wire tags). The litedbmodel conformance corpus uses PLAIN JSON numbers plus a
//! `{"$bigint": "<dec>"}` tag for JS-bigint-typed integers; we honor that codec here so a runtime
//! param TS tagged `$bigint` and one it left a plain number decode to the SAME i64 (value-equal),
//! WITHOUT loosening type/order comparison — the SQL text stays byte-asserted and params compare
//! by value. bc's numeric model has a single i64 int, so both forms collapse to `Value::Int`.

use behavior_contracts::Value;
use serde_json::Value as J;

/// Decode a bundle/scope JSON value into the bc runtime [`Value`].
///
/// Mirrors the Python/PHP `decode_value`: `{"$bigint": "<dec>"}` → int; a bare integral JSON
/// number → int, a fractional one → float; recurses arrays/objects (insertion order preserved).
/// Fails loudly on a `$bigint` string that is not a valid i64 (no silent fallback).
pub fn decode_value(x: &J) -> Result<Value, String> {
    match x {
        J::Null => Ok(Value::Null),
        J::Bool(b) => Ok(Value::Bool(*b)),
        J::String(s) => Ok(Value::Str(s.clone())),
        J::Number(n) => number_to_value(n),
        J::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for e in a {
                out.push(decode_value(e)?);
            }
            Ok(Value::Arr(out))
        }
        J::Object(o) => {
            if o.len() == 1 {
                if let Some(J::String(s)) = o.get("$bigint") {
                    let i: i64 = s
                        .parse()
                        .map_err(|_| format!("$bigint literal '{s}' is not an i64"))?;
                    return Ok(Value::Int(i));
                }
            }
            let mut out = Vec::with_capacity(o.len());
            for (k, v) in o {
                out.push((k.clone(), decode_value(v)?));
            }
            Ok(Value::Obj(out))
        }
    }
}

fn number_to_value(n: &serde_json::Number) -> Result<Value, String> {
    if let Some(i) = n.as_i64() {
        // A bare integral JSON number means int (matches the JS `number` safe-integer path).
        Ok(Value::Int(i))
    } else if let Some(f) = n.as_f64() {
        Ok(Value::Float(f))
    } else {
        Err(format!("number {n} exceeds the supported i64/f64 range"))
    }
}

/// Encode a bc runtime [`Value`] to pure JSON (diagnostics + conformance comparison form).
///
/// Mirrors the Python/PHP `encode_value`: ints round-trip as plain JSON numbers (the corpus is
/// within the JS safe-integer range, so the reference's decoded `$bigint`→number form matches).
/// This produces the same shape both sides compare against on the render/exec/tx axes.
pub fn encode_value(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Str(s) => J::String(s.clone()),
        Value::Arr(a) => J::Array(a.iter().map(encode_value).collect()),
        Value::Obj(o) => {
            let mut m = serde_json::Map::new();
            for (k, val) in o {
                m.insert(k.clone(), encode_value(val));
            }
            J::Object(m)
        }
    }
}

/// A bound input scope in bc's flat form.
pub type Scope = Vec<(String, Value)>;

/// Turn a bc object [`Value`] into a flat scope (linear list of top-level pairs).
///
/// The `__scope` surrogate port and the render input are always objects; a non-object is a
/// wiring bug and surfaced by the caller.
pub fn value_obj_to_scope(v: &Value) -> Option<Scope> {
    match v {
        Value::Obj(pairs) => Some(pairs.clone()),
        _ => None,
    }
}

/// Decode a JSON object into a flat scope (used for the runtime `input`).
pub fn decode_scope(input: &J) -> Result<Scope, String> {
    match decode_value(input)? {
        Value::Obj(pairs) => Ok(pairs),
        _ => Err("scp runtime: input must be an object".into()),
    }
}
