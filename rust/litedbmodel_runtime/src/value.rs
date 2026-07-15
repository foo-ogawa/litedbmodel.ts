//! litedbmodel v2 SCP — the bound-scope helpers over the native [`Node`] model.
//!
//! The runtime is NATIVE-ONLY (#8): it carries NO external JSON crate. The JSON ⇄ `Value` conversion + the
//! `{"$bigint":"<dec>"}` conformance codec live in [`crate::node`] (native, JSON-library-free); this
//! module only adds the flat-scope helpers over that model. bc's numeric model has a single i64 int,
//! so a param TS tagged `$bigint` and one it left a plain number both decode to `Value::Int`
//! (value-equal) — the SQL text stays byte-asserted and params compare by value.

use crate::node::{decode_value, Node};
use behavior_contracts::Value;

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

/// Decode a [`Node`] object into a flat scope (used for the runtime `input`).
pub fn decode_scope(input: &Node) -> Result<Scope, String> {
    match decode_value(input)? {
        Value::Obj(pairs) => Ok(pairs),
        _ => Err("scp runtime: input must be an object".into()),
    }
}
