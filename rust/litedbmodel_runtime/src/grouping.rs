//! litedbmodel v2 SCP — the SHARED relation-grouping CORE (#141), Rust port.
//!
//! The ONE implementation of relation key-identity + dedupe + parent grouping over bc [`Value`]
//! rows, byte-behaviour-identical to the TS SSoT `src/scp/grouping.ts`. It is consumed by BOTH
//! relation surfaces so there is a single source of truth (no duplicated grouping logic):
//!
//!   - the EAGER graph — the op-independent `pluck` / `group` wire leaves (`crate` leaf transports),
//!     which convert WireValue↔Value at the boundary and call THIS core;
//!   - the RUNTIME lazy / declarative path (`crate::relation`), which groups already-fetched rows
//!     over the SAME core.
//!
//! Nothing here touches SQL or a driver: it is pure in-memory grouping over already-fetched rows
//! (bc `Value::Obj` records). Ordered TUPLE keys are supported (composite keys), matching TS.

use std::collections::HashMap;

use behavior_contracts::Value;

/// A separator no scalar `String(v)` rendering contains, so distinct tuples never collide (matches
/// the TS `KEY_SEP`).
const KEY_SEP: &str = " ";

/// The stringified key identity for dedupe/grouping. Single scalar → its `String(v)` rendering; a
/// tuple → the renderings joined by [`KEY_SEP`] (matches TS `keyIdentity`).
pub fn key_identity(values: &[Value]) -> String {
    values
        .iter()
        .map(stringify_key)
        .collect::<Vec<_>>()
        .join(KEY_SEP)
}

/// Mirror of JS `String(v)` for the key identity: bool → `"true"`/`"false"`, a whole float prints as
/// an integer (a scanned INT column arrives as a whole `f64`), a fractional float its shortest
/// round-trip form, string verbatim, null → `"null"` (a null key is dropped before it is ever
/// stringified, so this arm never affects a grouping result — it exists only for totality).
fn stringify_key(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_finite() && f.fract() == 0.0 => (*f as i64).to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => s.clone(),
        // A composite/array is never a scalar key (keys are scalar columns); totality fallback only.
        other => format!("{other:?}"),
    }
}

/// A row field by column name (a bc `Value::Obj` is insertion-ordered pairs). `None` = the field is
/// ABSENT (the TS `undefined`), distinct from a present `Value::Null`.
fn field<'a>(row: &'a Value, col: &str) -> Option<&'a Value> {
    row.obj_get(col)
}

/// True iff a tuple column is ABSENT or `Value::Null` (the TS `v === undefined || v === null` drop).
fn is_missing(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

/// The deduped, non-null key TUPLES of `rows` over `key_cols` (insertion order preserved —
/// deterministic). A tuple is dropped if ANY of its key columns is absent/null (no partial keys);
/// deduped on the stringified tuple identity. Port of TS `dedupeKeyTuples`.
pub fn dedupe_key_tuples(rows: &[Value], key_cols: &[String]) -> Vec<Vec<Value>> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        let cells: Vec<Option<&Value>> = key_cols.iter().map(|c| field(row, c)).collect();
        if cells.iter().any(|v| is_missing(*v)) {
            continue;
        }
        let tuple: Vec<Value> = cells.into_iter().map(|v| v.unwrap().clone()).collect();
        if seen.insert(key_identity(&tuple)) {
            out.push(tuple);
        }
    }
    out
}

/// Group `children` by their `fk_cols` tuple identity (a null/absent key drops the child). Child list
/// order within a bucket is the input order (push order). Port of TS `groupByKey`. The bucket keys
/// are looked up (never iterated) by [`attach_to_parent`], so a `HashMap` is faithful.
pub fn group_by_key(children: Vec<Value>, fk_cols: &[String]) -> HashMap<String, Vec<Value>> {
    let mut by_key: HashMap<String, Vec<Value>> = HashMap::new();
    for child in children {
        // Compute the key from BORROWED cells (only the key columns are cloned — small), then MOVE the
        // whole child row into its bucket (each child belongs to exactly ONE bucket) — no per-row deep clone.
        let key = {
            let cells: Vec<Option<&Value>> = fk_cols.iter().map(|c| field(&child, c)).collect();
            if cells.iter().any(|v| is_missing(*v)) {
                continue;
            }
            let tuple: Vec<Value> = cells.into_iter().map(|v| v.unwrap().clone()).collect();
            key_identity(&tuple)
        };
        by_key.entry(key).or_default().push(child);
    }
    by_key
}

/// Distribute grouped children onto ONE parent per cardinality (port of TS `attachToParent`):
/// `single == false` (hasMany) → the child list as `Value::Arr` (`[]` when none); `single == true`
/// (belongsTo/hasOne) → the single child (or `Value::Null`). Keyed by the parent's `pk_cols` tuple
/// identity; a null/absent parent key matches nothing (`[]`/`null`).
pub fn attach_to_parent(
    parent: &Value,
    pk_cols: &[String],
    by_key: &HashMap<String, Vec<Value>>,
    single: bool,
) -> Value {
    let cells: Vec<Option<&Value>> = pk_cols.iter().map(|c| field(parent, c)).collect();
    let rows: Option<&Vec<Value>> = if cells.iter().any(|v| is_missing(*v)) {
        None
    } else {
        let tuple: Vec<Value> = cells.into_iter().map(|v| v.unwrap().clone()).collect();
        by_key.get(&key_identity(&tuple))
    };
    if !single {
        return Value::Arr(rows.cloned().unwrap_or_default());
    }
    match rows.and_then(|r| r.first()) {
        Some(child) => child.clone(),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Value {
        Value::Obj(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
    }
    fn cols(cs: &[&str]) -> Vec<String> {
        cs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn key_identity_matches_js_string() {
        // whole float → integer text (a scanned INT column), bool/string verbatim, tuple space-joined.
        assert_eq!(key_identity(&[Value::Float(1.0)]), "1");
        assert_eq!(key_identity(&[Value::Int(2)]), "2");
        assert_eq!(key_identity(&[Value::Str("x".into())]), "x");
        assert_eq!(key_identity(&[Value::Bool(true)]), "true");
        assert_eq!(key_identity(&[Value::Float(1.5)]), "1.5");
        assert_eq!(
            key_identity(&[Value::Int(1), Value::Str("a".into())]),
            "1 a"
        );
    }

    #[test]
    fn dedupe_drops_null_and_dedupes_preserving_order() {
        let rows = vec![
            row(&[("id", Value::Int(2))]),
            row(&[("id", Value::Int(1))]),
            row(&[("id", Value::Int(2))]), // dup
            row(&[("id", Value::Null)]),   // dropped (null)
            row(&[("other", Value::Int(9))]), // dropped (absent id)
        ];
        let keys = dedupe_key_tuples(&rows, &cols(&["id"]));
        let flat: Vec<i64> = keys
            .iter()
            .map(|t| match &t[0] {
                Value::Int(i) => *i,
                _ => panic!(),
            })
            .collect();
        assert_eq!(flat, vec![2, 1]); // insertion order, deduped, nulls/absent dropped
    }

    #[test]
    fn dedupe_composite_tuple() {
        let rows = vec![
            row(&[("t", Value::Int(1)), ("u", Value::Int(9))]),
            row(&[("t", Value::Int(1)), ("u", Value::Int(9))]), // dup tuple
            row(&[("t", Value::Int(1)), ("u", Value::Int(8))]),
            row(&[("t", Value::Int(1)), ("u", Value::Null)]), // dropped (partial null)
        ];
        let keys = dedupe_key_tuples(&rows, &cols(&["t", "u"]));
        assert_eq!(keys.len(), 2);
        assert_eq!(key_identity(&keys[0]), "1 9");
        assert_eq!(key_identity(&keys[1]), "1 8");
    }

    #[test]
    fn group_and_attach_has_many() {
        let parents = vec![row(&[("id", Value::Int(1))]), row(&[("id", Value::Int(2))])];
        let children = vec![
            row(&[("author_id", Value::Int(1)), ("t", Value::Str("a".into()))]),
            row(&[("author_id", Value::Int(1)), ("t", Value::Str("b".into()))]),
            row(&[("author_id", Value::Int(2)), ("t", Value::Str("c".into()))]),
            row(&[("author_id", Value::Null), ("t", Value::Str("x".into()))]), // dropped
        ];
        let by_key = group_by_key(children.clone(), &cols(&["author_id"]));
        // parent 1 → two children in input order
        let a1 = attach_to_parent(&parents[0], &cols(&["id"]), &by_key, false);
        match a1 {
            Value::Arr(items) => assert_eq!(items.len(), 2),
            _ => panic!("hasMany must be a list"),
        }
        // parent 2 → one child
        let a2 = attach_to_parent(&parents[1], &cols(&["id"]), &by_key, false);
        assert!(matches!(a2, Value::Arr(ref v) if v.len() == 1));
        // a parent with no matches → empty list
        let a3 = attach_to_parent(&row(&[("id", Value::Int(3))]), &cols(&["id"]), &by_key, false);
        assert!(matches!(a3, Value::Arr(ref v) if v.is_empty()));
    }

    #[test]
    fn attach_single_returns_first_or_null() {
        let children = vec![
            row(&[("post_id", Value::Int(5)), ("b", Value::Str("first".into()))]),
            row(&[("post_id", Value::Int(5)), ("b", Value::Str("second".into()))]),
        ];
        let by_key = group_by_key(children.clone(), &cols(&["post_id"]));
        // single → the FIRST matching child (input order)
        let one = attach_to_parent(&row(&[("id", Value::Int(5))]), &cols(&["id"]), &by_key, true);
        // bc `Value` has no PartialEq; probe the field instead of comparing the whole record.
        assert!(matches!(one.obj_get("b"), Some(Value::Str(s)) if s == "first"));
        // single, no match → null
        let none = attach_to_parent(&row(&[("id", Value::Int(6))]), &cols(&["id"]), &by_key, true);
        assert!(matches!(none, Value::Null));
    }
}
