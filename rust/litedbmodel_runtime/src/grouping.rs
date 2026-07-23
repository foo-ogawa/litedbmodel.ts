//! litedbmodel v2 SCP — the SHARED relation-grouping CORE (#141), Rust port.
//!
//! The ONE implementation of relation key-identity + dedupe + parent grouping over the RUNTIME-FREE
//! wire rows ([`WireValue`] — the type the generated native module speaks), byte-behaviour-identical
//! to the TS SSoT `src/scp/grouping.ts`. It works DIRECTLY on `WireValue` so the `pluck`/`group` leaf
//! transports carry NO `WireValue`↔`Value` conversion in the hot path (the read path never boxes into
//! bc's `Value`): the only place a wire number becomes a typed scalar is the generated module's final
//! de-box of its OUTPUT columns.
//!
//! Nothing here touches SQL or a driver: it is pure in-memory grouping over already-fetched rows
//! (`WireValue::Row` records). Ordered TUPLE keys are supported (composite keys), matching TS.

use std::collections::HashMap;

use crate::wire::{WireList, WireValue};

/// A separator no scalar key rendering contains, so distinct tuples never collide (matches TS `KEY_SEP`).
const KEY_SEP: &str = " ";

/// The stringified key identity for dedupe/grouping. Single scalar → its `String(v)` rendering; a
/// tuple → the renderings joined by [`KEY_SEP`] (matches TS `keyIdentity`).
pub fn key_identity(values: &[&WireValue]) -> String {
    values
        .iter()
        .map(|v| stringify_key(v))
        .collect::<Vec<_>>()
        .join(KEY_SEP)
}

/// Mirror of JS `String(v)` for the key identity over a WIRE scalar. A wire number is carried as a
/// canonical decimal string (`WireValue::Num`), so the key is that string NORMALIZED exactly as the
/// typed path would render it: a whole number prints as integer text (`"1.0"`→`"1"`), a fractional its
/// shortest round-trip form, bool `"true"`/`"false"`, string verbatim. Only the KEY columns are parsed
/// (a handful), never every cell. Null is dropped before it is ever stringified (totality arm only).
fn stringify_key(value: &WireValue) -> String {
    match value {
        WireValue::Null => "null".to_string(),
        WireValue::Bool(b) => b.to_string(),
        WireValue::Str(s) => s.clone(),
        WireValue::Num(s) => {
            // Normalize identically to the typed `stringify_key` (Int/whole-Float → integer text).
            if let Ok(i) = s.parse::<i64>() {
                i.to_string()
            } else if let Ok(f) = s.parse::<f64>() {
                if f.is_finite() && f.fract() == 0.0 {
                    (f as i64).to_string()
                } else {
                    f.to_string()
                }
            } else {
                s.clone()
            }
        }
        // A Row/List is never a scalar key (keys are scalar columns); totality fallback only.
        WireValue::Row(_) | WireValue::List(_) => String::new(),
    }
}

/// A row field by column name (a `WireValue::Row` is insertion-ordered pairs). `None` = the field is
/// ABSENT (the TS `undefined`), distinct from a present `WireValue::Null`.
fn field<'a>(row: &'a WireValue, col: &str) -> Option<&'a WireValue> {
    match row {
        WireValue::Row(r) => r.entries.iter().find(|(k, _)| k == col).map(|(_, v)| v),
        _ => None,
    }
}

/// Resolve each of `cols` to its POSITION in a sample `Row`. Every row of a SQL result set shares the
/// SAME column order, so this is resolved ONCE and reused across all rows — replacing the per-cell
/// linear scan with O(1) index access. `None` if `sample` is not a `Row`; an absent column resolves to
/// `usize::MAX` (its per-row lookup then reports absent, falling back to the name scan).
fn resolve_indices(sample: &WireValue, cols: &[String]) -> Option<Vec<usize>> {
    let entries = match sample {
        WireValue::Row(r) => &r.entries,
        _ => return None,
    };
    Some(
        cols.iter()
            .map(|c| entries.iter().position(|(k, _)| k == c).unwrap_or(usize::MAX))
            .collect(),
    )
}

/// Resolve the key-column indices from the first `Row` in `rows` (all rows of a result set share the
/// SAME column order), or all-absent (`usize::MAX`) if none is a `Row`. Callers resolve ONCE and pass
/// the result to [`attach_to_parent`] so the per-parent path carries NO index scan — even when the
/// parent set is a large intermediate relation level (a nested chain groups the middle level twice).
pub fn resolve_key_indices(rows: &[WireValue], cols: &[String]) -> Vec<usize> {
    rows.iter()
        .find_map(|r| resolve_indices(r, cols))
        .unwrap_or_else(|| vec![usize::MAX; cols.len()])
}

/// The key cells of `row` via precomputed `idx` (O(1) index access; verifies the column name still
/// matches, else falls back to the linear `field`). `None` if any key column is ABSENT or `Null`
/// (the no-partial-keys drop) — the same predicate as `field` + `is_missing`.
fn key_cells<'a>(row: &'a WireValue, cols: &[String], idx: &[usize]) -> Option<Vec<&'a WireValue>> {
    let entries = match row {
        WireValue::Row(r) => &r.entries,
        _ => return None,
    };
    let mut out = Vec::with_capacity(cols.len());
    for (c, &i) in cols.iter().zip(idx) {
        let cell = match entries.get(i) {
            Some((k, v)) if k == c => v,
            _ => field(row, c)?, // row shape differs from the sample — safe linear fallback
        };
        if matches!(cell, WireValue::Null) {
            return None;
        }
        out.push(cell);
    }
    Some(out)
}

/// The deduped, non-null key TUPLES of `rows` over `key_cols` (insertion order preserved —
/// deterministic). A tuple is dropped if ANY of its key columns is absent/null (no partial keys);
/// deduped on the stringified tuple identity. Port of TS `dedupeKeyTuples`.
pub fn dedupe_key_tuples(rows: &[WireValue], key_cols: &[String]) -> Vec<Vec<WireValue>> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<Vec<WireValue>> = Vec::new();
    let idx = rows.iter().find_map(|r| resolve_indices(r, key_cols));
    for row in rows {
        let cells = match idx.as_deref().and_then(|ix| key_cells(row, key_cols, ix)) {
            Some(c) => c,
            None => continue,
        };
        let ident = key_identity(&cells);
        if seen.insert(ident) {
            out.push(cells.into_iter().cloned().collect());
        }
    }
    out
}

/// Group `children` by their `fk_cols` tuple identity (a null/absent key drops the child). Child list
/// order within a bucket is the input order (push order). Port of TS `groupByKey`. The buckets hold
/// REFERENCES into `children` (no per-child clone — the caller borrows the children); each matched
/// child is cloned exactly ONCE, when it is nested into its parent by [`attach_to_parent`].
pub fn group_by_key<'a>(
    children: &'a [WireValue],
    fk_cols: &[String],
) -> HashMap<String, Vec<&'a WireValue>> {
    let mut by_key: HashMap<String, Vec<&'a WireValue>> = HashMap::new();
    let idx = children.iter().find_map(|c| resolve_indices(c, fk_cols));
    for child in children {
        let key = match idx.as_deref().and_then(|ix| key_cells(child, fk_cols, ix)) {
            Some(cells) => key_identity(&cells),
            None => continue,
        };
        by_key.entry(key).or_default().push(child);
    }
    by_key
}

/// Distribute grouped children onto ONE parent per cardinality (port of TS `attachToParent`):
/// `single == false` (hasMany) → the child list as `WireValue::List` (`[]` when none); `single == true`
/// (belongsTo/hasOne) → the single child (or `WireValue::Null`). Keyed by the parent's `pk_cols` tuple
/// identity; a null/absent parent key matches nothing (`[]`/`null`). The matched children are cloned
/// here (the ONE necessary clone — they become part of the parent's owned output).
pub fn attach_to_parent(
    parent: &WireValue,
    pk_cols: &[String],
    pk_idx: &[usize],
    by_key: &HashMap<String, Vec<&WireValue>>,
    single: bool,
) -> WireValue {
    // `pk_idx` is resolved ONCE by the caller (all parents share column order) — no per-parent scan.
    let rows: Option<&Vec<&WireValue>> = key_cells(parent, pk_cols, pk_idx)
        .map(|cells| key_identity(&cells))
        .and_then(|ident| by_key.get(&ident));
    if !single {
        let items = rows
            .map(|r| r.iter().map(|c| (*c).clone()).collect())
            .unwrap_or_default();
        return WireValue::List(WireList { items });
    }
    match rows.and_then(|r| r.first()) {
        Some(child) => (*child).clone(),
        None => WireValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WireRow;

    fn num(n: i64) -> WireValue {
        WireValue::Num(n.to_string())
    }
    fn row(pairs: &[(&str, WireValue)]) -> WireValue {
        WireValue::Row(WireRow {
            entries: pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
        })
    }
    fn cols(cs: &[&str]) -> Vec<String> {
        cs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn key_identity_matches_js_string() {
        // whole float → integer text (a scanned INT column), bool/string verbatim, tuple space-joined.
        assert_eq!(key_identity(&[&WireValue::Num("1.0".into())]), "1");
        assert_eq!(key_identity(&[&WireValue::Num("2".into())]), "2");
        assert_eq!(key_identity(&[&WireValue::Str("x".into())]), "x");
        assert_eq!(key_identity(&[&WireValue::Bool(true)]), "true");
        assert_eq!(key_identity(&[&WireValue::Num("1.5".into())]), "1.5");
        assert_eq!(
            key_identity(&[&WireValue::Num("1".into()), &WireValue::Str("a".into())]),
            "1 a"
        );
    }

    #[test]
    fn dedupe_drops_null_and_dedupes_preserving_order() {
        let rows = vec![
            row(&[("id", num(2))]),
            row(&[("id", num(1))]),
            row(&[("id", num(2))]),           // dup
            row(&[("id", WireValue::Null)]),  // dropped (null)
            row(&[("other", num(9))]),        // dropped (absent id)
        ];
        let keys = dedupe_key_tuples(&rows, &cols(&["id"]));
        let flat: Vec<String> = keys
            .iter()
            .map(|t| match &t[0] {
                WireValue::Num(s) => s.clone(),
                _ => panic!(),
            })
            .collect();
        assert_eq!(flat, vec!["2", "1"]); // insertion order, deduped, nulls/absent dropped
    }

    #[test]
    fn group_and_attach_hasmany() {
        let children = vec![
            row(&[("id", num(10)), ("author_id", num(1))]),
            row(&[("id", num(11)), ("author_id", num(2))]),
            row(&[("id", num(12)), ("author_id", num(1))]),
        ];
        let by_key = group_by_key(&children, &cols(&["author_id"]));
        let parent1 = row(&[("id", num(1))]);
        let nested = attach_to_parent(&parent1, &cols(&["id"]), &resolve_key_indices(std::slice::from_ref(&parent1), &cols(&["id"])), &by_key, false);
        match nested {
            WireValue::List(l) => assert_eq!(l.items.len(), 2), // posts 10 and 12
            _ => panic!("expected a list"),
        }
        // a parent with no children → empty list
        let parent9 = row(&[("id", num(9))]);
        match attach_to_parent(&parent9, &cols(&["id"]), &resolve_key_indices(std::slice::from_ref(&parent9), &cols(&["id"])), &by_key, false) {
            WireValue::List(l) => assert!(l.items.is_empty()),
            _ => panic!(),
        }
    }

    #[test]
    fn attach_belongs_to_single() {
        let children = vec![row(&[("id", num(5)), ("user_id", num(1))])];
        let by_key = group_by_key(&children, &cols(&["user_id"]));
        let parent = row(&[("id", num(1))]);
        match attach_to_parent(&parent, &cols(&["id"]), &resolve_key_indices(std::slice::from_ref(&parent), &cols(&["id"])), &by_key, true) {
            WireValue::Row(_) => {}
            _ => panic!("expected the single child row"),
        }
        let parent9 = row(&[("id", num(9))]);
        assert!(matches!(
            attach_to_parent(&parent9, &cols(&["id"]), &resolve_key_indices(std::slice::from_ref(&parent9), &cols(&["id"])), &by_key, true),
            WireValue::Null
        ));
    }

    #[test]
    fn composite_tuple_key() {
        let children = vec![
            row(&[("t", num(1)), ("p", num(9)), ("x", num(100))]),
            row(&[("t", num(1)), ("p", num(8)), ("x", num(200))]),
        ];
        let by_key = group_by_key(&children, &cols(&["t", "p"]));
        // parent (t=1, p=9) matches only the first child (full tuple, not cartesian).
        let parent = row(&[("t", num(1)), ("p", num(9))]);
        match attach_to_parent(&parent, &cols(&["t", "p"]), &resolve_key_indices(std::slice::from_ref(&parent), &cols(&["t", "p"])), &by_key, false) {
            WireValue::List(l) => assert_eq!(l.items.len(), 1),
            _ => panic!(),
        }
    }
}
