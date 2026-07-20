//! litedbmodel v2 SCP — the native-codegen EXEC seam (epic #123 / #124).
//!
//! The single execution point the litedbmodel-generated native modules (bc `rust-typed-native`) drive.
//! A bc-generated module emits a per-component handler trait
//! (`HandlerNR<comp> { type Wire: WireValue; fn node_*(ports, bound) -> Result<Wire, BehaviorError> }`)
//! and de-boxes each node's WIRE result INLINE. The `node_*` handler's ONLY remaining job is
//! op-agnostic: run the baked SQL + ordered params through the runtime [`Driver`] and hand the rows
//! back as a [`Wire`]. THIS module is that job — reused by every op, every dialect, every generated
//! module. It adds NO new execution engine: reads/writes funnel through the SAME central seam
//! ([`exec_context::execute`] / [`exec_context::run`]) the mode-2 runtime uses; batched relations reuse
//! [`crate::relation`]'s dedupe/group/align; the pg `?`→`$N` renumber reuses
//! [`crate::static_bundle::render_placeholders`]. The old hand-written `e1_native_proof/src/seam.rs`
//! (which re-implemented all of this over raw `rusqlite`) is retired — its logic lives here, Driver-backed.
//!
//! ## Wire classification is SSoT here; the orphan-rule bridge is a generated macro
//!
//! bc emits the `WireValue`/`WireRow`/`WireList`/`Probe`/`NumProbe` traits+enums INSIDE each generated
//! module (module-local). The Rust orphan rule therefore forbids `impl generated::WireValue for Wire`
//! from THIS crate. So the CLASSIFICATION logic lives here (the `rt_*` methods on [`Wire`] returning the
//! neutral [`RtProbe`]/[`RtNum`]), and the litedbmodel codegen emits a one-line [`wire_impls!`] macro
//! invocation per module that expands the module-local trait impls, each delegating to these `rt_*`
//! methods (bc C4: handlers/wire adapters are boundary-injected, never bc-generated — so the companion
//! is litedbmodel's generated output, not hand-written).

use behavior_contracts::Value;

use std::collections::HashMap;
use std::sync::Arc;

use crate::connection_routing::RoutingConfig;
use crate::driver::{Driver, RunInfo};
use crate::errors::SqlFailure;
use crate::exec_context::{self, ExecutionContext, StatementIntent, TxDecision};
use crate::static_bundle::render_placeholders;
use crate::tx_options::{Dialect as TxDialect, TransactionOptions};

// ── Wire: a node result's own wire datum (a driver row list, or a synthesized write summary) ──────

/// A node result's WIRE value — the uniform `node_*` handler return. Wraps the bc [`Value`] a driver
/// hands back: a SELECT/RETURNING read is `Value::Arr(rows)` (each row a `Value::Obj([(col,val)])`);
/// a non-returning write is the synthesized `Value::Arr([{changes,lastInsertRowid}])`. The generated
/// de-box classifies it via the [`wire_impls!`]-expanded trait impls, which delegate to the `rt_*`
/// methods below.
///
/// ## Zero-copy over the driver rows (#138)
///
/// The backing driver value is held behind an [`Arc`] so `as_list` / `elem_row` / `as_row` are refcount
/// bumps — they no longer DEEP-CLONE the whole row array (once) and each row (again), the pure native-side
/// duplication the pre-#138 `Wire(pub Value)` did on top of the driver's own row alloc. `Arc` (not `Rc`)
/// because the generated handler trait requires `Wire: Send` (bc#87 static-parallel relation dispatch).
///
/// A LIST wire focuses on the whole `Value::Arr` in `backing`; `elem_row(i)` returns a ROW wire that
/// SHARES the list's `backing` and selects element `i` via `elem` (no per-row Obj clone). A ROW wire
/// carries the shared column→index [`ColMap`] (`cols`) built ONCE per result set, so `probe_*(field)` is
/// an O(1) map lookup + positional access instead of the pre-#138 per-field linear scan (1 row was
/// O(cols²)). Descending into a nested field/element (`probe_row` / `probe_list` / an arr-of-arr element)
/// re-roots into a fresh `Arc` — a COLD path (relations are stitched Value-side, not de-boxed inline).
#[derive(Clone, Debug)]
pub struct Wire {
    /// The shared driver value this wire reads from (never deep-cloned on navigation).
    backing: Arc<Value>,
    /// `Some(i)` ⇒ the focused value is element `i` of the `Value::Arr` in `backing` (a list element,
    /// sharing the list's `Arc`); `None` ⇒ the focused value IS `*backing`.
    elem: Option<usize>,
    /// The column→index map for the focused ROW — `Some` when reached from a list (built once, threaded
    /// to every element ⇒ O(1) `probe_*`); `None` for a standalone row (linear-scan fallback).
    cols: Option<Arc<ColMap>>,
}

/// A row's column name → position in its ordered pair list. Built ONCE per result set and shared by every
/// row (via [`Arc`]) so `probe_*(field)` is O(1). Maps to the FIRST matching index (byte-equal to the
/// pre-#138 `pairs.iter().find`).
type ColMap = HashMap<String, usize>;

/// The unreachable-focus fallback (an out-of-range `elem`, which the navigation guards never produce).
static NULL_VALUE: Value = Value::Null;

/// Build the column→index map for a row Obj (first occurrence wins — matches linear-scan `find`). `None`
/// for a non-Obj (a list of scalars/rows-that-are-null has no columns to index).
fn build_col_map(v: &Value) -> Option<Arc<ColMap>> {
    if let Value::Obj(pairs) = v {
        let mut m = ColMap::with_capacity(pairs.len());
        for (i, (k, _)) in pairs.iter().enumerate() {
            m.entry(k.clone()).or_insert(i);
        }
        Some(Arc::new(m))
    } else {
        None
    }
}

/// Resolve a field to its position in a row's pairs: O(1) via the shared [`ColMap`] on a hit (verified
/// positionally so it stays byte-equal to a linear scan even under a shape drift), falling back to the
/// exact pre-#138 linear scan on a miss / no map (the genuinely-absent field is the de-box's error path).
fn field_index(cols: Option<&ColMap>, pairs: &[(String, Value)], field: &str) -> Option<usize> {
    if let Some(map) = cols {
        if let Some(&i) = map.get(field) {
            if pairs.get(i).map(|(k, _)| k.as_str()) == Some(field) {
                return Some(i);
            }
        }
    }
    pairs.iter().position(|(k, _)| k == field)
}

/// The neutral outcome of classifying one wire datum/attribute against a declared NON-numeric type —
/// the runtime-owned twin of a generated module's local `Probe<T>` (the [`wire_impls!`] macro maps one
/// to the other). `actual_wire_type` is the producer's wire tag (bc [`Value::type_name`]); `raw_value`
/// is the offending value stringified.
pub enum RtProbe<T> {
    Got(T),
    Wrong {
        actual_wire_type: String,
        raw_value: String,
    },
    Null {
        actual_wire_type: String,
        raw_value: String,
    },
    Absent,
}

/// The neutral outcome of classifying one wire datum/attribute against a declared NUMERIC type — the
/// runtime twin of a generated module's local `NumProbe`. `Got.raw` is the numeric text the de-box
/// parses + range-checks (so overflow is the generated module's to detect, byte-equal to run_behavior).
pub enum RtNum {
    Got {
        raw: String,
        actual_wire_type: String,
    },
    Wrong {
        actual_wire_type: String,
        raw_value: String,
    },
    Null {
        actual_wire_type: String,
        raw_value: String,
    },
    Absent,
}

/// bc [`Value::type_name`] as the producer wire tag string used in probe mismatch detail.
fn wire_tag(v: &Value) -> String {
    v.type_name().to_string()
}

/// Stringify a scalar value for the `raw_value` mismatch detail (numbers/bools/strings verbatim; a
/// container is named by its tag — a mismatch against a scalar type never inspects it further).
fn raw_of(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => s.clone(),
        other => other.type_name().to_string(),
    }
}

/// Classify ANY present value against a declared string type.
fn probe_string_value(v: &Value) -> RtProbe<String> {
    match v {
        Value::Str(s) => RtProbe::Got(s.clone()),
        Value::Null => RtProbe::Null {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
        _ => RtProbe::Wrong {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
    }
}

/// Classify ANY present value against a declared bool type.
fn probe_bool_value(v: &Value) -> RtProbe<bool> {
    match v {
        Value::Bool(b) => RtProbe::Got(*b),
        Value::Null => RtProbe::Null {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
        _ => RtProbe::Wrong {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
    }
}

/// Classify ANY present value against a declared numeric type (the raw text is what the de-box parses).
fn probe_number_value(v: &Value) -> RtNum {
    match v {
        Value::Int(i) => RtNum::Got {
            raw: i.to_string(),
            actual_wire_type: wire_tag(v),
        },
        Value::Float(f) => RtNum::Got {
            raw: f.to_string(),
            actual_wire_type: wire_tag(v),
        },
        Value::Null => RtNum::Null {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
        _ => RtNum::Wrong {
            actual_wire_type: wire_tag(v),
            raw_value: raw_of(v),
        },
    }
}

impl Wire {
    /// Wrap a driver value as a node-result wire (the whole array for a read, a single obj for a
    /// tx-chain statement). The de-box navigates it via the `rt_*` methods below without re-materializing
    /// the driver's rows.
    fn node(v: Value) -> Wire {
        Wire {
            backing: Arc::new(v),
            elem: None,
            cols: None,
        }
    }

    /// The focused value: `*backing`, or element `elem` of the `Value::Arr` in `backing`. The `elem`
    /// index is only ever set by [`Wire::rt_elem_row`] with `i < len`, so the `None`/out-of-range arm is
    /// unreachable for a validly-navigated wire (it yields the shared [`NULL_VALUE`]).
    fn focused(&self) -> &Value {
        match self.elem {
            None => &self.backing,
            Some(i) => match &*self.backing {
                Value::Arr(items) => items.get(i).unwrap_or(&NULL_VALUE),
                _ => &NULL_VALUE,
            },
        }
    }

    /// Re-root a nested field/element value into its own `Arc` (the COLD descent path — relations are
    /// stitched Value-side, so `probe_row`/`probe_list`/arr-of-arr are not on the hot read path). The
    /// row column map is precomputed so a subsequent `probe_*` on it stays O(1).
    fn reroot(v: &Value) -> Wire {
        let cols = build_col_map(v);
        Wire {
            backing: Arc::new(v.clone()),
            elem: None,
            cols,
        }
    }

    /// The wrapped value at this wire's focus (a row/row-list/attribute).
    pub fn value(&self) -> &Value {
        self.focused()
    }

    // ── WireValue (node result) classification ──
    pub fn rt_as_string(&self) -> RtProbe<String> {
        probe_string_value(self.focused())
    }
    pub fn rt_as_number(&self) -> RtNum {
        probe_number_value(self.focused())
    }
    pub fn rt_as_bool(&self) -> RtProbe<bool> {
        probe_bool_value(self.focused())
    }
    pub fn rt_as_row(&self) -> RtProbe<Wire> {
        let v = self.focused();
        match v {
            // A row at THIS focus: share `backing`/`elem` (no Obj clone), attach the column map so a
            // later `probe_*` is O(1).
            Value::Obj(_) => RtProbe::Got(Wire {
                backing: self.backing.clone(),
                elem: self.elem,
                cols: build_col_map(v),
            }),
            Value::Null => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            _ => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }
    pub fn rt_as_list(&self) -> RtProbe<Wire> {
        let v = self.focused();
        match v {
            // The list stays at THIS focus (share `backing`/`elem`); precompute the ELEMENT column map
            // ONCE here (from element 0) and thread it to every `elem_row` ⇒ O(1) `probe_*` per row.
            Value::Arr(items) => RtProbe::Got(Wire {
                backing: self.backing.clone(),
                elem: self.elem,
                cols: items.first().and_then(build_col_map),
            }),
            Value::Null => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            _ => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }

    // ── WireRow (an object) attribute probes ──
    pub fn rt_keys(&self) -> Vec<String> {
        match self.focused() {
            Value::Obj(pairs) => pairs.iter().map(|(k, _)| k.clone()).collect(),
            _ => Vec::new(),
        }
    }
    /// The value of a row field via the O(1) column map (falls back to a linear scan on a map miss).
    fn row_field(&self, field: &str) -> Option<&Value> {
        match self.focused() {
            Value::Obj(pairs) => {
                let idx = field_index(self.cols.as_deref(), pairs, field)?;
                Some(&pairs[idx].1)
            }
            _ => None,
        }
    }
    pub fn rt_probe_string(&self, field: &str) -> RtProbe<String> {
        match self.row_field(field) {
            None => RtProbe::Absent,
            Some(v) => probe_string_value(v),
        }
    }
    pub fn rt_probe_number(&self, field: &str) -> RtNum {
        match self.row_field(field) {
            None => RtNum::Absent,
            Some(v) => probe_number_value(v),
        }
    }
    pub fn rt_probe_bool(&self, field: &str) -> RtProbe<bool> {
        match self.row_field(field) {
            None => RtProbe::Absent,
            Some(v) => probe_bool_value(v),
        }
    }
    pub fn rt_probe_row(&self, field: &str) -> RtProbe<Wire> {
        match self.row_field(field) {
            None => RtProbe::Absent,
            Some(v @ Value::Obj(_)) => RtProbe::Got(Wire::reroot(v)),
            Some(v @ Value::Null) => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            Some(v) => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }
    pub fn rt_probe_list(&self, field: &str) -> RtProbe<Wire> {
        match self.row_field(field) {
            None => RtProbe::Absent,
            Some(v @ Value::Arr(_)) => RtProbe::Got(Wire::reroot(v)),
            Some(v @ Value::Null) => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            Some(v) => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }

    // ── WireList (an array) element probes ──
    pub fn rt_len(&self) -> usize {
        match self.focused() {
            Value::Arr(items) => items.len(),
            _ => 0,
        }
    }
    fn elem(&self, i: usize) -> Option<&Value> {
        match self.focused() {
            Value::Arr(items) => items.get(i),
            _ => None,
        }
    }
    pub fn rt_elem_string(&self, i: usize) -> RtProbe<String> {
        match self.elem(i) {
            None => RtProbe::Absent,
            Some(v) => probe_string_value(v),
        }
    }
    pub fn rt_elem_number(&self, i: usize) -> RtNum {
        match self.elem(i) {
            None => RtNum::Absent,
            Some(v) => probe_number_value(v),
        }
    }
    pub fn rt_elem_bool(&self, i: usize) -> RtProbe<bool> {
        match self.elem(i) {
            None => RtProbe::Absent,
            Some(v) => probe_bool_value(v),
        }
    }
    pub fn rt_elem_row(&self, i: usize) -> RtProbe<Wire> {
        match self.elem(i) {
            None => RtProbe::Absent,
            Some(Value::Obj(_)) => RtProbe::Got(self.elem_row_wire(i)),
            Some(v @ Value::Null) => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            Some(v) => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }
    /// Build the ROW wire for list element `i`. HOT PATH (`self.elem` is `None` ⇒ the list Arr IS
    /// `*backing`): SHARE `backing` and select element `i`, reusing the list's shared column map (`cols`)
    /// so no per-row Obj clone and no per-row map rebuild. Defensive arr-of-arr case (`self.elem` set):
    /// re-root the element (cold — generated reads never nest lists this way).
    fn elem_row_wire(&self, i: usize) -> Wire {
        if self.elem.is_none() {
            Wire {
                backing: self.backing.clone(),
                elem: Some(i),
                cols: self
                    .cols
                    .clone()
                    .or_else(|| self.elem(i).and_then(build_col_map)),
            }
        } else {
            self.elem(i)
                .map(Wire::reroot)
                .unwrap_or_else(|| Wire::node(Value::Null))
        }
    }
    pub fn rt_elem_list(&self, i: usize) -> RtProbe<Wire> {
        match self.elem(i) {
            None => RtProbe::Absent,
            Some(v @ Value::Arr(_)) => RtProbe::Got(Wire::reroot(v)),
            Some(v @ Value::Null) => RtProbe::Null {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
            Some(v) => RtProbe::Wrong {
                actual_wire_type: wire_tag(v),
                raw_value: raw_of(v),
            },
        }
    }
}

// ── Params: native ports → bc scalar Value (type-agnostic; the companion lists ports in order) ──────

/// Lower a native ports SCALAR field to a bound bc [`Value`]. The generated companion calls
/// `wp(&ports.f_pN)` for each `?` in placeholder order — it does not need the scalar's Rust type
/// (inference resolves it), so the companion stays a thin per-op param LIST, not a type table.
pub trait ToWireParam {
    fn to_wire_param(&self) -> Value;
}
impl ToWireParam for i64 {
    fn to_wire_param(&self) -> Value {
        Value::Int(*self)
    }
}
impl ToWireParam for f64 {
    fn to_wire_param(&self) -> Value {
        Value::Float(*self)
    }
}
impl ToWireParam for bool {
    fn to_wire_param(&self) -> Value {
        Value::Bool(*self)
    }
}
impl ToWireParam for String {
    fn to_wire_param(&self) -> Value {
        Value::Str(self.clone())
    }
}
impl ToWireParam for str {
    fn to_wire_param(&self) -> Value {
        Value::Str(self.to_string())
    }
}

/// A scalar ports field → bound [`Value`].
pub fn wp<T: ToWireParam + ?Sized>(v: &T) -> Value {
    v.to_wire_param()
}

/// Lower an IN-list / array-bound ports field to ONE bound param per dialect. Postgres binds the array
/// as a native `Value::Arr` (`= ANY($1)`); MySQL/SQLite bind the JSON-encoded array as ONE string param
/// (server-side `json_each(?)` / `JSON_TABLE(?)`). This is the SAME dialect split
/// [`crate::relation`]'s `bind_keys` makes for relation batches — a binding concern, not a SQL-text one.
pub trait ToWireArray {
    fn wire_elems(&self) -> Vec<Value>;
}
impl ToWireArray for Vec<i64> {
    fn wire_elems(&self) -> Vec<Value> {
        self.iter().map(|i| Value::Int(*i)).collect()
    }
}
impl ToWireArray for Vec<String> {
    fn wire_elems(&self) -> Vec<Value> {
        self.iter().map(|s| Value::Str(s.clone())).collect()
    }
}

/// An array ports field → ONE bound param — always a `Value::Arr`; DIALECT-BLIND (the Postgres-native-
/// array vs MySQL/SQLite-`json_each(?)`-JSON decision is the Driver param-binder's, invariant #3). See [`ToWireArray`].
pub fn wp_array<T: ToWireArray>(v: &T) -> Value {
    // Always a `Value::Arr` — the DIALECT decision (Postgres native array vs MySQL/SQLite `json_each(?)`
    // JSON string) is resolved by the Driver's param-binder (the array-bind SSoT), NOT here. The
    // generated companion never branches on dialect (invariant #3).
    Value::Arr(v.wire_elems())
}

// ── Executors: the op-agnostic query-exec functions the generated node_* handlers call ──────────────

/// Wrap a driver row list as a node-result [`Wire`].
fn rows_wire(rows: Vec<Value>) -> Wire {
    Wire::node(Value::Arr(rows))
}

/// The affected-row summary AS ONE row object `{changes, lastInsertRowid}` — the value a non-returning
/// write produces. A plain write wraps it in a row LIST ([`summary_wire`]); a tx-chain statement's
/// produced value is this SINGLE obj (the tx runner de-boxes each statement via `as_row`, not `as_list`).
fn summary_obj(info: RunInfo) -> Value {
    Value::Obj(vec![
        ("changes".to_string(), Value::Int(info.changes)),
        (
            "lastInsertRowid".to_string(),
            Value::Int(info.last_insert_rowid),
        ),
    ])
}

/// The single-summary row a NON-RETURNING plain write hands back: `[{changes, lastInsertRowid}]` —
/// byte-equal to the mode-2 `executeStaticWrite` shape (and the shape the codegen lowering bakes for a
/// no-RETURNING write's outType, a row LIST).
fn summary_wire(info: RunInfo) -> Wire {
    Wire::node(Value::Arr(vec![summary_obj(info)]))
}

/// The execution MODE — the DRIVER-SHAPE the op's result is collected in. This is the ONLY thing that
/// varies between ops at execution time: a `Rows` op (SELECT, or a RETURNING write) collects a row
/// list; a `Summary` op (a non-returning write) collects the affected-row summary. It is NOT the op and
/// NOT the DB — both are invisible to [`exec`] (the DB is the [`Driver`]'s polymorphism; the op is baked
/// into the SQL). Skip / batch-write / batched-relation / tx are SQL-or-param PREPARATION layered on top,
/// each ending in this ONE executor (except the batched-relation + tx seams, which reuse
/// [`crate::relation`] and the Driver tx seam respectively).
/// The execution MODE — the DRIVER-SHAPE the op's result is collected in, and the WIRE-SHAPE the
/// generated de-box expects. This closed set is the ONLY thing that varies between ops at execution
/// time; it is NOT the op and NOT the DB (both are invisible to [`exec`]). A plain read/write de-boxes a
/// row LIST (`as_list`); a tx-chain statement de-boxes a SINGLE produced row (`as_row`) — hence the
/// `*Single` variants. `Rows`/`RowSingle` collect a row list (SELECT / RETURNING write); `Summary`/
/// `SummarySingle` collect the affected-row summary (a non-returning write).
#[derive(Clone, Copy)]
pub enum ExecMode {
    /// SELECT / RETURNING write, de-boxed as a row LIST.
    Rows,
    /// Non-returning write, de-boxed as a `[{changes, lastInsertRowid}]` row LIST.
    Summary,
    /// A tx-chain RETURNING statement, de-boxed as a SINGLE row obj.
    RowSingle,
    /// A tx-chain non-returning statement, de-boxed as a SINGLE `{changes, lastInsertRowid}` obj.
    SummarySingle,
}

/// The connection SOURCE a generated read/write companion resolves its [`ExecutionContext`] from
/// (#135) — so a native op is routed by the SAME `connection_for` seam the mode-2 runtime uses:
///   - [`ConnSource::Driver`] — a single [`Driver`] (routing = None): every intent lands on that one
///     driver (byte-identical to the pre-#135 `for_driver` companion path).
///   - [`ConnSource::Routing`] — a [`RoutingConfig`] (registry + writer-sticky): a READ routes to the
///     reader pool, a WRITE / tx to the writer pool, named-DB by `intent.db` (design §3 steps 2-4).
///
/// The companion's `node_*` builds the ctx via [`ConnSource::ctx`] and hands it to [`exec`] — so
/// reader/writer routing is applied ONCE, in the central seam, never re-implemented per op. `Copy`
/// because it holds only references (the caller owns the driver / routing).
#[derive(Clone, Copy)]
pub enum ConnSource<'a> {
    /// A single driver — routing = None (byte-identical single-pool path).
    Driver(&'a dyn Driver),
    /// A routing config — reader/writer + named-DB resolution via [`exec_context::for_routing`].
    Routing(&'a RoutingConfig),
}

impl<'a> ConnSource<'a> {
    /// Build the [`ExecutionContext`] for ONE `node_*` statement: [`exec_context::for_driver`] for a
    /// single driver, [`exec_context::for_routing`] for a routing config (loud on an unregistered
    /// named connection — the same throw the mode-2 seam raises).
    pub fn ctx(&self) -> Result<ExecutionContext<'a, 'a>, SqlFailure> {
        match *self {
            ConnSource::Driver(d) => Ok(exec_context::for_driver(d)),
            ConnSource::Routing(r) => exec_context::for_routing(r),
        }
    }
}

/// THE native-codegen executor (epic #124 invariant): run the baked `sql` with ordered `params` through
/// the central seam on the given [`ExecutionContext`] and hand back the result as a [`Wire`]. It takes
/// ONLY `ctx` / `sql` / `params` / [`ExecMode`] — never an operation name, never a DB kind. The DB
/// difference (sqlite / pg / mysql, incl. MySQL's RETURNING emulation) is the [`Driver`] trait's
/// polymorphism, resolved by the ctx BELOW this function; the dialect SQL-text difference (`?`→`$N`,
/// `json_each` vs `= ANY`) is resolved ABOVE it at SQL-generation time. So there is exactly ONE executor
/// for all 20 ops × 3 dialects × {plain, tx} — not 60 — and it branches on neither op nor DB. A plain
/// read/write passes a fresh [`exec_context::for_driver`] ctx; a tx-chain statement passes the tx-scoped
/// ctx [`run_transaction`] threads (so its statements run on the ONE pinned tx connection).
pub fn exec(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
    mode: ExecMode,
) -> Result<Wire, SqlFailure> {
    // THE single dialect placeholder-resolution point (SSoT): the generated modules bake dialect-NEUTRAL
    // `?`; here — after the final SQL is assembled, using the CONNECTION's dialect (a ctx property, not a
    // per-op flag) — `?`→`$N` is renumbered for postgres via the existing `render_placeholders` (a no-op
    // for sqlite/mysql, which keep `?`). Applied ONCE, so a mid-fragment skip drop can never desync the
    // `$N` sequence (the latent bug of a generation-time renumber). mode-2 does NOT route through here
    // (it renders its own `$N`), so there is no double-conversion.
    let sql = render_placeholders(sql, ctx.driver().dialect());
    match mode {
        ExecMode::Rows => Ok(rows_wire(exec_context::execute(
            ctx,
            &sql,
            params,
            &StatementIntent::read(),
        )?)),
        ExecMode::Summary => Ok(summary_wire(exec_context::run(
            ctx,
            &sql,
            params,
            &StatementIntent::write(),
        )?)),
        ExecMode::RowSingle => Ok(Wire::node(
            exec_context::execute(ctx, &sql, params, &StatementIntent::read())?
                .into_iter()
                .next()
                .unwrap_or(Value::Null),
        )),
        ExecMode::SummarySingle => Ok(Wire::node(summary_obj(exec_context::run(
            ctx,
            &sql,
            params,
            &StatementIntent::write(),
        )?))),
    }
}

/// ONE WHERE fragment handed to [`build_skip_params`]: the bare predicate SQL (baked literal, NO connector),
/// whether it is PRESENT (a required fragment is always present; a skip-optional fragment is present iff
/// its bc#139 Option head is `Some`), and its bound params (empty when absent).
pub struct SkipFrag {
    pub sql: String,
    pub present: bool,
    pub params: Vec<Value>,
}

/// The generic SKIP-aware READ exec (owner SKIP-args model). ASSEMBLES the query from baked literals:
/// `head` + the PRESENT `frags` joined ` WHERE `/` AND ` + `tail`, binding head params, then each
/// present fragment's params in order, then tail params. The `?`→`$N` renumber for postgres happens
/// HERE, post-assembly (the fragments keep `?`), reusing [`render_placeholders`] — the SAME renumber
/// the mode-2 runtime applies. Byte-identical SQL to the mode-2 assembly for a given present-set.
pub fn build_skip_params(
    head: &str,
    head_params: &[Value],
    frags: &[SkipFrag],
    tail: &str,
    tail_params: &[Value],
) -> (String, Vec<Value>) {
    let mut sql = String::from(head);
    let mut params: Vec<Value> = head_params.to_vec();
    let mut first = true;
    for f in frags {
        if !f.present {
            continue;
        }
        sql.push_str(if first { " WHERE " } else { " AND " });
        sql.push_str(&f.sql);
        first = false;
        params.extend(f.params.iter().cloned());
    }
    sql.push_str(tail);
    params.extend(tail_params.iter().cloned());
    // The assembled SQL keeps `?`; the ONE dialect renumber happens in `exec` (from the connection's
    // dialect) — so a dropped mid-fragment can never desync the `$N` sequence. The caller runs the pair
    // through the SINGLE `exec` — there is NO skip-specific executor.
    (sql, params)
}

/// The param-shape DESCRIPTOR for a multi-column array bind — resolved ONCE from the dialect at the SQL
/// generation stage (the SAME layer that bakes json_each-vs-UNNEST), then carried on the artifact so the
/// marshaling is dialect-BLIND: it follows the descriptor and never names a DB. Reusable across the
/// batch-write and composite-relation array binds (and the go/ts/py/php ports) — no per-op special case.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ArrayParamShape {
    /// Postgres UNNEST: ONE native array param PER column (`$N::T[]`), in column order.
    PerColumn,
    /// MySQL/SQLite json_each/JSON_TABLE: ONE JSON param (the op-specific rows/tuples), bound to every `?`.
    SingleJson,
}

/// Build the bind params for a batch write from the records as PARALLEL columns (`columns[j]` names
/// `cells[j]`, the already bc-`Value` cells for column j) per the generation-stage `shape` DESCRIPTOR —
/// the SINGLE input-marshaling SSoT the native companion AND the mode-2 render
/// ([`crate::static_bundle::render_statements`]) both call (there is NO second zip). `n_params` = the
/// statement's `?` count (SingleJson binds the ONE zipped JSON to every `?`). Dialect-blind: the shape is
/// decided once at SQL generation, never re-inspected here. The caller runs the params through the SINGLE
/// [`exec`] — there is NO batch-specific executor.
pub fn build_batch_params(
    columns: &[&str],
    cells: &[Vec<Value>],
    shape: ArrayParamShape,
    n_params: usize,
) -> Vec<Value> {
    match shape {
        // v1 UNNEST: one array param per column, in column order (`UNNEST($1::T[], $2::T[], …)`).
        ArrayParamShape::PerColumn => cells.iter().map(|c| Value::Arr(c.clone())).collect(),
        // v2 json_each/JSON_TABLE: ZIP the columns into `[{col:val,…},…]` (compact JSON) and bind to every
        // `?`. A JSON batch cell bool → 1/0 (MySQL TINYINT(1); SQLite json_each coerces either) — uniform,
        // dialect-blind. Scalars encode via the shared `compact_value` (byte-equal to the retired json_cell).
        ArrayParamShape::SingleJson => {
            let n = cells.first().map(|c| c.len()).unwrap_or(0);
            let rows: Vec<Value> = (0..n)
                .map(|i| {
                    Value::Obj(
                        columns
                            .iter()
                            .enumerate()
                            .map(|(j, col)| {
                                let cell = cells
                                    .get(j)
                                    .and_then(|a| a.get(i))
                                    .cloned()
                                    .unwrap_or(Value::Null);
                                let cell = match cell {
                                    Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
                                    other => other,
                                };
                                (col.to_string(), cell)
                            })
                            .collect(),
                    )
                })
                .collect();
            let json = crate::node::compact_value(&Value::Arr(rows));
            (0..n_params).map(|_| Value::Str(json.clone())).collect()
        }
    }
}

// ── Transaction: the RETURNING-chain envelope over the central tx seam ──────────────────────────────

/// The TRANSACTION ENVELOPE: open ONE tx-scoped [`ExecutionContext`] over `driver` (BEGIN on the
/// per-execution-owned connection) and run `body` (the generated chain runner). Each of the chain's
/// statements calls the SAME [`exec`] with the tx-scoped ctx `body` receives, so the whole RETURNING
/// chain runs on the ONE pinned connection; then COMMIT on `Ok`, ROLLBACK on `Err` — returning `true`
/// when it committed, `false` when it rolled back (the `{committed}` result the tx proof asserts; a
/// chain error is a legitimate rollback, NOT propagated). Reuses [`exec_context::with_transaction_decided`]
/// (the SAME per-execution-ownership tx mechanism the mode-2 write-tx path drives) — no new tx engine.
pub fn run_transaction<R, E>(
    driver: &dyn Driver,
    body: impl FnOnce(&ExecutionContext) -> Result<R, E>,
) -> Result<bool, SqlFailure> {
    let ctx = exec_context::for_driver(driver);
    exec_context::with_transaction_decided(&ctx, |tx_ctx| {
        Ok(match body(tx_ctx) {
            Ok(_) => TxDecision::Commit(true),
            Err(_) => TxDecision::Rollback(false),
        })
    })
}

/// The OPTIONS-aware, RETRYING native transaction envelope (#135 routing/isolation + #136 retry): open
/// the tx from a [`ConnSource`] (a single driver, or the WRITER pool of a named `connection` in a
/// [`RoutingConfig`]) and drive the SAME retry loop [`exec_context::transaction_decided_on`] the mode-2
/// write-tx path uses. That loop applies, in ONE place (no native re-implementation): the isolation
/// prelude ([`isolation_prelude`]), `rollback_only` (dry-run), the nested-tx JOIN, AND the #81 RETRY
/// loop — a retryable error (deadlock / serialization / connection) re-runs the WHOLE body on a FRESH
/// connection, classified by the SHARED [`crate::tx_options::is_retryable_tx_error`] SSoT (no parallel
/// native predicate). Returns `true` iff the tx COMMITTED (`rollback_only` ⇒ `false`; a non-retryable
/// or retry-exhausted error re-raises as [`SqlFailure`], which the consumer maps to committed:false).
///
/// RETRY on the native path (#136) is litedbmodel-side and bc-INDEPENDENT: the body is `impl Fn`
/// (re-runnable per attempt); the companion re-supplies the input each attempt via an input-BUILDER
/// closure (`make_in: impl Fn() -> InNR<comp>`, called inside the body), so NO bc `Clone` derive on the
/// generated `InNR<comp>` is needed. The body returns its failure as a [`SqlFailure`] so the retry loop
/// can classify it (the companion recovers it from the bc runner's error message, whose OP_FAILED text
/// embeds the original driver message the SSoT matches on).
pub fn run_transaction_on<R>(
    src: ConnSource,
    connection: Option<&str>,
    dialect: TxDialect,
    options: &TransactionOptions,
    body: impl Fn(&ExecutionContext) -> Result<R, SqlFailure>,
) -> Result<bool, SqlFailure> {
    let ctx = src.ctx()?;
    let committed =
        exec_context::transaction_decided_on(&ctx, connection, dialect, options, |tx_ctx| {
            // A body failure propagates as a SqlFailure ⇒ the retry loop classifies + retries/re-raises;
            // success COMMITs (the loop rewrites this to ROLLBACK under rollback_only, returning `true`).
            body(tx_ctx)?;
            Ok(TxDecision::Commit(true))
        })?;
    // rollback_only (dry-run) committed nothing even though the body succeeded ⇒ report false.
    Ok(committed && !options.rollback_only)
}

/// The [`wire_impls!`] macro — expands the module-local `WireValue`/`WireRow`/`WireList` trait impls for
/// [`Wire`] in the GENERATED companion (the orphan-rule bridge: the traits are local to the generated
/// module, so the impls must live in the companion crate, but every method delegates to [`Wire`]'s
/// `rt_*` classification here — the classification stays single-sourced in the runtime). The trait /
/// enum names (`WireValue`/`WireRow`/`WireList`/`Probe`/`NumProbe`) are referenced UNQUALIFIED so
/// macro_rules resolves them at the CALL site — the companion brings them in with
/// `use super::generated_<op>::*;` before invoking `litedbmodel_runtime::wire_impls!();` (a `$m:path`
/// metavariable cannot legally prefix `::Type` in type position, so the module is imported, not passed).
#[macro_export]
macro_rules! wire_impls {
    () => {
        impl WireValue for $crate::Wire {
            type Row = $crate::Wire;
            fn as_string(&self) -> Probe<String> {
                $crate::__wire_probe!(self.rt_as_string())
            }
            fn as_number(&self) -> NumProbe {
                $crate::__wire_num!(self.rt_as_number())
            }
            fn as_bool(&self) -> Probe<bool> {
                $crate::__wire_probe!(self.rt_as_bool())
            }
            fn as_row(&self) -> Probe<Self::Row> {
                $crate::__wire_probe!(self.rt_as_row())
            }
            fn as_list(&self) -> Probe<<Self::Row as WireRow>::List> {
                $crate::__wire_probe!(self.rt_as_list())
            }
        }
        impl WireRow for $crate::Wire {
            type List = $crate::Wire;
            fn keys(&self) -> Vec<String> {
                self.rt_keys()
            }
            fn probe_string(&self, field: &str) -> Probe<String> {
                $crate::__wire_probe!(self.rt_probe_string(field))
            }
            fn probe_number(&self, field: &str) -> NumProbe {
                $crate::__wire_num!(self.rt_probe_number(field))
            }
            fn probe_bool(&self, field: &str) -> Probe<bool> {
                $crate::__wire_probe!(self.rt_probe_bool(field))
            }
            fn probe_row(&self, field: &str) -> Probe<Self> {
                $crate::__wire_probe!(self.rt_probe_row(field))
            }
            fn probe_list(&self, field: &str) -> Probe<Self::List> {
                $crate::__wire_probe!(self.rt_probe_list(field))
            }
        }
        impl WireList for $crate::Wire {
            type Row = $crate::Wire;
            fn len(&self) -> usize {
                self.rt_len()
            }
            fn elem_string(&self, i: usize) -> Probe<String> {
                $crate::__wire_probe!(self.rt_elem_string(i))
            }
            fn elem_number(&self, i: usize) -> NumProbe {
                $crate::__wire_num!(self.rt_elem_number(i))
            }
            fn elem_bool(&self, i: usize) -> Probe<bool> {
                $crate::__wire_probe!(self.rt_elem_bool(i))
            }
            fn elem_row(&self, i: usize) -> Probe<Self::Row> {
                $crate::__wire_probe!(self.rt_elem_row(i))
            }
            fn elem_list(&self, i: usize) -> Probe<Self> {
                $crate::__wire_probe!(self.rt_elem_list(i))
            }
        }
    };
}

/// Map a runtime [`RtProbe`] to the call-site module-local `Probe` (internal — used by [`wire_impls!`]).
#[macro_export]
#[doc(hidden)]
macro_rules! __wire_probe {
    ($e:expr) => {
        match $e {
            $crate::RtProbe::Got(t) => Probe::Got(t),
            $crate::RtProbe::Wrong {
                actual_wire_type,
                raw_value,
            } => Probe::Wrong {
                actual_wire_type,
                raw_value,
            },
            $crate::RtProbe::Null {
                actual_wire_type,
                raw_value,
            } => Probe::Null {
                actual_wire_type,
                raw_value,
            },
            $crate::RtProbe::Absent => Probe::Absent,
        }
    };
}

/// Map a runtime [`RtNum`] to the call-site module-local `NumProbe` (internal — used by [`wire_impls!`]).
#[macro_export]
#[doc(hidden)]
macro_rules! __wire_num {
    ($e:expr) => {
        match $e {
            $crate::RtNum::Got {
                raw,
                actual_wire_type,
            } => NumProbe::Got {
                raw,
                actual_wire_type,
            },
            $crate::RtNum::Wrong {
                actual_wire_type,
                raw_value,
            } => NumProbe::Wrong {
                actual_wire_type,
                raw_value,
            },
            $crate::RtNum::Null {
                actual_wire_type,
                raw_value,
            } => NumProbe::Null {
                actual_wire_type,
                raw_value,
            },
            $crate::RtNum::Absent => NumProbe::Absent,
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::SqliteDriver;
    use crate::exec_context::for_driver;

    fn seeded() -> SqliteDriver {
        SqliteDriver::in_memory(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
            "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'b')".to_string(),
        ])
        .unwrap()
    }

    /// `exec(Rows)` hands back a row LIST wire the generated de-box reads via `as_list`/`elem_row`.
    #[test]
    fn exec_rows_returns_row_list_wire() {
        let d = seeded();
        let w = exec(
            &for_driver(&d),
            "SELECT id, name FROM t WHERE id = ?",
            &[Value::Int(2)],
            ExecMode::Rows,
        )
        .unwrap();
        assert_eq!(w.rt_len(), 1);
        match w.rt_elem_row(0) {
            RtProbe::Got(row) => {
                assert!(matches!(row.rt_probe_number("id"), RtNum::Got { .. }));
                assert!(matches!(row.rt_probe_string("name"), RtProbe::Got(ref s) if s == "b"));
                // an absent column is Absent (the de-box turns that into a MISSING_PROP failure).
                assert!(matches!(row.rt_probe_string("nope"), RtProbe::Absent));
            }
            _ => panic!("expected a row"),
        }
    }

    /// `exec(Summary)` synthesizes the `[{changes, lastInsertRowid}]` list a non-returning write returns.
    #[test]
    fn exec_summary_returns_affected_summary() {
        let d = seeded();
        let w = exec(
            &for_driver(&d),
            "DELETE FROM t WHERE name = ?",
            &[Value::Str("b".into())],
            ExecMode::Summary,
        )
        .unwrap();
        let row = match w.rt_elem_row(0) {
            RtProbe::Got(r) => r,
            _ => panic!("summary row"),
        };
        assert!(matches!(row.rt_probe_number("changes"), RtNum::Got { ref raw, .. } if raw == "2"));
        assert!(matches!(
            row.rt_probe_number("lastInsertRowid"),
            RtNum::Got { .. }
        ));
    }

    /// A present skip fragment is assembled; an absent one drops (the SKIP-args model over baked literals):
    /// `build_skip_params` marshals the present fragments, the SINGLE `exec` runs them (no skip executor).
    #[test]
    fn build_skip_params_assembles_only_present_fragments() {
        let d = seeded();
        let run = |frags: &[SkipFrag]| {
            let (sql, params) =
                build_skip_params("SELECT id FROM t", &[], frags, " ORDER BY id ASC", &[]);
            exec(
                &crate::exec_context::for_driver(&d),
                &sql,
                &params,
                ExecMode::Rows,
            )
            .unwrap()
        };
        let present = run(&[SkipFrag {
            sql: "name = ?".into(),
            present: true,
            params: vec![Value::Str("b".into())],
        }]);
        assert_eq!(present.rt_len(), 2);
        let dropped = run(&[SkipFrag {
            sql: "name = ?".into(),
            present: false,
            params: vec![],
        }]);
        assert_eq!(dropped.rt_len(), 3); // fragment dropped ⇒ all rows
    }

    /// REGRESSION (#124 placeholder unification): a MID-fragment optional skip that drops must NOT desync
    /// the remaining placeholders. Here the MIDDLE fragment (`name = ?`) is absent while a fragment BEFORE
    /// (`id >= ?`) and AFTER (`id <= ?`) it are present — so the after-fragment's param must still bind to
    /// the SECOND `?`, not the third. The single runtime renumber over the FINAL assembled SQL guarantees
    /// this (a generation-time `$N` would have frozen the after-fragment at `$3` and desynced on pg); on
    /// sqlite the `?` bind order is what proves the assembly aligned the params. `bymaybe` (optional at the
    /// TAIL) can never expose this — the dropped-in-the-middle case can.
    #[test]
    fn build_skip_params_mid_optional_drop_keeps_placeholders_aligned() {
        let d = seeded(); // rows id 1='a', 2='b', 3='b'
        let frags = |mid_present: bool| {
            vec![
                SkipFrag {
                    sql: "id >= ?".into(),
                    present: true,
                    params: vec![Value::Int(2)],
                },
                SkipFrag {
                    sql: "name = ?".into(),
                    present: mid_present,
                    params: if mid_present {
                        vec![Value::Str("nope".into())]
                    } else {
                        vec![]
                    },
                },
                SkipFrag {
                    sql: "id <= ?".into(),
                    present: true,
                    params: vec![Value::Int(3)],
                },
            ]
        };
        let run = |mid_present: bool| {
            let (sql, params) = build_skip_params(
                "SELECT id FROM t",
                &[],
                &frags(mid_present),
                " ORDER BY id ASC",
                &[],
            );
            exec(
                &crate::exec_context::for_driver(&d),
                &sql,
                &params,
                ExecMode::Rows,
            )
            .unwrap()
        };
        // Middle DROPPED: `WHERE id >= ? AND id <= ?` bound [2, 3] → rows 2,3. If the after-fragment's `?`
        // had desynced (bound 3 to a stale third slot), the count would be wrong / the query would error.
        assert_eq!(run(false).rt_len(), 2);
        // Middle PRESENT: `WHERE id >= ? AND name = ? AND id <= ?` bound [2,'nope',3] → 0 rows (no 'nope').
        assert_eq!(run(true).rt_len(), 0);
    }

    /// `build_batch_params` (SingleJson) zips the parallel column arrays into ONE json_each param the
    /// SINGLE `exec` runs (N rows, 1 stmt) — the batch has NO dedicated executor.
    #[test]
    fn build_batch_params_zips_one_statement() {
        let d = seeded();
        let cells = vec![
            vec![Value::Int(10), Value::Int(11)],
            vec![Value::Str("x".into()), Value::Str("y".into())],
        ];
        let sql = "INSERT INTO t (id, name) SELECT json_extract(value,'$.id'), json_extract(value,'$.name') FROM json_each(?)";
        let params = build_batch_params(
            &["id", "name"],
            &cells,
            ArrayParamShape::SingleJson,
            sql.matches('?').count(),
        );
        // ONE zipped JSON param bound to the single `?`.
        assert!(
            matches!(&params[..], [Value::Str(s)] if s == r#"[{"id":10,"name":"x"},{"id":11,"name":"y"}]"#)
        );
        let w = exec(
            &crate::exec_context::for_driver(&d),
            sql,
            &params,
            ExecMode::Summary,
        )
        .unwrap();
        // non-returning ⇒ summary list; 2 rows inserted.
        let row = match w.rt_elem_row(0) {
            RtProbe::Got(r) => r,
            _ => panic!("summary"),
        };
        assert!(matches!(row.rt_probe_number("changes"), RtNum::Got { ref raw, .. } if raw == "2"));
    }

    /// FIND HARD-LIMIT on the NATIVE read path (#135): a capped find whose baked `LIMIT hardLimit + 1`
    /// fetches MORE than the cap trips a `LimitExceededError` (`context: find`) — the SAME shared
    /// [`crate::errors::check_find_hard_limit`] the mode-2 `assert_find_guard` calls (so native ≡ mode-2).
    /// This exercises the REAL native seam the litedbmodel companion's guarded read entry runs: `exec`
    /// against the seeded DB with the `LIMIT cap + 1` baked SQL, then the shared post-fetch guard on the
    /// de-boxed row count. The de-box (bc's, proven byte-equal elsewhere) does not change the count, so a
    /// guard over the fetched Wire's row count is a faithful native find-guard test.
    #[test]
    fn native_find_read_enforces_hard_limit() {
        use crate::errors::{check_find_hard_limit, LimitExceededError, LIMIT_CONTEXT_FIND};
        // 5 rows seeded; cap = 2 ⇒ the compile bakes `LIMIT hardLimit + 1` = 3.
        let d = SqliteDriver::in_memory(&[
            "CREATE TABLE u (id INTEGER PRIMARY KEY)".to_string(),
            "INSERT INTO u (id) VALUES (1),(2),(3),(4),(5)".to_string(),
        ])
        .unwrap();
        let cap = 2i64;
        // The native read: `exec(Rows)` on the `LIMIT cap + 1` baked SQL — at most 3 rows fetched.
        let wire = exec(
            &for_driver(&d),
            "SELECT id FROM u ORDER BY id ASC LIMIT 3",
            &[],
            ExecMode::Rows,
        )
        .unwrap();
        let count = wire.rt_len() as i64;
        assert_eq!(count, 3); // cap + 1 ⇒ the TRUE total is known to EXCEED the cap.

        // OVER cap ⇒ the shared guard trips a LimitExceededError(find), byte-identical to what mode-2
        // (which now calls the SAME `check_find_hard_limit`) would raise for this count/cap/model.
        let err = check_find_hard_limit(cap, count, Some("u")).unwrap_err();
        assert_eq!(err.context, LIMIT_CONTEXT_FIND);
        assert_eq!(err.limit, cap);
        assert_eq!(err.count, count);
        assert_eq!(err.model.as_deref(), Some("u"));
        assert_eq!(err.relation, None);
        // The message is the shared byte-identical render (`more than <limit>` for the find context).
        assert_eq!(
            err.message,
            LimitExceededError::new(cap, count, LIMIT_CONTEXT_FIND, Some("u".into()), None).message
        );

        // WITHIN cap ⇒ no throw: a higher cap over the SAME fetched count passes cleanly.
        assert!(check_find_hard_limit(count, count, Some("u")).is_ok());
        assert!(check_find_hard_limit(count + 1, count, Some("u")).is_ok());
    }

    /// The tx envelope commits on Ok and rolls back on Err (atomicity).
    #[test]
    fn run_transaction_commits_and_rolls_back() {
        let d = seeded();
        let ok = run_transaction(&d, |ctx| {
            exec(
                ctx,
                "INSERT INTO t (id, name) VALUES (?, ?)",
                &[Value::Int(9), Value::Str("z".into())],
                ExecMode::Summary,
            )
            .map_err(|_| ())
        })
        .unwrap();
        assert!(ok);
        assert_eq!(
            exec(
                &for_driver(&d),
                "SELECT id FROM t WHERE id = 9",
                &[],
                ExecMode::Rows
            )
            .unwrap()
            .rt_len(),
            1
        );

        let committed = run_transaction(&d, |ctx| {
            exec(
                ctx,
                "INSERT INTO t (id, name) VALUES (?, ?)",
                &[Value::Int(8), Value::Str("q".into())],
                ExecMode::Summary,
            )
            .map_err(|_| ())?;
            // a duplicate PK fails ⇒ Err ⇒ rollback ⇒ id 8 must NOT persist.
            exec(
                ctx,
                "INSERT INTO t (id, name) VALUES (?, ?)",
                &[Value::Int(9), Value::Str("dup".into())],
                ExecMode::Summary,
            )
            .map_err(|_| ())
        })
        .unwrap();
        assert!(!committed);
        assert_eq!(
            exec(
                &for_driver(&d),
                "SELECT id FROM t WHERE id = 8",
                &[],
                ExecMode::Rows
            )
            .unwrap()
            .rt_len(),
            0
        );
    }

    // ── #135: reader/writer routing + options-aware tx on the NATIVE seam ──────────────────────────

    use crate::connection_routing::{
        ConnectionRegistry, ReaderWriterPools, RoutingConfig, WriterStickyClock,
    };
    use crate::driver::{forwarding_tx, forwarding_tx_no_begin, PreparedStatement};
    use crate::exec_context::TxConnection;
    use crate::tx_options::{Dialect as TxD, IsolationLevel, TransactionOptions};
    use std::sync::{Arc, Mutex};

    /// A Send+Sync recording driver: every prepared SQL (through the seam) is pushed to a shared log —
    /// so a routing test can assert WHICH pool (reader/writer) a native op landed on, and a tx test can
    /// assert the BEGIN / SET ISOLATION / COMMIT / ROLLBACK ordering. It can also INJECT a retryable
    /// failure the first `fail_remaining` times a statement equal to `fail_sql` runs (#136 retry test):
    /// the error message carries a phrase the SHARED `is_retryable_tx_error` SSoT classifies as retryable.
    struct RecDriver {
        log: Arc<Mutex<Vec<String>>>,
        fail_sql: Option<String>,
        fail_remaining: Arc<Mutex<u32>>,
    }
    struct RecStmt {
        log: Arc<Mutex<Vec<String>>>,
        sql: String,
        fail_sql: Option<String>,
        fail_remaining: Arc<Mutex<u32>>,
    }
    impl RecStmt {
        /// If this SQL is the injected fault target and a fault is still owed, consume one and return the
        /// retryable failure; else `Ok`.
        fn maybe_fail(&self) -> Result<(), SqlFailure> {
            if self.fail_sql.as_deref() == Some(self.sql.as_str()) {
                let mut left = self.fail_remaining.lock().unwrap();
                if *left > 0 {
                    *left -= 1;
                    return Err(SqlFailure {
                        kind: "retryable".into(),
                        policy: "retry".into(),
                        sqlite_code: None,
                        // A phrase `is_retryable_tx_error` matches (SSoT, tx_options.rs) — no native predicate.
                        message: "deadlock detected".into(),
                    });
                }
            }
            Ok(())
        }
    }
    impl PreparedStatement for RecStmt {
        fn all(&mut self, _p: &[Value]) -> Result<Vec<Value>, SqlFailure> {
            self.log.lock().unwrap().push(self.sql.clone());
            self.maybe_fail()?;
            Ok(vec![Value::Obj(vec![("id".into(), Value::Int(1))])])
        }
        fn run(&mut self, _p: &[Value]) -> Result<RunInfo, SqlFailure> {
            self.log.lock().unwrap().push(self.sql.clone());
            self.maybe_fail()?;
            Ok(RunInfo {
                changes: 1,
                last_insert_rowid: 0,
            })
        }
    }
    impl Driver for RecDriver {
        fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
            Box::new(RecStmt {
                log: self.log.clone(),
                sql: sql.to_string(),
                fail_sql: self.fail_sql.clone(),
                fail_remaining: self.fail_remaining.clone(),
            })
        }
        fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
            forwarding_tx(self)
        }
        fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
            forwarding_tx_no_begin(self)
        }
    }
    fn rec() -> (Arc<dyn Driver + Send + Sync>, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(RecDriver {
                log: log.clone(),
                fail_sql: None,
                fail_remaining: Arc::new(Mutex::new(0)),
            }),
            log,
        )
    }
    /// A recording driver that fails the FIRST `times` runs of `fail_sql` with a retryable error.
    fn rec_failing(
        fail_sql: &str,
        times: u32,
    ) -> (Arc<dyn Driver + Send + Sync>, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(RecDriver {
                log: log.clone(),
                fail_sql: Some(fail_sql.to_string()),
                fail_remaining: Arc::new(Mutex::new(times)),
            }),
            log,
        )
    }
    fn split_routing(
        reader: Arc<dyn Driver + Send + Sync>,
        writer: Arc<dyn Driver + Send + Sync>,
    ) -> RoutingConfig {
        RoutingConfig {
            registry: ConnectionRegistry::from_default(ReaderWriterPools::split(reader, writer))
                .build()
                .unwrap(),
            sticky: WriterStickyClock::disabled(),
        }
    }

    /// A native READ routes to the READER pool and a native WRITE to the WRITER pool — the SAME
    /// `connection_for` intent split the mode-2 seam uses, now reached from the native exec via
    /// [`ConnSource::Routing`]. Read hits reader only; write hits writer only.
    #[test]
    fn native_read_write_route_to_reader_writer() {
        let (reader, log_r) = rec();
        let (writer, log_w) = rec();
        let routing = split_routing(reader, writer);
        let ctx = ConnSource::Routing(&routing).ctx().unwrap();

        // READ → reader pool.
        exec(&ctx, "SELECT id FROM u", &[], ExecMode::Rows).unwrap();
        assert_eq!(*log_r.lock().unwrap(), vec!["SELECT id FROM u".to_string()]);
        assert!(
            log_w.lock().unwrap().is_empty(),
            "a read must NOT hit the writer"
        );

        // WRITE → writer pool.
        exec(
            &ctx,
            "INSERT INTO u (id) VALUES (9)",
            &[],
            ExecMode::Summary,
        )
        .unwrap();
        assert_eq!(
            *log_w.lock().unwrap(),
            vec!["INSERT INTO u (id) VALUES (9)".to_string()]
        );
        assert_eq!(
            log_r.lock().unwrap().len(),
            1,
            "the writer INSERT must NOT hit the reader"
        );
    }

    /// [`ConnSource::Driver`] (single pool) routes every intent to that one driver — byte-identical to
    /// the pre-#135 `for_driver` companion path (read AND write land on the SAME log).
    #[test]
    fn native_single_source_is_backward_compatible() {
        let (solo, log) = rec();
        let ctx = ConnSource::Driver(solo.as_ref()).ctx().unwrap();
        exec(&ctx, "SELECT 1", &[], ExecMode::Rows).unwrap();
        exec(&ctx, "INSERT INTO u VALUES (1)", &[], ExecMode::Summary).unwrap();
        assert_eq!(log.lock().unwrap().len(), 2);
    }

    /// A native tx routed by [`run_transaction_on`] runs its whole BEGIN…COMMIT on the WRITER pool
    /// (a tx is a write) and applies the isolation prelude — PG issues `SET TRANSACTION ISOLATION
    /// LEVEL …` as the first in-tx statement. The reader pool is never touched.
    #[test]
    fn native_tx_routes_to_writer_and_sets_isolation() {
        let (reader, log_r) = rec();
        let (writer, log_w) = rec();
        let routing = split_routing(reader, writer);
        let opts = TransactionOptions {
            isolation: Some(IsolationLevel::Serializable),
            ..Default::default()
        };
        let committed = run_transaction_on(
            ConnSource::Routing(&routing),
            None,
            TxD::Postgres,
            &opts,
            |ctx| exec(ctx, "UPDATE u SET n = 1", &[], ExecMode::SummarySingle).map(|_| ()),
        )
        .unwrap();
        assert!(committed);
        let w = log_w.lock().unwrap().clone();
        assert!(
            log_r.lock().unwrap().is_empty(),
            "a tx must NOT hit the reader"
        );
        // BEGIN → SET ISOLATION (PG post-BEGIN) → body UPDATE → COMMIT, all on the writer.
        assert_eq!(w[0], "BEGIN");
        assert_eq!(w[1], "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        assert_eq!(w[2], "UPDATE u SET n = 1");
        assert_eq!(w[3], "COMMIT");
    }

    /// `rollback_only` (dry-run): the body runs but the tx ends in ROLLBACK, and `run_transaction_on`
    /// reports committed:false — nothing persists even though the chain succeeded.
    #[test]
    fn native_tx_rollback_only_dry_run() {
        let (d, log) = rec();
        let opts = TransactionOptions {
            rollback_only: true,
            ..Default::default()
        };
        let committed = run_transaction_on(
            ConnSource::Driver(d.as_ref()),
            None,
            TxD::Sqlite,
            &opts,
            |ctx| {
                exec(
                    ctx,
                    "INSERT INTO u VALUES (1)",
                    &[],
                    ExecMode::SummarySingle,
                )
                .map(|_| ())
            },
        )
        .unwrap();
        assert!(!committed, "rollback_only ⇒ committed:false");
        let l = log.lock().unwrap().clone();
        assert!(l.contains(&"INSERT INTO u VALUES (1)".to_string()));
        assert!(
            l.contains(&"ROLLBACK".to_string()),
            "must ROLLBACK, not COMMIT"
        );
        assert!(!l.contains(&"COMMIT".to_string()));
    }

    /// A native tx whose body errors with a NON-retryable failure rolls back and re-raises as a
    /// SqlFailure (atomicity) — mirroring mode-2 `execute_transaction_bundle` (the consumer maps the Err
    /// to committed:false). retry_on_error default ON does NOT retry a non-retryable error (one attempt).
    #[test]
    fn native_tx_body_error_rolls_back() {
        let (d, log) = rec();
        let result = run_transaction_on(
            ConnSource::Driver(d.as_ref()),
            None,
            TxD::Sqlite,
            &TransactionOptions::default(),
            |ctx| {
                exec(
                    ctx,
                    "INSERT INTO u VALUES (1)",
                    &[],
                    ExecMode::SummarySingle,
                )
                .map(|_| ())?;
                Err::<(), SqlFailure>(SqlFailure {
                    kind: "constraint_violation".into(),
                    policy: "fail".into(),
                    sqlite_code: None,
                    message: "UNIQUE constraint failed".into(),
                })
            },
        );
        assert!(result.is_err(), "a non-retryable body error re-raises");
        let l = log.lock().unwrap().clone();
        assert!(l.contains(&"ROLLBACK".to_string()));
        // Exactly ONE attempt (non-retryable ⇒ no retry): a single BEGIN.
        assert_eq!(l.iter().filter(|s| *s == "BEGIN").count(), 1);
    }

    /// #136 NATIVE TX RETRY: a retryable failure (deadlock) on the first attempt re-runs the WHOLE tx on
    /// a FRESH connection and commits on the second — driven by the SHARED retry loop
    /// [`exec_context::transaction_decided_on`] + the SHARED [`crate::tx_options::is_retryable_tx_error`]
    /// SSoT (no native retry predicate). The input-builder closure re-supplies the body each attempt
    /// (bc-independent — no `Clone` on any generated struct).
    #[test]
    fn native_tx_retries_on_retryable_error_then_commits() {
        // Fail the INSERT once (retryable) → attempt 1 rolls back, attempt 2 succeeds + commits.
        let (d, log) = rec_failing("INSERT INTO u VALUES (1)", 1);
        let opts = TransactionOptions {
            retry_on_error: true,
            retry_limit: 3,
            retry_duration_ms: 0, // no backoff sleep in the test
            ..Default::default()
        };
        // A per-attempt input builder (the companion's `make_in`): rebuilt each attempt, no Clone needed.
        let make_row = || 1i64;
        let committed = run_transaction_on(
            ConnSource::Driver(d.as_ref()),
            None,
            TxD::Sqlite,
            &opts,
            |ctx| {
                let _row = make_row();
                exec(
                    ctx,
                    "INSERT INTO u VALUES (1)",
                    &[],
                    ExecMode::SummarySingle,
                )
                .map(|_| ())
            },
        )
        .unwrap();
        assert!(committed, "the retried attempt commits");
        let l = log.lock().unwrap().clone();
        // TWO attempts: BEGIN … (INSERT fails) ROLLBACK … BEGIN (INSERT ok) COMMIT.
        assert_eq!(
            l.iter().filter(|s| *s == "BEGIN").count(),
            2,
            "retry ran a SECOND attempt on a fresh connection"
        );
        assert!(l.contains(&"ROLLBACK".to_string()), "attempt 1 rolled back");
        assert_eq!(
            l.last().map(String::as_str),
            Some("COMMIT"),
            "attempt 2 committed"
        );
    }

    /// #136: a retryable error that NEVER clears within `retry_limit` exhausts retries and re-raises.
    #[test]
    fn native_tx_retry_exhausts_then_reraises() {
        // Fail more times than retry_limit ⇒ every attempt fails ⇒ re-raise.
        let (d, log) = rec_failing("INSERT INTO u VALUES (1)", 9);
        let opts = TransactionOptions {
            retry_on_error: true,
            retry_limit: 2,
            retry_duration_ms: 0,
            ..Default::default()
        };
        let result = run_transaction_on(
            ConnSource::Driver(d.as_ref()),
            None,
            TxD::Sqlite,
            &opts,
            |ctx| {
                exec(
                    ctx,
                    "INSERT INTO u VALUES (1)",
                    &[],
                    ExecMode::SummarySingle,
                )
                .map(|_| ())
            },
        );
        assert!(
            result.is_err(),
            "retry exhausted ⇒ the retryable error re-raises"
        );
        assert_eq!(
            log.lock().unwrap().iter().filter(|s| *s == "BEGIN").count(),
            2,
            "exactly retry_limit (2) attempts"
        );
    }
}
