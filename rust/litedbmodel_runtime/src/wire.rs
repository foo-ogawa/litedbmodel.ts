//! BC-OWNED shared wire + error-value types (#164 wire-passthrough / #165 `--shared-types-import`).
//!
//! HAND-AUTHORED stand-in for post-#164/#165 bc regen — will be replaced by regen. These are the
//! BC-owned, BC-generated types the `rust-typed-native` emitter emits into a SHARED import
//! (`--shared-types-import`, bc#165) so the op-agnostic leaf transports (`crate::leaves`) and the
//! GENERATED covered runners (`orm_bench/src/gen/behaviors_generated.rs`) reference the SAME concrete
//! `WireValue` / `Probe` / `BehaviorError` — the leaf BUILDS the wire + the Error Value at the wire
//! boundary, the runner de-boxes it. Until bc emits these into `go/`-style shared package the types
//! live here verbatim (moved from the module-local definitions the generated module used to carry).
//!
//! The bodies are byte-for-byte the module-local originals; only their HOME moved (module-local →
//! this shared crate module) so a single definition is shared, per #165.

use std::collections::BTreeMap;

// ── The BC-owned structured failure value (the "Error Value", scp-error.md) ─────────────────────
// ErrorKind — what went wrong (the closed set of scp-error.md). A concrete enum: the covered plane
// carries no strings-as-tags and no dynamic kind lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    TypeMismatch,
    MissingField,
    Overflow,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::TypeMismatch => "typeMismatch",
            ErrorKind::MissingField => "missingField",
            ErrorKind::Overflow => "overflow",
        }
    }
}

// ErrorDetail — the structured, recoverable payload a failure carries (scp-error.md "The Error
// Value"). The LEAF produces it at the wire boundary (it is the only party holding both the declared
// type and the raw wire datum); the runner transports it verbatim. Concrete fields — no boxed Value,
// no serialized blob in a string: `expected_type` is Portable Type Notation, a rendering of a
// STATICALLY DECLARED type, so nothing walks a type at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ErrorDetail {
    pub kind: Option<ErrorKind>,
    pub model: Option<String>,
    pub field: Option<String>,
    pub expected_type: Option<String>,
    pub actual_wire_type: Option<String>,
    pub raw_value: Option<String>,
    pub context: BTreeMap<String, String>,
}

// BehaviorError — the concrete failure type carried across the covered read plane. A covered runner
// returns `Result<T, BehaviorError>`; the leaf returns the SAME type so the runner transports its
// `detail` (the leaf's structured Error Value) verbatim. Codes match run_behavior (byte-equal).
#[derive(Debug, Clone)]
pub struct BehaviorError {
    pub code: String,
    pub message: String,
    pub detail: Option<Box<ErrorDetail>>,
}

impl BehaviorError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        BehaviorError { code: code.into(), message: message.into(), detail: None }
    }
    /// The same failure carrying the leaf's structured Error Value.
    pub fn with_detail(code: impl Into<String>, message: impl Into<String>, detail: ErrorDetail) -> Self {
        BehaviorError { code: code.into(), message: message.into(), detail: Some(Box::new(detail)) }
    }
    /// The stable failure code (byte-equal to run_behavior).
    pub fn code(&self) -> &str {
        &self.code
    }
    /// The structured payload, if this failure is about a datum.
    pub fn detail(&self) -> Option<&ErrorDetail> {
        self.detail.as_deref()
    }
}

impl std::fmt::Display for BehaviorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for BehaviorError {}

// ── The BC-owned wire value (#164) ──────────────────────────────────────────────────────────────
// strict de-box wire seam — the BC-OWNED, BC-GENERATED concrete WireValue (+ WireRow/WireList): the
// consumer's single transport builds + returns it, implements NOTHING over it. The generated INLINE de-box
// classifies it against the STATICALLY declared type and owns strictness (required/optional, present/absent,
// error assembly, fail-closed). Self-contained concrete types — no trait, no boxed runtime value.
// Probe<T> / NumProbe — the outcome of classifying one wire attribute against a declared type. Got carries
// the matched value (a NumProbe's raw numeric text, which the de-box parses + range-checks so overflow is
// BC's to detect); actual_wire_type is the producer's own wire tag (S/N/BOOL/M/L/NULL); raw_value is the
// offending value stringified. Concrete enums — no boxed runtime value.
pub enum Probe<T> {
    Got(T),
    Wrong { actual_wire_type: String, raw_value: String },
    Null { actual_wire_type: String, raw_value: String },
    Absent,
}

pub enum NumProbe {
    Got { raw: String, actual_wire_type: String },
    Wrong { actual_wire_type: String, raw_value: String },
    Null { actual_wire_type: String, raw_value: String },
    Absent,
}

// WireValue — the BC-OWNED, BC-GENERATED, self-contained (runtime-free) generic result the consumer's
// single op-agnostic transport returns. The consumer BUILDS it as it reads the producer (the raw I/O act)
// — WireValue::Str/Num/Bool/Null/Row/List, or the int()/float() constructors — and implements NOTHING over
// it. Numbers carry raw text (BC parses + range-checks → overflow is BC's to detect). A composite is a
// nested WireRow / WireList. NO trait, NO consumer classification protocol.
#[derive(Clone)]
pub enum WireValue {
    Str(String),
    Num(String),
    Bool(bool),
    Null,
    Row(WireRow),
    List(WireList),
}

// WireRow / WireList — a wire map (a DynamoDB "M") / wire array (an "L"). Native Vec-backed; the generated
// de-box probes them by declared field / element and owns strictness (required/optional, present/absent,
// error assembly). row/list probes return a BORROW (Probe<&WireRow>/Probe<&WireList>) so nested decode is
// zero-copy — only leaf scalars are cloned into the covered typed struct.
#[derive(Clone)]
pub struct WireRow {
    pub entries: Vec<(String, WireValue)>,
}

#[derive(Clone)]
pub struct WireList {
    pub items: Vec<WireValue>,
}

impl WireValue {
    // constructors the consumer's transport uses to build numeric results (numbers ride as raw text).
    pub fn int(n: i64) -> Self {
        WireValue::Num(n.to_string())
    }
    pub fn float(n: f64) -> Self {
        WireValue::Num(n.to_string())
    }
    /// Unwrap a top-level `List` result into its raw wire rows (the op-agnostic leaves take/return
    /// `&[WireValue]`, so an intermediate node holds `Vec<WireValue>` — the list ITEMS, still raw wire,
    /// never de-boxed). A non-list wire (never produced by the leaves) degrades to a single-element vec.
    pub fn into_items(self) -> Vec<WireValue> {
        match self {
            WireValue::List(l) => l.items,
            other => vec![other],
        }
    }
    // the top result is always present (Some(self)); the field/elem probes share the same classifiers.
    pub fn as_string(&self) -> Probe<String> {
        probe_string_at(Some(self))
    }
    pub fn as_number(&self) -> NumProbe {
        probe_number_at(Some(self))
    }
    pub fn as_bool(&self) -> Probe<bool> {
        probe_bool_at(Some(self))
    }
    pub fn as_row(&self) -> Probe<&WireRow> {
        probe_row_at(Some(self))
    }
    pub fn as_list(&self) -> Probe<&WireList> {
        probe_list_at(Some(self))
    }
    fn tag(&self) -> &'static str {
        match self {
            WireValue::Str(_) => "S",
            WireValue::Num(_) => "N",
            WireValue::Bool(_) => "BOOL",
            WireValue::Null => "NULL",
            WireValue::Row(_) => "M",
            WireValue::List(_) => "L",
        }
    }
    fn raw(&self) -> String {
        match self {
            WireValue::Str(s) => s.clone(),
            WireValue::Num(s) => s.clone(),
            WireValue::Bool(b) => b.to_string(),
            WireValue::Null => "null".to_string(),
            WireValue::Row(_) | WireValue::List(_) => "[composite]".to_string(),
        }
    }
}

impl WireRow {
    fn get(&self, field: &str) -> Option<&WireValue> {
        self.entries.iter().find(|(k, _)| k.as_str() == field).map(|(_, v)| v)
    }
    pub fn keys(&self) -> Vec<String> {
        self.entries.iter().map(|(k, _)| k.clone()).collect()
    }
    pub fn probe_string(&self, field: &str) -> Probe<String> {
        probe_string_at(self.get(field))
    }
    pub fn probe_number(&self, field: &str) -> NumProbe {
        probe_number_at(self.get(field))
    }
    pub fn probe_bool(&self, field: &str) -> Probe<bool> {
        probe_bool_at(self.get(field))
    }
    pub fn probe_row(&self, field: &str) -> Probe<&WireRow> {
        probe_row_at(self.get(field))
    }
    pub fn probe_list(&self, field: &str) -> Probe<&WireList> {
        probe_list_at(self.get(field))
    }
}

impl WireList {
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn elem_string(&self, i: usize) -> Probe<String> {
        probe_string_at(self.items.get(i))
    }
    pub fn elem_number(&self, i: usize) -> NumProbe {
        probe_number_at(self.items.get(i))
    }
    pub fn elem_bool(&self, i: usize) -> Probe<bool> {
        probe_bool_at(self.items.get(i))
    }
    pub fn elem_row(&self, i: usize) -> Probe<&WireRow> {
        probe_row_at(self.items.get(i))
    }
    pub fn elem_list(&self, i: usize) -> Probe<&WireList> {
        probe_list_at(self.items.get(i))
    }
}

// the ONE classifier per kind, over an Option<&WireValue> (None = absent attribute/element). The BC-owned
// WireValue variant IS the wire tag — no consumer classification.
fn probe_string_at(v: Option<&WireValue>) -> Probe<String> {
    match v {
        None => Probe::Absent,
        Some(WireValue::Str(s)) => Probe::Got(s.clone()),
        Some(WireValue::Null) => Probe::Null { actual_wire_type: "NULL".to_string(), raw_value: "null".to_string() },
        Some(o) => Probe::Wrong { actual_wire_type: o.tag().to_string(), raw_value: o.raw() },
    }
}
fn probe_number_at(v: Option<&WireValue>) -> NumProbe {
    match v {
        None => NumProbe::Absent,
        Some(WireValue::Num(s)) => NumProbe::Got { raw: s.clone(), actual_wire_type: "N".to_string() },
        Some(WireValue::Null) => NumProbe::Null { actual_wire_type: "NULL".to_string(), raw_value: "null".to_string() },
        Some(o) => NumProbe::Wrong { actual_wire_type: o.tag().to_string(), raw_value: o.raw() },
    }
}
fn probe_bool_at(v: Option<&WireValue>) -> Probe<bool> {
    match v {
        None => Probe::Absent,
        Some(WireValue::Bool(b)) => Probe::Got(*b),
        Some(WireValue::Null) => Probe::Null { actual_wire_type: "NULL".to_string(), raw_value: "null".to_string() },
        Some(o) => Probe::Wrong { actual_wire_type: o.tag().to_string(), raw_value: o.raw() },
    }
}
fn probe_row_at(v: Option<&WireValue>) -> Probe<&WireRow> {
    match v {
        None => Probe::Absent,
        Some(WireValue::Row(r)) => Probe::Got(r),
        Some(WireValue::Null) => Probe::Null { actual_wire_type: "NULL".to_string(), raw_value: "null".to_string() },
        Some(o) => Probe::Wrong { actual_wire_type: o.tag().to_string(), raw_value: o.raw() },
    }
}
fn probe_list_at(v: Option<&WireValue>) -> Probe<&WireList> {
    match v {
        None => Probe::Absent,
        Some(WireValue::List(l)) => Probe::Got(l),
        Some(WireValue::Null) => Probe::Null { actual_wire_type: "NULL".to_string(), raw_value: "null".to_string() },
        Some(o) => Probe::Wrong { actual_wire_type: o.tag().to_string(), raw_value: o.raw() },
    }
}
