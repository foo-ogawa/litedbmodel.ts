//! litedbmodel v2 SCP — the runtime's NATIVE JSON value model + expression evaluator (JSON-library-free).
//!
//! The rust runtime is NATIVE-ONLY (#8): it carries NO external JSON crate and does NOT depend on bc's
//! `ir` feature (`evaluate_expression`/`run_behavior`, both JSON-crate-based, are retired). The
//! published bundle is a pure-JSON artifact, but the runtime parses it through its OWN native parser
//! ([`Node::parse`], a hand-written recursive-descent JSON reader) into this crate's own [`Node`] tree,
//! then walks + evaluates `Node`s natively. No external JSON crate is linked, at any layer.
//!
//! [`Node`] provides a small `get`/`as_str`/`as_array`/`as_i64`/`as_u64`/`is_null`/`as_object` API so
//! the migration off the old external-JSON-value type was mechanical, and adds the NATIVE closed-set
//! expression evaluator ([`eval_expr`]) that replaces bc's `evaluate_expression`.
//! The evaluated operators are exactly the closed set the litedbmodel corpora use for deferred param
//! slots / skip / map-`over` / component `output`:
//!   `ref` · `refOpt` · `coalesce` · `ne`/`eq` · `not` · `and`/`or` · `arr` · `obj` · `cond` ·
//!   `concat` · scalar literals (string/int/float/bool/null).
//! Semantics are byte-identical to bc's `evaluate` (ported verbatim: `refOpt` short-circuits a null
//! intermediate, `eq/ne` is null-or-same-scalar, `coalesce` takes the first non-null, etc.). An
//! out-of-set operator FAILS CLOSED (`UNKNOWN_OP`) — never a silent default.

use behavior_contracts::{deep_equals, Value};
use std::fmt::Write as _;

/// A native, order-preserving JSON value (JSON-library-free). Numbers keep the int/float distinction
/// (a bare integral JSON number is `Int`, matching the runtime's `decode_value`).
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<Node>),
    /// An ordered object (insertion order preserved — byte-true to the JSON source).
    Object(Vec<(String, Node)>),
}

impl Node {
    /// Object field by key (mirrors `the JSON value type's get(&str)`), or `None`.
    pub fn get(&self, key: &str) -> Option<&Node> {
        match self {
            Node::Object(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
    /// The string, if this is a `Str` (mirrors `as_str`).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Node::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
    /// The array slice, if this is an `Array` (mirrors `as_array`).
    pub fn as_array(&self) -> Option<&[Node]> {
        match self {
            Node::Array(a) => Some(a.as_slice()),
            _ => None,
        }
    }
    /// The object pairs, if this is an `Object` (mirrors `as_object`, but ordered).
    pub fn as_object(&self) -> Option<&[(String, Node)]> {
        match self {
            Node::Object(o) => Some(o.as_slice()),
            _ => None,
        }
    }
    /// The i64, if this is an integral number (mirrors `as_i64`).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Node::Int(i) => Some(*i),
            _ => None,
        }
    }
    /// The u64, if this is a non-negative integral number (mirrors `as_u64`).
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Node::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }
    /// True when this is JSON `null` (mirrors `is_null`).
    pub fn is_null(&self) -> bool {
        matches!(self, Node::Null)
    }
    /// A shared `Null` for `.get(...).unwrap_or(&Node::NULL)`-style access.
    pub const NULL: Node = Node::Null;

    /// Compact JSON text (JS `JSON.stringify` form, no spaces) — used by diagnostics / test compares.
    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        write_node_compact(self, &mut out);
        out
    }
}

/// `Display` for [`Node`] = compact JSON (so `node.to_string()` yields the byte-true JSON form).
impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_json_string())
    }
}

fn write_node_compact(n: &Node, out: &mut String) {
    match n {
        Node::Null => out.push_str("null"),
        Node::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Node::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Node::Float(fl) => {
            if fl.fract() == 0.0 && fl.is_finite() {
                let _ = write!(out, "{}", *fl as i64);
            } else {
                let _ = write!(out, "{fl}");
            }
        }
        Node::Str(s) => write_json_string(s, out),
        Node::Array(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_node_compact(e, out);
            }
            out.push(']');
        }
        Node::Object(pairs) => {
            out.push('{');
            for (i, (k, v)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_node_compact(v, out);
            }
            out.push('}');
        }
    }
}

// ── NATIVE JSON parser (JSON-library-free) — the runner decodes the wire bundle through THIS ───────
// The runtime carries NO external JSON crate (not even an optional dep): the runners parse the wire bundle
// bytes into a [`Node`] through this hand-written recursive-descent parser. Object key order is
// preserved (insertion order), numbers keep the int/float split, and the standard JSON escapes are
// decoded. Fail-closed on malformed input (never a silent partial parse).
impl Node {
    /// Parse a JSON document into a [`Node`]. Returns an error message on malformed input.
    pub fn parse(input: &str) -> Result<Node, String> {
        let bytes = input.as_bytes();
        let mut p = Parser { b: bytes, i: 0 };
        p.skip_ws();
        let v = p.parse_value()?;
        p.skip_ws();
        if p.i != bytes.len() {
            return Err(format!("trailing content after JSON value at byte {}", p.i));
        }
        Ok(v)
    }
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }
    fn parse_value(&mut self) -> Result<Node, String> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => Ok(Node::Str(self.parse_string()?)),
            Some(b't') | Some(b'f') => self.parse_bool(),
            Some(b'n') => self.parse_null(),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            other => Err(format!("unexpected byte {other:?} at {}", self.i)),
        }
    }
    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!("expected '{}' at byte {}", c as char, self.i))
        }
    }
    fn parse_object(&mut self) -> Result<Node, String> {
        self.expect(b'{')?;
        let mut pairs = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(Node::Object(pairs));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            let val = self.parse_value()?;
            pairs.push((key, val));
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                other => {
                    return Err(format!(
                        "expected ',' or '}}' in object, got {other:?} at {}",
                        self.i
                    ))
                }
            }
        }
        Ok(Node::Object(pairs))
    }
    fn parse_array(&mut self) -> Result<Node, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Node::Array(items));
        }
        loop {
            let val = self.parse_value()?;
            items.push(val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                other => {
                    return Err(format!(
                        "expected ',' or ']' in array, got {other:?} at {}",
                        self.i
                    ))
                }
            }
        }
        Ok(Node::Array(items))
    }
    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut s = String::new();
        loop {
            match self.peek() {
                None => return Err("unterminated string".to_string()),
                Some(b'"') => {
                    self.i += 1;
                    break;
                }
                Some(b'\\') => {
                    self.i += 1;
                    match self.peek() {
                        Some(b'"') => s.push('"'),
                        Some(b'\\') => s.push('\\'),
                        Some(b'/') => s.push('/'),
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'b') => s.push('\u{8}'),
                        Some(b'f') => s.push('\u{c}'),
                        Some(b'u') => {
                            let hex: String = (0..4)
                                .map(|k| {
                                    self.b.get(self.i + 1 + k).copied().unwrap_or(b'0') as char
                                })
                                .collect();
                            let cp = u32::from_str_radix(&hex, 16)
                                .map_err(|_| format!("bad \\u escape at {}", self.i))?;
                            // Surrogate pair handling (astral plane).
                            if (0xD800..=0xDBFF).contains(&cp) {
                                // Expect a following \uXXXX low surrogate.
                                let lo_start = self.i + 5;
                                if self.b.get(lo_start) == Some(&b'\\')
                                    && self.b.get(lo_start + 1) == Some(&b'u')
                                {
                                    let lohex: String = (0..4)
                                        .map(|k| {
                                            self.b.get(lo_start + 2 + k).copied().unwrap_or(b'0')
                                                as char
                                        })
                                        .collect();
                                    let lo = u32::from_str_radix(&lohex, 16)
                                        .map_err(|_| "bad low surrogate".to_string())?;
                                    let c = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                    s.push(char::from_u32(c).unwrap_or('\u{FFFD}'));
                                    self.i += 6; // consumed \uXXXX low (the +5 below adds the high's)
                                } else {
                                    s.push('\u{FFFD}');
                                }
                            } else {
                                s.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                            }
                            self.i += 4;
                        }
                        other => return Err(format!("bad escape {other:?} at {}", self.i)),
                    }
                    self.i += 1;
                }
                Some(_) => {
                    // Copy a UTF-8 char (may be multi-byte) verbatim.
                    let start = self.i;
                    let mut end = self.i + 1;
                    while end < self.b.len() && (self.b[end] & 0xC0) == 0x80 {
                        end += 1;
                    }
                    s.push_str(
                        std::str::from_utf8(&self.b[start..end]).map_err(|e| e.to_string())?,
                    );
                    self.i = end;
                }
            }
        }
        Ok(s)
    }
    fn parse_bool(&mut self) -> Result<Node, String> {
        if self.b[self.i..].starts_with(b"true") {
            self.i += 4;
            Ok(Node::Bool(true))
        } else if self.b[self.i..].starts_with(b"false") {
            self.i += 5;
            Ok(Node::Bool(false))
        } else {
            Err(format!("bad literal at {}", self.i))
        }
    }
    fn parse_null(&mut self) -> Result<Node, String> {
        if self.b[self.i..].starts_with(b"null") {
            self.i += 4;
            Ok(Node::Null)
        } else {
            Err(format!("bad literal at {}", self.i))
        }
    }
    fn parse_number(&mut self) -> Result<Node, String> {
        let start = self.i;
        let mut is_float = false;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' => self.i += 1,
                b'.' | b'e' | b'E' | b'+' | b'-' => {
                    is_float = true;
                    self.i += 1;
                }
                _ => break,
            }
        }
        let tok = std::str::from_utf8(&self.b[start..self.i]).map_err(|e| e.to_string())?;
        if !is_float {
            if let Ok(i) = tok.parse::<i64>() {
                return Ok(Node::Int(i));
            }
        }
        tok.parse::<f64>()
            .map(Node::Float)
            .map_err(|_| format!("bad number '{tok}' at {start}"))
    }
}

// ── value ⇄ Node conversion + the $bigint conformance codec (JSON-library-free) ────────────────────

/// Decode a bundle/scope [`Node`] into the bc runtime [`Value`] (native port of the retired
/// the retired external-crate `decode_value`): `{"$bigint":"<dec>"}` → int; a bare integral number → int, a
/// fractional one → float; recurse arrays/objects (insertion order preserved). Fails loudly on a
/// `$bigint` string that is not a valid i64 (no silent fallback).
pub fn decode_value(x: &Node) -> Result<Value, String> {
    match x {
        Node::Null => Ok(Value::Null),
        Node::Bool(b) => Ok(Value::Bool(*b)),
        Node::Str(s) => Ok(Value::Str(s.clone())),
        Node::Int(i) => Ok(Value::Int(*i)),
        Node::Float(f) => Ok(Value::Float(*f)),
        Node::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for e in a {
                out.push(decode_value(e)?);
            }
            Ok(Value::Arr(out))
        }
        Node::Object(o) => {
            if o.len() == 1 && o[0].0 == "$bigint" {
                if let Node::Str(s) = &o[0].1 {
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

/// Encode a bc runtime [`Value`] into a native [`Node`] (native port of the retired external-JSON-crate
/// `encode_value`): ints round-trip as plain JSON numbers (the corpus is within the JS safe-integer
/// range, so the reference's decoded `$bigint`→number form matches).
pub fn encode_value(v: &Value) -> Node {
    match v {
        Value::Null => Node::Null,
        Value::Bool(b) => Node::Bool(*b),
        Value::Int(i) => Node::Int(*i),
        Value::Float(f) => Node::Float(*f),
        Value::Str(s) => Node::Str(s.clone()),
        Value::Arr(a) => Node::Array(a.iter().encode_all()),
        Value::Obj(o) => Node::Object(
            o.iter()
                .map(|(k, val)| (k.clone(), encode_value(val)))
                .collect(),
        ),
    }
}

trait EncodeAll {
    fn encode_all(self) -> Vec<Node>;
}
impl<'a, I: Iterator<Item = &'a Value>> EncodeAll for I {
    fn encode_all(self) -> Vec<Node> {
        self.map(encode_value).collect()
    }
}

// ── native JSON compaction (JS JSON.stringify form — no external JSON crate) ──────────────────────────────

/// Serialize a [`Value`] with no inter-token spaces (JS `JSON.stringify` form) — the IN-list single
/// param encoder. Native writer (no external JSON crate): `"`/`\`/control chars escape, unicode left raw,
/// a whole-valued float prints as an integer.
pub fn compact_value(v: &Value) -> String {
    let mut out = String::new();
    write_compact(v, &mut out);
    out
}

fn write_compact(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Value::Float(f) => {
            if f.fract() == 0.0 && f.is_finite() {
                let _ = write!(out, "{}", *f as i64);
            } else {
                let _ = write!(out, "{f}");
            }
        }
        Value::Str(s) => write_json_string(s, out),
        Value::Arr(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_compact(e, out);
            }
            out.push(']');
        }
        Value::Obj(pairs) => {
            out.push('{');
            for (i, (k, val)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_compact(val, out);
            }
            out.push('}');
        }
    }
}

/// Write a JSON string literal NATIVELY (JS JSON.stringify form): `"`/`\` + C0 control chars escape,
/// unicode left raw.
pub fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// ── NATIVE closed-set expression evaluator (replaces bc::evaluate_expression) ────────────────────

/// A native evaluation failure (byte-equal codes to bc's `ExprFailure` for the covered set).
#[derive(Debug, Clone)]
pub struct EvalError {
    pub code: &'static str,
    pub message: String,
}

impl EvalError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        EvalError {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

type EvalResult = Result<Value, EvalError>;

/// Evaluate a deferred expression [`Node`] against a flat scope → a runtime [`Value`]. Native port
/// of bc's `evaluate` for the closed operator set the litedbmodel corpora use. Fail-closed on an
/// out-of-set operator (`UNKNOWN_OP`) — never a silent default. Scalar leaves evaluate to themselves.
pub fn eval_expr(node: &Node, scope: &[(String, Value)]) -> EvalResult {
    match node {
        Node::Null => Ok(Value::Null),
        Node::Bool(b) => Ok(Value::Bool(*b)),
        // A bare integral literal must be within the JS safe range (|i| <= 2^53-1) — byte-identical to
        // bc's `evaluate` §2.3 classification. Beyond it, bc fails INVALID_LITERAL (an out-of-range
        // literal must be authored as the tagged `{int:"…"}` form). This applies ONLY to a bare int
        // LITERAL NODE in an expression; i64 values flowing through the param/scope materialization
        // path (a ref-resolved bound value, e.g. an i64::MAX id) never pass through here and are
        // unaffected — they bind exactly.
        Node::Int(i) => {
            const SAFE: i64 = 9_007_199_254_740_991; // 2^53 - 1
            if !(-SAFE..=SAFE).contains(i) {
                return Err(EvalError::new(
                    "INVALID_LITERAL",
                    format!("integral literal {i} exceeds safe range; use {{int:\"…\"}}"),
                ));
            }
            Ok(Value::Int(*i))
        }
        Node::Float(f) => Ok(Value::Float(*f)),
        Node::Str(s) => Ok(Value::Str(s.clone())),
        Node::Array(_) => Err(EvalError::new(
            "INVALID_NODE",
            "a bare array is not an expression node (use {arr:[…]})",
        )),
        Node::Object(pairs) => {
            if pairs.len() != 1 {
                // A `{"$bigint":"…"}` scalar tag can appear as a literal param; decode it directly.
                if pairs.len() == 1 {
                    // (unreachable — len checked) kept for clarity
                }
                // Object literals that are not single-op nodes are only the $bigint tag (len 1) or
                // a genuine `{obj:…}` (handled below). Anything else is an invalid expr node.
                return Err(EvalError::new(
                    "INVALID_NODE",
                    format!(
                        "expression object must have exactly one op key (got {})",
                        pairs.len()
                    ),
                ));
            }
            let (op, arg) = &pairs[0];
            eval_op(op, arg, scope)
        }
    }
}

fn eval_op(op: &str, arg: &Node, scope: &[(String, Value)]) -> EvalResult {
    match op {
        "$bigint" => match arg {
            Node::Str(s) => s
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| EvalError::new("INVALID_LITERAL", format!("$bigint '{s}' is not an i64"))),
            _ => Err(EvalError::new("INVALID_LITERAL", "$bigint expects a string")),
        },
        "ref" | "refOpt" => eval_ref(op, arg, scope),
        "coalesce" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "coalesce expects [a,b]"))?;
            if a.len() != 2 {
                return Err(EvalError::new("INVALID_NODE", "coalesce expects exactly 2 args"));
            }
            match eval_expr(&a[0], scope)? {
                Value::Null => eval_expr(&a[1], scope),
                other => Ok(other),
            }
        }
        "eq" | "ne" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "eq/ne expects [a,b]"))?;
            if a.len() != 2 {
                return Err(EvalError::new("INVALID_NODE", "eq/ne expects exactly 2 args"));
            }
            let x = eval_expr(&a[0], scope)?;
            let y = eval_expr(&a[1], scope)?;
            let equal = value_equals(&x, &y)?;
            Ok(Value::Bool(if op == "eq" { equal } else { !equal }))
        }
        "not" => {
            // Unary op: bc wraps the operand in a 1-element array (`{not:[expr]}`).
            let operand = unary_arg("not", arg)?;
            let a = require_bool(&eval_expr(operand, scope)?, "not")?;
            Ok(Value::Bool(!a))
        }
        "and" | "or" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "and/or expects [a,b]"))?;
            if a.len() != 2 {
                return Err(EvalError::new("INVALID_NODE", "and/or expects exactly 2 args"));
            }
            let left = require_bool(&eval_expr(&a[0], scope)?, op)?;
            if op == "and" && !left {
                return Ok(Value::Bool(false));
            }
            if op == "or" && left {
                return Ok(Value::Bool(true));
            }
            let right = require_bool(&eval_expr(&a[1], scope)?, op)?;
            Ok(Value::Bool(right))
        }
        "cond" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "cond expects [if,then,else]"))?;
            if a.len() != 3 {
                return Err(EvalError::new("INVALID_NODE", "cond expects exactly 3 args"));
            }
            let c = require_bool(&eval_expr(&a[0], scope)?, "cond")?;
            if c {
                eval_expr(&a[1], scope)
            } else {
                eval_expr(&a[2], scope)
            }
        }
        "concat" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "concat expects an array"))?;
            // n-ary, min 2 args — byte-identical to bc's `evaluate`: an arity < 2 is invalid IR
            // (expression-ir.md §2.1/§3/§6), NOT a lenient single/empty-string result.
            if a.len() < 2 {
                return Err(EvalError::new(
                    "INVALID_NODE",
                    format!("concat expects >= 2 args, got {}", a.len()),
                ));
            }
            let mut s = String::new();
            for part in a {
                match eval_expr(part, scope)? {
                    Value::Str(p) => s.push_str(&p),
                    other => {
                        return Err(EvalError::new(
                            "TYPE_MISMATCH",
                            format!("concat: string parts only (got {})", type_name(&other)),
                        ))
                    }
                }
            }
            Ok(Value::Str(s))
        }
        "arr" => {
            let a = arg
                .as_array()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "arr expects an array of element exprs"))?;
            let mut out = Vec::with_capacity(a.len());
            for e in a {
                out.push(eval_expr(e, scope)?);
            }
            Ok(Value::Arr(out))
        }
        "obj" => {
            let o = arg
                .as_object()
                .ok_or_else(|| EvalError::new("INVALID_NODE", "obj expects an object of field exprs"))?;
            let mut out = Vec::with_capacity(o.len());
            for (k, v) in o {
                if k == "__proto__" {
                    return Err(EvalError::new("FORBIDDEN_KEY", "__proto__ is a forbidden object key"));
                }
                out.push((k.clone(), eval_expr(v, scope)?));
            }
            Ok(Value::Obj(out))
        }
        other => Err(EvalError::new(
            "UNKNOWN_OP",
            format!("unknown expression operator '{other}' (native evaluator covers the closed litedbmodel set)"),
        )),
    }
}

/// Native port of bc's `eval_ref` (ref / refOpt): walk the scope path. `refOpt` short-circuits to
/// null on a null intermediate; `ref` fails NULL_REF. A missing head is UNKNOWN_BINDING for BOTH
/// (the runtime normalizes optional heads to present-as-null before evaluation, so a covered
/// `refOpt` head is always present as null, never missing).
fn eval_ref(op: &str, arg: &Node, scope: &[(String, Value)]) -> EvalResult {
    let path = arg
        .as_array()
        .ok_or_else(|| EvalError::new("INVALID_NODE", format!("{op} expects a path array")))?;
    if path.is_empty() || !path.iter().all(|p| matches!(p, Node::Str(_))) {
        return Err(EvalError::new(
            "INVALID_NODE",
            format!("{op} expects a non-empty string path"),
        ));
    }
    let head = path[0].as_str().unwrap();
    let mut cur: Value = match scope.iter().find(|(k, _)| k == head) {
        Some((_, v)) => v.clone(),
        None => {
            return Err(EvalError::new(
                "UNKNOWN_BINDING",
                format!("unknown binding: {head}"),
            ))
        }
    };
    for seg_node in &path[1..] {
        let seg = seg_node.as_str().unwrap();
        cur = match cur {
            Value::Null => {
                if op == "refOpt" {
                    return Ok(Value::Null);
                }
                return Err(EvalError::new(
                    "NULL_REF",
                    format!("null intermediate at .{seg} (use ?.)"),
                ));
            }
            Value::Obj(pairs) => match pairs.iter().find(|(k, _)| k == seg) {
                Some((_, v)) => v.clone(),
                None => {
                    return Err(EvalError::new(
                        "MISSING_PROP",
                        format!("missing property .{seg}"),
                    ))
                }
            },
            other => {
                return Err(EvalError::new(
                    "TYPE_MISMATCH",
                    format!("cannot access .{seg} on {}", type_name(&other)),
                ))
            }
        };
    }
    Ok(cur)
}

/// Native port of bc's `value_equals`: null-or-same-scalar equality (obj/arr equality is undefined).
fn value_equals(a: &Value, b: &Value) -> Result<bool, EvalError> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(matches!(a, Value::Null) && matches!(b, Value::Null));
    }
    let ta = type_name(a);
    let tb = type_name(b);
    if ta != tb {
        return Err(EvalError::new(
            "TYPE_MISMATCH",
            format!("eq/ne: same type only (got {ta}×{tb})"),
        ));
    }
    if ta == "arr" || ta == "obj" {
        return Err(EvalError::new(
            "TYPE_MISMATCH",
            "eq/ne: obj/arr equality is undefined in v1",
        ));
    }
    Ok(deep_equals(a, b))
}

/// The single operand of a unary op (`{op:[expr]}`) — bc's `arg_unary` (a 1-element args array).
fn unary_arg<'a>(op: &str, arg: &'a Node) -> Result<&'a Node, EvalError> {
    let a = arg
        .as_array()
        .ok_or_else(|| EvalError::new("INVALID_NODE", format!("{op} expects an args array")))?;
    if a.len() != 1 {
        return Err(EvalError::new(
            "INVALID_NODE",
            format!("{op} expects 1 arg"),
        ));
    }
    Ok(&a[0])
}

fn require_bool(v: &Value, ctx: &str) -> Result<bool, EvalError> {
    match v {
        Value::Bool(b) => Ok(*b),
        other => Err(EvalError::new(
            "TYPE_MISMATCH",
            format!(
                "{ctx}: bool expected, got {} (no truthiness)",
                type_name(other)
            ),
        )),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "string",
        Value::Arr(_) => "arr",
        Value::Obj(_) => "obj",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── guard 1: bare int LITERAL safe-range (superset-safe match to bc's evaluate §2.3) ──

    #[test]
    fn int_literal_within_safe_range_ok() {
        // 2^53-1 is the boundary — still valid.
        let n = Node::Int(9_007_199_254_740_991);
        assert!(matches!(
            eval_expr(&n, &[]),
            Ok(Value::Int(9_007_199_254_740_991))
        ));
    }

    #[test]
    fn int_literal_beyond_safe_range_fails_invalid_literal() {
        // 2^53 exceeds the safe range → INVALID_LITERAL (bc's exact code + message form).
        let n = Node::Int(9_007_199_254_740_992);
        match eval_expr(&n, &[]) {
            Err(e) => {
                assert_eq!(e.code, "INVALID_LITERAL");
                assert!(
                    e.message.contains("exceeds safe range"),
                    "msg: {}",
                    e.message
                );
            }
            Ok(v) => panic!("expected INVALID_LITERAL, got Ok({v:?})"),
        }
        // i64::MAX (a far-out-of-range literal) also fails closed.
        assert_eq!(
            eval_expr(&Node::Int(i64::MAX), &[]).unwrap_err().code,
            "INVALID_LITERAL"
        );
    }

    #[test]
    fn i64_param_value_via_ref_roundtrips_exact() {
        // CRITICAL: the literal safe-range guard must NOT touch the param/value materialization path.
        // An i64::MAX bound value resolved through a `{ref:[…]}` flows as a Value (not a literal node)
        // and MUST round-trip EXACTLY — the coverage i64 bigint params depend on this.
        let scope = vec![("big".to_string(), Value::Int(i64::MAX))];
        let expr = Node::Object(vec![(
            "ref".to_string(),
            Node::Array(vec![Node::Str("big".to_string())]),
        )]);
        assert!(matches!(eval_expr(&expr, &scope), Ok(Value::Int(i)) if i == i64::MAX));
        // A beyond-safe-range i64 in scope also round-trips (it is a VALUE, not a literal).
        let scope2 = vec![("v".to_string(), Value::Int(9_007_199_254_740_992))];
        let expr2 = Node::Object(vec![(
            "ref".to_string(),
            Node::Array(vec![Node::Str("v".to_string())]),
        )]);
        assert!(matches!(
            eval_expr(&expr2, &scope2),
            Ok(Value::Int(9_007_199_254_740_992))
        ));
    }

    // ── guard 2: concat arity (superset-safe match — bc requires >= 2 args) ──

    #[test]
    fn concat_arity_below_two_fails_invalid_node() {
        let s = vec![("s".to_string(), Value::Str("x".to_string()))];
        let one = Node::Object(vec![(
            "concat".to_string(),
            Node::Array(vec![Node::Str("only".to_string())]),
        )]);
        match eval_expr(&one, &s) {
            Err(e) => assert_eq!(e.code, "INVALID_NODE"),
            Ok(v) => panic!("expected INVALID_NODE for 1-arg concat, got Ok({v:?})"),
        }
        let empty = Node::Object(vec![("concat".to_string(), Node::Array(vec![]))]);
        assert_eq!(eval_expr(&empty, &s).unwrap_err().code, "INVALID_NODE");
    }

    #[test]
    fn concat_two_or_more_args_ok() {
        let two = Node::Object(vec![(
            "concat".to_string(),
            Node::Array(vec![Node::Str("a".to_string()), Node::Str("b".to_string())]),
        )]);
        assert!(matches!(eval_expr(&two, &[]), Ok(Value::Str(ref s)) if s == "ab"));
    }
}
