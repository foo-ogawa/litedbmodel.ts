//! Native database-parameter encoding for runtime values. This module has no parser or IR evaluator.

use std::fmt::Write as _;

use behavior_contracts::Value;

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
        Value::Float(f) if f.fract() == 0.0 && f.is_finite() => {
            let _ = write!(out, "{}", *f as i64);
        }
        Value::Float(f) => {
            let _ = write!(out, "{f}");
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

pub fn array_param_json(elems: &[Value], mysql_bool: bool) -> String {
    if !mysql_bool {
        return compact_value(&Value::Arr(elems.to_vec()));
    }
    let mapped = elems
        .iter()
        .map(|e| match e {
            Value::Bool(b) => Value::Int(i64::from(*b)),
            other => other.clone(),
        })
        .collect();
    compact_value(&Value::Arr(mapped))
}

pub(crate) fn write_json_string(s: &str, out: &mut String) {
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
