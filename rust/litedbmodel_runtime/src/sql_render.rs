//! Native SQL placeholder and deferred PostgreSQL array-cast rendering.

use behavior_contracts::Value;

#[doc(hidden)]
pub const PG_ARRAY_CAST_TOKEN: &str = "@@PG_ARRAY_CAST@@";

pub fn render_placeholders(sql: &str, dialect_name: &str) -> String {
    if dialect_name != "postgres" {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    let mut in_string = false;
    for ch in sql.chars() {
        if in_string {
            out.push(ch);
            if ch == '\'' {
                in_string = false;
            }
        } else if ch == '\'' {
            out.push(ch);
            in_string = true;
        } else if ch == '?' {
            index += 1;
            out.push('$');
            out.push_str(&index.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

fn infer_pg_array_type(values: &[Value]) -> &'static str {
    match values.first() {
        None => "text[]",
        Some(Value::Bool(_)) => "boolean[]",
        Some(Value::Int(_)) => "int[]",
        Some(Value::Float(_)) => {
            if values
                .iter()
                .all(|v| matches!(v, Value::Float(f) if f.fract() == 0.0))
            {
                "int[]"
            } else {
                "numeric[]"
            }
        }
        _ => "text[]",
    }
}

#[doc(hidden)]
pub fn resolve_pg_array_cast(sql: &str, values: &[Value]) -> String {
    match sql.find(PG_ARRAY_CAST_TOKEN) {
        None => sql.to_string(),
        Some(at) => format!(
            "{}{}{}",
            &sql[..at],
            infer_pg_array_type(values),
            &sql[at + PG_ARRAY_CAST_TOKEN.len()..]
        ),
    }
}
