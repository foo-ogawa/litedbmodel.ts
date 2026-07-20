//! Native relation batch primitives. This module contains no relation descriptor parser or tree walker.

use std::collections::{HashMap, HashSet};

use behavior_contracts::Value;

use crate::codegen_exec::{wp, ArrayParamShape, ToWireParam};
use crate::errors::{LimitExceededError, SqlFailure, LIMIT_CONTEXT_RELATION};
use crate::exec_context::{self, ExecutionContext, StatementIntent};
use crate::static_bundle::{render_placeholders, resolve_pg_array_cast};

pub(crate) fn key_identity(values: &[Value]) -> String {
    values
        .iter()
        .map(stringify_key)
        .collect::<Vec<_>>()
        .join(" ")
}

fn stringify_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) if value.fract() == 0.0 && value.is_finite() => {
            (*value as i64).to_string()
        }
        Value::Float(value) => value.to_string(),
        Value::Str(value) => value.clone(),
        other => format!("{other:?}"),
    }
}

pub(crate) fn dedupe_tuples(tuples: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let mut seen = HashSet::new();
    tuples
        .iter()
        .filter(|tuple| !tuple.iter().any(|value| matches!(value, Value::Null)))
        .filter(|tuple| seen.insert(key_identity(tuple)))
        .cloned()
        .collect()
}

fn tuple_param(tuples: &[Vec<Value>]) -> Value {
    Value::Str(crate::node::compact_value(&Value::Arr(
        tuples.iter().cloned().map(Value::Arr).collect(),
    )))
}

pub fn build_relation_params(
    sql: &str,
    columns: &[Vec<Value>],
    shape: ArrayParamShape,
) -> (String, Vec<Value>) {
    let rows = (0..columns.first().map_or(0, Vec::len))
        .map(|row| columns.iter().map(|column| column[row].clone()).collect())
        .collect::<Vec<Vec<Value>>>();
    let rows = dedupe_tuples(&rows);
    let columns = (0..columns.len())
        .map(|column| rows.iter().map(|row| row[column].clone()).collect())
        .collect::<Vec<Vec<Value>>>();
    let sql = columns.iter().fold(sql.to_string(), |rendered, column| {
        resolve_pg_array_cast(&rendered, column)
    });
    let params = match (columns.len(), shape) {
        (1, _) => vec![Value::Arr(columns[0].clone())],
        (_, ArrayParamShape::PerColumn) => columns.into_iter().map(Value::Arr).collect(),
        _ => vec![tuple_param(&rows)],
    };
    (sql, params)
}

/// Single relation batch semantic authority: empty short-circuit, dedupe, cast, bind, exec and cap.
pub fn execute_relation_batch(
    ctx: &ExecutionContext,
    sql: &str,
    columns: &[Vec<Value>],
    shape: ArrayParamShape,
    hard_limit: Option<i64>,
    target_table: Option<&str>,
    relation: &str,
) -> Result<Vec<Value>, SqlFailure> {
    if columns.first().is_none_or(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let (sql, params) = build_relation_params(sql, columns, shape);
    let sql = render_placeholders(&sql, ctx.driver().dialect());
    let rows = exec_context::execute(ctx, &sql, &params, &StatementIntent::read())?;
    if let Some(limit) = hard_limit {
        LimitExceededError::check(
            limit,
            rows.len() as i64,
            LIMIT_CONTEXT_RELATION,
            target_table.map(str::to_string),
            Some(relation.to_string()),
        )
        .map_err(|error| SqlFailure {
            kind: "limit_exceeded".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: error.to_string(),
        })?;
    }
    Ok(rows)
}

pub(crate) type RelationBatch<C> = HashMap<String, Vec<C>>;

pub(crate) fn group_children<C>(
    rows: Vec<C>,
    target_key_of: impl Fn(&C) -> Vec<Value>,
) -> RelationBatch<C> {
    let mut batch = HashMap::new();
    for row in rows {
        batch
            .entry(key_identity(&target_key_of(&row)))
            .or_insert_with(Vec::new)
            .push(row);
    }
    batch
}

pub(crate) fn matched_children<C: Clone>(tuple: &[Value], batch: &RelationBatch<C>) -> Vec<C> {
    if tuple.iter().any(|value| matches!(value, Value::Null)) {
        return Vec::new();
    }
    batch.get(&key_identity(tuple)).cloned().unwrap_or_default()
}

pub fn hydrate_children<P, K: IntoKeyTuple, C: Clone>(
    parents: Vec<P>,
    key_of: impl Fn(&P) -> K,
    children: Vec<C>,
    child_key_of: impl Fn(&C) -> Vec<Value>,
) -> Vec<(P, Vec<C>)> {
    let tuples = parents
        .iter()
        .map(|parent| key_of(parent).into_key_tuple())
        .collect::<Vec<_>>();
    let batch = group_children(children, child_key_of);
    parents
        .into_iter()
        .zip(tuples.iter())
        .map(|(parent, tuple)| (parent, matched_children(tuple, &batch)))
        .collect()
}

pub trait IntoKeyTuple {
    fn into_key_tuple(self) -> Vec<Value>;
}

impl<T: ToWireParam> IntoKeyTuple for T {
    fn into_key_tuple(self) -> Vec<Value> {
        vec![wp(&self)]
    }
}

impl<A: ToWireParam, B: ToWireParam> IntoKeyTuple for (A, B) {
    fn into_key_tuple(self) -> Vec<Value> {
        vec![wp(&self.0), wp(&self.1)]
    }
}
