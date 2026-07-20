//! Interpreter-only adapter from published relation metadata to the native relation batch core.

use std::collections::HashMap;

use behavior_contracts::Value;

use crate::codegen_exec::ArrayParamShape;
use crate::driver::Driver;
use crate::errors::{RuntimeError, SqlFailure};
use crate::node::Node;
use crate::runtime::execute_bundle;
use litedbmodel_runtime::relation::{
    execute_relation_batch, group_children, matched_children, RelationBatch,
};

struct RelationOp {
    name: String,
    kind: String,
    parent_keys: Vec<String>,
    target_keys: Vec<String>,
    key_shape: ArrayParamShape,
    connection: Option<String>,
    sql: String,
    target_table: Option<String>,
    hard_limit: Option<i64>,
}

fn string(node: &Node, key: &str) -> String {
    node.get(key)
        .and_then(Node::as_str)
        .unwrap_or("")
        .to_string()
}

fn keys(node: &Node, many: &str, one: &str) -> Vec<String> {
    node.get(many)
        .and_then(Node::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Node::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| vec![string(node, one)])
}

fn op_from_node(node: &Node) -> RelationOp {
    RelationOp {
        name: string(node, "name"),
        kind: string(node, "kind"),
        parent_keys: keys(node, "parentKeys", "parentKey"),
        target_keys: keys(node, "targetKeys", "targetKey"),
        key_shape: match node.get("keyShape").and_then(Node::as_str) {
            Some("per_column") => ArrayParamShape::PerColumn,
            _ => ArrayParamShape::SingleJson,
        },
        connection: node
            .get("connection")
            .and_then(Node::as_str)
            .map(str::to_string),
        sql: string(node, "sql"),
        target_table: node
            .get("targetTable")
            .and_then(Node::as_str)
            .map(str::to_string),
        hard_limit: node.get("hardLimit").and_then(Node::as_i64),
    }
}

fn field<'a>(row: &'a Value, key: &str) -> Option<&'a Value> {
    match row {
        Value::Obj(fields) => fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value),
        _ => None,
    }
}

fn tuples(rows: &[Value], keys: &[String]) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            keys.iter()
                .map(|key| field(row, key).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

fn fetch(
    op: &RelationOp,
    parents: &[Vec<Value>],
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let columns = (0..op.parent_keys.len())
        .map(|column| parents.iter().map(|row| row[column].clone()).collect())
        .collect::<Vec<Vec<Value>>>();
    execute_relation_batch(
        &crate::exec_context::for_driver(driver),
        &op.sql,
        &columns,
        op.key_shape,
        op.hard_limit,
        op.target_table.as_deref(),
        &op.name,
    )
}

fn grouped(op: &RelationOp, rows: Vec<Value>) -> RelationBatch<Value> {
    group_children(rows, |row| {
        op.target_keys
            .iter()
            .map(|key| field(row, key).cloned().unwrap_or(Value::Null))
            .collect()
    })
}

fn distributed(op: &RelationOp, tuple: &[Value], batch: &RelationBatch<Value>) -> Value {
    let rows = matched_children(tuple, batch);
    if op.kind == "hasMany" {
        Value::Arr(rows)
    } else {
        rows.into_iter().next().unwrap_or(Value::Null)
    }
}

pub fn stitch_relation(
    descriptor: &Node,
    mut parents: Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let op = op_from_node(descriptor);
    let parent_tuples = tuples(&parents, &op.parent_keys);
    let batch = grouped(&op, fetch(&op, &parent_tuples, driver)?);
    for (parent, tuple) in parents.iter_mut().zip(parent_tuples.iter()) {
        if let Value::Obj(fields) = parent {
            fields.push((op.name.clone(), distributed(&op, tuple, &batch)));
        }
    }
    Ok(parents)
}

pub fn stitch_relation_tree(
    descriptor: &Node,
    parents: Vec<Value>,
    driver: &dyn Driver,
) -> Result<Vec<Value>, RuntimeError> {
    let name = string(descriptor, "name");
    let mut stitched = stitch_relation(descriptor, parents, driver)?;
    let mut children = Vec::new();
    for parent in &stitched {
        if let Some(Value::Arr(rows)) = field(parent, &name) {
            children.extend(rows.iter().cloned());
        }
    }
    if let Some(nested) = descriptor.get("childRelations").and_then(Node::as_array) {
        for child in nested {
            children = stitch_relation_tree(child, children, driver)?;
        }
        let mut hydrated = children.into_iter();
        for parent in &mut stitched {
            if let Some(Value::Arr(rows)) = match parent {
                Value::Obj(fields) => fields
                    .iter_mut()
                    .find(|(field, _)| field == &name)
                    .map(|(_, value)| value),
                _ => None,
            } {
                for row in rows {
                    *row = hydrated.next().expect("relation tree cardinality drift");
                }
            }
        }
    }
    Ok(stitched)
}

fn driver_for<'a>(
    op: &RelationOp,
    primary: &'a (dyn Driver + Sync),
    connections: &'a HashMap<String, &'a (dyn Driver + Sync)>,
) -> Result<&'a (dyn Driver + Sync), SqlFailure> {
    match &op.connection {
        None => Ok(primary),
        Some(name) => connections.get(name).copied().ok_or_else(|| SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!(
                "cross-DB relation '{}': connection '{}' is not registered",
                op.name, name
            ),
        }),
    }
}

pub fn read_bundle_pooled(
    bundle: &Node,
    input: &Node,
    driver: &(dyn Driver + Sync),
    with_names: &[String],
    connections: &HashMap<String, &(dyn Driver + Sync)>,
) -> Result<Value, RuntimeError> {
    let mut rows = match execute_bundle(bundle, input, driver)? {
        Value::Arr(rows) => rows,
        _ => {
            return Err(RuntimeError::Sql(SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: "read behavior output is not a row list".into(),
            }))
        }
    };
    for name in with_names {
        let descriptor = bundle
            .get("relations")
            .and_then(|relations| relations.get(name))
            .ok_or_else(|| SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: format!("relation '{name}' is not declared"),
            })?;
        let op = op_from_node(descriptor);
        let relation_driver = driver_for(&op, driver, connections)?;
        rows = stitch_relation_tree(descriptor, rows, relation_driver)?;
    }
    Ok(Value::Arr(rows))
}

/// Single-driver interpreter oracle entry. Kept separate from the pooled cross-DB adapter so native
/// SQLite consumers do not acquire a `Sync` requirement merely to run conformance.
pub fn read_bundle(
    bundle: &Node,
    input: &Node,
    driver: &dyn Driver,
    with_names: &[String],
) -> Result<Value, RuntimeError> {
    let mut rows = match execute_bundle(bundle, input, driver)? {
        Value::Arr(rows) => rows,
        _ => {
            return Err(RuntimeError::Sql(SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: "read behavior output is not a row list".into(),
            }))
        }
    };
    for name in with_names {
        let descriptor = bundle
            .get("relations")
            .and_then(|relations| relations.get(name))
            .ok_or_else(|| SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: format!("relation '{name}' is not declared"),
            })?;
        rows = stitch_relation_tree(descriptor, rows, driver)?;
    }
    Ok(Value::Arr(rows))
}
