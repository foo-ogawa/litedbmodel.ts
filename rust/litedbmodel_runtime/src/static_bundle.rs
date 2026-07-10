//! litedbmodel v2 SCP — the STATIC, PORTABLE makeSQL bundle RUNTIME (Rust port, epic #43/#45).
//!
//! Byte-for-byte port of the TS `src/scp/makesql/static-bundle.ts` + `makesql.ts` + `handler.ts`
//! runtime halves — the SOLE makeSQL read/render path — mirroring the audited Python/Go/PHP sibling
//! ports. It consumes the PRE-COMPILED, portable artifacts the corpus ships (a read `ReadGraph` = a
//! bc `ComponentGraphIR` of `__makeSqlNode` surrogate nodes + per-node STATIC statement templates),
//! and EXECUTES them via the shared behavior-contracts Rust crate ([`run_behavior`] drives
//! map / Φ-merge / wiring; [`evaluate_expression`] resolves the deferred value-specs + skip). This
//! module re-implements NO generic evaluator and does NO SQL re-derivation — every statement's
//! `sql` is fixed text; the runtime only evaluates its deferred params + skip, resolves the WHERE
//! connector from the present set, assembles + renders placeholders, and binds.
//!
//! A statement template (StaticStatement) is `{sql, params, skip?, whereFragment?}`:
//! - `sql` — complete tuned dialect text (`?` placeholders), value-independent.
//! - `params` — deferred value-specs = closed-set bc Expression IR, 1:1 with the top `?`.
//! - `skip` — optional bc presence expression; truthy ⇒ the whole statement drops.
//! - `whereFragment` — a bare predicate body; the runtime prepends ` WHERE `/` AND ` from the
//!   present set (a skipped earlier fragment never leaves a dangling connector).
//!
//! An IN-list value-spec is the marker `{"__jsonArray": <spec>, "dialect": <d>}`: postgres binds the
//! array as-is (a text[] param); mysql/sqlite JSON-encode it to a single param (server-side
//! expansion). This mirrors the TS `evalSpec`.

use behavior_contracts::{evaluate_expression, run_behavior, ComponentExec, ExecOutcome, Value};
use serde_json::Value as J;

use crate::driver::Driver;
use crate::errors::{re_error_to_sql_failure, SqlFailure};
use crate::value::{encode_value, Scope};

/// The synthetic port that carries a SQL node's render scope (mirrors TS SCOPE_PORT).
pub const SCOPE_PORT: &str = "__scope";
/// The makeSQL catalog leaf name every rewritten SQL node references (mirrors TS NODE_COMPONENT).
pub const NODE_COMPONENT: &str = "__makeSqlNode";

/// The result of rendering: final SQL text + flat params (1:1 with `?`/`$N`).
pub struct RenderedSql {
    pub sql: String,
    pub params: Vec<Value>,
}

// ── makeSQL assembly (port of makesql.ts assembleMakeSQL / composeMakeSQL) ─────

/// A concrete makeSQL after value evaluation: fixed sql text + a flat value list.
struct MakeSqlNode {
    sql: String,
    params: Vec<Value>,
}

/// Split `sql` on `?` and interleave each concrete param (mirrors TS assembleMakeSQL). Our concrete
/// runtime nodes carry only bound values (nested-makeSQL splicing is compile-time in the corpus
/// text), so this is the value-fill flatten with a placeholder/param arity check.
fn assemble_make_sql(node: &MakeSqlNode) -> Result<(String, Vec<Value>), String> {
    let chunks: Vec<&str> = node.sql.split('?').collect();
    if chunks.len() - 1 != node.params.len() {
        return Err(format!(
            "makeSQL placeholder/param mismatch: {} '?' vs {} params in {:?}",
            chunks.len() - 1,
            node.params.len(),
            node.sql
        ));
    }
    let mut sql = String::from(chunks[0]);
    let mut params = Vec::with_capacity(node.params.len());
    for (i, p) in node.params.iter().enumerate() {
        sql.push('?');
        sql.push_str(chunks[i + 1]);
        params.push(p.clone());
    }
    Ok((sql, params))
}

/// Concatenate the assembled sql + params of every present node (mirrors TS composeMakeSQL).
fn compose_make_sql(nodes: &[MakeSqlNode]) -> Result<(String, Vec<Value>), String> {
    let mut sql = String::new();
    let mut params: Vec<Value> = Vec::new();
    for n in nodes {
        let (s, p) = assemble_make_sql(n)?;
        sql.push_str(&s);
        params.extend(p);
    }
    Ok((sql, params))
}

// ── Dialect placeholder render (port of handler.ts renderPlaceholders) ─────────

/// Rewrite `?` → the dialect placeholder form: PG `$N` (quote-aware), MySQL/SQLite keep `?`.
/// Byte-for-byte port of the TS renderPlaceholders: a `?` inside a single-quoted string literal is
/// NOT a placeholder.
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

// ── Deferred value-spec evaluation (port of static-bundle.ts evalSpec) ─────────

/// Serialize JSON with no inter-token spaces (matches JS `JSON.stringify`).
fn compact_json(v: &J) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
}

/// Evaluate one deferred value-spec against the scope, handling the `__jsonArray` marker: postgres
/// keeps the array as-is (a text[] param); mysql/sqlite JSON-encode it to ONE string param.
/// Everything else is a plain bc Expression IR value. The target dialect for an IN-list rides the
/// marker's own `dialect` field (compiled TS-side), so no ambient dialect is threaded here.
fn eval_spec(spec: &J, scope: &Scope) -> Result<Value, String> {
    if let Some(inner) = spec.get("__jsonArray") {
        let arr_v = evaluate_expression(inner, scope).map_err(|e| e.message)?;
        let arr = match arr_v {
            Value::Arr(a) => a,
            _ => {
                return Err("static-bundle: IN-list value-spec did not evaluate to an array".into())
            }
        };
        let spec_dialect = spec.get("dialect").and_then(|d| d.as_str()).unwrap_or("");
        if spec_dialect == "postgres" {
            return Ok(Value::Arr(arr)); // bound as ONE text[] param
        }
        // Single JSON param — compact form matching the TS JSON.stringify byte shape.
        let encoded = J::Array(arr.iter().map(encode_value).collect());
        return Ok(Value::Str(compact_json(&encoded)));
    }
    evaluate_expression(spec, scope).map_err(|e| e.message)
}

// ── Statement-list render (port of static-bundle.ts renderStatements) ──────────

/// Evaluate a list of static statement templates against a scope → final SQL + params. Byte-for-byte
/// port of the TS renderStatements: drop skipped statements (skip truthy), resolve each WHERE-
/// fragment's ` WHERE `/` AND ` connector from the present set, compose + render placeholders.
pub fn render_statements(
    statements: &[J],
    dialect_name: &str,
    scope: &Scope,
) -> Result<RenderedSql, String> {
    let mut nodes: Vec<MakeSqlNode> = Vec::new();
    let mut where_seen = false;
    for stmt in statements {
        if let Some(skip) = stmt.get("skip") {
            if !skip.is_null() {
                let drop = evaluate_expression(skip, scope).map_err(|e| e.message)?;
                if !matches!(drop, Value::Null | Value::Bool(false)) {
                    continue; // truthy ⇒ drop the whole statement
                }
            }
        }
        let mut sql_text = stmt
            .get("sql")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        if stmt.get("whereFragment") == Some(&J::Bool(true)) {
            sql_text = if where_seen {
                format!(" AND {sql_text}")
            } else {
                format!(" WHERE {sql_text}")
            };
            where_seen = true;
        }
        let mut params: Vec<Value> = Vec::new();
        if let Some(specs) = stmt.get("params").and_then(|p| p.as_array()) {
            for spec in specs {
                params.push(eval_spec(spec, scope)?);
            }
        }
        nodes.push(MakeSqlNode {
            sql: sql_text,
            params,
        });
    }
    let (sql, params) = compose_make_sql(&nodes)?;
    Ok(RenderedSql {
        sql: render_placeholders(&sql, dialect_name),
        params,
    })
}

// ── Input normalization (SSoT-driven — mirrors TS normalizeInput) ─────────────

/// The surrogate IR's first component (`components[0]`).
fn primary_component(graph: &J) -> Option<&J> {
    graph
        .get("ir")
        .and_then(|ir| ir.get("components"))
        .and_then(|c| c.as_array())
        .and_then(|c| c.first())
}

/// Normalize omitted OPTIONAL heads to present-as-null (absent-key SKIP). Optional = the read
/// graph's component schema-optional ports OR the graph's `optionalHeads` (SKIP-guarded / refOpt).
fn normalize_read_graph_input(graph: &J, input: &Scope) -> Scope {
    let mut out = input.clone();
    let present = |scope: &Scope, k: &str| scope.iter().any(|(sk, _)| sk == k);
    if let Some(comp) = primary_component(graph) {
        if let Some(ports) = comp.get("inputPorts").and_then(|p| p.as_object()) {
            for (port, schema) in ports {
                let required = schema.get("required") == Some(&J::Bool(true));
                if !required && !present(&out, port) {
                    out.push((port.clone(), Value::Null));
                }
            }
        }
    }
    if let Some(heads) = graph.get("optionalHeads").and_then(|h| h.as_array()) {
        for head in heads.iter().filter_map(|h| h.as_str()) {
            if !present(&out, head) {
                out.push((head.to_string(), Value::Null));
            }
        }
    }
    out
}

// ── ReadGraph render axis (port of static-bundle.ts renderReadPrimary) ─────────

/// The first body node id that has compiled statements (the SELECT the relations map over).
fn primary_node_id(graph: &J) -> Result<String, String> {
    let comp = primary_component(graph)
        .ok_or_else(|| "static-bundle: read graph has no component".to_string())?;
    let by_id = graph
        .get("statementsById")
        .and_then(|s| s.as_object())
        .ok_or_else(|| "static-bundle: read graph has no statementsById".to_string())?;
    let body = comp
        .get("body")
        .and_then(|b| b.as_array())
        .ok_or_else(|| "static-bundle: read graph component has no body".to_string())?;
    for n in body {
        if let Some(id) = n.get("id").and_then(|i| i.as_str()) {
            if by_id.contains_key(id) {
                return Ok(id.to_string());
            }
        }
    }
    Err("static-bundle: read graph has no primary node to render".into())
}

/// The static statement templates for a node id (a JSON array).
fn statements_for(graph: &J, node_id: &str) -> Vec<J> {
    graph
        .get("statementsById")
        .and_then(|s| s.get(node_id))
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default()
}

/// Render the PRIMARY read node's statements of a ReadGraph → dialect SQL + params (the render axis
/// for conformance golden). The primary node is the first body node in the surrogate IR order (map
/// nodes reference it). Optional heads are normalized to present-as-null first.
pub fn render_read_primary(graph: &J, input: &Scope) -> Result<RenderedSql, String> {
    let primary_id = primary_node_id(graph)?;
    let scope = normalize_read_graph_input(graph, input);
    let dialect_name = graph.get("dialect").and_then(|d| d.as_str()).unwrap_or("");
    render_statements(&statements_for(graph, &primary_id), dialect_name, &scope)
}

// ── ReadGraph execution (port of static-bundle.ts executeReadGraph) ────────────

/// Convert a rendered param to a driver-bindable value (mirrors TS `toDriverParam`).
///
/// bc evaluates integers to a plain i64, so no bigint narrowing is needed; a bool is left as-is
/// (bound as 0/1 by the driver). An object [`Value::Obj`] is an emit payload (`{obj:…}`) serialized
/// to compact JSON for a text column — byte-identical to the TS `JSON.stringify` (no spaces).
pub(crate) fn to_driver_param(v: &Value) -> Value {
    match v {
        Value::Obj(_) => Value::Str(compact_json(&encode_value(v))),
        other => other.clone(),
    }
}

/// The makeSQL handler registry: one render→execute handler behind the `__makeSqlNode` catalog
/// leaf; the per-node static statements are keyed by nodeId.
struct ReadGraphHandlers<'a> {
    driver: &'a dyn Driver,
    graph: &'a J,
    dialect: String,
}

/// Render one read node's static statements against `scope` and execute them on `driver`.
///
/// The ONE render→execute step behind a `__makeSqlNode`. Shared by the serial handler
/// ([`ReadGraphHandlers`]) and the parallel dispatch path ([`dispatch_read_nodes_parallel`]) so both
/// produce byte-identical SQL + params — the parallel path only changes WHICH thread runs this, not
/// WHAT it runs. Takes `driver` by `&dyn Driver` so a `Sync` pooled driver can service concurrent
/// calls (each acquiring its own pooled connection).
fn render_and_execute_node(
    driver: &dyn Driver,
    graph: &J,
    dialect: &str,
    node_id: &str,
    scope: &Scope,
) -> ExecOutcome {
    if graph
        .get("statementsById")
        .and_then(|s| s.get(node_id))
        .is_none()
    {
        return ExecOutcome::Error(format!("static-bundle: no statements for node '{node_id}'"));
    }
    let stmts = statements_for(graph, node_id);
    let rendered = match render_statements(&stmts, dialect, scope) {
        Ok(r) => r,
        Err(e) => return ExecOutcome::Error(e),
    };
    let params: Vec<Value> = rendered.params.iter().map(to_driver_param).collect();
    let mut stmt = driver.prepare(&rendered.sql);
    match stmt.all(&params) {
        Ok(rows) => ExecOutcome::Ok(Value::Arr(rows)),
        Err(e) => ExecOutcome::Error(e.message),
    }
}

impl ReadGraphHandlers<'_> {
    fn render_and_execute(&self, node_id: &str, scope: &Scope) -> ExecOutcome {
        render_and_execute_node(self.driver, self.graph, &self.dialect, node_id, scope)
    }
}

impl ComponentExec for ReadGraphHandlers<'_> {
    fn exec(
        &mut self,
        _component: &str,
        _ports: &[(String, Value)],
        _bound: Option<&Value>,
    ) -> Option<ExecOutcome> {
        // run_behavior always dispatches through exec_ctx (we need the node id → statements).
        Some(ExecOutcome::Error(
            "static-bundle: makeSQL handler requires the node identity (exec_ctx)".into(),
        ))
    }

    fn exec_ctx(
        &mut self,
        node_id: &str,
        _component: &str,
        ports: &[(String, Value)],
        _bound: Option<&Value>,
    ) -> Option<ExecOutcome> {
        let scope = match ports.iter().find(|(k, _)| k == SCOPE_PORT).map(|(_, v)| v) {
            Some(Value::Obj(pairs)) => pairs.clone(),
            _ => {
                return Some(ExecOutcome::Error(format!(
                    "static-bundle: node '{node_id}' surrogate scope did not evaluate to an object"
                )))
            }
        };
        Some(self.render_and_execute(node_id, &scope))
    }
}

/// Execute a compiled ReadGraph via bc `run_behavior` + a makeSQL handler: bc drives map iteration /
/// wire binding / Φ output; the handler renders each node's static statements against the evaluated
/// `__scope` and runs REAL SQL. Returns the component's Φ output. Byte-true to the TS
/// executeReadGraph — the "bc composes, makeSQL executes" design.
pub fn execute_read_graph(
    graph: &J,
    input: &Scope,
    driver: &dyn Driver,
) -> Result<Value, SqlFailure> {
    let ir = graph
        .get("ir")
        .ok_or_else(|| plain_failure("scp runtime: readGraph has no ir"))?;
    let dialect = graph
        .get("dialect")
        .and_then(|d| d.as_str())
        .unwrap_or("")
        .to_string();
    let name = graph.get("name").and_then(|n| n.as_str());
    let normalized = normalize_read_graph_input(graph, input);
    let mut handlers = ReadGraphHandlers {
        driver,
        graph,
        dialect,
    };
    run_behavior(ir, &mut handlers, &normalized, name)
        .map_err(|e| re_error_to_sql_failure(&e.to_string()))
}

// ── PRODUCTION live/pooled read execution with executor-layer sibling fan-out (#40) ─
//
// `execute_read_graph` above rides bc's SERIAL `run_behavior` (the SQLite conformance path — the
// published bc Rust crate has no async/parallel `run_behavior`, unlike TS `runBehaviorAsync` or
// the Go/Python bc cores whose `run_plan` parallelizes stage members natively). To give the LIVE
// PG/MySQL path the SAME production fan-out its siblings have, `execute_read_graph_pooled` re-drives
// the read-graph orchestration in the EXECUTOR layer over a `Sync` pooled driver: each plan stage is
// dispatched through bc `run_plan_parallel` (the `dispatch_read_nodes_parallel` seam), so a stage of
// INDEPENDENT sibling read nodes runs on scoped worker threads — each checking out its own pooled
// connection — bounded by the plan's `concurrency` (default 16). `run_plan_parallel` COMMITS stage
// outcomes in ascending declaration order, so the assembled Φ output is byte-identical to the serial
// path regardless of which sibling query finishes first (determinism is not a function of completion
// order). A graph with no parallelizable stage (every plan stage ≤ 1 member — the single-relation /
// conformance shape), `concurrency <= 1`, or any node shape this executor does not reproduce exactly
// falls back to the SERIAL `run_behavior` path UNCHANGED, so those reads are bit-for-bit as before.

/// Execute a compiled ReadGraph on a `Sync` (pooled) driver, dispatching INDEPENDENT sibling read
/// nodes of a plan stage CONCURRENTLY (bc `run_plan_parallel`, capped at the plan `concurrency`).
///
/// The production live PG/MySQL read entry point (#40). A single-relation / zero-sibling read graph
/// (or `concurrency <= 1`, or an unsupported node shape) transparently falls back to the SERIAL
/// [`execute_read_graph`], so it behaves EXACTLY as before. The Φ output is byte-identical to the
/// serial path either way — parallelism changes only the wall-clock, never the result.
pub fn execute_read_graph_pooled(
    graph: &J,
    input: &Scope,
    driver: &(dyn Driver + Sync),
) -> Result<Value, SqlFailure> {
    let ir = graph
        .get("ir")
        .ok_or_else(|| plain_failure("scp runtime: readGraph has no ir"))?;
    let dialect = graph
        .get("dialect")
        .and_then(|d| d.as_str())
        .unwrap_or("")
        .to_string();
    let name = graph.get("name").and_then(|n| n.as_str());
    let normalized = normalize_read_graph_input(graph, input);

    let comp = match component_for(ir, name) {
        Some(c) => c,
        None => return execute_read_graph(graph, input, driver), // serial fallback (fail there)
    };

    // Only take the parallel orchestration when the plan actually has a fan-out stage AND every
    // body node is a shape this executor reproduces bit-for-bit; otherwise the serial bc path.
    if !plan_has_parallel_stage(comp) || !orchestrator_supports(comp) {
        return execute_read_graph(graph, input, driver);
    }

    orchestrate_read_graph(graph, comp, &normalized, &dialect, driver)
}

/// The entry component (`name`, else the first) of a surrogate IR.
fn component_for<'a>(ir: &'a J, name: Option<&str>) -> Option<&'a J> {
    let comps = ir.get("components").and_then(|c| c.as_array())?;
    match name {
        Some(n) => comps
            .iter()
            .find(|c| c.get("name").and_then(|x| x.as_str()) == Some(n)),
        None => comps.first(),
    }
}

/// Does the component's plan carry a stage with ≥ 2 members AND `concurrency > 1`? (The only shape
/// where parallel dispatch can change the wall-clock; anything else is the serial fallback.)
fn plan_has_parallel_stage(comp: &J) -> bool {
    let plan = match comp.get("plan").filter(|p| !p.is_null()) {
        Some(p) => p,
        None => return false,
    };
    let concurrency = plan
        .get("concurrency")
        .and_then(|c| c.as_i64())
        .unwrap_or(1);
    if concurrency <= 1 {
        return false;
    }
    plan.get("groups")
        .and_then(|g| g.as_array())
        .map(|groups| {
            groups
                .iter()
                .any(|st| st.as_array().map(|m| m.len() >= 2).unwrap_or(false))
        })
        .unwrap_or(false)
}

/// Is every body node a shape [`orchestrate_read_graph`] reproduces byte-identically to bc? We
/// support `cond`, `componentRef`, and a SIMPLE `map` (`over` + `as`, no `when`/`into`/`batched`);
/// anything richer routes to the serial `run_behavior` fallback so its result is guaranteed exact.
fn orchestrator_supports(comp: &J) -> bool {
    let body = match comp.get("body").and_then(|b| b.as_array()) {
        Some(b) => b,
        None => return false,
    };
    body.iter().all(|n| {
        if n.get("cond").is_some() {
            return true;
        }
        if let Some(m) = n.get("map") {
            // A simple map only: no per-element guard / zip / batch (none appear in read graphs;
            // if one ever does, the serial fallback still executes it correctly).
            return m.get("when").is_none()
                && m.get("into").is_none()
                && m.get("batched").is_none();
        }
        n.get("component").is_some()
    })
}

/// Executor-layer read-graph orchestration mirroring bc `run_behavior`, but dispatching each plan
/// stage through [`run_plan_parallel`] so a stage of independent siblings runs concurrently on the
/// `Sync` pooled driver. Stages run sequentially (a later stage sees the committed prior-stage
/// results); WITHIN a stage the members are independent (no intra-stage data edge), so parallel
/// execution over a read-only shared scope is deterministic — `run_plan_parallel` commits in
/// declaration order, so the result equals the serial path byte-for-byte.
fn orchestrate_read_graph(
    graph: &J,
    comp: &J,
    input: &Scope,
    dialect: &str,
    driver: &(dyn Driver + Sync),
) -> Result<Value, SqlFailure> {
    let body = comp
        .get("body")
        .and_then(|b| b.as_array())
        .ok_or_else(|| plain_failure("scp runtime: read graph component has no body"))?;
    let output = comp.get("output").cloned().unwrap_or(J::Null);
    let plan = comp.get("plan").filter(|p| !p.is_null());
    let concurrency = plan
        .and_then(|p| p.get("concurrency"))
        .and_then(|c| c.as_i64())
        .unwrap_or(1);
    let stages: Vec<Vec<usize>> = match plan
        .and_then(|p| p.get("groups"))
        .and_then(|g| g.as_array())
    {
        Some(groups) => groups
            .iter()
            .map(|st| {
                st.as_array()
                    .map(|m| {
                        m.iter()
                            .filter_map(|i| i.as_u64().map(|x| x as usize))
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect(),
        None => (0..body.len()).map(|i| vec![i]).collect(),
    };

    // Committed results accumulate across stages (a later map's `over` refs an earlier node).
    let mut results: Vec<(String, Value)> = Vec::new();

    for stage in &stages {
        let mut ordered = stage.clone();
        ordered.sort_unstable();

        // Read-only scope shared by every sibling in this stage: input + prior-stage results.
        let base: Scope = {
            let mut s = input.clone();
            s.extend(results.iter().cloned());
            s
        };

        // One OpSpec per stage member; a flat independent stage (siblings carry no parent inside
        // the stage — read-graph relation nodes depend only on the PRIOR stage's primary).
        let ops: Vec<OpSpec> = ordered
            .iter()
            .map(|&i| OpSpec {
                id: body[i]
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                parent: None,
                bind_field: None,
                relation_kind: None,
                policy: None,
            })
            .collect();
        let plan_spec = ExecutionPlanSpec {
            groups: vec![(0..ordered.len()).collect()],
            concurrency,
        };

        let exec = |op: &OpSpec, _bound: Option<&Value>| -> ExecOutcome {
            let idx = match body
                .iter()
                .position(|n| n.get("id").and_then(|v| v.as_str()) == Some(op.id.as_str()))
            {
                Some(i) => i,
                None => {
                    return ExecOutcome::Error(format!("orchestrate: unknown node '{}'", op.id))
                }
            };
            exec_read_node(driver, graph, dialect, &body[idx], &base)
        };

        let run = match run_plan_parallel(Some(&plan_spec), &ops, exec) {
            Ok(r) => r,
            Err(e) => return Err(plain_failure(&format!("orchestrate: {}", e.message))),
        };

        // Commit this stage's outcomes into `results` in ascending body order (deterministic).
        let tree = run.final_tree();
        let mut committed: Vec<(usize, String, Value)> = Vec::new();
        for (id, v) in &tree {
            if let Some(idx) = body
                .iter()
                .position(|n| n.get("id").and_then(|x| x.as_str()) == Some(id.as_str()))
            {
                committed.push((idx, id.clone(), v.clone()));
            }
        }
        committed.sort_by_key(|(idx, _, _)| *idx);
        for (_, id, v) in committed {
            results.push((id, v));
        }
    }

    // Φ output: evaluate the component `output` against input + all committed node results.
    let scope: Scope = {
        let mut s = input.clone();
        s.extend(results.iter().cloned());
        s
    };
    evaluate_expression(&output, &scope).map_err(|e| re_error_to_sql_failure(&e.message))
}

/// Compute ONE read-graph body node's value against `base` scope, mirroring bc `run_behavior`:
/// a `cond` evaluates its `{cond:[if,then,else]}`; a `componentRef` renders + executes its
/// `__scope`-port statements; a simple `map` iterates `over`, rendering + executing per element
/// under the `as` binding, and collects the per-element rows in order (byte-identical to bc).
fn exec_read_node(
    driver: &(dyn Driver + Sync),
    graph: &J,
    dialect: &str,
    node: &J,
    base: &Scope,
) -> ExecOutcome {
    if let Some(c) = node.get("cond") {
        let cond_expr = serde_json::json!({ "cond": [c.get("if"), c.get("then"), c.get("else")] });
        return match evaluate_expression(&cond_expr, base) {
            Ok(v) => ExecOutcome::Ok(v),
            Err(e) => ExecOutcome::Error(e.message),
        };
    }

    let node_id = node.get("id").and_then(|v| v.as_str()).unwrap_or("");

    if let Some(m) = node.get("map") {
        let over = match evaluate_expression(m.get("over").unwrap_or(&J::Null), base) {
            Ok(v) => v,
            Err(e) => return ExecOutcome::Error(e.message),
        };
        let arr = match over {
            Value::Arr(a) => a,
            _ => return ExecOutcome::Error(format!("map '{node_id}': 'over' is not an array")),
        };
        let as_name = m.get("as").and_then(|v| v.as_str()).unwrap_or("$");
        let ports_j = m
            .get("ports")
            .cloned()
            .unwrap_or(J::Object(Default::default()));
        let mut out: Vec<Value> = Vec::with_capacity(arr.len());
        for el in &arr {
            let mut scope = base.clone();
            scope.push((as_name.to_string(), el.clone()));
            match render_node_ports(driver, graph, dialect, node_id, &ports_j, &scope) {
                Ok(v) => out.push(v),
                Err(e) => return ExecOutcome::Error(e),
            }
        }
        return ExecOutcome::Ok(Value::Arr(out));
    }

    // componentRef: evaluate the `__scope` port in `base`, then render + execute.
    let ports_j = node
        .get("ports")
        .cloned()
        .unwrap_or(J::Object(Default::default()));
    match render_node_ports(driver, graph, dialect, node_id, &ports_j, base) {
        Ok(v) => ExecOutcome::Ok(v),
        Err(e) => ExecOutcome::Error(e),
    }
}

/// Evaluate a makeSQL node's ports (the surrogate `__scope`) against `scope`, then render + execute
/// its static statements — the SAME render→execute the serial handler runs, so SQL + params match.
fn render_node_ports(
    driver: &(dyn Driver + Sync),
    graph: &J,
    dialect: &str,
    node_id: &str,
    ports_j: &J,
    scope: &Scope,
) -> Result<Value, String> {
    let ports = eval_ports(ports_j, scope)?;
    let node_scope = match ports.iter().find(|(k, _)| k == SCOPE_PORT).map(|(_, v)| v) {
        Some(Value::Obj(pairs)) => pairs.clone(),
        _ => {
            return Err(format!(
                "static-bundle: node '{node_id}' surrogate scope did not evaluate to an object"
            ))
        }
    };
    match render_and_execute_node(driver, graph, dialect, node_id, &node_scope) {
        ExecOutcome::Ok(v) => Ok(v),
        ExecOutcome::Error(e) => Err(e),
    }
}

/// Evaluate a bc `{obj:{…}}` (or any Expression) ports node to a flat `[(port, Value)]` (mirrors
/// bc `eval_ports`): each port value is a deferred Expression IR resolved against `scope`.
fn eval_ports(ports: &J, scope: &Scope) -> Result<Vec<(String, Value)>, String> {
    let obj = ports
        .as_object()
        .ok_or_else(|| "static-bundle: node ports must be an object".to_string())?;
    let mut out = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        out.push((
            k.clone(),
            evaluate_expression(v, scope).map_err(|e| e.message)?,
        ));
    }
    Ok(out)
}

// ── Parallel read-relation dispatch (executor-layer fan-out; #40) ──────────────
//
// bc's Rust `run_behavior` owns the read-graph orchestration (map iteration / Φ-merge / wiring)
// and calls the makeSQL handler SYNCHRONOUSLY per node through a `FnMut` seam (`run_plan`). The
// published `behavior-contracts` 0.2.0 crate exposes NO async/parallel `run_behavior` — only the
// lower-level `run_plan_parallel`, which dispatches the INDEPENDENT members of a plan stage on
// scoped worker threads bounded by `concurrency`. So sibling-relation parallelism cannot live
// INSIDE a single `run_behavior` call without forking bc; per the issue we put the fan-out in the
// EXECUTOR layer around it.
//
// [`dispatch_read_nodes_parallel`] is that seam: given the independent read nodes of one plan stage
// (each `(node_id, scope)`, already resolved against the primary's rows — the tx-DAG carries no
// intra-stage data edge between siblings), it renders + executes them CONCURRENTLY via
// `run_plan_parallel`, each on its own thread checking out its own pooled connection from a `Sync`
// pooled driver — real parallel DB I/O. Results are committed in ascending declaration order, so
// the assembled output is deterministic and byte-identical to the serial path (concurrency changes
// only the wall-clock, never the result). The conformance corpus ships single-relation read graphs,
// so `execute_read_graph` (serial `run_behavior`) remains the byte-invariant conformance path; this
// capability is exercised by the multi-sibling latency proof in `tests/`.

use behavior_contracts::{run_plan_parallel, ExecutionPlanSpec, OpSpec};

/// Dispatch a stage's INDEPENDENT read nodes concurrently against a `Sync` (pooled) driver.
///
/// `nodes` are `(node_id, scope)` pairs with NO intra-stage parent dependency (sibling relations
/// mapping over the same primary). Renders + executes each via [`render_and_execute_node`] on scoped
/// worker threads (bounded by `concurrency`, default the plan's 16), then returns each node's rows
/// in the SAME order as `nodes` — deterministic regardless of finish order. This is the executor's
/// parallel fan-out; the SQL text + params per node are byte-identical to the serial path.
pub fn dispatch_read_nodes_parallel<D: Driver + Sync>(
    driver: &D,
    graph: &J,
    dialect: &str,
    nodes: &[(String, Scope)],
    concurrency: i64,
) -> Result<Vec<Value>, SqlFailure> {
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    // One flat stage of independent ops (no parents) — the bounded-parallel dispatch shape.
    let ops: Vec<OpSpec> = nodes
        .iter()
        .map(|(id, _)| OpSpec {
            id: id.clone(),
            parent: None,
            bind_field: None,
            relation_kind: None,
            policy: None,
        })
        .collect();
    let plan = ExecutionPlanSpec {
        groups: vec![(0..nodes.len()).collect()],
        concurrency,
    };
    // Map node id → its scope for the (thread-shared, read-only) exec closure.
    let by_id: std::collections::HashMap<&str, &Scope> =
        nodes.iter().map(|(id, s)| (id.as_str(), s)).collect();

    let exec = |op: &OpSpec, _bound: Option<&Value>| -> ExecOutcome {
        let scope = match by_id.get(op.id.as_str()) {
            Some(s) => *s,
            None => {
                return ExecOutcome::Error(format!("parallel dispatch: unknown node '{}'", op.id))
            }
        };
        render_and_execute_node(driver, graph, dialect, &op.id, scope)
    };

    let result = run_plan_parallel(Some(&plan), &ops, exec)
        .map_err(|e| plain_failure(&format!("parallel dispatch: {}", e.message)))?;

    // Commit order = declaration order (run_plan_parallel guarantees deterministic results).
    let tree = result.final_tree();
    let mut out: Vec<Value> = Vec::with_capacity(nodes.len());
    for (id, _) in nodes {
        match tree.iter().find(|(k, _)| k == id) {
            Some((_, v)) => out.push(v.clone()),
            None => out.push(Value::Arr(Vec::new())), // a skipped node contributes no rows
        }
    }
    Ok(out)
}

// ── Tx op render (port of tx.ts renderStatement) ───────────────────────────────

/// Render a tx statement's makeSQL op `{sql, params}` against the tx scope: evaluate each deferred
/// Expression-IR param, assemble + render placeholders (the SAME assemble the read path uses).
pub fn render_tx_op(op: &J, scope: &Scope, dialect_name: &str) -> Result<RenderedSql, String> {
    let sql_text = op
        .get("sql")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let mut concrete: Vec<Value> = Vec::new();
    if let Some(specs) = op.get("params").and_then(|p| p.as_array()) {
        for spec in specs {
            concrete.push(evaluate_expression(spec, scope).map_err(|e| e.message)?);
        }
    }
    let (sql, params) = assemble_make_sql(&MakeSqlNode {
        sql: sql_text,
        params: concrete,
    })?;
    Ok(RenderedSql {
        sql: render_placeholders(&sql, dialect_name),
        params,
    })
}

// ── small helper ───────────────────────────────────────────────────────────────

/// A structural error carrying a `SQLITE_` tag is re-surfaced with the mapped kind; otherwise a
/// plain `driver_error`.
fn plain_failure(message: &str) -> SqlFailure {
    if message.contains("SQLITE_") {
        re_error_to_sql_failure(message)
    } else {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: message.to_string(),
        }
    }
}
