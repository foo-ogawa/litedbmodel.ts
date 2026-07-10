// litedbmodel v2 SCP — the STATIC, PORTABLE makeSQL bundle RUNTIME (Go port, epic #43/#45).
//
// Byte-for-byte port of the TS src/scp/makesql/static-bundle.ts + makesql.ts + handler.ts runtime
// halves — the SOLE makeSQL read/render path. It consumes the PRE-COMPILED, portable artifacts the
// corpus ships (a read ReadGraph = a bc ComponentGraphIR of `__makeSqlNode` surrogate nodes +
// per-node STATIC statement templates), and EXECUTES them via the shared behavior-contracts Go
// core (RunBehavior drives map / Φ-merge / wiring; EvaluateExpression resolves the deferred
// value-specs + skip). This file re-implements NO generic evaluator and does NO SQL re-derivation —
// every statement's `sql` is fixed text; the runtime only evaluates its deferred params + skip,
// resolves the WHERE connector from the present set, assembles + renders placeholders, and binds.
//
// A statement template (StaticStatement) is `{sql, params, skip?, whereFragment?}`:
//   - sql           — complete tuned dialect text (`?` placeholders), value-independent.
//   - params        — deferred value-specs = closed-set bc Expression IR, 1:1 with the top `?`.
//   - skip          — optional bc presence expression; truthy ⇒ the whole statement drops.
//   - whereFragment — a bare predicate body; the runtime prepends ` WHERE `/` AND ` from the
//                     present set (a skipped earlier fragment never leaves a dangling connector).
//
// An IN-list value-spec is the marker `{"__jsonArray": <spec>, "dialect": <d>}`: postgres binds the
// array as-is (a text[] param); mysql/sqlite JSON-encode it to a single param (server-side
// expansion). This mirrors the TS evalSpec.

package litedbmodel_runtime

import (
	"fmt"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// Rendered is the output of rendering: final SQL text + flat params (1:1 with `?`/`$N`, each a bc
// runtime Value).
type Rendered struct {
	SQL    string
	Params []bc.Value
}

// ReadGraph is the parsed static read artifact: the surrogate bc IR + per-node static statement
// templates + the optional heads. All heavy fields stay raw JNodes so bc evaluates their
// Expression-IR slots (never pre-decoded), byte-true to the TS ReadGraph.
type ReadGraph struct {
	Dialect          string
	Name             string
	IR               bc.JNode              // the surrogate ComponentGraphIR ({irVersion, exprVersion, components})
	StatementsByID   map[string][]bc.JNode // nodeId → ordered StaticStatement templates
	OptionalHeads    []string
	primaryComponent *bc.JObj // components[0] (for input normalization + primary-node lookup)
}

// ReadGraphFromJObj parses a readGraph object (from a bundle or a render vector) into a ReadGraph.
func ReadGraphFromJObj(obj *bc.JObj) (*ReadGraph, error) {
	g := &ReadGraph{StatementsByID: map[string][]bc.JNode{}}
	if d, ok := obj.Get("dialect"); ok {
		g.Dialect, _ = d.(string)
	}
	if n, ok := obj.Get("name"); ok {
		g.Name, _ = n.(string)
	}
	irN, ok := obj.Get("ir")
	if !ok {
		return nil, fmt.Errorf("scp runtime: readGraph has no ir")
	}
	g.IR = irN
	if irObj, ok := irN.(*bc.JObj); ok {
		if compsN, ok := irObj.Get("components"); ok {
			if comps, ok := compsN.([]bc.JNode); ok && len(comps) > 0 {
				g.primaryComponent, _ = comps[0].(*bc.JObj)
			}
		}
	}
	if sbN, ok := obj.Get("statementsById"); ok {
		if sb, ok := sbN.(*bc.JObj); ok {
			for _, id := range sb.Keys {
				if arr, ok := sb.Vals[id].([]bc.JNode); ok {
					g.StatementsByID[id] = arr
				}
			}
		}
	}
	if oh, ok := obj.Get("optionalHeads"); ok {
		if arr, ok := oh.([]bc.JNode); ok {
			for _, h := range arr {
				if s, ok := h.(string); ok {
					g.OptionalHeads = append(g.OptionalHeads, s)
				}
			}
		}
	}
	return g, nil
}

// ── makeSQL assembly (port of makesql.ts assembleMakeSQL / composeMakeSQL) ─────

// makeSQLNode is a concrete makeSQL after value evaluation: fixed sql text + a flat value list.
type makeSQLNode struct {
	sql    string
	params []bc.Value
}

// assembleMakeSQL splits `sql` on `?` and interleaves each concrete param; a nested makeSQL splices
// its assembled sql + flows its params (mirrors TS assembleMakeSQL). (Our concrete runtime nodes
// carry only bound values — nested makeSQL splicing is handled at compile time in the corpus text —
// so this is the value-fill flatten with a placeholder/param arity check.)
func assembleMakeSQL(node makeSQLNode) (string, []bc.Value, error) {
	chunks := strings.Split(node.sql, "?")
	if len(chunks)-1 != len(node.params) {
		return "", nil, fmt.Errorf("makeSQL placeholder/param mismatch: %d '?' vs %d params in %q",
			len(chunks)-1, len(node.params), node.sql)
	}
	var sb strings.Builder
	sb.WriteString(chunks[0])
	params := make([]bc.Value, 0, len(node.params))
	for i, p := range node.params {
		sb.WriteString("?")
		sb.WriteString(chunks[i+1])
		params = append(params, p)
	}
	return sb.String(), params, nil
}

// composeMakeSQL concatenates the assembled sql + params of every present node (mirrors TS composeMakeSQL).
func composeMakeSQL(nodes []makeSQLNode) (string, []bc.Value, error) {
	var sb strings.Builder
	var params []bc.Value
	for _, n := range nodes {
		s, p, err := assembleMakeSQL(n)
		if err != nil {
			return "", nil, err
		}
		sb.WriteString(s)
		params = append(params, p...)
	}
	return sb.String(), params, nil
}

// ── Dialect placeholder render (port of handler.ts renderPlaceholders) ─────────

// renderPlaceholders rewrites `?` → the dialect placeholder form: PG `$N` (quote-aware), MySQL/SQLite
// keep `?`. Byte-for-byte port of the TS renderPlaceholders: a `?` inside a single-quoted string
// literal is NOT a placeholder.
func renderPlaceholders(sql, dialectName string) string {
	if dialectName != "postgres" {
		return sql
	}
	var out strings.Builder
	index := 0
	inString := false
	for _, ch := range sql {
		if inString {
			out.WriteRune(ch)
			if ch == '\'' {
				inString = false
			}
		} else if ch == '\'' {
			out.WriteRune(ch)
			inString = true
		} else if ch == '?' {
			index++
			fmt.Fprintf(&out, "$%d", index)
		} else {
			out.WriteRune(ch)
		}
	}
	return out.String()
}

// ── Deferred value-spec evaluation (port of static-bundle.ts evalSpec) ─────────

// toRenderParam narrows a bc-evaluated value to the render-boundary form (mirrors TS toDriverParam):
// a bc int (int64) within the JS safe-integer range collapses to a JS number (float64), so a
// coalesce/arithmetic integer literal encodes as a plain corpus number — NOT a `{$bigint}` tag —
// exactly as the TS reference captured it. Values outside the safe range keep int64 (a real bigint).
func toRenderParam(v bc.Value) bc.Value {
	if i, ok := v.(int64); ok {
		const maxSafe = int64(9007199254740991)
		if i >= -maxSafe && i <= maxSafe {
			return float64(i)
		}
	}
	return v
}

// evalSpec evaluates one deferred value-spec against the scope, handling the `__jsonArray` marker:
// postgres keeps the array as-is (a text[] param); mysql/sqlite JSON-encode it to ONE string param.
func evalSpec(spec bc.JNode, scope *bc.Obj, dialectName string) (bc.Value, error) {
	if specObj, ok := spec.(*bc.JObj); ok {
		if _, hasMarker := specObj.Get("__jsonArray"); hasMarker {
			innerN, _ := specObj.Get("__jsonArray")
			arrV, err := bc.EvaluateExpression(innerN, scope)
			if err != nil {
				return nil, err
			}
			arr, ok := arrV.([]bc.Value)
			if !ok {
				return nil, fmt.Errorf("static-bundle: IN-list value-spec did not evaluate to an array")
			}
			specDialect := ""
			if dN, ok := specObj.Get("dialect"); ok {
				specDialect, _ = dN.(string)
			}
			narrowed := make([]bc.Value, len(arr))
			for i, e := range arr {
				narrowed[i] = toRenderParam(e)
			}
			if specDialect == "postgres" {
				return narrowed, nil // bound as ONE text[] param
			}
			return jsStringify(narrowed), nil // single JSON param (server-side expansion)
		}
	}
	v, err := bc.EvaluateExpression(spec, scope)
	if err != nil {
		return nil, err
	}
	return toRenderParam(v), nil
}

// ── Statement-list render (port of static-bundle.ts renderStatements) ──────────

// renderStatements evaluates a list of static statement templates against a scope → final SQL +
// params. Byte-for-byte port of the TS renderStatements: drop skipped statements, resolve each
// WHERE-fragment's ` WHERE `/` AND ` connector from the present set, compose + render placeholders.
func renderStatements(statements []bc.JNode, dialectName string, scope *bc.Obj) (Rendered, error) {
	var nodes []makeSQLNode
	whereSeen := false
	for _, stmtN := range statements {
		stmt, ok := stmtN.(*bc.JObj)
		if !ok {
			return Rendered{}, fmt.Errorf("static-bundle: statement is not an object")
		}
		if skipN, ok := stmt.Get("skip"); ok {
			drop, err := bc.EvaluateExpression(skipN, scope)
			if err != nil {
				return Rendered{}, err
			}
			if drop != nil {
				if b, isBool := drop.(bool); !isBool || b {
					continue // truthy (non-nil, non-false) ⇒ drop the whole statement
				}
			}
		}
		sqlText := ""
		if s, ok := stmt.Get("sql"); ok {
			sqlText, _ = s.(string)
		}
		if wf, ok := stmt.Get("whereFragment"); ok {
			if b, isBool := wf.(bool); isBool && b {
				if whereSeen {
					sqlText = " AND " + sqlText
				} else {
					sqlText = " WHERE " + sqlText
				}
				whereSeen = true
			}
		}
		var params []bc.Value
		if pN, ok := stmt.Get("params"); ok {
			if arr, ok := pN.([]bc.JNode); ok {
				for _, spec := range arr {
					v, err := evalSpec(spec, scope, dialectName)
					if err != nil {
						return Rendered{}, err
					}
					params = append(params, v)
				}
			}
		}
		nodes = append(nodes, makeSQLNode{sql: sqlText, params: params})
	}
	sql, params, err := composeMakeSQL(nodes)
	if err != nil {
		return Rendered{}, err
	}
	return Rendered{SQL: renderPlaceholders(sql, dialectName), Params: params}, nil
}

// ── Input normalization (SSoT-driven — mirrors TS normalizeInput) ─────────────

// normalizeReadGraphInput normalizes omitted OPTIONAL heads to present-as-null. Optional = the
// component's schema-optional ports OR the graph's optionalHeads (SKIP-guarded / refOpt heads).
func normalizeReadGraphInput(g *ReadGraph, input *bc.Obj) *bc.Obj {
	out := bc.NewObj()
	for _, k := range input.Keys {
		out.Set(k, input.Vals[k])
	}
	if g.primaryComponent != nil {
		if ipN, ok := g.primaryComponent.Get("inputPorts"); ok {
			if ports, ok := ipN.(*bc.JObj); ok {
				for _, port := range ports.Keys {
					required := false
					if schema, ok := ports.Vals[port].(*bc.JObj); ok {
						if r, ok := schema.Get("required"); ok {
							if b, ok := r.(bool); ok {
								required = b
							}
						}
					}
					if !required {
						if _, present := out.Get(port); !present {
							out.Set(port, nil)
						}
					}
				}
			}
		}
	}
	for _, head := range g.OptionalHeads {
		if _, present := out.Get(head); !present {
			out.Set(head, nil)
		}
	}
	return out
}

// ── ReadGraph render axis (port of static-bundle.ts renderReadPrimary) ─────────

// RenderReadPrimary renders the PRIMARY read node's statements of a ReadGraph → dialect SQL + params
// (the render axis for conformance golden). The primary node is the first body node in the surrogate
// IR order (map nodes reference it). Optional heads are normalized to present-as-null first.
func RenderReadPrimary(g *ReadGraph, input *bc.Obj) (Rendered, error) {
	primaryID, err := g.primaryNodeID()
	if err != nil {
		return Rendered{}, err
	}
	scope := normalizeReadGraphInput(g, input)
	return renderStatements(g.StatementsByID[primaryID], g.Dialect, scope)
}

// primaryNodeID returns the first body node id that has compiled statements (the SELECT the
// relations map over).
func (g *ReadGraph) primaryNodeID() (string, error) {
	if g.primaryComponent == nil {
		return "", fmt.Errorf("static-bundle: read graph has no component")
	}
	bodyN, ok := g.primaryComponent.Get("body")
	if !ok {
		return "", fmt.Errorf("static-bundle: read graph component has no body")
	}
	body, _ := bodyN.([]bc.JNode)
	for _, nN := range body {
		n, ok := nN.(*bc.JObj)
		if !ok {
			continue
		}
		idV, _ := n.Get("id")
		id, _ := idV.(string)
		if _, has := g.StatementsByID[id]; has {
			return id, nil
		}
	}
	return "", fmt.Errorf("static-bundle: read graph has no primary node to render")
}

// ── ReadGraph execution (port of static-bundle.ts executeReadGraph) ────────────

// readGraphHandlers is the makeSQL handler registry: one render→execute handler behind the
// `__makeSqlNode` catalog leaf; the per-node static statements are keyed by nodeId.
type readGraphHandlers struct {
	db      SQLDB
	graph   *ReadGraph
	dialect string
}

// Exec satisfies bc.ComponentExec (unused — the node identity is required; see ExecCtx).
func (h *readGraphHandlers) Exec(component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	return bc.ErrOutcome("static-bundle: makeSQL handler requires the node identity (ExecCtx)"), true
}

// ExecCtx renders the node's static statements against the surrogate `__scope` port and runs REAL SQL.
func (h *readGraphHandlers) ExecCtx(nodeID, component string, ports *bc.Obj, bound bc.Value) (bc.ExecOutcome, bool) {
	stmts, ok := h.graph.StatementsByID[nodeID]
	if !ok {
		return bc.ErrOutcome(fmt.Sprintf("static-bundle: no statements for node '%s'", nodeID)), true
	}
	scopeV, _ := ports.Get(scopePort)
	scope, ok := scopeV.(*bc.Obj)
	if !ok {
		return bc.ErrOutcome(fmt.Sprintf("static-bundle: node '%s' surrogate scope did not evaluate to an object", nodeID)), true
	}
	rendered, err := renderStatements(stmts, h.dialect, scope)
	if err != nil {
		return bc.ErrOutcome(err.Error()), true
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	rows, qerr := queryRows(h.db, rendered.SQL, args)
	if qerr != nil {
		return bc.ErrOutcome(mapSqliteError(qerr).Error()), true
	}
	return bc.OkOutcome(rows), true
}

// ExecuteReadGraph executes a compiled ReadGraph via bc RunBehavior + a makeSQL handler: bc drives
// map iteration / wire binding / Φ output; the handler renders each node's static statements against
// the evaluated `__scope` and runs REAL SQL. Returns the component's Φ output. Byte-true to the TS
// executeReadGraph — the "bc composes, makeSQL executes" design.
func ExecuteReadGraph(g *ReadGraph, input *bc.Obj, db SQLDB) (bc.Value, error) {
	handlers := &readGraphHandlers{db: db, graph: g, dialect: g.Dialect}
	normalized := normalizeReadGraphInput(g, input)
	out, err := bc.RunBehavior(g.IR, handlers, normalized, g.Name)
	if err != nil {
		return nil, reErrorToSqlFailure(err)
	}
	return out, nil
}
