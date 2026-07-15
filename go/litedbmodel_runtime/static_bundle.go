// litedbmodel v2 SCP — the STATIC, PORTABLE makeSQL bundle RUNTIME (Go port, epic #43/#45).
//
// Byte-for-byte port of the TS src/scp/makesql/static-bundle.ts + makesql.ts + handler.ts runtime
// halves — the SOLE makeSQL read/render path. It consumes the PRE-COMPILED, portable artifacts the
// corpus ships (a read ReadGraph = compileBehaviors' REAL Select/Count/map ComponentGraphIR +
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
	"sync"

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
			// MySQL/SQLite single-JSON IN-list param. A BOOLEAN element is encoded as `1`/`0` for
			// MySQL (NOT JSON `true`/`false`): MySQL's `JSON_UNQUOTE(v)` yields the STRING `'true'`,
			// which coerces to `0` against a TINYINT(1) — a silent mismatch. `1`/`0` is what v1's
			// `col IN (?)` bound. SQLite's `json_each` coerces JSON booleans natively (plain form).
			if specDialect == "mysql" {
				for i, e := range narrowed {
					if b, ok := e.(bool); ok {
						if b {
							narrowed[i] = int64(1)
						} else {
							narrowed[i] = int64(0)
						}
					}
				}
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

// ── Deferred PG array-cast resolution (#46 — mirrors compile-relation.ts) ──────

// pgArrayCastToken is the DEFERRED PG array-cast placeholder: emitted in the STATIC SQL where the
// `= ANY(?::<T>[])` element type is unknown at symbolic compile (a schema-less `whereIn`). Resolved
// at render from the BOUND array via inferPgArrayType — the same render-layer step as `?`→`$N`.
const pgArrayCastToken = "@@PG_ARRAY_CAST@@"

// inferPgArrayType ports the ORIGINAL inferPgArrayType (v1 LazyRelation): the element type inferred
// from the sample values (no sqlCast at this schema-less surface). A bc integer arrives as int64;
// a non-integer number as float64.
func inferPgArrayType(values []bc.Value) string {
	if len(values) == 0 {
		return "text[]"
	}
	switch values[0].(type) {
	case bool:
		return "boolean[]"
	case int64, int:
		return "int[]"
	case float64:
		// A float64 that is an exact integer collapsed from a bc int (toRenderParam) is still an
		// int key; only a genuine fractional value is numeric.
		allInt := true
		for _, v := range values {
			if f, ok := v.(float64); !ok || f != float64(int64(f)) {
				allInt = false
				break
			}
		}
		if allInt {
			return "int[]"
		}
		return "numeric[]"
	default:
		return "text[]"
	}
}

// resolvePgArrayCast resolves the FIRST unresolved cast token to the element type inferred from
// values (mirrors TS resolvePgArrayCast). SQL with no token is unchanged.
func resolvePgArrayCast(sql string, values []bc.Value) string {
	at := strings.Index(sql, pgArrayCastToken)
	if at < 0 {
		return sql
	}
	return sql[:at] + inferPgArrayType(values) + sql[at+len(pgArrayCastToken):]
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
		// Resolve any deferred PG array cast (#46) from the bound array param, left-to-right —
		// each postgres __jsonArray param resolves exactly one cast token in order.
		if dialectName == "postgres" {
			for _, p := range params {
				if arr, ok := p.([]bc.Value); ok {
					if !strings.Contains(sqlText, pgArrayCastToken) {
						break
					}
					sqlText = resolvePgArrayCast(sqlText, arr)
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

// ── ReadGraph execution (NATIVE, interpreter-free — executeReadGraphNative below) ──

// RenderExecuteNode renders ONE read node's static statements against `scope` and runs REAL SQL on
// `db`, returning the row list. It is the render->execute SSoT for a real Select/Count/map node (#12)
// (ExecCtx above). Exported so the codegen bench cell's generated-module handler runs the SAME
// render+execute the interpreter handler runs — the ONLY difference being that the generated
// de-interpreted module (NOT RunBehavior) drives the call. Byte-identical rows to the ir path.
func RenderExecuteNode(g *ReadGraph, nodeID, dialect string, scope *bc.Obj, db SQLDB) ([]bc.Value, error) {
	stmts, ok := g.StatementsByID[nodeID]
	if !ok {
		return nil, fmt.Errorf("static-bundle: no statements for node '%s'", nodeID)
	}
	rendered, err := renderStatements(stmts, dialect, scope)
	if err != nil {
		return nil, err
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	rows, qerr := queryRows(db, rendered.SQL, args)
	if qerr != nil {
		return nil, mapSqliteError(qerr)
	}
	return rows, nil
}

// PrimaryNodeID exposes the read graph's primary render node id (the SELECT relations map over) so
// the codegen bench cell's handler can render it. Exported companion to RenderExecuteNode.
func (g *ReadGraph) PrimaryNodeID() (string, error) { return g.primaryNodeID() }

// ExecuteReadGraph executes a compiled ReadGraph NATIVELY (no bc RunBehavior / no IR interpreter):
// it walks the surrogate component's body nodes in order via the CLOSED-SET native orchestration
// (executeReadGraphNative) — each componentRef node renders + executes its static statements against
// the evaluated scope, a map node iterates its parent node's rows (per-element `$as` scope) and
// collects the aligned child row-lists, and the component `output` obj assembles the Φ result by ref.
// Every statement's SQL is fixed text carried verbatim; only the deferred typed param slots + skip are
// evaluated. Byte-identical to the former RunBehavior path for the closed read-graph shapes the
// makeSQL corpus emits (single sequential componentRef reads + a single relationKind:single map).
func ExecuteReadGraph(g *ReadGraph, input *bc.Obj, db SQLDB) (bc.Value, error) {
	normalized := normalizeReadGraphInput(g, input)
	out, err := executeReadGraphNative(g, normalized, db)
	if err != nil {
		return nil, reErrorToSqlFailure(err)
	}
	return out, nil
}

// executeReadGraphNative is the NATIVE, interpreter-free read-graph orchestration for the closed set
// of makeSQL read shapes. It reads the STATIC component structure (body node ids, the map `as`/`over`
// wiring, the `output` obj) as pure data — it never calls bc.RunBehavior and never walks the generic
// Expression IR (the per-statement param slots are still evaluated natively by renderStatements, the
// typed-param-binding contract). Fail-closed: an out-of-set body node / output ref panics-equivalent
// (returns an error) rather than silently degrading.
func executeReadGraphNative(g *ReadGraph, scope *bc.Obj, db SQLDB) (bc.Value, error) {
	comp := g.primaryComponent
	if comp == nil {
		return nil, fmt.Errorf("static-bundle: read graph has no component")
	}
	bodyN, _ := comp.Get("body")
	body, _ := bodyN.([]bc.JNode)

	// nodeResults holds each body node's produced value (a []bc.Value for a plain read; a
	// []bc.Value of []bc.Value for a map). Assembled into the output by ref. Guarded by a mutex —
	// independent same-stage nodes run concurrently (below).
	nodeResults := map[string]bc.Value{}
	var mu sync.Mutex
	getResult := func(id string) (bc.Value, bool) {
		mu.Lock()
		defer mu.Unlock()
		v, ok := nodeResults[id]
		return v, ok
	}
	setResult := func(id string, v bc.Value) {
		mu.Lock()
		nodeResults[id] = v
		mu.Unlock()
	}

	// execOne renders+executes one body node (plain read or map) against the scope.
	execOne := func(n *bc.JObj) error {
		idV, _ := n.Get("id")
		id, _ := idV.(string)
		if mapN, isMap := n.Get("map"); isMap {
			// A relationKind:single map (`authors = posts.map(...)`): iterate the parent node's rows,
			// bind the element under the map's `as` var, render+execute the child node per element,
			// collect the aligned per-element child row-lists.
			mapObj, ok := mapN.(*bc.JObj)
			if !ok {
				return fmt.Errorf("static-bundle: map node '%s' is not an object", id)
			}
			asV, _ := mapObj.Get("as")
			asVar, _ := asV.(string)
			overN, _ := mapObj.Get("over")
			overRef, err := singleRefID(overN)
			if err != nil {
				return fmt.Errorf("static-bundle: map node '%s' over: %w", id, err)
			}
			overV, has := getResult(overRef)
			parentRows, ok := overV.([]bc.Value)
			if !has || !ok {
				return fmt.Errorf("static-bundle: map node '%s' over '%s' did not produce a row list", id, overRef)
			}
			built := make([]bc.Value, 0, len(parentRows))
			for _, pr := range parentRows {
				elemScope := bc.NewObj()
				for _, k := range scope.Keys {
					elemScope.Set(k, scope.Vals[k])
				}
				elemScope.Set(asVar, pr)
				childRows, err := RenderExecuteNode(g, id, g.Dialect, elemScope, db)
				if err != nil {
					return err
				}
				built = append(built, bc.Value(childRows))
			}
			setResult(id, bc.Value(built))
			return nil
		}
		// A plain componentRef read node: render + execute its static statements against the scope.
		if _, has := g.StatementsByID[id]; !has {
			return fmt.Errorf("static-bundle: read graph body node '%s' has no static statements (out-of-set shape)", id)
		}
		rows, err := RenderExecuteNode(g, id, g.Dialect, scope, db)
		if err != nil {
			return err
		}
		setResult(id, bc.Value(rows))
		return nil
	}

	// Stage plan: the component `plan.groups` lists body-index groups run in order; the INDEPENDENT
	// members of a group are dispatched CONCURRENTLY (bounded by plan.concurrency) — bc's staged
	// exec model, reproduced natively (the generated typed-native module uses the same scoped-worker
	// fan-out). Failure precedence is the LOWEST body index in the group (committed in ascending
	// index order). No plan → sequential in body order (a single implicit group).
	groups, concurrency := readPlan(comp, len(body))
	for _, group := range groups {
		if err := runStageConcurrent(group, body, concurrency, execOne); err != nil {
			return nil, err
		}
	}

	return assembleReadOutput(comp, nodeResults)
}

// readPlan extracts the component's staged exec plan: the ordered index groups + the concurrency
// bound. A missing/malformed plan falls back to ONE group of all body indices in order (sequential).
func readPlan(comp *bc.JObj, bodyLen int) ([][]int, int) {
	planN, ok := comp.Get("plan")
	plan, ok2 := planN.(*bc.JObj)
	if !ok || !ok2 {
		all := make([]int, bodyLen)
		for i := range all {
			all[i] = i
		}
		return [][]int{all}, 1
	}
	concurrency := 1
	if cN, ok := plan.Get("concurrency"); ok {
		if dv, err := bc.DecodeValue(cN); err == nil {
			if i, ok := dv.(int64); ok && i > 0 {
				concurrency = int(i)
			}
		}
	}
	var groups [][]int
	if gN, ok := plan.Get("groups"); ok {
		if arr, ok := gN.([]bc.JNode); ok {
			for _, gn := range arr {
				if idxArr, ok := gn.([]bc.JNode); ok {
					var grp []int
					for _, iN := range idxArr {
						if dv, err := bc.DecodeValue(iN); err == nil {
							if i, ok := dv.(int64); ok {
								grp = append(grp, int(i))
							}
						}
					}
					groups = append(groups, grp)
				}
			}
		}
	}
	if len(groups) == 0 {
		all := make([]int, bodyLen)
		for i := range all {
			all[i] = i
		}
		groups = [][]int{all}
	}
	return groups, concurrency
}

// runStageConcurrent dispatches the INDEPENDENT members of one plan stage (body indices) on a
// bounded goroutine pool (semaphore of `concurrency`), then returns the LOWEST-index error (ascending
// failure precedence, byte-matching the interpreter's committed order). A single-member stage runs
// inline (no goroutine).
func runStageConcurrent(group []int, body []bc.JNode, concurrency int, execOne func(*bc.JObj) error) error {
	if len(group) <= 1 || concurrency <= 1 {
		for _, idx := range group {
			n, ok := body[idx].(*bc.JObj)
			if !ok {
				return fmt.Errorf("static-bundle: body node %d is not an object", idx)
			}
			if err := execOne(n); err != nil {
				return err
			}
		}
		return nil
	}
	errs := make([]error, len(group))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for pos, idx := range group {
		n, ok := body[idx].(*bc.JObj)
		if !ok {
			errs[pos] = fmt.Errorf("static-bundle: body node %d is not an object", idx)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(pos int, n *bc.JObj) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[pos] = execOne(n)
		}(pos, n)
	}
	wg.Wait()
	// Ascending-index failure precedence (the group is emitted in ascending body index).
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// singleRefID extracts the single node id from a `{ref:[id]}` wiring expr (fail-closed on any other
// shape — the map `over` in the covered set is always a single-segment ref to a prior body node).
func singleRefID(n bc.JNode) (string, error) {
	o, ok := n.(*bc.JObj)
	if !ok {
		return "", fmt.Errorf("expected a {ref:[id]} object")
	}
	refN, ok := o.Get("ref")
	if !ok {
		return "", fmt.Errorf("expected a ref key")
	}
	arr, ok := refN.([]bc.JNode)
	if !ok || len(arr) != 1 {
		return "", fmt.Errorf("expected a single-segment ref")
	}
	s, ok := arr[0].(string)
	if !ok {
		return "", fmt.Errorf("expected a string ref segment")
	}
	return s, nil
}

// assembleReadOutput builds the component Φ output from `nodeResults` per the component's `output`
// obj: either a single `{ref:[id]}` (the whole output IS that node's value) or an `{obj:{k:{ref:[id]}}}`
// (an object keyed by field, each a node ref). Fail-closed on any other output shape.
func assembleReadOutput(comp *bc.JObj, nodeResults map[string]bc.Value) (bc.Value, error) {
	outN, ok := comp.Get("output")
	if !ok {
		return nil, fmt.Errorf("static-bundle: component has no output")
	}
	outObj, ok := outN.(*bc.JObj)
	if !ok {
		return nil, fmt.Errorf("static-bundle: component output is not an object")
	}
	// {ref:[id]} — the whole output is one node's value.
	if id, err := singleRefID(outObj); err == nil {
		v, has := nodeResults[id]
		if !has {
			return nil, fmt.Errorf("static-bundle: output ref '%s' has no node result", id)
		}
		return v, nil
	}
	// {obj:{field:{ref:[id]}}} — an object of node refs.
	objN, ok := outObj.Get("obj")
	if !ok {
		return nil, fmt.Errorf("static-bundle: component output is neither a ref nor an obj")
	}
	fields, ok := objN.(*bc.JObj)
	if !ok {
		return nil, fmt.Errorf("static-bundle: component output obj is not an object")
	}
	result := bc.NewObj()
	for _, field := range fields.Keys {
		id, err := singleRefID(fields.Vals[field])
		if err != nil {
			return nil, fmt.Errorf("static-bundle: output field '%s': %w", field, err)
		}
		v, has := nodeResults[id]
		if !has {
			return nil, fmt.Errorf("static-bundle: output field '%s' ref '%s' has no node result", field, id)
		}
		result.Set(field, v)
	}
	return result, nil
}
