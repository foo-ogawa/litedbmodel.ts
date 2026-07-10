// litedbmodel v2 SCP — write-time transaction runtime (Go port of src/scp/write-runtime.ts; spec §6).
//
// Executes a derived TransactionPlan (carried pure-JSON in the §8 bundle) against a real SQL
// transaction with gate-first short-circuit. It renders each ordered statement with the SAME
// makeSQL assemble+renderPlaceholders the read path uses, and drives an explicit BEGIN / COMMIT / ROLLBACK
// envelope through the SAME database/sql seam — no separate interpreter, no re-derivation of the
// plan (the plan is honored, not rebuilt, spec §6). Ported byte-true to write-runtime.ts incl. the
// three gate rules and the `$.entity` RETURNING-row exposure.

package litedbmodel_runtime

import (
	"database/sql"
	"fmt"
	"regexp"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// selectPrefixRe matches a leading SELECT verb (tx.ts execStatement: a gate SELECT yields rows).
var selectPrefixRe = regexp.MustCompile(`(?i)\bselect\b`)

// renderTxOp renders a tx statement's makeSQL op `{sql, params}` against the tx scope (tx.ts
// renderStatement): evaluate each deferred Expression-IR param to a concrete value, build a concrete
// makeSQL, assemble + render placeholders. The SAME assemble/render the read path uses.
func renderTxOp(op *bc.JObj, scope *bc.Obj, dialectName string) (Rendered, error) {
	sqlText := ""
	if s, ok := op.Get("sql"); ok {
		sqlText, _ = s.(string)
	}
	var concrete []bc.Value
	if pN, ok := op.Get("params"); ok {
		if arr, ok := pN.([]bc.JNode); ok {
			for _, spec := range arr {
				v, err := bc.EvaluateExpression(spec, scope)
				if err != nil {
					return Rendered{}, err
				}
				concrete = append(concrete, v)
			}
		}
	}
	sqlOut, params, err := assembleMakeSQL(makeSQLNode{sql: sqlText, params: concrete})
	if err != nil {
		return Rendered{}, err
	}
	return Rendered{SQL: renderPlaceholders(sqlOut, dialectName), Params: params}, nil
}

// ShortCircuitReason is why a transaction did not commit (a gate short-circuit, not a driver error).
type ShortCircuitReason string

const (
	ReasonRequiresAbsent      ShortCircuitReason = "requires_absent"
	ReasonUniqueCollision     ShortCircuitReason = "unique_collision"
	ReasonIdempotentDuplicate ShortCircuitReason = "idempotent_duplicate"
)

// ShortCircuit records which gate stopped the transaction.
type ShortCircuit struct {
	StatementID string
	Reason      ShortCircuitReason
}

// TransactionResult is the structured outcome of executing a TransactionPlan (write-runtime.ts).
type TransactionResult struct {
	Committed    bool
	ShortCircuit *ShortCircuit
	Entity       *bc.Obj // body-write RETURNING row ($.entity), or nil
	Executed     []string
	// ReturnedRows: for a BATCH write (gate-free, ref-free plan, entityFrom empty) the RETURNING rows
	// of every body statement in order (createMany's "all created rows"); nil for a gate-first Command.
	ReturnedRows [][]bc.Value
}

// execStatementResult is one statement's rows + affected-row count.
type execStatementResult struct {
	rows    []bc.Value
	changes int64
}

// gateShortCircuit evaluates a gate rule on a statement result → the short-circuit reason, or ""
// to continue (write-runtime.ts gateShortCircuit).
func gateShortCircuit(gate string, r execStatementResult) ShortCircuitReason {
	switch gate {
	case "existsElseRollback":
		if len(r.rows) == 0 {
			return ReasonRequiresAbsent
		}
	case "insertedElseRollback":
		if r.changes == 0 {
			return ReasonUniqueCollision
		}
	case "insertedElseNoop":
		if r.changes == 0 {
			return ReasonIdempotentDuplicate
		}
	}
	return ""
}

// execTxStatement renders + executes one statement's makeSQL op in the given scope (tx.ts
// execStatement / renderStatement): evaluate the deferred Expression-IR params, assemble + render
// placeholders (the SAME assemble the read path uses). A SELECT (leading verb) or a RETURNING write
// returns rows (changes = row count); a non-returning write returns changes = affected. A driver
// error is a mapped SqlFailure.
func execTxStatement(db SQLDB, op *bc.JObj, scope *bc.Obj, dialect Dialect) (execStatementResult, error) {
	rendered, err := renderTxOp(op, scope, dialect.Name())
	if err != nil {
		return execStatementResult{}, err
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	head := rendered.SQL
	if len(head) > 8 {
		head = head[:8]
	}
	hasReturn := selectPrefixRe.MatchString(head) || returningRe.MatchString(rendered.SQL)
	if hasReturn {
		rows, err := queryRows(db, rendered.SQL, args)
		if err != nil {
			return execStatementResult{}, err
		}
		return execStatementResult{rows: rows, changes: int64(len(rows))}, nil
	}
	changes, _, err := execWrite(db, rendered.SQL, args)
	if err != nil {
		return execStatementResult{}, err
	}
	return execStatementResult{rows: nil, changes: changes}, nil
}

// txPlanStatements reads the plan's ordered statements, entityFrom, and returns them.
type txStatement struct {
	id    string
	role  string // 'body' / 'gate:*' / 'derive' / 'edge' / 'emit'
	gate  string // "" when not a gate
	binds string // "" when this statement's row is not bound for downstream $.ref (WS8a composite)
	op    *bc.JObj
}

func parseTxPlan(plan *bc.JObj) (statements []txStatement, entityFrom string, err error) {
	if ef, ok := plan.Get("entityFrom"); ok {
		entityFrom, _ = ef.(string)
	}
	stmtsN, ok := plan.Get("statements")
	if !ok {
		return nil, "", fmt.Errorf("scp write: transaction plan has no statements")
	}
	stmts, ok := stmtsN.([]bc.JNode)
	if !ok {
		return nil, "", fmt.Errorf("scp write: transaction plan statements is not an array")
	}
	for _, sN := range stmts {
		s, ok := sN.(*bc.JObj)
		if !ok {
			return nil, "", fmt.Errorf("scp write: transaction statement is not an object")
		}
		var st txStatement
		if idN, ok := s.Get("id"); ok {
			st.id, _ = idN.(string)
		}
		if rN, ok := s.Get("role"); ok {
			st.role, _ = rN.(string)
		}
		if gN, ok := s.Get("gate"); ok {
			st.gate, _ = gN.(string)
		}
		if bN, ok := s.Get("binds"); ok {
			st.binds, _ = bN.(string)
		}
		if opN, ok := s.Get("op"); ok {
			st.op, _ = opN.(*bc.JObj)
		}
		if st.op == nil {
			return nil, "", fmt.Errorf("scp write: transaction statement '%s' has no op", st.id)
		}
		statements = append(statements, st)
	}
	return statements, entityFrom, nil
}

// TxDB is the transaction-capable database/sql surface (a *sql.DB satisfies Begin).
type TxDB interface {
	Begin() (*sql.Tx, error)
}

// ExecuteTransactionBundle executes a §8 bundle's derived transaction plan (spec §6/§8) as ONE
// real transaction with gate-first short-circuit (runtime.ts executeTransactionBundle +
// write-runtime.ts executeTransaction). It consumes ONLY the serialized plan (pure JSON) + the
// render pipeline + a SQL driver, never re-deriving the plan.
func ExecuteTransactionBundle(bundle *SqlBundle, input *bc.Obj, db TxDB) (TransactionResult, error) {
	if bundle.Transaction == nil {
		return TransactionResult{}, fmt.Errorf("scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)")
	}
	dialect, err := DialectFor(bundle.Dialect)
	if err != nil {
		return TransactionResult{}, err
	}
	return executeTransaction(db, bundle.Transaction, input, dialect)
}

// executeTransaction runs a plan as one transaction with gate-first short-circuit (byte-true to
// write-runtime.ts executeTransaction): statements run in the plan's fixed order; a failing gate
// ROLLBACKs and the remaining statements never execute. On success COMMITs and returns the
// `$.entity` RETURNING row.
func executeTransaction(db TxDB, plan *bc.JObj, input *bc.Obj, dialect Dialect) (TransactionResult, error) {
	statements, entityFrom, err := parseTxPlan(plan)
	if err != nil {
		return TransactionResult{}, err
	}
	tx, err := db.Begin()
	if err != nil {
		return TransactionResult{}, mapSqliteError(err)
	}

	// The evolving scope: input names at the top level (bc flat scope) + the body RETURNING row
	// exposed under `__entity` once the body runs. Defaults live in the plan/schema (no ad-hoc code
	// default is injected here).
	scope := bc.NewObj()
	for _, k := range input.Keys {
		scope.Set(k, input.Vals[k])
	}
	var entity *bc.Obj
	var executed []string
	// Batch mode (createMany/updateMany/deleteMany): gate-free, ref-free plan (entityFrom empty, every
	// statement a plain body) — accumulate each body statement's RETURNING rows in order.
	isBatch := entityFrom == ""
	for _, s := range statements {
		if s.gate != "" || s.binds != "" || s.role != "body" {
			isBatch = false
			break
		}
	}
	var returnedRows [][]bc.Value

	for _, stmt := range statements {
		result, err := execTxStatement(tx, stmt.op, scope, dialect)
		if err != nil {
			_ = tx.Rollback()
			return TransactionResult{}, err
		}
		executed = append(executed, stmt.id)

		// Gate-first: a failing gate short-circuits — ROLLBACK and STOP (tail never executes).
		if stmt.gate != "" {
			if reason := gateShortCircuit(stmt.gate, result); reason != "" {
				if rbErr := tx.Rollback(); rbErr != nil {
					return TransactionResult{}, mapSqliteError(rbErr)
				}
				return TransactionResult{
					Committed:    false,
					ShortCircuit: &ShortCircuit{StatementID: stmt.id, Reason: reason},
					Entity:       nil,
					Executed:     executed,
				}, nil
			}
		}

		// Capture the SOLE body RETURNING row as `$.entity` (WS5 single-write back-compat).
		if stmt.id == entityFrom {
			if len(result.rows) > 0 {
				if row, ok := result.rows[0].(*bc.Obj); ok {
					entity = row
					scope.Set(entityRoot, entity)
				}
			}
		}

		// WS8a composite: bind THIS statement's RETURNING row under its `binds` name so a later
		// `$.ref.<binds>.<field>` resolves against it (the tx-DAG data-dependency edge). Self-
		// describing — the runtime binds the row the plan told it to; no re-derivation.
		if stmt.binds != "" && len(result.rows) > 0 {
			if row, ok := result.rows[0].(*bc.Obj); ok {
				scope.Set(stmt.binds, row)
			}
		}

		if isBatch && stmt.role == "body" && len(result.rows) > 0 {
			returnedRows = append(returnedRows, result.rows)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return TransactionResult{}, mapSqliteError(err)
	}
	return TransactionResult{Committed: true, Entity: entity, Executed: executed, ReturnedRows: returnedRows}, nil
}
