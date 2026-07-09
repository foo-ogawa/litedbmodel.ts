// litedbmodel v2 SCP — write-time transaction runtime (Go port of src/scp/write-runtime.ts; spec §6).
//
// Executes a derived TransactionPlan (carried pure-JSON in the §8 bundle) against a real SQL
// transaction with gate-first short-circuit. It renders each ordered statement with the SAME
// normative RenderOperation the read path uses, and drives an explicit BEGIN / COMMIT / ROLLBACK
// envelope through the SAME database/sql seam — no separate interpreter, no re-derivation of the
// plan (the plan is honored, not rebuilt, spec §6). Ported byte-true to write-runtime.ts incl. the
// three gate rules and the `$.entity` RETURNING-row exposure.

package litedbmodel_runtime

import (
	"database/sql"
	"fmt"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

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

// execTxStatement renders + executes one statement's compiled op in the given scope
// (write-runtime.ts execStatement). SELECT/RETURNING returns rows (changes = row count);
// a non-returning write returns changes = affected. A driver error is a mapped SqlFailure.
func execTxStatement(db SQLDB, op *bc.JObj, scope *bc.Obj, dialect Dialect) (execStatementResult, error) {
	rendered, err := RenderOperation(op, scope, dialect)
	if err != nil {
		return execStatementResult{}, err
	}
	args := make([]any, len(rendered.Params))
	for i, p := range rendered.Params {
		args[i] = toDriverParam(p)
	}
	hasReturn := operationComponent(op) == "Select" || returningRe.MatchString(rendered.SQL)
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
	id   string
	gate string // "" when not a gate
	op   *bc.JObj
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
		if gN, ok := s.Get("gate"); ok {
			st.gate, _ = gN.(string)
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

		// Capture the body RETURNING row as `$.entity` for the derive/edges/emits stages.
		if stmt.id == entityFrom {
			if len(result.rows) > 0 {
				if row, ok := result.rows[0].(*bc.Obj); ok {
					entity = row
					scope.Set(entityRoot, entity)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return TransactionResult{}, mapSqliteError(err)
	}
	return TransactionResult{Committed: true, Entity: entity, Executed: executed}, nil
}
