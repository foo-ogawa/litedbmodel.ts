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
// to continue (tx.ts gateShortCircuit). An unknown / forward-incompatible gate rule is a FAIL-CLOSED
// error (aligned with Python + Rust + TS + PHP): a corrupt gate MUST NOT silently continue (fail-open
// would skip a malformed gate and let the write COMMIT).
func gateShortCircuit(gate string, r execStatementResult) (ShortCircuitReason, error) {
	switch gate {
	case "existsElseRollback":
		if len(r.rows) == 0 {
			return ReasonRequiresAbsent, nil
		}
	case "insertedElseRollback":
		if r.changes == 0 {
			return ReasonUniqueCollision, nil
		}
	case "insertedElseNoop":
		if r.changes == 0 {
			return ReasonIdempotentDuplicate, nil
		}
	default:
		return "", fmt.Errorf("scp write: unknown gate rule '%s'", gate)
	}
	return "", nil
}

// execTxStatement renders + executes one statement's makeSQL op in the given scope (tx.ts
// execStatement / renderStatement): evaluate the deferred Expression-IR params, assemble + render
// placeholders (the SAME assemble the read path uses). A SELECT (leading verb) or a RETURNING write
// returns rows (changes = row count); a non-returning write returns changes = affected. A driver
// error is a mapped SqlFailure.
//
// Every statement runs through the CENTRAL SEAM on the tx-scoped ctx (§2/§3): the seam resolves the
// tx's OWNED connection (per-execution ownership) and runs there — a SELECT/RETURNING via Execute, a
// non-returning write via Run. No direct db.Query / db.Exec — the tx-owned *sql.Tx is the ONLY driver
// contact (both carry WriteIntent: a tx statement always targets the writer / tx connection).
func execTxStatement(ctx *ExecutionContext, op *bc.JObj, scope *bc.Obj, dialect Dialect) (execStatementResult, error) {
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
		rows, err := Execute(ctx, rendered.SQL, args, WriteIntent())
		if err != nil {
			return execStatementResult{}, err
		}
		return execStatementResult{rows: rows, changes: int64(len(rows))}, nil
	}
	info, err := Run(ctx, rendered.SQL, args, WriteIntent())
	if err != nil {
		return execStatementResult{}, err
	}
	return execStatementResult{rows: nil, changes: info.Changes}, nil
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
	// Backward-compat wrapper (§6): wrap `db` in a thin ExecutionContext whose ConnectionFor resolves
	// the tx-owned *sql.Tx once WithTransactionDecided pins it. `db` must be BOTH the base connection
	// provider (SQLDB) and tx-capable (TxDB) — a *sql.DB is both.
	baseDB, ok := db.(SQLDB)
	if !ok {
		return TransactionResult{}, fmt.Errorf("scp write: transaction db is not a base SQLDB connection provider")
	}
	return executeTransactionCtx(ContextForDB(baseDB), db, bundle.Transaction, input, dialect)
}

// executeTransactionCtx runs a plan as one transaction with **per-execution connection ownership**
// (§3) + gate-first short-circuit (byte-true to write-runtime.ts executeTransaction). It hands the
// whole plan to [WithTransactionDecided], which acquires ONE *sql.Tx (Go's connection-owning
// primitive — BEGIN issued by database/sql), pins it into a tx-scoped ctx so every statement resolves
// THAT connection via the seam, and COMMITs / ROLLBACKs it on the SAME *sql.Tx per the body's
// decision. Statements run in the plan's fixed order; a failing gate returns a ROLLBACK decision
// (committed:false — a legitimate outcome, NOT an error) and the tail never executes; a driver error
// returns an error (⇒ rollback + re-raise). On success COMMITs and returns the `$.entity` RETURNING
// row. Concurrent transactions each own a DISTINCT *sql.Tx ⇒ isolated (no shared-tx state).
func executeTransactionCtx(ctx *ExecutionContext, db TxDB, plan *bc.JObj, input *bc.Obj, dialect Dialect) (TransactionResult, error) {
	statements, entityFrom, err := parseTxPlan(plan)
	if err != nil {
		return TransactionResult{}, err
	}

	// Batch mode (createMany/updateMany/deleteMany): gate-free, ref-free plan (entityFrom empty, every
	// statement a plain body) — accumulate each body statement's RETURNING rows in order.
	isBatch := entityFrom == ""
	for _, s := range statements {
		if s.gate != "" || s.binds != "" || s.role != "body" {
			isBatch = false
			break
		}
	}

	return WithTransactionDecided(ctx, db, func(txCtx *ExecutionContext) (TransactionResult, TxDecision, error) {
		// The evolving scope: input names at the top level (bc flat scope) + the body RETURNING row
		// exposed under `__entity` once the body runs. Defaults live in the plan/schema (no ad-hoc
		// code default is injected here).
		scope := bc.NewObj()
		for _, k := range input.Keys {
			scope.Set(k, input.Vals[k])
		}
		var entity *bc.Obj
		var executed []string
		var returnedRows [][]bc.Value

		for _, stmt := range statements {
			result, err := execTxStatement(txCtx, stmt.op, scope, dialect)
			if err != nil {
				// A driver error ⇒ the combinator ROLLBACKs the owned *sql.Tx + re-raises.
				return TransactionResult{}, Commit(), err
			}
			executed = append(executed, stmt.id)

			// Gate-first: a failing gate short-circuits — ROLLBACK and STOP (tail never executes).
			if stmt.gate != "" {
				reason, gErr := gateShortCircuit(stmt.gate, result)
				if gErr != nil {
					// Unknown gate rule (fail-closed): ROLLBACK + surface the error — never COMMIT.
					return TransactionResult{}, Commit(), gErr
				}
				if reason != "" {
					// A failed gate: the combinator ROLLBACKs the owned *sql.Tx and returns this
					// value (committed:false) — a legitimate outcome, NOT an error.
					return TransactionResult{
						Committed:    false,
						ShortCircuit: &ShortCircuit{StatementID: stmt.id, Reason: reason},
						Entity:       nil,
						Executed:     executed,
					}, Rollback(), nil
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

		// All statements succeeded: COMMIT the owned *sql.Tx and return the entity.
		return TransactionResult{Committed: true, Entity: entity, Executed: executed, ReturnedRows: returnedRows}, Commit(), nil
	})
}
