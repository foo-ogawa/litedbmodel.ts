// ════════════════════════════════════════════════════════════════════════════
// IR-cell bundle serializer (py + php) — epic #107 P4 / #112.
// ════════════════════════════════════════════════════════════════════════════
//
// The py/php "ir" cell is litedbmodel's SHIPPED python / php runtime INTERPRETER
// (execute_bundle / read_bundle / execute_transaction_bundle) executing the 19 bench
// ops. That interpreter consumes the language-neutral, JSON-serializable §8 bundle —
// NOT the in-memory TS SqlBundle. This build step serializes each op's bundle to JSON so
// the py/php cells (which never import the TS compiler) can run them, exactly as the
// conformance corpus (conformance/vectors-livedb/livedb.json) feeds the py/php runtimes.
//
// Per-op bundle source:
//   • read / read+rel / single-write / tx (16 ops): ops.ts buildOps(dialect).bundle —
//     the SAME symbolic bundle the native cells + oracle use (input bound at run time).
//   • batch write (createMany/upsertMany/updateMany): the interpreter's write path is
//     execute_transaction_bundle over a TRANSACTION bundle. ops.ts models these as
//     native-codegen STATEMENT bundles whose symbolic `{__batchRows}` marker the py/php
//     static-bundle assembler does not resolve (it is a codegen / TS-static-bundle
//     construct). So — mirroring conformance/gen-livedb.ts exactly — the batch tx bundle
//     is compiled via the PUBLIC compileCreateManyBundle / compileUpdateManyBundle with
//     the fixed bench input baked in (input bound at compile time, as the corpus does).
//     The py/php runtime then runs the REAL tx: BEGIN, the JSON/UNNEST batch INSERT,
//     the mysql RETURNING re-select emulation, COMMIT.
//
// Emits bundles.json into BOTH adapters/py and adapters/php (each adapter self-contained,
// mirroring the per-language generated/ dirs of the rust/go/ts native cells).
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { compileCreateManyBundle, compileUpdateManyBundle, deriveTransactionPlan } from '../../../../dist/scp/index.cjs';
import { buildOps } from '../../ops';
import { ORM_DIALECTS, type OrmDialect } from '../../contract';

const HERE = dirname(fileURLToPath(import.meta.url));

// The fixed batch inputs — byte-identical to ops.ts (BATCH_EMAILS/BATCH_NAMES + the
// upsertMany conflict prefix + the updateMany id key set).
const BATCH_EMAILS = Array.from({ length: 10 }, (_, i) => `many${i}@bench.com`);
const BATCH_NAMES = Array.from({ length: 10 }, (_, i) => `Many ${i}`);
const UPSERT_EMAILS = ['user1@example.com', 'user2@example.com', ...BATCH_EMAILS.slice(0, 8)];

/** The batch tx bundles (interpreter write path) — compiled via the public API, input baked (gen-livedb pattern). */
function batchTxBundles(dialect: OrmDialect): Record<string, unknown> {
  const createRecs = BATCH_EMAILS.map((email, i) => ({ email, name: BATCH_NAMES[i] }));
  const upsertRecs = UPSERT_EMAILS.map((email, i) => ({ email, name: BATCH_NAMES[i] }));
  const updRecs = Array.from({ length: 10 }, (_, i) => ({ id: i + 1, name: BATCH_NAMES[i] }));
  return {
    createMany: compileCreateManyBundle('CreateMany', { tableName: 'benchmark_users', records: createRecs, rawRecords: createRecs }, dialect),
    upsertMany: compileCreateManyBundle('UpsertMany', { tableName: 'benchmark_users', records: upsertRecs, rawRecords: upsertRecs, onConflict: ['email'], onConflictUpdate: 'all' }, dialect),
    updateMany: compileUpdateManyBundle('UpdateMany', { tableName: 'benchmark_users', keyColumns: ['id'], updateColumns: ['name'], records: updRecs, rawRecords: updRecs } as never, dialect),
  };
}

const BATCH_IDS = new Set(['createMany', 'upsertMany', 'updateMany']);
// Single writes: the py/php interpreter's write path is execute_transaction_bundle. ops.ts models
// these as read/exec bundles (readGraph + statement); running a no-returning write through the
// read-graph works on sqlite but breaks on psycopg (the cursor has no result set to fetch). So wrap
// ops.ts's ALREADY-COMPILED statement (symbolic `?` + Expression-IR params) into a single-body tx
// plan via the PUBLIC deriveTransactionPlan — zero SQL/port re-authoring. entityFrom = the base write,
// so upsert's RETURNING id surfaces as the tx `entity` (→ [{id}]); create/update have no RETURNING (→ null).
const SINGLE_WRITE_IDS = new Set(['create', 'update', 'upsert']);

/** Wrap ops.ts's compiled single-write statement into a single-body transaction bundle. */
function singleWriteTxBundle(op: ReturnType<typeof buildOps>[number], dialect: OrmDialect): unknown {
  const st = (op.bundle as { statement: unknown }).statement;
  const plan = deriveTransactionPlan('create', [{ op: st, label: op.id, name: 'entity', effects: {} }] as never, { effects: {} } as never, dialect);
  return { dialect, name: op.id, statement: st, transaction: plan, optionalHeads: [], relations: {} };
}

interface SerOp {
  readonly kind: string;
  readonly input: Record<string, unknown>;
  readonly withRel: string | null;
  readonly bundle: unknown;
}

function serialize(): Record<string, Record<string, SerOp>> {
  const out: Record<string, Record<string, SerOp>> = {};
  for (const dialect of ORM_DIALECTS) {
    const batch = batchTxBundles(dialect);
    const byOp: Record<string, SerOp> = {};
    for (const op of buildOps(dialect)) {
      let bundle: unknown = op.bundle;
      let input: Record<string, unknown> = op.input;
      if (BATCH_IDS.has(op.id)) {
        bundle = batch[op.id];
        input = {}; // batch input baked into the tx bundle at compile (corpus parity)
      } else if (SINGLE_WRITE_IDS.has(op.id)) {
        bundle = singleWriteTxBundle(op, dialect);
      } else if (op.id === 'filterPaginateSort' && dialect === 'postgres') {
        // The `published` input head is BOOLEAN on postgres (a strict driver rejects int=bool), int on
        // sqlite/mysql — mirror the native cell's per-dialect input literal (rust main.rs fps_published_input).
        input = { ...op.input, published: true };
      }
      byOp[op.id] = { kind: op.kind, input, withRel: op.withRel ?? null, bundle };
    }
    out[dialect] = byOp;
  }
  return out;
}

function main(): void {
  const data = JSON.stringify(serialize(), null, 1);
  for (const adapter of ['py', 'php']) {
    const dir = join(HERE, '..', adapter);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'bundles.json'), data);
  }
  process.stderr.write('ir bundles serialized → adapters/{py,php}/bundles.json\n');
}

main();
