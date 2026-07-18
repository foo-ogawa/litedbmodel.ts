// Verify the 19 ops: (1) compile, (2) generate rust+go, (3) run via the mode-2 oracle. Reports per-op
// status so any op whose SCP shape doesn't compile/generate/run is surfaced (not silently dropped).
// Run: npx tsx benchmark/crosslang/verify.ts
import Database from 'better-sqlite3';
import { generateCodegenArtifact, executeBundle, readBundle, executeTransactionBundle } from '../../dist/scp/index.cjs';
import { buildOps } from './ops';
import { ddl, seedStatements } from './orm-domain';

const REGISTERED = ['go-typed-native', 'php', 'python', 'rust-typed-native', 'typescript', 'typescript-native', 'typescript-typed'];

function freshDb() {
  const db = new Database(':memory:');
  for (const s of ddl('sqlite')) db.exec(s);
  const tx = db.transaction(() => {
    for (const s of seedStatements('sqlite')) db.prepare(s.sql).run(...(s.params as never[]));
  });
  tx();
  return db;
}

function short(e: unknown): string {
  return (e instanceof Error ? e.message : String(e)).split('\n')[0].slice(0, 100);
}

const ops = buildOps();
let genOk = 0;
let runOk = 0;
const fails: string[] = [];
console.log(`op                    kind       gen(rust/go)  oracle-run`);
for (const op of ops) {
  let g = '';
  try {
    generateCodegenArtifact(op.bundle, 'rust', REGISTERED, op.resolve);
    generateCodegenArtifact(op.bundle, 'go', REGISTERED, op.resolve);
    g = 'OK';
    genOk++;
  } catch (e) {
    g = 'FAIL: ' + short(e);
    fails.push(`${op.id} gen: ${short(e)}`);
  }
  let r = '';
  const db = freshDb();
  try {
    if (op.kind === 'tx') {
      const res = executeTransactionBundle(op.bundle, op.input as never, { db: db as never });
      r = `OK committed=${(res as { committed: boolean }).committed}`;
    } else if (op.withRel) {
      const rows = readBundle(op.bundle, op.input as never, { db: db as never, with: { [op.withRel]: true } as never }) as unknown[];
      r = `OK rows=${rows.length}`;
    } else {
      const out = executeBundle(op.bundle, op.input as never, { db: db as never });
      r = `OK ${Array.isArray(out) ? `rows=${out.length}` : typeof out}`;
    }
    runOk++;
  } catch (e) {
    r = 'FAIL: ' + short(e);
    fails.push(`${op.id} run: ${short(e)}`);
  }
  db.close();
  console.log(`${op.id.padEnd(22)}${op.kind.padEnd(11)}${g.slice(0, 12).padEnd(14)}${r}`);
}
console.log(`\ngen rust+go: ${genOk}/${ops.length}   oracle-run: ${runOk}/${ops.length}`);
if (fails.length) {
  console.log('\nFAILURES:');
  for (const f of fails) console.log('  - ' + f);
  process.exit(1);
}
