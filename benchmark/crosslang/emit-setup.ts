// ════════════════════════════════════════════════════════════════════════════
// Cross-lang ORM-bench SETUP emitter — the SINGLE seed source for EVERY cell.
// ════════════════════════════════════════════════════════════════════════════
//
//   npx tsx benchmark/crosslang/emit-setup.ts
//
// Reads the ONE schema+seed SSoT (`orm-domain.ts`: ddl / deleteStatements / seedStatements /
// dropStatements) and writes, per dialect, a `.setup/<dialect>.json` that ALL EIGHT bench cells
// (rust/go/python/php × native/sdk) load at runtime and exec VERBATIM. No cell hand-writes a seed:
// each execs the identical `schema` (once, at open) + `delete`+`insert` (the canonical 110-user
// fixture, re-applied per op). This is data, not codegen — one artifact, every language reads it.
//
// The `insert` statements are the parameterised `seedStatements` rendered to LITERAL SQL (params
// substituted) so a cell needs ZERO param-binding to seed — it just execs strings. Rendering is
// deterministic + dialect-correct (pg booleans → TRUE/FALSE; strings single-quoted with `''` escape).

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ddl, deleteStatements, seedStatements, dropStatements, pgSeqResetStatements,
  type OrmDialect, type SeedStmt,
} from './orm-domain';

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(HERE, '.setup');
const DIALECTS: OrmDialect[] = ['sqlite', 'mysql', 'postgres'];

// Render one parameterised seed statement to a LITERAL-SQL string (the `?` placeholders never occur
// inside the domain's string literals, so a positional `?`→param substitution is exact).
function renderLiteral(stmt: SeedStmt): string {
  let i = 0;
  return stmt.sql.replace(/\?/g, () => {
    const p = stmt.params[i++];
    if (typeof p === 'number') return String(p);
    if (typeof p === 'boolean') return p ? 'TRUE' : 'FALSE';
    if (p === null || p === undefined) return 'NULL';
    return `'${String(p).replace(/'/g, "''")}'`;
  });
}

interface SetupDoc {
  readonly dialect: OrmDialect;
  readonly users: number; // the canonical user count — a self-describing proof knob for every cell.
  readonly schema: string[]; // drop + create, applied ONCE at open.
  readonly delete: string[]; // empty every table (child→parent), applied before each re-seed.
  readonly insert: string[]; // the canonical fixture as literal INSERTs (+ pg SERIAL fixups).
}

mkdirSync(OUT_DIR, { recursive: true });
for (const dialect of DIALECTS) {
  const inserts = seedStatements(dialect).map(renderLiteral);
  if (dialect === 'postgres') inserts.push(...pgSeqResetStatements());
  const doc: SetupDoc = {
    dialect,
    users: seedStatements(dialect).filter((s) => s.sql.startsWith('INSERT INTO benchmark_users')).length,
    schema: [...dropStatements(dialect), ...ddl(dialect)],
    delete: deleteStatements(dialect),
    insert: inserts,
  };
  const path = join(OUT_DIR, `${dialect}.json`);
  writeFileSync(path, JSON.stringify(doc, null, 2) + '\n');
  console.error(`  ✓ ${path} — users=${doc.users}, schema=${doc.schema.length}, insert=${doc.insert.length}`);
}
