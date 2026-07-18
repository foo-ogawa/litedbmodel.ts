// GEN: emit the go-typed-native modules + the seed DBs + the fairness SQL manifest for the latency bench.
// Run: npx vitest run --config benchmark/crosslang/latency/vitest.config.ts benchmark/crosslang/latency/gen.test.ts
import { describe, it, expect } from 'vitest';
import { writeFileSync, mkdirSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import { generateCodegenArtifact } from '../../../dist/scp/index.cjs'; // SAME bundled instance as behaviors.ts / ts-ir.ts
import { registeredLanguages } from 'behavior-contracts'; // strings only; vitest resolves bc here
import { ddl, seedStatements } from '../orm-domain';
import { benchOps, RESOLVE, REL_SCALES } from './behaviors';

const HERE = __dirname;
const ART = join(HERE, '.artifacts');
const GO_BEHAVIORS = join(HERE, 'go-cell', 'behaviors');
const REGISTERED = registeredLanguages();
// The rust cell reuses rust/e1_native_proof's ALREADY-COMMITTED, proven-native modules (per the
// coordinator: "rust-native cell = your proof crate"). Cross-check that those modules bake the SAME SQL
// as this bench's go modules, so all three cells provably do byte-identical logical work.
const RUST_PROOF_SRC = join(HERE, '..', '..', '..', 'rust', 'e1_native_proof', 'src');
const RUST_MODULE_OF: Record<string, string> = {
  findunique: 'generated_findunique.rs',
  relsingle: 'generated_relsingle.rs',
  createuser: 'generated_createuser.rs',
  createmany: 'generated_createmany.rs',
};

/** Extract every baked SQL literal (`f_sql`/`Sql:`) from a generated module, for the fairness check. */
function bakedSql(code: string): string[] {
  return [...code.matchAll(/(?:f_sql|Sql):\s*"((?:SELECT|INSERT|UPDATE|DELETE)[^"]*)"/g)].map((m) => m[1]);
}

describe('latency-bench gen — go modules + seeds + fairness manifest', () => {
  it('emits go-typed-native modules whose baked SQL matches the rust cell (same logical work)', () => {
    mkdirSync(ART, { recursive: true });
    mkdirSync(GO_BEHAVIORS, { recursive: true });
    const manifest: Record<string, string[]> = {};

    for (const op of benchOps()) {
      const go = generateCodegenArtifact(op.bundle, 'go', REGISTERED, RESOLVE);
      const rust = generateCodegenArtifact(op.bundle, 'rust', REGISTERED, RESOLVE);
      // Each generated go module is STANDALONE (its own BehaviorError/T0/ExpectedSpecVersions) — the go
      // equivalent of the rust crate's per-op `mod`. Put each in its OWN package (behaviors/<op>) so the
      // shared boilerplate does not collide, renaming `package behaviors` → `package <op>`.
      const pkgDir = join(GO_BEHAVIORS, op.id);
      mkdirSync(pkgDir, { recursive: true });
      const goCode = go.module.code.replace(/^package behaviors$/m, `package ${op.id}`);
      writeFileSync(join(pkgDir, 'gen.go'), goCode);
      // FAIRNESS: the go + rust cells bake the SAME SQL from the SAME bundle (only the language differs).
      const goSql = bakedSql(go.module.code);
      const rustSql = bakedSql(rust.module.code);
      expect(goSql).toEqual(rustSql);
      expect(goSql.length).toBeGreaterThan(0);
      // FAIRNESS (rust cell): the proof crate's COMMITTED module the rust bench actually runs bakes the
      // same SQL too (so the rust cell is provably running this op's SQL, not a stale/different one).
      const proofRust = readFileSync(join(RUST_PROOF_SRC, RUST_MODULE_OF[op.id]), 'utf8');
      expect(bakedSql(proofRust)).toEqual(goSql);
      manifest[op.id] = goSql;
      // RUNTIME-FREE at the SOURCE level: the go generated module imports only std packages (like the
      // rust module's `std::cell`), NEVER a bc runtime. Comment-strip first (the provenance comment names
      // bc by design), then assert no reference to the behavior-contracts go runtime package remains
      // (`coderuntime` / `dslcontracts` / the `behavior-contracts/go` import path).
      const stripped = go.module.code.replace(/\/\/[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '');
      for (const bcPkg of ['coderuntime', 'dslcontracts', 'behavior-contracts/go']) {
        expect(stripped).not.toContain(bcPkg);
      }
    }
    writeFileSync(join(ART, 'baked-sql.json'), JSON.stringify(manifest, null, 2));
  });

  it('writes the shared seed DBs (read seed + write seed) every cell copies', () => {
    // READ seed — the orm-domain dataset (users/posts/comments) the read ops query.
    const readDb = join(ART, 'read.db');
    if (existsSync(readDb)) rmSync(readDb);
    const rdb = new Database(readDb);
    for (const s of ddl('sqlite')) rdb.exec(s);
    for (const s of seedStatements('sqlite')) rdb.prepare(s.sql).run(...(s.params as never[]));
    rdb.close();

    // WRITE seed — a small clean users table (3 rows); the write ops insert UNIQUE rows per iteration.
    const writeDb = join(ART, 'write.db');
    if (existsSync(writeDb)) rmSync(writeDb);
    const wdb = new Database(writeDb);
    for (const s of ddl('sqlite')) wdb.exec(s);
    for (let id = 1; id <= 3; id++) wdb.prepare('INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)').run(id, `user${id}@example.com`, `User ${id}`);
    wdb.close();

    expect(readFileSync(join(ART, 'baked-sql.json'), 'utf8').length).toBeGreaterThan(0);
    expect(existsSync(readDb)).toBe(true);
    expect(existsSync(writeDb)).toBe(true);
  });

  it('writes the SCALED relation seed (rel.db) — one author per child-count scale', () => {
    const relDb = join(ART, 'rel.db');
    if (existsSync(relDb)) rmSync(relDb);
    const db = new Database(relDb);
    for (const s of ddl('sqlite')) db.exec(s);
    // INDEX the join keys — without them every relation call full-scans the 11k-comment table, and that
    // scan (identical for all cells) DOMINATES + buries the per-cell decode/clone/stitch cost we want to
    // measure. With the indexes the query is an index-seek fetching exactly the N children, so the
    // measured latency is the per-cell processing of those N rows — where the O(N) clone/alloc shows.
    db.exec('CREATE INDEX idx_posts_author ON benchmark_posts(author_id)');
    db.exec('CREATE INDEX idx_comments_post ON benchmark_comments(post_id)');
    const insPost = db.prepare('INSERT INTO benchmark_posts (id, title, author_id) VALUES (?, ?, ?)');
    const insComment = db.prepare('INSERT INTO benchmark_comments (id, body, post_id) VALUES (?, ?, ?)');
    let pid = 0;
    let cid = 0;
    const tx = db.transaction(() => {
      for (const sc of REL_SCALES) {
        for (let p = 0; p < sc.posts; p++) {
          pid++;
          insPost.run(pid, `post-${pid}`, sc.author);
          for (let c = 0; c < sc.commentsPerPost; c++) {
            cid++;
            insComment.run(cid, `comment-${cid}`, pid);
          }
        }
      }
    });
    tx();
    // Assert each scale's author yields exactly its declared child count (fairness: the seed IS the shape).
    for (const sc of REL_SCALES) {
      const n = (db.prepare(
        'SELECT COUNT(*) AS n FROM benchmark_comments c JOIN benchmark_posts p ON c.post_id = p.id WHERE p.author_id = ?',
      ).get(sc.author) as { n: number }).n;
      expect(n).toBe(sc.children);
    }
    db.close();
    expect(existsSync(relDb)).toBe(true);
  });
});
