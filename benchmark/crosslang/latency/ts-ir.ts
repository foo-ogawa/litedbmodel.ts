// The ts-IR INTERPRETER cell — a PLAIN STANDALONE process (run via tsx), uniform with the go/rust
// binaries: self-measures the hot path and writes a flat CSV. It runs each op through litedbmodel's
// SHIPPING boxed read-graph interpreter (`executeBundle`/`readBundle` → `executeReadGraph` /
// `executeStaticWrite`): per call it walks the compiled read-graph IR, BOXES values, renders each node's
// SQL from the makeSQL fragments, executes on better-sqlite3, and materializes rows — the exact path the
// native codegen replaces. (bc's generic `runBehavior` interpreter is NOT wired to SQL anywhere in this
// repo — `executeReadGraph` is litedbmodel's own IR-walking runtime and the fairer, real baseline.)
//
// FAIRNESS: litedbmodel's default runtime re-prepares each call (`driver.prepare(sql).all(…)`); to
// isolate the codegen-vs-interpretation difference from the SHARED sql-parse cost (the native cells now
// reuse prepared statements via prepare_cached / a stmt cache), this cell wraps the driver in a
// prepared-statement cache too — the SAME optimization, applied symmetrically. Everything else is the
// real interpreter.
//
// Usage: tsx ts-ir.ts <read_db> <write_db> <warmup> <iters> <out_csv>
import { writeFileSync, copyFileSync, rmSync, existsSync } from 'node:fs';
import Database from 'better-sqlite3';
import { executeBundle, readBundle } from '../../../dist/scp/index.cjs'; // bundled instance (tsx-loadable)
import { benchOps, REL_SCALES } from './behaviors';

/** Wrap a better-sqlite3 Database so `.prepare(sql)` returns a CACHED statement (reused across calls),
 * forwarding every other method to the real db. The interpreter still walks the IR + boxes per call —
 * only the redundant re-parse of the static SQL is removed, symmetric with the native cells. */
function cachingDb(db: InstanceType<typeof Database>): InstanceType<typeof Database> {
  const cache = new Map<string, unknown>();
  return new Proxy(db, {
    get(target, prop, recv) {
      if (prop === 'prepare') {
        return (sql: string) => {
          let st = cache.get(sql);
          if (st === undefined) { st = (target as never as { prepare(s: string): unknown }).prepare(sql); cache.set(sql, st); }
          return st;
        };
      }
      const v = Reflect.get(target, prop, recv);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  }) as never;
}

function timeUs(fn: () => void): number {
  const t0 = process.hrtime.bigint();
  fn();
  return Number(process.hrtime.bigint() - t0) / 1000;
}

function main(): void {
  const [readDbPath, writeSeed, relDbPath, warmupS, itersS, outCsv] = process.argv.slice(2);
  if (!outCsv) throw new Error('usage: tsx ts-ir.ts <read_db> <write_db> <rel_db> <warmup> <iters> <out_csv>');
  const WARMUP = Number(warmupS);
  const ITERS = Number(itersS);

  const ops = new Map(benchOps().map((o) => [o.id, o]));
  const readDb = cachingDb(new Database(readDbPath, { readonly: true }));
  const writeCopy = `${writeSeed}.ts.work`;
  if (existsSync(writeCopy)) rmSync(writeCopy);
  copyFileSync(writeSeed, writeCopy);
  const writeDb = cachingDb(new Database(writeCopy));

  const samples: string[] = ['op,us'];
  const record = (op: string, us: number) => samples.push(`${op},${us.toFixed(3)}`);

  // ── findunique (point read) — executeReadGraph (boxed) ──
  {
    const b = ops.get('findunique')!.bundle;
    const run = (i: number) => executeBundle(b, { email: `user${(i % 100) + 1}@example.com` } as never, { db: readDb as never });
    for (let i = 0; i < WARMUP; i++) run(i);
    for (let i = 0; i < ITERS; i++) record('findunique', timeUs(() => run(i)));
  }
  // ── relsingle (batched relation) — readBundle with the 'comments' prefetch (parent + 1 batched child) ──
  {
    const b = ops.get('relsingle')!.bundle;
    const run = () => readBundle(b, { author_id: 7 } as never, { db: readDb as never, with: { comments: true } as never });
    for (let i = 0; i < WARMUP; i++) run();
    for (let i = 0; i < ITERS; i++) record('relsingle', timeUs(run));
  }
  // ── createuser (single write, RETURNING) — executeStaticWrite (boxed) — UNIQUE email per iter ──
  {
    const b = ops.get('createuser')!.bundle;
    const run = (tag: string) => executeBundle(b, { email: `cu_${tag}@example.com`, name: 'Bench' } as never, { db: writeDb as never });
    for (let i = 0; i < WARMUP; i++) run(`w${i}`);
    for (let i = 0; i < ITERS; i++) record('createuser', timeUs(() => run(`${i}_${process.hrtime.bigint()}`)));
  }
  // ── createmany (batch write: ONE json_each INSERT for 10) — executeStaticWrite — UNIQUE rows per iter ──
  {
    const b = ops.get('createmany')!.bundle;
    const run = (tag: string) => {
      const emails = Array.from({ length: 10 }, (_, k) => `cm_${tag}_${k}@example.com`);
      const names = Array.from({ length: 10 }, (_, k) => `BM_${tag}_${k}`);
      executeBundle(b, { emails, names } as never, { db: writeDb as never });
    };
    for (let i = 0; i < WARMUP; i++) run(`w${i}`);
    for (let i = 0; i < ITERS; i++) record('createmany', timeUs(() => run(`${i}_${process.hrtime.bigint()}`)));
  }

  // ── SCALED relation sweep — the SAME relsingle op at growing child counts (10 → 10000) ──
  {
    const b = ops.get('relsingle')!.bundle;
    const relDb = cachingDb(new Database(relDbPath, { readonly: true }));
    for (const sc of REL_SCALES) {
      const run = () => readBundle(b, { author_id: sc.author } as never, { db: relDb as never, with: { comments: true } as never });
      const wu = Math.min(WARMUP, sc.iters);
      for (let i = 0; i < wu; i++) run();
      for (let i = 0; i < sc.iters; i++) record(sc.id, timeUs(run));
    }
    (relDb as never as { close(): void }).close();
  }

  (readDb as never as { close(): void }).close();
  (writeDb as never as { close(): void }).close();
  rmSync(writeCopy);
  writeFileSync(outCsv, samples.join('\n') + '\n');
  process.stderr.write(`ts-IR bench done: 4 ops × ${ITERS} iters → ${outCsv}\n`);
}

main();
