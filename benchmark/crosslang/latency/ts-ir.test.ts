// The ts-IR INTERPRETER cell — the HONEST interpreter baseline. It runs each op through litedbmodel's
// SHIPPING boxed read-graph interpreter (`executeBundle`/`readBundle` → `executeReadGraph` /
// `executeStaticWrite`): per call it walks the compiled read-graph IR, BOXES values, renders each node's
// SQL from the makeSQL fragments, executes on better-sqlite3, and materializes rows — the exact path the
// native codegen replaces. (bc's generic `runBehavior` interpreter is NOT wired to SQL anywhere in this
// repo — `executeReadGraph` is litedbmodel's own IR-walking runtime and the fairer, real baseline.)
//
// Times the WHOLE hot path (build input → interpret = box + assemble SQL + exec + materialize) and writes
// RAW per-iteration samples (µs) to `.results/ts_ir.csv`. Reads run on the shared read seed; writes run
// on a fresh copy of the write seed with a UNIQUE input per iteration.
import { describe, it, expect } from 'vitest';
import { writeFileSync, copyFileSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import { executeBundle, readBundle } from '../../../src/scp/index';
import { benchOps } from './behaviors';

const HERE = __dirname;
const ART = join(HERE, '.artifacts');
const RESULTS = join(HERE, '.results');
const WARMUP = Number(process.env.BENCH_WARMUP ?? 1000);
const ITERS = Number(process.env.BENCH_ITERS ?? 10000);

/** ns → µs, from a monotonic clock (the whole hot path per iteration). */
function timeUs(fn: () => void): number {
  const t0 = process.hrtime.bigint();
  fn();
  return Number(process.hrtime.bigint() - t0) / 1000;
}

describe('ts-IR interpreter cell', () => {
  it(`measures 4 ops × ${ITERS} iters through executeBundle/readBundle → .results/ts_ir.csv`, () => {
    const ops = new Map(benchOps().map((o) => [o.id, o]));
    const readDb = new Database(join(ART, 'read.db'), { readonly: true });
    const writeSeed = join(ART, 'write.db');
    const writeCopy = join(ART, 'ts_write.db');
    if (existsSync(writeCopy)) rmSync(writeCopy);
    copyFileSync(writeSeed, writeCopy);
    const writeDb = new Database(writeCopy);

    const samples: string[] = ['op,us'];
    const record = (op: string, us: number) => samples.push(`${op},${us}`);

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
      for (let i = 0; i < ITERS; i++) {
        const tag = `${i}_${process.hrtime.bigint()}`;
        record('createuser', timeUs(() => run(tag)));
      }
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
      for (let i = 0; i < ITERS; i++) {
        const tag = `${i}_${process.hrtime.bigint()}`;
        record('createmany', timeUs(() => run(tag)));
      }
    }

    readDb.close();
    writeDb.close();
    rmSync(writeCopy);
    writeFileSync(join(RESULTS, 'ts_ir.csv'), samples.join('\n') + '\n');
    expect(samples.length).toBe(1 + 4 * ITERS);
  });
});
