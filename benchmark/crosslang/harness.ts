// ════════════════════════════════════════════════════════════════════════════
// Cross-language harness (epic #44) — subprocess orchestration + aggregation.
// ════════════════════════════════════════════════════════════════════════════
//
// Spawns ONE adapter subprocess per live cell, drives it through the contract, and
// aggregates the raw samples into the MatrixResult (metrics.ts owns the math). A
// cell that fails (missing toolchain / build error / protocol error) is captured
// as an error row rather than aborting the whole matrix — every declared cell is
// accounted for honestly.

import { spawn, type ChildProcess } from 'node:child_process';
import { statSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
import {
  encodeMessage, decodeMessages, CROSSLANG_CASE_IDS, CROSSLANG_MICRO_CASE_IDS,
  type Request, type Response, type CrosslangCaseId, type CrosslangMicroCaseId,
} from './contract.js';
import {
  summarizeLatency, throughputOpsPerSec, coldStartMs,
  type CellResult, type CaseResult, type MatrixResult,
} from './metrics.js';
import { liveCells, type CellSpec } from './registry.js';

export interface HarnessConfig {
  warmup: number;
  iterations: number;
  throughputIterations: number;
  concurrency: number;
  microWarmup: number;
  microIterations: number;
  cases?: CrosslangCaseId[];
  responseTimeoutMs?: number;
}

export const DEFAULT_CONFIG: HarnessConfig = {
  warmup: 200,
  iterations: 2000,
  throughputIterations: 2000,
  concurrency: 1,
  microWarmup: 1000,
  microIterations: 20000,
  responseTimeoutMs: 180_000,
};

export class AdapterProcess {
  private child: ChildProcess;
  private buffer = '';
  private queue: Response[] = [];
  private waiters: ((msg: Response) => void)[] = [];
  readonly spawnedAtEpochMs: number;
  private exited = false;
  private exitError?: Error;

  constructor(spec: NonNullable<CellSpec['spawn']>) {
    this.spawnedAtEpochMs = Date.now();
    this.child = spawn(spec.command, spec.args, {
      cwd: spec.cwd ?? process.cwd(),
      stdio: ['pipe', 'pipe', 'inherit'],
      env: { ...process.env, ...(spec.env ?? {}) },
    });
    this.child.stdout?.setEncoding('utf8');
    this.child.stdout?.on('data', (chunk: string) => this.onData(chunk));
    this.child.on('exit', (code) => {
      this.exited = true;
      if (code && code !== 0) this.exitError = new Error(`adapter exited with code ${code}`);
    });
    this.child.on('error', (err) => {
      this.exited = true;
      this.exitError = err;
    });
  }

  private onData(chunk: string): void {
    this.buffer += chunk;
    const { messages, rest } = decodeMessages<Response>(this.buffer);
    this.buffer = rest;
    for (const msg of messages) {
      const waiter = this.waiters.shift();
      if (waiter) waiter(msg);
      else this.queue.push(msg);
    }
  }

  private nextResponse(timeoutMs: number): Promise<Response> {
    const queued = this.queue.shift();
    if (queued) return Promise.resolve(queued);
    if (this.exited) return Promise.reject(this.exitError ?? new Error('adapter exited before responding'));
    return new Promise<Response>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waiters.indexOf(wrapped);
        if (idx !== -1) this.waiters.splice(idx, 1);
        reject(new Error(`timed out after ${timeoutMs}ms waiting for adapter response`));
      }, timeoutMs);
      const wrapped = (msg: Response): void => {
        clearTimeout(timer);
        resolve(msg);
      };
      this.waiters.push(wrapped);
    });
  }

  private send(req: Request): void {
    this.child.stdin?.write(encodeMessage(req));
  }

  async request(req: Request, timeoutMs: number): Promise<Response> {
    this.send(req);
    const res = await this.nextResponse(timeoutMs);
    if (res.kind === 'error') throw new Error(`adapter error: ${res.message}${res.stack ? `\n${res.stack}` : ''}`);
    return res;
  }

  async awaitReady(timeoutMs: number): Promise<Extract<Response, { kind: 'ready' }>> {
    const res = await this.nextResponse(timeoutMs);
    if (res.kind !== 'ready') throw new Error(`expected 'ready' first, got '${res.kind}'`);
    return res;
  }

  shutdown(): void {
    if (!this.exited) {
      try { this.send({ kind: 'shutdown' }); } catch { /* stdin may be closed */ }
    }
    setTimeout(() => { if (!this.exited) this.child.kill('SIGKILL'); }, 2000).unref?.();
  }
}

async function runCell(spec: CellSpec, config: HarnessConfig): Promise<CellResult> {
  const timeout = config.responseTimeoutMs ?? DEFAULT_CONFIG.responseTimeoutMs!;
  const proc = new AdapterProcess(spec.spawn!);
  const cases = config.cases ?? [...CROSSLANG_CASE_IDS];
  const microCases = CROSSLANG_MICRO_CASE_IDS.filter((c) => (cases as string[]).includes(c)) as CrosslangMicroCaseId[];

  try {
    const ready = await proc.awaitReady(timeout);
    const cold = coldStartMs(proc.spawnedAtEpochMs, ready.readyAtEpochMs);

    const caseResults: Record<string, CaseResult> = {};
    for (const caseId of cases) {
      const cost = (await proc.request({ kind: 'cost', case: caseId }, timeout)) as Extract<Response, { kind: 'cost' }>;
      const run = (await proc.request({ kind: 'run', case: caseId, warmup: config.warmup, iterations: config.iterations }, timeout)) as Extract<Response, { kind: 'run' }>;
      const tp = (await proc.request({ kind: 'throughput', case: caseId, iterations: config.throughputIterations, concurrency: config.concurrency }, timeout)) as Extract<Response, { kind: 'throughput' }>;
      caseResults[caseId] = {
        case: caseId,
        latency: summarizeLatency(run.samplesMs),
        throughputOpsPerSec: throughputOpsPerSec(tp.completed, tp.elapsedMs),
        queries: cost.queries,
        rows: cost.rows,
      };
    }

    const micro: Record<string, ReturnType<typeof summarizeLatency>> = {};
    for (const caseId of microCases) {
      const res = (await proc.request({ kind: 'micro', case: caseId, warmup: config.microWarmup, iterations: config.microIterations }, timeout)) as Extract<Response, { kind: 'micro' }>;
      micro[caseId] = summarizeLatency(res.samplesMs);
    }

    const rss = (await proc.request({ kind: 'rss' }, timeout)) as Extract<Response, { kind: 'rss' }>;

    return {
      language: spec.language, impl: spec.impl,
      coldStartMs: cold, rssBytes: rss.rssBytes,
      artifactSizeBytes: artifactSizeBytes(spec),
      cases: caseResults, micro,
    };
  } catch (err) {
    return {
      language: spec.language, impl: spec.impl,
      cases: {}, micro: {},
      error: err instanceof Error ? err.message.split('\n')[0] : String(err),
    };
  } finally {
    proc.shutdown();
  }
}

function artifactSizeBytes(spec: CellSpec): number | undefined {
  const cmd = spec.spawn?.command;
  if (!cmd) return undefined;
  try {
    const resolved = cmd.startsWith('/') ? cmd : `${spec.spawn?.cwd ?? process.cwd()}/${cmd}`;
    const st = statSync(resolved);
    return st.isFile() ? st.size : undefined;
  } catch {
    return undefined;
  }
}

export async function runMatrix(config: HarnessConfig = DEFAULT_CONFIG): Promise<MatrixResult> {
  const cells: CellResult[] = [];
  const t0 = performance.now();
  for (const spec of liveCells()) {
    process.stderr.write(`\n▶ cell ${spec.language}/${spec.impl} …\n`);
    const result = await runCell(spec, config);
    cells.push(result);
    if (result.error) process.stderr.write(`  ✗ ${result.error}\n`);
    else process.stderr.write(`  cold ${result.coldStartMs?.toFixed(0)}ms · rss ${((result.rssBytes ?? 0) / 1024 / 1024).toFixed(1)}MB\n`);
  }
  process.stderr.write(`\nMatrix complete in ${((performance.now() - t0) / 1000).toFixed(1)}s\n`);
  return {
    generatedAt: new Date().toISOString(),
    iterations: config.iterations,
    warmup: config.warmup,
    microIterations: config.microIterations,
    cells,
  };
}
