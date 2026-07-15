import type { DefineEmbedFn } from 'embedoc';

// NOTE: this embed is defined inline in index.ts (the embedoc entry file) and
// declares its own `defineEmbed` helper on purpose:
//
//  1. Single file — embedoc loads embeds via tsx `tsImport`, and in this
//     environment a transitively-imported `.ts` file (e.g.
//     `import x from './benchmark_table.ts'`) is NOT transpiled by tsx on the
//     ESM->CJS bridge path — node then parses the raw TypeScript and fails on
//     TS-only syntax ("Unexpected strict mode reserved word" on `interface`).
//     Keeping the single embed in the entry file (which tsx DOES transform)
//     makes the drift gate load.
//
//  2. Local `defineEmbed` — importing the *value* `defineEmbed` from the
//     `embedoc` package fails under tsx's resolver ("No exports main defined").
//     `defineEmbed` is just an identity/typing helper (`(d) => d`), so we
//     inline it and pull only the *type* from embedoc (type imports are erased,
//     no runtime resolution).

const defineEmbed: DefineEmbedFn = (definition) => definition;

interface BenchmarkRow {
  Operation: string;
  ORM: string;
  Median: string;
  IQR: string;
  StdDev: string;
  Min: string;
  Max: string;
  Iterations: string;
}

const benchmarkTable = defineEmbed({
  dependsOn: ['benchmark_results'],

  async render(ctx) {
    const rows = await ctx.datasources['benchmark_results']!.query('') as BenchmarkRow[];

    // Group by operation
    const byOperation = new Map<string, Map<string, number>>();
    for (const row of rows) {
      if (!byOperation.has(row.Operation)) {
        byOperation.set(row.Operation, new Map());
      }
      byOperation.get(row.Operation)!.set(row.ORM, parseFloat(row.Median));
    }

    // Find fastest for each operation
    const fastest = new Map<string, { orm: string; value: number }>();
    for (const [op, orms] of byOperation) {
      let min = Infinity;
      let minOrm = '';
      for (const [orm, val] of orms) {
        if (val < min) {
          min = val;
          minOrm = orm;
        }
      }
      fastest.set(op, { orm: minOrm, value: min });
    }

    // Build table rows
    const ormOrder = ['litedbmodel', 'Kysely', 'Drizzle', 'TypeORM', 'Prisma'];
    const tableRows: string[][] = [];

    for (const [op, orms] of byOperation) {
      const fastestInfo = fastest.get(op)!;
      const row: string[] = [op];

      for (const orm of ormOrder) {
        const val = orms.get(orm);
        if (val === undefined) {
          row.push('N/A');
        } else {
          const formatted = `${val.toFixed(2)}ms`;
          // Mark fastest with bold and trophy (allow ties within 0.01ms)
          if (Math.abs(val - fastestInfo.value) < 0.01) {
            row.push(`**${formatted}** 🏆`);
          } else {
            row.push(formatted);
          }
        }
      }
      tableRows.push(row);
    }

    const markdown = ctx.markdown.table(
      ['Operation', 'litedbmodel', 'Kysely', 'Drizzle', 'TypeORM', 'Prisma'],
      tableRows
    );

    return { content: markdown };
  },
});

// embedoc expects `embeds` export
export const embeds = {
  benchmark_table: benchmarkTable,
};

// For direct import compatibility
export default { embeds };
