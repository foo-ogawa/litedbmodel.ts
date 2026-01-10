import { defineEmbed } from 'embedoc';

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

export default defineEmbed({
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

