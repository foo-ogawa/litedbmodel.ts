import benchmarkTable from './benchmark_table.ts';

// Note: embedoc expects module.embeds but tsImport returns module.default.embeds
// This is a workaround - also export at top level
export const embeds = {
  benchmark_table: benchmarkTable,
};

// For direct import compatibility
export default { embeds };

