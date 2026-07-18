import { defineConfig } from 'vitest/config';

// Dedicated config so the latency-bench generators/cells (outside `test/**`) run under vitest's
// resolver (behavior-contracts is ESM-only; the reference imports from src). Mirrors conformance/.
export default defineConfig({
  test: {
    include: ['benchmark/crosslang/latency/**/*.test.ts'],
    testTimeout: 600_000,
    hookTimeout: 600_000,
  },
});
