import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['test/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html'],
      include: ['src/**/*.ts'],
      exclude: ['src/index.ts'],
    },
    testTimeout: 10000,
    hookTimeout: 10000,
    // Use forks pool for native module compatibility
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    deps: {
      // Ensure native modules and dynamic imports work correctly
      interopDefault: true,
    },
  },
  resolve: {
    conditions: ['node'],
    alias: {
      // Resolve dynamic requires in DBHandler.ts
      './drivers/postgres': path.resolve(__dirname, 'src/drivers/postgres'),
      './drivers/sqlite': path.resolve(__dirname, 'src/drivers/sqlite'),
      './drivers/SqliteHelper': path.resolve(__dirname, 'src/drivers/SqliteHelper'),
      './drivers/PostgresHelper': path.resolve(__dirname, 'src/drivers/PostgresHelper'),
    },
  },
});

