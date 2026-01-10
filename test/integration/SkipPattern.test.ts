/**
 * Integration tests for SKIP pattern in createMany/updateMany
 * Tests that:
 * - createMany: SKIPped columns use DB DEFAULT
 * - updateMany: SKIPped columns retain existing values
 */
import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  DBModel,
  model,
  column,
  closeAllPools,
  SKIP,
  ColumnsOf,
} from '../../src';
import type { DBConfig } from '../../src';

// Skip integration tests if SKIP_INTEGRATION_TESTS=1 is set
const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

// Test Configuration
const testConfig: DBConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433'),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};

// Test model - declare outside describe for type inference
// eslint-disable-next-line prefer-const
let SkipTest: typeof SkipTestModel & ColumnsOf<SkipTestModel>;

@model('skip_test')
class SkipTestModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() status?: string;
}
SkipTest = SkipTestModel.asModel();
type SkipTest = SkipTestModel;

describe.skipIf(skipIntegrationTests)('SKIP Pattern Integration Tests (PostgreSQL)', () => {
  beforeAll(async () => {
    // Set global config
    DBModel.setConfig(testConfig);

    // Create test table
    await DBModel.execute(`
      DROP TABLE IF EXISTS skip_test CASCADE;
      CREATE TABLE skip_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) DEFAULT 'default@example.com',
        status VARCHAR(50) DEFAULT 'active'
      );
    `);
  });

  afterAll(async () => {
    await DBModel.execute('DROP TABLE IF EXISTS skip_test CASCADE');
    await closeAllPools();
  });

  beforeEach(async () => {
    await DBModel.execute('TRUNCATE TABLE skip_test RESTART IDENTITY CASCADE');
  });

  describe('createMany with SKIP', () => {
    it('should use DEFAULT for SKIPped columns in first record', async () => {
      await SkipTest.transaction(async () => {
        // First record SKIPs email, second has email
        await SkipTest.createMany([
          [[SkipTest.name, 'John'], [SkipTest.email, SKIP]],
          [[SkipTest.name, 'Jane'], [SkipTest.email, 'jane@test.com']],
        ]);
      });

      const users = await SkipTest.find([]);
      expect(users).toHaveLength(2);

      const john = users.find(u => u.name === 'John');
      const jane = users.find(u => u.name === 'Jane');

      expect(john?.email).toBe('default@example.com');  // DEFAULT
      expect(jane?.email).toBe('jane@test.com');
    });

    it('should use DEFAULT for SKIPped columns in later records', async () => {
      await SkipTest.transaction(async () => {
        // First record has email, second SKIPs
        await SkipTest.createMany([
          [[SkipTest.name, 'John'], [SkipTest.email, 'john@test.com']],
          [[SkipTest.name, 'Jane'], [SkipTest.email, SKIP]],
        ]);
      });

      const users = await SkipTest.find([]);
      expect(users).toHaveLength(2);

      const john = users.find(u => u.name === 'John');
      const jane = users.find(u => u.name === 'Jane');

      expect(john?.email).toBe('john@test.com');
      expect(jane?.email).toBe('default@example.com');  // DEFAULT
    });

    it('should use DEFAULT for all when column is SKIPped in all records', async () => {
      await SkipTest.transaction(async () => {
        await SkipTest.createMany([
          [[SkipTest.name, 'John'], [SkipTest.email, SKIP]],
          [[SkipTest.name, 'Jane'], [SkipTest.email, SKIP]],
        ]);
      });

      const users = await SkipTest.find([]);
      expect(users).toHaveLength(2);
      expect(users[0].email).toBe('default@example.com');
      expect(users[1].email).toBe('default@example.com');
    });
  });

  describe('updateMany with SKIP', () => {
    beforeEach(async () => {
      // Insert test data
      await DBModel.execute(`
        INSERT INTO skip_test (name, email, status) VALUES
        ('John', 'john@test.com', 'active'),
        ('Jane', 'jane@test.com', 'active'),
        ('Bob', 'bob@test.com', 'inactive')
      `);
    });

    it('should retain existing value when column is SKIPped', async () => {
      await SkipTest.transaction(async () => {
        // Update John's status, SKIP email
        // Update Jane's email, SKIP status
        await SkipTest.updateMany([
          [[SkipTest.id, 1], [SkipTest.email, SKIP], [SkipTest.status, 'updated']],
          [[SkipTest.id, 2], [SkipTest.email, 'new-jane@test.com'], [SkipTest.status, SKIP]],
        ], {
          keyColumns: SkipTest.id,
        });
      });

      const users = await SkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      // John: email unchanged (SKIPped), status updated
      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('updated');

      // Jane: email updated, status unchanged (SKIPped)
      expect(jane?.email).toBe('new-jane@test.com');
      expect(jane?.status).toBe('active');  // Retained
    });

    it('should handle SKIP in first record only', async () => {
      await SkipTest.transaction(async () => {
        await SkipTest.updateMany([
          [[SkipTest.id, 1], [SkipTest.email, SKIP], [SkipTest.status, 'first-updated']],
          [[SkipTest.id, 2], [SkipTest.email, 'second@test.com'], [SkipTest.status, 'second-updated']],
        ], {
          keyColumns: SkipTest.id,
        });
      });

      const users = await SkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained (SKIP)
      expect(john?.status).toBe('first-updated');
      expect(jane?.email).toBe('second@test.com');
      expect(jane?.status).toBe('second-updated');
    });

    it('should be no-op when all columns SKIPped except key', async () => {
      await SkipTest.transaction(async () => {
        // When all value columns are SKIPped, updateMany should be no-op
        // (no UPDATE query is executed)
        await SkipTest.updateMany([
          [[SkipTest.id, 1], [SkipTest.email, SKIP], [SkipTest.status, SKIP]],
          [[SkipTest.id, 2], [SkipTest.email, SKIP], [SkipTest.status, SKIP]],
        ], {
          keyColumns: SkipTest.id,
        });
      });

      // All values should be unchanged (no update was executed)
      const users = await SkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');
      expect(john?.status).toBe('active');
      expect(jane?.email).toBe('jane@test.com');
      expect(jane?.status).toBe('active');
    });
  });

  describe('find/update with SKIP (existing behavior)', () => {
    beforeEach(async () => {
      await DBModel.execute(`
        INSERT INTO skip_test (name, email, status) VALUES
        ('John', 'john@test.com', 'active'),
        ('Jane', 'jane@test.com', 'inactive')
      `);
    });

    it('should exclude SKIPped conditions in find', async () => {
      // Should find only John (status condition is SKIPped)
      const users = await SkipTest.find([
        [SkipTest.name, 'John'],
        [SkipTest.status, SKIP],  // This condition is excluded
      ]);
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('John');
    });

    it('should exclude SKIPped values in update', async () => {
      await SkipTest.transaction(async () => {
        await SkipTest.update(
          [[SkipTest.id, 1]],
          [
            [SkipTest.status, 'updated'],
            [SkipTest.email, SKIP],  // Don't update email
          ]
        );
      });

      const john = await SkipTest.findOne([[SkipTest.id, 1]]);
      expect(john?.status).toBe('updated');
      expect(john?.email).toBe('john@test.com');  // Unchanged
    });
  });
});
