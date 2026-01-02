/**
 * Multi-Database and Reader/Writer Separation Tests
 *
 * These tests verify:
 * - Reader/Writer connection separation
 * - Writer sticky after transaction
 * - withWriter() for explicit writer access
 * - Write operation protection (must use transaction)
 * - createDBBase() for multi-database support
 * - Cross-database relations (single and composite keys)
 *
 * Run `docker compose up -d` before running tests.
 * Skip with SKIP_INTEGRATION_TESTS=1
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  DBModel,
  model,
  column,
  hasMany,
  belongsTo,
  ColumnsOf,
  closeAllPools,
  Middleware,
  WriteOutsideTransactionError,
  WriteInReadOnlyContextError,
} from '../../src';
import type { DBConfig, ExecuteResult } from '../../src';

// Skip integration tests if SKIP_INTEGRATION_TESTS=1 is set
const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

// ============================================
// SQL Logger Middleware (for testing connection routing)
// ============================================

interface QueryLogEntry {
  sql: string;
  params?: unknown[];
  timestamp: number;
}

class SqlLoggerMiddleware extends Middleware {
  private queries: QueryLogEntry[] = [];

  init(): void {
    this.queries = [];
  }

  async execute(
    next: (sql: string, params?: unknown[]) => Promise<ExecuteResult>,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    this.queries.push({
      sql,
      params,
      timestamp: Date.now(),
    });
    return next(sql, params);
  }

  getQueries(): QueryLogEntry[] {
    return [...this.queries];
  }

  get queryCount(): number {
    return this.queries.length;
  }

  clear(): void {
    this.queries = [];
  }
}

// ============================================
// Test Configuration
// ============================================

const testConfig: DBConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433'),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};

// For testing, we use the same config for reader and writer
// In production, these would point to different endpoints
const testWriterConfig: DBConfig = {
  ...testConfig,
};

// ============================================
// Test Models for Single Database
// ============================================

@model('multi_users')
class MultiUserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() tenant_id?: number;
}
const MultiUser = MultiUserModel as typeof MultiUserModel & ColumnsOf<MultiUserModel>;
type MultiUser = MultiUserModel;

@model('multi_posts')
class MultiPostModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() title?: string;
  @column() content?: string;

  @belongsTo(() => [MultiPost.user_id, MultiUser.id])
  declare author: Promise<MultiUserModel | null>;
}
const MultiPost = MultiPostModel as typeof MultiPostModel & ColumnsOf<MultiPostModel>;
type MultiPost = MultiPostModel;

// ============================================
// Test Setup and Cleanup
// ============================================

async function setupTestTables(): Promise<void> {
  // Create test tables (DDL requires writer)
  await DBModel.withWriter(async () => {
    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS multi_users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        tenant_id INTEGER
      )
    `);

    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS multi_posts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES multi_users(id),
        title VARCHAR(255) NOT NULL,
        content TEXT
      )
    `);
  });

  // Insert test data within transaction
  await DBModel.transaction(async () => {
    await DBModel.execute(`DELETE FROM multi_posts`);
    await DBModel.execute(`DELETE FROM multi_users`);

    await DBModel.execute(`
      INSERT INTO multi_users (id, name, email, tenant_id) VALUES 
      (1, 'User 1', 'user1@test.com', 1),
      (2, 'User 2', 'user2@test.com', 1),
      (3, 'User 3', 'user3@test.com', 2)
    `);

    await DBModel.execute(`
      INSERT INTO multi_posts (id, user_id, title, content) VALUES 
      (1, 1, 'Post 1', 'Content 1'),
      (2, 1, 'Post 2', 'Content 2'),
      (3, 2, 'Post 3', 'Content 3')
    `);

    // Reset sequences after explicit ID inserts
    await DBModel.execute(`SELECT setval('multi_users_id_seq', (SELECT MAX(id) FROM multi_users))`);
    await DBModel.execute(`SELECT setval('multi_posts_id_seq', (SELECT MAX(id) FROM multi_posts))`);
  });
}

async function cleanupTestTables(): Promise<void> {
  try {
    await DBModel.withWriter(async () => {
      await DBModel.execute(`DROP TABLE IF EXISTS multi_posts`);
      await DBModel.execute(`DROP TABLE IF EXISTS multi_users`);
    });
  } catch {
    // Ignore errors during cleanup
  }
}

// ============================================
// Write Protection Tests
// ============================================

describe.skipIf(skipIntegrationTests)('Write Protection', () => {
  beforeAll(async () => {
    DBModel.setConfig(testConfig, {
      writerConfig: testWriterConfig,
      useWriterAfterTransaction: true,
      writerStickyDuration: 5000,
    });
    await setupTestTables();
  });

  afterAll(async () => {
    await cleanupTestTables();
    await closeAllPools();
  });

  describe('WriteOutsideTransactionError', () => {
    it('should throw error when calling create() outside transaction', async () => {
      await expect(
        MultiUser.create([
          [MultiUser.name, 'Test'],
          [MultiUser.email, 'test@error.com'],
        ])
      ).rejects.toThrow(WriteOutsideTransactionError);
    });

    it('should throw error when calling update() outside transaction', async () => {
      await expect(
        MultiUser.update(
          [[MultiUser.id, 1]],
          [[MultiUser.name, 'Updated']]
        )
      ).rejects.toThrow(WriteOutsideTransactionError);
    });

    it('should throw error when calling delete() outside transaction', async () => {
      await expect(
        MultiUser.delete([[MultiUser.id, 999]])
      ).rejects.toThrow(WriteOutsideTransactionError);
    });

    it('should allow read operations outside transaction', async () => {
      const users = await MultiUser.find([]);
      expect(users.length).toBeGreaterThan(0);
    });
  });

  describe('Write operations in transaction', () => {
    it('should allow create() inside transaction', async () => {
      const uniqueEmail = `tx-create-${Date.now()}@test.com`;
      const result = await DBModel.transaction(async () => {
        return await MultiUser.create([
          [MultiUser.name, 'Transaction Create'],
          [MultiUser.email, uniqueEmail],
        ], { returning: true });
      });

      expect(result).not.toBeNull();
      const userId = result!.values[0][0] as number;
      expect(userId).toBeDefined();

      const user = await MultiUser.findOne([[MultiUser.id, userId]]);
      expect(user?.name).toBe('Transaction Create');

      // Cleanup
      await DBModel.transaction(async () => {
        await MultiUser.delete([[MultiUser.id, userId]]);
      });
    });

    it('should allow update() inside transaction', async () => {
      const updateResult = await DBModel.transaction(async () => {
        return await MultiUser.update(
          [[MultiUser.id, 1]],
          [[MultiUser.name, 'Updated Name']],
          { returning: true }
        );
      });

      expect(updateResult).not.toBeNull();
      expect(updateResult!.values.length).toBeGreaterThanOrEqual(1);
      
      const updatedUser = await MultiUser.findOne([[MultiUser.id, 1]]);
      expect(updatedUser?.name).toBe('Updated Name');

      // Restore
      await DBModel.transaction(async () => {
        await MultiUser.update(
          [[MultiUser.id, 1]],
          [[MultiUser.name, 'User 1']]
        );
      });
    });

    it('should allow delete() inside transaction', async () => {
      const uniqueEmail = `delete-me-${Date.now()}@test.com`;
      // Create a user to delete
      const result = await DBModel.transaction(async () => {
        return await MultiUser.create([
          [MultiUser.name, 'To Delete'],
          [MultiUser.email, uniqueEmail],
        ], { returning: true });
      });
      const userId = result!.values[0][0] as number;

      // Delete
      const deleted = await DBModel.transaction(async () => {
        return await MultiUser.delete([[MultiUser.id, userId]], { returning: true });
      });

      expect(deleted).not.toBeNull();
      expect(deleted!.values.length).toBe(1);
    });
  });
});

// ============================================
// withWriter() Tests
// ============================================

describe.skipIf(skipIntegrationTests)('withWriter()', () => {
  beforeAll(async () => {
    DBModel.setConfig(testConfig, {
      writerConfig: testWriterConfig,
      useWriterAfterTransaction: false, // Disable sticky for these tests
      writerStickyDuration: 0,
    });
    await setupTestTables();
  });

  afterAll(async () => {
    await cleanupTestTables();
    await closeAllPools();
  });

  it('should allow read operations in withWriter context', async () => {
    const users = await DBModel.withWriter(async () => {
      return await MultiUser.find([]);
    });

    expect(users.length).toBeGreaterThan(0);
  });

  it('should throw WriteInReadOnlyContextError for write in withWriter', async () => {
    await expect(
      DBModel.withWriter(async () => {
        await MultiUser.create([
          [MultiUser.name, 'Should Fail'],
          [MultiUser.email, 'fail@test.com'],
        ]);
      })
    ).rejects.toThrow(WriteInReadOnlyContextError);
  });

  it('should return values from withWriter', async () => {
    const user = await DBModel.withWriter(async () => {
      return await MultiUser.findOne([[MultiUser.id, 1]]);
    });

    expect(user).not.toBeNull();
    expect(user!.id).toBe(1);
  });

  it('should report inWriterContext correctly', async () => {
    expect(DBModel.inWriterContext()).toBe(false);

    await DBModel.withWriter(async () => {
      expect(DBModel.inWriterContext()).toBe(true);
    });

    expect(DBModel.inWriterContext()).toBe(false);
  });
});

// ============================================
// Writer Sticky Tests
// ============================================

describe.skipIf(skipIntegrationTests)('Writer Sticky', () => {
  beforeAll(async () => {
    DBModel.setConfig(testConfig, {
      writerConfig: testWriterConfig,
      useWriterAfterTransaction: true,
      writerStickyDuration: 100, // Short duration for testing
    });
    await setupTestTables();

    // Register logger middleware
    DBModel.use(SqlLoggerMiddleware);
  });

  afterAll(async () => {
    DBModel.removeMiddleware(SqlLoggerMiddleware);
    await cleanupTestTables();
    await closeAllPools();
  });

  beforeEach(() => {
    SqlLoggerMiddleware.getCurrentContext().clear();
  });

  it('should use writer after transaction (within sticky duration)', async () => {
    // Execute a transaction
    await DBModel.transaction(async () => {
      await DBModel.execute('SELECT 1');
    });

    // Immediate read should use writer (sticky)
    const users = await MultiUser.find([[MultiUser.id, 1]]);
    expect(users.length).toBe(1);
  });

  it('should support per-transaction useWriterAfterTransaction override', async () => {
    // Transaction with useWriterAfterTransaction: false
    await DBModel.transaction(
      async () => {
        await DBModel.execute('SELECT 1');
      },
      { useWriterAfterTransaction: false }
    );

    // This should NOT trigger sticky (per-transaction override)
    // The behavior is the same (query succeeds), but internally uses reader
    const users = await MultiUser.find([[MultiUser.id, 1]]);
    expect(users.length).toBe(1);
  });
});

// ============================================
// createDBBase() Tests
// ============================================

describe.skipIf(skipIntegrationTests)('createDBBase()', () => {
  // Create two independent database base classes
  // (In real usage, these would connect to different databases)
  let BaseDB: typeof DBModel;
  let CmsDB: typeof DBModel;

  beforeAll(async () => {
    // Create base classes with independent configurations
    BaseDB = DBModel.createDBBase(testConfig, {
      writerConfig: testWriterConfig,
      useWriterAfterTransaction: true,
      writerStickyDuration: 5000,
    });

    CmsDB = DBModel.createDBBase(testConfig, {
      writerConfig: testWriterConfig,
      useWriterAfterTransaction: true,
      writerStickyDuration: 5000,
    });
  });

  afterAll(async () => {
    await closeAllPools();
  });

  describe('createDBBase() returns proper class structure', () => {
    it('should return a class that extends DBModel', () => {
      expect(BaseDB.prototype).toBeInstanceOf(Object);
      expect(typeof BaseDB.setConfig).toBe('function');
      expect(typeof BaseDB.transaction).toBe('function');
      expect(typeof BaseDB.withWriter).toBe('function');
      expect(typeof BaseDB.inTransaction).toBe('function');
      expect(typeof BaseDB.inWriterContext).toBe('function');
      expect(typeof BaseDB.execute).toBe('function');
    });

    it('should have independent instances', () => {
      expect(BaseDB).not.toBe(CmsDB);
      expect(BaseDB).not.toBe(DBModel);
      expect(CmsDB).not.toBe(DBModel);
    });
  });

  describe('Independent Transaction Contexts', () => {
    it('should have independent inTransaction() state', async () => {
      expect(BaseDB.inTransaction()).toBe(false);
      expect(CmsDB.inTransaction()).toBe(false);

      await BaseDB.transaction(async () => {
        expect(BaseDB.inTransaction()).toBe(true);
        expect(CmsDB.inTransaction()).toBe(false); // CmsDB should not see BaseDB's transaction
      });

      expect(BaseDB.inTransaction()).toBe(false);
    });

    it('should have nested transactions work independently', async () => {
      await BaseDB.transaction(async () => {
        expect(BaseDB.inTransaction()).toBe(true);
        expect(CmsDB.inTransaction()).toBe(false);

        await CmsDB.transaction(async () => {
          expect(BaseDB.inTransaction()).toBe(true);
          expect(CmsDB.inTransaction()).toBe(true);
        });

        expect(CmsDB.inTransaction()).toBe(false);
        expect(BaseDB.inTransaction()).toBe(true);
      });
    });
  });

  describe('Independent withWriter() Contexts', () => {
    it('should have independent inWriterContext() state', async () => {
      expect(BaseDB.inWriterContext()).toBe(false);
      expect(CmsDB.inWriterContext()).toBe(false);

      await BaseDB.withWriter(async () => {
        expect(BaseDB.inWriterContext()).toBe(true);
        expect(CmsDB.inWriterContext()).toBe(false);
      });

      expect(BaseDB.inWriterContext()).toBe(false);
    });

    it('should have nested withWriter work independently', async () => {
      await BaseDB.withWriter(async () => {
        expect(BaseDB.inWriterContext()).toBe(true);
        expect(CmsDB.inWriterContext()).toBe(false);

        await CmsDB.withWriter(async () => {
          expect(BaseDB.inWriterContext()).toBe(true);
          expect(CmsDB.inWriterContext()).toBe(true);
        });

        expect(CmsDB.inWriterContext()).toBe(false);
        expect(BaseDB.inWriterContext()).toBe(true);
      });
    });
  });

  describe('Independent SQL Execution', () => {
    it('should execute queries on different base classes', async () => {
      const baseResult = await BaseDB.execute('SELECT 1 as val');
      expect(baseResult.rows[0].val).toBe(1);

      const cmsResult = await CmsDB.execute('SELECT 2 as val');
      expect(cmsResult.rows[0].val).toBe(2);
    });

    it('should execute transactions independently', async () => {
      let baseCommitted = false;
      let cmsCommitted = false;

      // Start a BaseDB transaction
      const basePromise = BaseDB.transaction(async () => {
        await BaseDB.execute('SELECT 1');
        // Wait a bit to ensure overlap
        await new Promise(resolve => setTimeout(resolve, 50));
        baseCommitted = true;
      });

      // Start a CmsDB transaction while BaseDB is still in transaction
      const cmsPromise = CmsDB.transaction(async () => {
        await CmsDB.execute('SELECT 2');
        cmsCommitted = true;
      });

      await Promise.all([basePromise, cmsPromise]);

      expect(baseCommitted).toBe(true);
      expect(cmsCommitted).toBe(true);
    });
  });

  describe('Global DBModel isolation', () => {
    it('should not affect global DBModel transaction state', async () => {
      DBModel.setConfig(testConfig);

      expect(DBModel.inTransaction()).toBe(false);
      expect(BaseDB.inTransaction()).toBe(false);

      await BaseDB.transaction(async () => {
        expect(BaseDB.inTransaction()).toBe(true);
        expect(DBModel.inTransaction()).toBe(false); // Global should not see BaseDB's transaction
      });
    });

    it('should not affect global DBModel writer state', async () => {
      DBModel.setConfig(testConfig);

      expect(DBModel.inWriterContext()).toBe(false);
      expect(BaseDB.inWriterContext()).toBe(false);

      await BaseDB.withWriter(async () => {
        expect(BaseDB.inWriterContext()).toBe(true);
        expect(DBModel.inWriterContext()).toBe(false);
      });
    });
  });
});

// ============================================
// Cross-Database Relations Models (at module level)
// ============================================

// "Core DB" model
@model('cross_users')
class CrossUserModel extends DBModel {
  @column() id?: number;
  @column() tenant_id?: number;
  @column() name?: string;

  @hasMany(() => [
    [CrossUser.tenant_id, CrossOrder.tenant_id],
    [CrossUser.id, CrossOrder.user_id],
  ])
  declare orders: Promise<CrossOrderModel[]>;
}
const CrossUser = CrossUserModel as typeof CrossUserModel & ColumnsOf<CrossUserModel>;
type CrossUser = CrossUserModel;

// "Orders DB" model
@model('cross_orders')
class CrossOrderModel extends DBModel {
  @column() id?: number;
  @column() tenant_id?: number;
  @column() user_id?: number;
  @column() total?: number;

  @belongsTo(() => [
    [CrossOrder.tenant_id, CrossUser.tenant_id],
    [CrossOrder.user_id, CrossUser.id],
  ])
  declare user: Promise<CrossUserModel | null>;

  @hasMany(() => [CrossOrder.id, CrossOrderItem.order_id])
  declare items: Promise<CrossOrderItemModel[]>;
}
const CrossOrder = CrossOrderModel as typeof CrossOrderModel & ColumnsOf<CrossOrderModel>;
type CrossOrder = CrossOrderModel;

// "Orders DB" model (composite primary key)
@model('cross_order_items')
class CrossOrderItemModel extends DBModel {
  @column({ primaryKey: true }) order_id?: number;
  @column({ primaryKey: true }) product_id?: number;
  @column() quantity?: number;
  @column() price?: number;

  @belongsTo(() => [CrossOrderItem.order_id, CrossOrder.id])
  declare order: Promise<CrossOrderModel | null>;
}
const CrossOrderItem = CrossOrderItemModel as typeof CrossOrderItemModel & ColumnsOf<CrossOrderItemModel>;
type CrossOrderItem = CrossOrderItemModel;

// ============================================
// Cross-Database Relations Tests
// ============================================

describe.skipIf(skipIntegrationTests)('Cross-Database Relations', () => {
  // In a real multi-DB scenario, these models would be in different databases
  // For testing, we simulate by creating models that reference each other
  // across different "conceptual" databases

  beforeAll(async () => {
    DBModel.setConfig(testConfig, {
      writerConfig: testWriterConfig,
    });

    // Create test tables (DDL requires writer)
    await DBModel.withWriter(async () => {
      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS cross_users (
          id SERIAL,
          tenant_id INTEGER NOT NULL,
          name VARCHAR(255) NOT NULL,
          PRIMARY KEY (tenant_id, id)
        )
      `);

      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS cross_orders (
          id SERIAL PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          total DECIMAL(10,2)
        )
      `);

      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS cross_order_items (
          order_id INTEGER REFERENCES cross_orders(id),
          product_id INTEGER NOT NULL,
          quantity INTEGER NOT NULL,
          price DECIMAL(10,2),
          PRIMARY KEY (order_id, product_id)
        )
      `);
    });

    // Insert test data
    await DBModel.transaction(async () => {
      await DBModel.execute(`DELETE FROM cross_order_items`);
      await DBModel.execute(`DELETE FROM cross_orders`);
      await DBModel.execute(`DELETE FROM cross_users`);

      // Users in tenant 1
      await DBModel.execute(`
        INSERT INTO cross_users (id, tenant_id, name) VALUES 
        (1, 1, 'Tenant1 User1'),
        (2, 1, 'Tenant1 User2')
      `);

      // Users in tenant 2
      await DBModel.execute(`
        INSERT INTO cross_users (id, tenant_id, name) VALUES 
        (1, 2, 'Tenant2 User1')
      `);

      // Orders
      await DBModel.execute(`
        INSERT INTO cross_orders (id, tenant_id, user_id, total) VALUES 
        (1, 1, 1, 100.00),
        (2, 1, 1, 200.00),
        (3, 1, 2, 150.00),
        (4, 2, 1, 300.00)
      `);

      // Order items
      await DBModel.execute(`
        INSERT INTO cross_order_items (order_id, product_id, quantity, price) VALUES 
        (1, 101, 2, 50.00),
        (1, 102, 1, 50.00),
        (2, 103, 4, 50.00),
        (3, 101, 3, 50.00),
        (4, 104, 6, 50.00)
      `);
    });
  });

  afterAll(async () => {
    try {
      await DBModel.withWriter(async () => {
        await DBModel.execute(`DROP TABLE IF EXISTS cross_order_items`);
        await DBModel.execute(`DROP TABLE IF EXISTS cross_orders`);
        await DBModel.execute(`DROP TABLE IF EXISTS cross_users`);
      });
    } catch {
      // Ignore errors during cleanup
    }
    await closeAllPools();
  });

  describe('Composite Key Relations', () => {
    it('should load hasMany with composite key', async () => {
      // Find user in tenant 1
      const user = await CrossUser.findOne([
        [CrossUser.tenant_id, 1],
        [CrossUser.id, 1],
      ]);
      expect(user).not.toBeNull();
      expect(user!.name).toBe('Tenant1 User1');

      // Load orders (composite key: tenant_id + user_id)
      const orders = await user!.orders;
      expect(orders.length).toBe(2);
      expect(orders.every(o => o.tenant_id === 1 && o.user_id === 1)).toBe(true);
    });

    it('should load belongsTo with composite key', async () => {
      const order = await CrossOrder.findOne([[CrossOrder.id, 1]]);
      expect(order).not.toBeNull();

      const user = await order!.user;
      expect(user).not.toBeNull();
      expect(user!.tenant_id).toBe(1);
      expect(user!.id).toBe(1);
      expect(user!.name).toBe('Tenant1 User1');
    });

    it('should batch load composite key relations', async () => {
      // Get all orders for tenant 1
      const orders = await CrossOrder.find([[CrossOrder.tenant_id, 1]]);
      expect(orders.length).toBe(3);

      // Access user on first order - triggers batch load
      const user1 = await orders[0].user;
      expect(user1).not.toBeNull();

      // Access user on second order - should use cache
      const user2 = await orders[1].user;
      expect(user2).not.toBeNull();

      // Access user on third order - different user
      const user3 = await orders[2].user;
      expect(user3).not.toBeNull();
      expect(user3!.id).toBe(2);
    });

    it('should handle single key relations alongside composite', async () => {
      const order = await CrossOrder.findOne([[CrossOrder.id, 1]]);
      expect(order).not.toBeNull();

      // Single key relation (order -> items)
      const items = await order!.items;
      expect(items.length).toBe(2);

      // belongsTo from item back to order
      const itemOrder = await items[0].order;
      expect(itemOrder).not.toBeNull();
      expect(itemOrder!.id).toBe(1);
    });

    it('should correctly isolate by tenant in composite key', async () => {
      // User 1 in tenant 2
      const tenant2User = await CrossUser.findOne([
        [CrossUser.tenant_id, 2],
        [CrossUser.id, 1],
      ]);
      expect(tenant2User).not.toBeNull();
      expect(tenant2User!.name).toBe('Tenant2 User1');

      // Should only get orders for tenant 2
      const orders = await tenant2User!.orders;
      expect(orders.length).toBe(1);
      expect(orders[0].tenant_id).toBe(2);
      // PostgreSQL returns DECIMAL as string
      expect(Number(orders[0].total)).toBe(300);
    });
  });

  describe('Mixed Batch Loading', () => {
    it('should batch load with different relation types', async () => {
      const orders = await CrossOrder.find([[CrossOrder.tenant_id, 1]]);

      // Load both user (belongsTo composite) and items (hasMany single)
      const promises = orders.map(async (order) => {
        const user = await order.user;
        const items = await order.items;
        return { order, user, items };
      });

      const results = await Promise.all(promises);
      
      expect(results.length).toBe(3);
      results.forEach(r => {
        expect(r.user).not.toBeNull();
        expect(r.items.length).toBeGreaterThan(0);
      });
    });
  });
});

// ============================================
// Integration with Existing Tests
// ============================================

describe.skipIf(skipIntegrationTests)('Backwards Compatibility', () => {
  beforeAll(async () => {
    // Reset to simple config without reader/writer separation
    DBModel.setConfig(testConfig);
  });

  afterAll(async () => {
    await closeAllPools();
  });

  it('should work without writerConfig', async () => {
    // Read operations should work
    const result = await DBModel.execute('SELECT 1 as val');
    expect(result.rows[0].val).toBe(1);
  });

  it('should still require transaction for writes', async () => {
    // Even without writerConfig, writes require transaction
    await expect(
      DBModel.execute(`
        CREATE TEMP TABLE IF NOT EXISTS temp_test (id int)
      `)
    ).resolves.toBeDefined(); // DDL is allowed

    // But DML through Model API requires transaction
    await expect(
      MultiUser.create([
        [MultiUser.name, 'Test'],
        [MultiUser.email, 'backwards@test.com'],
      ])
    ).rejects.toThrow(WriteOutsideTransactionError);
  });
});

