/**
 * Integration tests for SKIP pattern in createMany/updateMany
 * Tests that:
 * - createMany: SKIPped columns use DB DEFAULT (grouped by SKIP pattern)
 * - updateMany: SKIPped columns retain existing values (batch with SKIP flags)
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
import {
  skipIntegrationTests,
  pgConfig,
  getMysqlBase,
  getSqliteBase,
  bindModelToBase,
} from '../helpers/setup';

// ============================================
// PostgreSQL Tests
// ============================================

@model('pg_skip_test')
class PgSkipTestModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() status?: string;
  @column() score?: number;
}
const PgSkipTest = PgSkipTestModel.asModel();
type PgSkipTest = PgSkipTestModel;

describe.skipIf(skipIntegrationTests)('SKIP Pattern - PostgreSQL', () => {
  beforeAll(async () => {
    DBModel.setConfig(pgConfig);

    await DBModel.execute(`
      DROP TABLE IF EXISTS pg_skip_test CASCADE;
      CREATE TABLE pg_skip_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) DEFAULT 'default@example.com',
        status VARCHAR(50) DEFAULT 'active',
        score INTEGER DEFAULT 0
      );
    `);
  });

  afterAll(async () => {
    await DBModel.execute('DROP TABLE IF EXISTS pg_skip_test CASCADE');
  });

  beforeEach(async () => {
    await DBModel.execute('TRUNCATE TABLE pg_skip_test RESTART IDENTITY CASCADE');
  });

  describe('createMany with SKIP', () => {
    it('should handle different SKIP patterns using grouped INSERT', async () => {
      await PgSkipTest.transaction(async () => {
        // Pattern 1: email SKIPped (should get DEFAULT)
        // Pattern 2: status SKIPped (should get DEFAULT)
        // Pattern 3: both email and status provided
        await PgSkipTest.createMany([
          [[PgSkipTest.name, 'John'], [PgSkipTest.email, SKIP], [PgSkipTest.status, 'admin']],
          [[PgSkipTest.name, 'Jane'], [PgSkipTest.email, 'jane@test.com'], [PgSkipTest.status, SKIP]],
          [[PgSkipTest.name, 'Bob'], [PgSkipTest.email, 'bob@test.com'], [PgSkipTest.status, 'user']],
          [[PgSkipTest.name, 'Alice'], [PgSkipTest.email, SKIP], [PgSkipTest.status, 'mod']],
        ]);
      });

      const users = await PgSkipTest.find([]);
      expect(users).toHaveLength(4);

      const john = users.find(u => u.name === 'John');
      const jane = users.find(u => u.name === 'Jane');
      const bob = users.find(u => u.name === 'Bob');
      const alice = users.find(u => u.name === 'Alice');

      // John: email DEFAULT, status provided
      expect(john?.email).toBe('default@example.com');
      expect(john?.status).toBe('admin');

      // Jane: email provided, status DEFAULT
      expect(jane?.email).toBe('jane@test.com');
      expect(jane?.status).toBe('active');

      // Bob: both provided
      expect(bob?.email).toBe('bob@test.com');
      expect(bob?.status).toBe('user');

      // Alice: same pattern as John
      expect(alice?.email).toBe('default@example.com');
      expect(alice?.status).toBe('mod');
    });

    it('should handle multiple columns SKIPped', async () => {
      await PgSkipTest.transaction(async () => {
        await PgSkipTest.createMany([
          [[PgSkipTest.name, 'User1'], [PgSkipTest.email, SKIP], [PgSkipTest.status, SKIP], [PgSkipTest.score, SKIP]],
          [[PgSkipTest.name, 'User2'], [PgSkipTest.email, 'user2@test.com'], [PgSkipTest.status, SKIP], [PgSkipTest.score, 100]],
        ]);
      });

      const users = await PgSkipTest.find([]);
      const user1 = users.find(u => u.name === 'User1');
      const user2 = users.find(u => u.name === 'User2');

      expect(user1?.email).toBe('default@example.com');
      expect(user1?.status).toBe('active');
      expect(user1?.score).toBe(0);

      expect(user2?.email).toBe('user2@test.com');
      expect(user2?.status).toBe('active');
      expect(user2?.score).toBe(100);
    });
  });

  describe('updateMany with SKIP (batch with SKIP flags)', () => {
    beforeEach(async () => {
      await DBModel.execute(`
        INSERT INTO pg_skip_test (name, email, status, score) VALUES
        ('John', 'john@test.com', 'active', 10),
        ('Jane', 'jane@test.com', 'active', 20),
        ('Bob', 'bob@test.com', 'inactive', 30)
      `);
    });

    it('should retain existing value when column is SKIPped using batch', async () => {
      await PgSkipTest.transaction(async () => {
        await PgSkipTest.updateMany([
          [[PgSkipTest.id, 1], [PgSkipTest.email, SKIP], [PgSkipTest.status, 'updated']],
          [[PgSkipTest.id, 2], [PgSkipTest.email, 'new-jane@test.com'], [PgSkipTest.status, SKIP]],
        ], {
          keyColumns: PgSkipTest.id,
        });
      });

      const users = await PgSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained (SKIP)
      expect(john?.status).toBe('updated');

      expect(jane?.email).toBe('new-jane@test.com');
      expect(jane?.status).toBe('active');  // Retained (SKIP)
    });

    it('should handle multiple columns SKIPped in different rows', async () => {
      await PgSkipTest.transaction(async () => {
        await PgSkipTest.updateMany([
          [[PgSkipTest.id, 1], [PgSkipTest.email, SKIP], [PgSkipTest.status, SKIP], [PgSkipTest.score, 100]],
          [[PgSkipTest.id, 2], [PgSkipTest.email, 'new@test.com'], [PgSkipTest.status, 'mod'], [PgSkipTest.score, SKIP]],
          [[PgSkipTest.id, 3], [PgSkipTest.email, SKIP], [PgSkipTest.status, 'admin'], [PgSkipTest.score, SKIP]],
        ], {
          keyColumns: PgSkipTest.id,
        });
      });

      const users = await PgSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);
      const bob = users.find(u => u.id === 3);

      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('active');  // Retained
      expect(john?.score).toBe(100);

      expect(jane?.email).toBe('new@test.com');
      expect(jane?.status).toBe('mod');
      expect(jane?.score).toBe(20);  // Retained

      expect(bob?.email).toBe('bob@test.com');  // Retained
      expect(bob?.status).toBe('admin');
      expect(bob?.score).toBe(30);  // Retained
    });

    it('should handle null values distinct from SKIP', async () => {
      await PgSkipTest.transaction(async () => {
        await PgSkipTest.updateMany([
          [[PgSkipTest.id, 1], [PgSkipTest.email, null], [PgSkipTest.status, SKIP]],  // email = NULL
          [[PgSkipTest.id, 2], [PgSkipTest.email, SKIP], [PgSkipTest.status, null]],  // status = NULL
        ], {
          keyColumns: PgSkipTest.id,
        });
      });

      const users = await PgSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBeNull();  // Set to NULL
      expect(john?.status).toBe('active');  // Retained (SKIP)

      expect(jane?.email).toBe('jane@test.com');  // Retained (SKIP)
      expect(jane?.status).toBeNull();  // Set to NULL
    });
  });

  describe('Single Record Operations with SKIP', () => {
    it('should exclude SKIPped columns in single create (uses DB DEFAULT)', async () => {
      await PgSkipTest.transaction(async () => {
        await PgSkipTest.create([
          [PgSkipTest.name, 'SingleUser'],
          [PgSkipTest.email, SKIP],  // Should use DB DEFAULT
          [PgSkipTest.status, 'admin'],
        ]);
      });

      const user = await PgSkipTest.findOne([[PgSkipTest.name, 'SingleUser']]);
      expect(user).not.toBeNull();
      expect(user?.email).toBe('default@example.com');  // DB DEFAULT
      expect(user?.status).toBe('admin');
    });

    it('should exclude SKIPped values in single update', async () => {
      // Insert test data first
      await DBModel.execute(`
        INSERT INTO pg_skip_test (name, email, status) VALUES
        ('John', 'john@test.com', 'active')
      `);

      await PgSkipTest.transaction(async () => {
        await PgSkipTest.update(
          [[PgSkipTest.id, 1]],
          [
            [PgSkipTest.status, 'updated'],
            [PgSkipTest.email, SKIP],
          ]
        );
      });

      const john = await PgSkipTest.findOne([[PgSkipTest.id, 1]]);
      expect(john?.status).toBe('updated');
      expect(john?.email).toBe('john@test.com');  // Unchanged
    });

    it('should exclude SKIPped conditions in find', async () => {
      await DBModel.execute(`
        INSERT INTO pg_skip_test (name, email, status) VALUES
        ('John', 'john@test.com', 'active'),
        ('Jane', 'jane@test.com', 'inactive')
      `);

      const users = await PgSkipTest.find([
        [PgSkipTest.name, 'John'],
        [PgSkipTest.status, SKIP],
      ]);
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('John');
    });
  });
});

// ============================================
// PostgreSQL Array/JSON Tests
// ============================================

@model('pg_array_skip_test')
class PgArraySkipTestModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() name?: string;
  @column.intArray() int_arr?: number[];
  @column.stringArray() str_arr?: string[];
  @column.booleanArray() bool_arr?: (boolean | null)[];
  @column.json<Record<string, unknown>>() json_data?: Record<string, unknown>;
  @column.json<unknown[]>() json_arr?: unknown[];
}
const PgArraySkipTest = PgArraySkipTestModel.asModel();
type PgArraySkipTest = PgArraySkipTestModel;

describe.skipIf(skipIntegrationTests)('SKIP Pattern - PostgreSQL Array/JSON', () => {
  beforeAll(async () => {
    DBModel.setConfig(pgConfig);

    await DBModel.execute(`
      DROP TABLE IF EXISTS pg_array_skip_test CASCADE;
      CREATE TABLE pg_array_skip_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        int_arr INTEGER[] DEFAULT '{1,2,3}',
        str_arr TEXT[] DEFAULT '{"a","b"}',
        bool_arr BOOLEAN[] DEFAULT '{true,false}',
        json_data JSONB DEFAULT '{"default": true}',
        json_arr JSONB DEFAULT '[1,2,3]'
      );
    `);
  });

  afterAll(async () => {
    await DBModel.execute('DROP TABLE IF EXISTS pg_array_skip_test CASCADE');
  });

  beforeEach(async () => {
    await DBModel.execute('TRUNCATE TABLE pg_array_skip_test RESTART IDENTITY CASCADE');
  });

  describe('createMany with Array/JSON and SKIP', () => {
    it('should handle SKIP on array columns using DB DEFAULT', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.createMany([
          // Pattern 1: int_arr SKIPped
          [[PgArraySkipTest.name, 'User1'], [PgArraySkipTest.int_arr, SKIP], [PgArraySkipTest.str_arr, ['x', 'y']]],
          // Pattern 2: str_arr SKIPped
          [[PgArraySkipTest.name, 'User2'], [PgArraySkipTest.int_arr, [10, 20]], [PgArraySkipTest.str_arr, SKIP]],
          // Pattern 3: all provided
          [[PgArraySkipTest.name, 'User3'], [PgArraySkipTest.int_arr, [100]], [PgArraySkipTest.str_arr, ['z']]],
        ]);
      });

      const users = await PgArraySkipTest.find([]);
      expect(users).toHaveLength(3);

      const user1 = users.find(u => u.name === 'User1');
      const user2 = users.find(u => u.name === 'User2');
      const user3 = users.find(u => u.name === 'User3');

      // User1: int_arr DEFAULT, str_arr provided
      expect(user1?.int_arr).toEqual([1, 2, 3]);  // DEFAULT
      expect(user1?.str_arr).toEqual(['x', 'y']);

      // User2: int_arr provided, str_arr DEFAULT
      expect(user2?.int_arr).toEqual([10, 20]);
      expect(user2?.str_arr).toEqual(['a', 'b']);  // DEFAULT

      // User3: all provided
      expect(user3?.int_arr).toEqual([100]);
      expect(user3?.str_arr).toEqual(['z']);
    });

    it('should handle SKIP on JSON columns using DB DEFAULT', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.createMany([
          // Pattern 1: json_data SKIPped
          [[PgArraySkipTest.name, 'User1'], [PgArraySkipTest.json_data, SKIP], [PgArraySkipTest.json_arr, [10, 20]]],
          // Pattern 2: json_arr SKIPped
          [[PgArraySkipTest.name, 'User2'], [PgArraySkipTest.json_data, { custom: 'value' }], [PgArraySkipTest.json_arr, SKIP]],
          // Pattern 3: both SKIPped (same pattern)
          [[PgArraySkipTest.name, 'User3'], [PgArraySkipTest.json_data, SKIP], [PgArraySkipTest.json_arr, SKIP]],
          [[PgArraySkipTest.name, 'User4'], [PgArraySkipTest.json_data, SKIP], [PgArraySkipTest.json_arr, SKIP]],
        ]);
      });

      const users = await PgArraySkipTest.find([]);
      expect(users).toHaveLength(4);

      const user1 = users.find(u => u.name === 'User1');
      const user2 = users.find(u => u.name === 'User2');
      const user3 = users.find(u => u.name === 'User3');
      const user4 = users.find(u => u.name === 'User4');

      expect(user1?.json_data).toEqual({ default: true });  // DEFAULT
      expect(user1?.json_arr).toEqual([10, 20]);

      expect(user2?.json_data).toEqual({ custom: 'value' });
      expect(user2?.json_arr).toEqual([1, 2, 3]);  // DEFAULT

      expect(user3?.json_data).toEqual({ default: true });  // DEFAULT
      expect(user3?.json_arr).toEqual([1, 2, 3]);  // DEFAULT

      expect(user4?.json_data).toEqual({ default: true });  // DEFAULT
      expect(user4?.json_arr).toEqual([1, 2, 3]);  // DEFAULT
    });

    it('should handle mixed array/JSON with SKIP in same batch', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.createMany([
          [[PgArraySkipTest.name, 'A'], [PgArraySkipTest.int_arr, [1]], [PgArraySkipTest.str_arr, SKIP], [PgArraySkipTest.json_data, SKIP]],
          [[PgArraySkipTest.name, 'B'], [PgArraySkipTest.int_arr, SKIP], [PgArraySkipTest.str_arr, ['b']], [PgArraySkipTest.json_data, { b: 1 }]],
          [[PgArraySkipTest.name, 'C'], [PgArraySkipTest.int_arr, SKIP], [PgArraySkipTest.str_arr, SKIP], [PgArraySkipTest.json_data, SKIP]],
        ]);
      });

      const users = await PgArraySkipTest.find([]);
      expect(users).toHaveLength(3);

      const a = users.find(u => u.name === 'A');
      const b = users.find(u => u.name === 'B');
      const c = users.find(u => u.name === 'C');

      expect(a?.int_arr).toEqual([1]);
      expect(a?.str_arr).toEqual(['a', 'b']);  // DEFAULT
      expect(a?.json_data).toEqual({ default: true });  // DEFAULT

      expect(b?.int_arr).toEqual([1, 2, 3]);  // DEFAULT
      expect(b?.str_arr).toEqual(['b']);
      expect(b?.json_data).toEqual({ b: 1 });

      expect(c?.int_arr).toEqual([1, 2, 3]);  // DEFAULT
      expect(c?.str_arr).toEqual(['a', 'b']);  // DEFAULT
      expect(c?.json_data).toEqual({ default: true });  // DEFAULT
    });
  });

  describe('updateMany with Array/JSON and SKIP', () => {
    beforeEach(async () => {
      await DBModel.execute(`
        INSERT INTO pg_array_skip_test (name, int_arr, str_arr, bool_arr, json_data, json_arr) VALUES
        ('User1', '{10,20}', '{"x","y"}', '{true,true}', '{"key": "val1"}', '[1]'),
        ('User2', '{30,40}', '{"a","b"}', '{false,false}', '{"key": "val2"}', '[2]'),
        ('User3', '{50}', '{"z"}', '{true}', '{"key": "val3"}', '[3]')
      `);
    });

    it('should retain existing array values when SKIP is used', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.updateMany([
          [[PgArraySkipTest.id, 1], [PgArraySkipTest.int_arr, [999]], [PgArraySkipTest.str_arr, SKIP]],
          [[PgArraySkipTest.id, 2], [PgArraySkipTest.int_arr, SKIP], [PgArraySkipTest.str_arr, ['new', 'arr']]],
        ], {
          keyColumns: PgArraySkipTest.id,
        });
      });

      const users = await PgArraySkipTest.find([]);
      const user1 = users.find(u => u.id === 1);
      const user2 = users.find(u => u.id === 2);

      expect(user1?.int_arr).toEqual([999]);
      expect(user1?.str_arr).toEqual(['x', 'y']);  // Retained (SKIP)

      expect(user2?.int_arr).toEqual([30, 40]);  // Retained (SKIP)
      expect(user2?.str_arr).toEqual(['new', 'arr']);
    });

    it('should retain existing JSON values when SKIP is used', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.updateMany([
          [[PgArraySkipTest.id, 1], [PgArraySkipTest.json_data, { updated: true }], [PgArraySkipTest.json_arr, SKIP]],
          [[PgArraySkipTest.id, 2], [PgArraySkipTest.json_data, SKIP], [PgArraySkipTest.json_arr, ['a', 'b', 'c']]],
          [[PgArraySkipTest.id, 3], [PgArraySkipTest.json_data, SKIP], [PgArraySkipTest.json_arr, SKIP]],
        ], {
          keyColumns: PgArraySkipTest.id,
        });
      });

      const users = await PgArraySkipTest.find([]);
      const user1 = users.find(u => u.id === 1);
      const user2 = users.find(u => u.id === 2);
      const user3 = users.find(u => u.id === 3);

      expect(user1?.json_data).toEqual({ updated: true });
      expect(user1?.json_arr).toEqual([1]);  // Retained (SKIP)

      expect(user2?.json_data).toEqual({ key: 'val2' });  // Retained (SKIP)
      expect(user2?.json_arr).toEqual(['a', 'b', 'c']);

      // User3: both SKIPped - should be unchanged
      expect(user3?.json_data).toEqual({ key: 'val3' });
      expect(user3?.json_arr).toEqual([3]);
    });

    it('should handle null distinct from SKIP for array/JSON columns', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.updateMany([
          [[PgArraySkipTest.id, 1], [PgArraySkipTest.int_arr, null], [PgArraySkipTest.json_data, SKIP]],
          [[PgArraySkipTest.id, 2], [PgArraySkipTest.int_arr, SKIP], [PgArraySkipTest.json_data, null]],
        ], {
          keyColumns: PgArraySkipTest.id,
        });
      });

      const users = await PgArraySkipTest.find([]);
      const user1 = users.find(u => u.id === 1);
      const user2 = users.find(u => u.id === 2);

      expect(user1?.int_arr).toBeNull();  // Set to NULL
      expect(user1?.json_data).toEqual({ key: 'val1' });  // Retained (SKIP)

      expect(user2?.int_arr).toEqual([30, 40]);  // Retained (SKIP)
      expect(user2?.json_data).toBeNull();  // Set to NULL
    });

    it('should handle all column types with different SKIP patterns', async () => {
      await PgArraySkipTest.transaction(async () => {
        await PgArraySkipTest.updateMany([
          [
            [PgArraySkipTest.id, 1],
            [PgArraySkipTest.name, 'Updated1'],
            [PgArraySkipTest.int_arr, [1, 1, 1]],
            [PgArraySkipTest.str_arr, SKIP],
            [PgArraySkipTest.bool_arr, [false]],
            [PgArraySkipTest.json_data, SKIP],
            [PgArraySkipTest.json_arr, [100]],
          ],
          [
            [PgArraySkipTest.id, 2],
            [PgArraySkipTest.name, SKIP],
            [PgArraySkipTest.int_arr, SKIP],
            [PgArraySkipTest.str_arr, ['updated']],
            [PgArraySkipTest.bool_arr, SKIP],
            [PgArraySkipTest.json_data, { new: 'data' }],
            [PgArraySkipTest.json_arr, SKIP],
          ],
        ], {
          keyColumns: PgArraySkipTest.id,
        });
      });

      const users = await PgArraySkipTest.find([]);
      const user1 = users.find(u => u.id === 1);
      const user2 = users.find(u => u.id === 2);

      // User1: name, int_arr, bool_arr, json_arr updated; str_arr, json_data retained
      expect(user1?.name).toBe('Updated1');
      expect(user1?.int_arr).toEqual([1, 1, 1]);
      expect(user1?.str_arr).toEqual(['x', 'y']);  // Retained
      expect(user1?.bool_arr).toEqual([false]);
      expect(user1?.json_data).toEqual({ key: 'val1' });  // Retained
      expect(user1?.json_arr).toEqual([100]);

      // User2: str_arr, json_data updated; name, int_arr, bool_arr, json_arr retained
      expect(user2?.name).toBe('User2');  // Retained
      expect(user2?.int_arr).toEqual([30, 40]);  // Retained
      expect(user2?.str_arr).toEqual(['updated']);
      expect(user2?.bool_arr).toEqual([false, false]);  // Retained
      expect(user2?.json_data).toEqual({ new: 'data' });
      expect(user2?.json_arr).toEqual([2]);  // Retained
    });
  });
});

// ============================================
// MySQL Tests (using createDBBase)
// ============================================

describe.skipIf(skipIntegrationTests)('SKIP Pattern - MySQL', () => {
  let MysqlBase: typeof DBModel;
  let MysqlSkipTest: typeof MysqlSkipTestModel & ColumnsOf<MysqlSkipTestModel>;

  @model('mysql_skip_test')
  class MysqlSkipTestModel extends DBModel {
    @column({ primaryKey: true }) id?: number;
    @column() name?: string;
    @column() email?: string;
    @column() status?: string;
    @column() score?: number;
  }

  beforeAll(async () => {
    MysqlBase = getMysqlBase();
    MysqlSkipTest = bindModelToBase(MysqlSkipTestModel, MysqlBase).asModel();

    await MysqlBase.execute(`DROP TABLE IF EXISTS mysql_skip_test`);
    await MysqlBase.execute(`
      CREATE TABLE mysql_skip_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) DEFAULT 'default@example.com',
        status VARCHAR(50) DEFAULT 'active',
        score INT DEFAULT 0
      )
    `);
  });

  afterAll(async () => {
    try {
      await MysqlBase.execute('DROP TABLE IF EXISTS mysql_skip_test');
    } catch {
      // Ignore errors during cleanup
    }
  });

  beforeEach(async () => {
    await MysqlBase.execute('TRUNCATE TABLE mysql_skip_test');
  });

  describe('Single create with SKIP', () => {
    it('should exclude SKIPped columns in single create (uses DB DEFAULT)', async () => {
      await MysqlSkipTest.transaction(async () => {
        await MysqlSkipTest.create([
          [MysqlSkipTest.name, 'SingleUser'],
          [MysqlSkipTest.email, SKIP],  // Should use DB DEFAULT
          [MysqlSkipTest.status, 'admin'],
        ]);
      });

      const user = await MysqlSkipTest.findOne([[MysqlSkipTest.name, 'SingleUser']]);
      expect(user).not.toBeNull();
      expect(user?.email).toBe('default@example.com');  // DB DEFAULT
      expect(user?.status).toBe('admin');
    });
  });

  describe('createMany with SKIP', () => {
    it('should handle different SKIP patterns using grouped INSERT', async () => {
      await MysqlSkipTest.transaction(async () => {
        await MysqlSkipTest.createMany([
          [[MysqlSkipTest.name, 'John'], [MysqlSkipTest.email, SKIP], [MysqlSkipTest.status, 'admin']],
          [[MysqlSkipTest.name, 'Jane'], [MysqlSkipTest.email, 'jane@test.com'], [MysqlSkipTest.status, SKIP]],
          [[MysqlSkipTest.name, 'Bob'], [MysqlSkipTest.email, 'bob@test.com'], [MysqlSkipTest.status, 'user']],
        ]);
      });

      const users = await MysqlSkipTest.find([]);
      expect(users).toHaveLength(3);

      const john = users.find(u => u.name === 'John');
      const jane = users.find(u => u.name === 'Jane');
      const bob = users.find(u => u.name === 'Bob');

      expect(john?.email).toBe('default@example.com');
      expect(john?.status).toBe('admin');

      expect(jane?.email).toBe('jane@test.com');
      expect(jane?.status).toBe('active');

      expect(bob?.email).toBe('bob@test.com');
      expect(bob?.status).toBe('user');
    });
  });

  describe('updateMany with SKIP', () => {
    beforeEach(async () => {
      await MysqlBase.execute(`
        INSERT INTO mysql_skip_test (name, email, status, score) VALUES
        ('John', 'john@test.com', 'active', 10),
        ('Jane', 'jane@test.com', 'active', 20),
        ('Bob', 'bob@test.com', 'inactive', 30)
      `);
    });

    it('should retain existing value when column is SKIPped using batch', async () => {
      await MysqlSkipTest.transaction(async () => {
        await MysqlSkipTest.updateMany([
          [[MysqlSkipTest.id, 1], [MysqlSkipTest.email, SKIP], [MysqlSkipTest.status, 'updated']],
          [[MysqlSkipTest.id, 2], [MysqlSkipTest.email, 'new-jane@test.com'], [MysqlSkipTest.status, SKIP]],
        ], {
          keyColumns: MysqlSkipTest.id,
        });
      });

      const users = await MysqlSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('updated');

      expect(jane?.email).toBe('new-jane@test.com');
      expect(jane?.status).toBe('active');  // Retained
    });

    it('should handle multiple columns SKIPped in different rows', async () => {
      await MysqlSkipTest.transaction(async () => {
        await MysqlSkipTest.updateMany([
          [[MysqlSkipTest.id, 1], [MysqlSkipTest.email, SKIP], [MysqlSkipTest.status, SKIP], [MysqlSkipTest.score, 100]],
          [[MysqlSkipTest.id, 2], [MysqlSkipTest.email, 'new@test.com'], [MysqlSkipTest.status, 'mod'], [MysqlSkipTest.score, SKIP]],
        ], {
          keyColumns: MysqlSkipTest.id,
        });
      });

      const users = await MysqlSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('active');  // Retained
      expect(john?.score).toBe(100);

      expect(jane?.email).toBe('new@test.com');
      expect(jane?.status).toBe('mod');
      expect(jane?.score).toBe(20);  // Retained
    });
  });
});

// ============================================
// SQLite Tests (using createDBBase)
// ============================================

describe.skipIf(skipIntegrationTests)('SKIP Pattern - SQLite', () => {
  let SqliteBase: typeof DBModel;
  let SqliteSkipTest: typeof SqliteSkipTestModel & ColumnsOf<SqliteSkipTestModel>;

  @model('lite_skip_test')
  class SqliteSkipTestModel extends DBModel {
    @column({ primaryKey: true }) id?: number;
    @column() name?: string;
    @column() email?: string;
    @column() status?: string;
    @column() score?: number;
  }

  beforeAll(async () => {
    SqliteBase = getSqliteBase();
    SqliteSkipTest = bindModelToBase(SqliteSkipTestModel, SqliteBase).asModel();

    await SqliteBase.execute(`
      CREATE TABLE IF NOT EXISTS lite_skip_test (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT DEFAULT 'default@example.com',
        status TEXT DEFAULT 'active',
        score INTEGER DEFAULT 0
      )
    `);
  });

  afterAll(async () => {
    // SQLite in-memory DB is cleaned up automatically
  });

  beforeEach(async () => {
    // Delete data and reset autoincrement counter
    await SqliteBase.execute('DELETE FROM lite_skip_test');
    await SqliteBase.execute("DELETE FROM sqlite_sequence WHERE name = 'lite_skip_test'");
  });

  describe('Single create with SKIP', () => {
    it('should exclude SKIPped columns in single create (uses DB DEFAULT)', async () => {
      await SqliteSkipTest.transaction(async () => {
        await SqliteSkipTest.create([
          [SqliteSkipTest.name, 'SingleUser'],
          [SqliteSkipTest.email, SKIP],  // Should use DB DEFAULT
          [SqliteSkipTest.status, 'admin'],
        ]);
      });

      const user = await SqliteSkipTest.findOne([[SqliteSkipTest.name, 'SingleUser']]);
      expect(user).not.toBeNull();
      expect(user?.email).toBe('default@example.com');  // DB DEFAULT
      expect(user?.status).toBe('admin');
    });
  });

  describe('createMany with SKIP (grouped INSERT)', () => {
    it('should handle different SKIP patterns', async () => {
      await SqliteSkipTest.transaction(async () => {
        await SqliteSkipTest.createMany([
          [[SqliteSkipTest.name, 'John'], [SqliteSkipTest.email, SKIP], [SqliteSkipTest.status, 'admin']],
          [[SqliteSkipTest.name, 'Jane'], [SqliteSkipTest.email, 'jane@test.com'], [SqliteSkipTest.status, SKIP]],
          [[SqliteSkipTest.name, 'Bob'], [SqliteSkipTest.email, 'bob@test.com'], [SqliteSkipTest.status, 'user']],
        ]);
      });

      const users = await SqliteSkipTest.find([]);
      expect(users).toHaveLength(3);

      const john = users.find(u => u.name === 'John');
      const jane = users.find(u => u.name === 'Jane');
      const bob = users.find(u => u.name === 'Bob');

      expect(john?.email).toBe('default@example.com');
      expect(john?.status).toBe('admin');

      expect(jane?.email).toBe('jane@test.com');
      expect(jane?.status).toBe('active');

      expect(bob?.email).toBe('bob@test.com');
      expect(bob?.status).toBe('user');
    });
  });

  describe('updateMany with SKIP', () => {
    beforeEach(async () => {
      await SqliteBase.execute(`
        INSERT INTO lite_skip_test (name, email, status, score) VALUES
        ('John', 'john@test.com', 'active', 10),
        ('Jane', 'jane@test.com', 'active', 20),
        ('Bob', 'bob@test.com', 'inactive', 30)
      `);
    });

    it('should retain existing value when column is SKIPped using batch', async () => {
      await SqliteSkipTest.transaction(async () => {
        await SqliteSkipTest.updateMany([
          [[SqliteSkipTest.id, 1], [SqliteSkipTest.email, SKIP], [SqliteSkipTest.status, 'updated']],
          [[SqliteSkipTest.id, 2], [SqliteSkipTest.email, 'new-jane@test.com'], [SqliteSkipTest.status, SKIP]],
        ], {
          keyColumns: SqliteSkipTest.id,
        });
      });

      const users = await SqliteSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('updated');

      expect(jane?.email).toBe('new-jane@test.com');
      expect(jane?.status).toBe('active');  // Retained
    });

    it('should handle multiple columns SKIPped in different rows', async () => {
      await SqliteSkipTest.transaction(async () => {
        await SqliteSkipTest.updateMany([
          [[SqliteSkipTest.id, 1], [SqliteSkipTest.email, SKIP], [SqliteSkipTest.status, SKIP], [SqliteSkipTest.score, 100]],
          [[SqliteSkipTest.id, 2], [SqliteSkipTest.email, 'new@test.com'], [SqliteSkipTest.status, 'mod'], [SqliteSkipTest.score, SKIP]],
        ], {
          keyColumns: SqliteSkipTest.id,
        });
      });

      const users = await SqliteSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBe('john@test.com');  // Retained
      expect(john?.status).toBe('active');  // Retained
      expect(john?.score).toBe(100);

      expect(jane?.email).toBe('new@test.com');
      expect(jane?.status).toBe('mod');
      expect(jane?.score).toBe(20);  // Retained
    });

    it('should handle null values distinct from SKIP', async () => {
      await SqliteSkipTest.transaction(async () => {
        await SqliteSkipTest.updateMany([
          [[SqliteSkipTest.id, 1], [SqliteSkipTest.email, null], [SqliteSkipTest.status, SKIP]],
          [[SqliteSkipTest.id, 2], [SqliteSkipTest.email, SKIP], [SqliteSkipTest.status, null]],
        ], {
          keyColumns: SqliteSkipTest.id,
        });
      });

      const users = await SqliteSkipTest.find([]);
      const john = users.find(u => u.id === 1);
      const jane = users.find(u => u.id === 2);

      expect(john?.email).toBeNull();  // Set to NULL
      expect(john?.status).toBe('active');  // Retained

      expect(jane?.email).toBe('jane@test.com');  // Retained
      expect(jane?.status).toBeNull();  // Set to NULL
    });
  });
});

// Final cleanup
afterAll(async () => {
  await closeAllPools();
});
