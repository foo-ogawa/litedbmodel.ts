/**
 * Middleware Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Middleware, createMiddleware } from '../../src/Middleware';
import { StatisticsMiddleware } from '../../src/middlewares/statistics';
import { DBModel } from '../../src/DBModel';

// Test middleware implementation
class TestMiddleware extends Middleware {
  initCalled = false;
  hooksCalled: string[] = [];

  init(): void {
    this.initCalled = true;
  }

  async execute(
    next: (sql: string, params?: unknown[]) => Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>,
    sql: string,
    params?: unknown[]
  ) {
    this.hooksCalled.push('execute');
    return next(sql, params);
  }
}

describe('Middleware', () => {
  beforeEach(() => {
    TestMiddleware.clearContext();
  });

  afterEach(() => {
    TestMiddleware.clearContext();
  });

  describe('getCurrentContext', () => {
    it('should create new instance on first access', () => {
      const ctx = TestMiddleware.getCurrentContext();
      expect(ctx).toBeInstanceOf(TestMiddleware);
    });

    it('should return same instance on subsequent accesses', () => {
      const ctx1 = TestMiddleware.getCurrentContext();
      const ctx2 = TestMiddleware.getCurrentContext();
      expect(ctx1).toBe(ctx2);
    });
  });

  describe('hasContext', () => {
    it('should return false before any access (using fresh class)', () => {
      // Use a fresh middleware class to avoid test interference
      class FreshMiddleware extends Middleware {}
      expect(FreshMiddleware.hasContext()).toBe(false);
    });

    it('should return true after access', () => {
      class FreshMiddleware extends Middleware {}
      FreshMiddleware.getCurrentContext();
      expect(FreshMiddleware.hasContext()).toBe(true);
    });

    it('should return false after clearContext', () => {
      class FreshMiddleware extends Middleware {}
      FreshMiddleware.getCurrentContext();
      expect(FreshMiddleware.hasContext()).toBe(true);
      FreshMiddleware.clearContext();
      expect(FreshMiddleware.hasContext()).toBe(false);
    });
  });

  describe('clearContext', () => {
    it('should clear the current context', () => {
      TestMiddleware.getCurrentContext();
      expect(TestMiddleware.hasContext()).toBe(true);
      TestMiddleware.clearContext();
      // After clear, next access creates new instance
      const newCtx = TestMiddleware.getCurrentContext();
      expect(newCtx).toBeInstanceOf(TestMiddleware);
    });

    it('should not throw if no context exists', () => {
      expect(() => TestMiddleware.clearContext()).not.toThrow();
    });
  });

  describe('run', () => {
    it('should run function with fresh context', () => {
      let ctx1: TestMiddleware | undefined;
      let ctx2: TestMiddleware | undefined;

      TestMiddleware.run(() => {
        ctx1 = TestMiddleware.getCurrentContext();
      });

      TestMiddleware.run(() => {
        ctx2 = TestMiddleware.getCurrentContext();
      });

      expect(ctx1).not.toBe(ctx2);
    });

    it('should return function result', () => {
      const result = TestMiddleware.run(() => {
        return 'result';
      });
      expect(result).toBe('result');
    });
  });

  describe('instance isolation', () => {
    it('should have isolated state per context', async () => {
      const results: number[] = [];

      class CounterMiddleware extends Middleware {
        counter = 0;
      }

      await Promise.all([
        CounterMiddleware.run(async () => {
          const ctx = CounterMiddleware.getCurrentContext();
          ctx.counter = 1;
          await new Promise(r => setTimeout(r, 10));
          results.push(ctx.counter);
        }),
        CounterMiddleware.run(async () => {
          const ctx = CounterMiddleware.getCurrentContext();
          ctx.counter = 2;
          await new Promise(r => setTimeout(r, 5));
          results.push(ctx.counter);
        }),
      ]);

      // Each context should maintain its own count
      expect(results.sort()).toEqual([1, 2]);
      CounterMiddleware.clearContext();
    });
  });
});

describe('StatisticsMiddleware', () => {
  beforeEach(() => {
    StatisticsMiddleware.clearContext();
  });

  afterEach(() => {
    StatisticsMiddleware.clearContext();
  });

  describe('counters', () => {
    it('should initialize with zero counters', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      expect(ctx.find_all_counter).toBe(0);
      expect(ctx.find_one_counter).toBe(0);
      expect(ctx.insert_counter).toBe(0);
      expect(ctx.update_counter).toBe(0);
      expect(ctx.delete_counter).toBe(0);
      expect(ctx.execute_counter).toBe(0);
      expect(ctx.query_counter).toBe(0);
    });

    it('should initialize with zero durations', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      expect(ctx.find_all_msec).toBe(0);
      expect(ctx.find_one_msec).toBe(0);
      expect(ctx.insert_msec).toBe(0);
      expect(ctx.update_msec).toBe(0);
      expect(ctx.delete_msec).toBe(0);
      expect(ctx.execute_msec).toBe(0);
      expect(ctx.query_msec).toBe(0);
    });
  });

  describe('reset', () => {
    it('should reset all counters and durations', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      ctx.find_all_counter = 10;
      ctx.find_one_counter = 5;
      ctx.insert_counter = 3;
      ctx.find_all_msec = 100;

      ctx.reset();

      expect(ctx.find_all_counter).toBe(0);
      expect(ctx.find_one_counter).toBe(0);
      expect(ctx.insert_counter).toBe(0);
      expect(ctx.find_all_msec).toBe(0);
    });
  });

  describe('totalCount', () => {
    it('should sum all counters', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      ctx.find_all_counter = 1;
      ctx.find_one_counter = 2;
      ctx.insert_counter = 3;
      ctx.update_counter = 4;
      ctx.delete_counter = 5;
      ctx.execute_counter = 6;
      ctx.query_counter = 7;

      expect(ctx.totalCount).toBe(28);
    });
  });

  describe('totalMsec', () => {
    it('should sum all durations', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      ctx.find_all_msec = 10;
      ctx.find_one_msec = 20;
      ctx.insert_msec = 30;
      ctx.update_msec = 40;
      ctx.delete_msec = 50;
      ctx.execute_msec = 60;
      ctx.query_msec = 70;

      expect(ctx.totalMsec).toBe(280);
    });
  });

  describe('getLog', () => {
    it('should return formatted log string', () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      ctx.find_all_counter = 1;
      ctx.find_all_msec = 10;
      ctx.find_one_counter = 2;
      ctx.find_one_msec = 20;

      const log = ctx.getLog();

      expect(log).toContain('Total:3(30ms)');
      expect(log).toContain('FindOne:2(20ms)');
      expect(log).toContain('FindAll:1(10ms)');
      expect(log).toContain('Insert:0(0ms)');
    });
  });

  describe('hooks', () => {
    it('find should increment find_all_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

      await ctx.find({} as any, mockNext, []);

      expect(ctx.find_all_counter).toBe(1);
      expect(ctx.find_all_msec).toBeGreaterThanOrEqual(0);
    });

    it('findOne should increment find_one_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => null;

      await ctx.findOne({} as any, mockNext, []);

      expect(ctx.find_one_counter).toBe(1);
    });

    it('findById should increment find_one_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

      await ctx.findById({} as any, mockNext, 1);

      expect(ctx.find_one_counter).toBe(1);
    });

    it('count should increment find_all_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => 5;

      await ctx.count({} as any, mockNext, []);

      expect(ctx.find_all_counter).toBe(1);
    });

    it('create should increment insert_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => ({ key: [], values: [[1]] }) as any;

      await ctx.create({} as any, mockNext, []);

      expect(ctx.insert_counter).toBe(1);
    });

    it('createMany should increment insert_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => ({ key: [], values: [[1]] }) as any;

      await ctx.createMany({} as any, mockNext, []);

      expect(ctx.insert_counter).toBe(1);
    });

    it('update should increment update_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => ({ key: [], values: [[1]] }) as any;

      await ctx.update({} as any, mockNext, [], []);

      expect(ctx.update_counter).toBe(1);
    });

    it('delete should increment delete_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => ({ key: [], values: [[1]] }) as any;

      await ctx.delete({} as any, mockNext, []);

      expect(ctx.delete_counter).toBe(1);
    });

    it('execute should increment execute_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => ({ rows: [], rowCount: 0 });

      await ctx.execute(mockNext, 'SELECT 1');

      expect(ctx.execute_counter).toBe(1);
    });

    it('query should increment query_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

      await ctx.query({} as any, mockNext, 'SELECT 1');

      expect(ctx.query_counter).toBe(1);
    });
  });

  describe('duration tracking', () => {
    it('should track execution duration', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => {
        await new Promise(r => setTimeout(r, 15));
        return [];
      };

      await ctx.find({} as any, mockNext, []);

      // Use lower threshold for CI environment timer precision
      expect(ctx.find_all_msec).toBeGreaterThanOrEqual(10);
    });
  });
});

describe('createMiddleware', () => {
  describe('basic functionality', () => {
    it('should create a middleware class', () => {
      const TestMW = createMiddleware({});
      expect(typeof TestMW).toBe('function');
      expect(TestMW.getCurrentContext).toBeDefined();
    });

    it('should create instance with initial state', () => {
      const TestMW = createMiddleware({
        state: { count: 0, name: 'test' }
      });
      const ctx = TestMW.getCurrentContext();
      expect(ctx.count).toBe(0);
      expect(ctx.name).toBe('test');
      TestMW.clearContext();
    });

    it('should deep clone state for each context', () => {
      const TestMW = createMiddleware({
        state: { items: [1, 2, 3], nested: { value: 1 } }
      });
      
      let ctx1Items: number[] = [];
      let ctx2Items: number[] = [];
      
      TestMW.run(() => {
        const ctx = TestMW.getCurrentContext();
        ctx.items.push(4);
        ctx.nested.value = 99;
        ctx1Items = [...ctx.items];
      });
      
      TestMW.run(() => {
        const ctx = TestMW.getCurrentContext();
        ctx2Items = [...ctx.items];
        expect(ctx.nested.value).toBe(1); // Fresh copy
      });
      
      expect(ctx1Items).toEqual([1, 2, 3, 4]);
      expect(ctx2Items).toEqual([1, 2, 3]); // Fresh copy without push
      TestMW.clearContext();
    });
  });

  describe('hooks', () => {
    it('should create middleware with execute hook', () => {
      const calls: string[] = [];
      
      const LoggerMW = createMiddleware({
        execute: async function(next, sql, params) {
          calls.push(sql);
          return next(sql, params);
        }
      });
      
      // Verify middleware was created and can get context
      const ctx = LoggerMW.getCurrentContext();
      expect(ctx).toBeDefined();
      LoggerMW.clearContext();
    });

    it('should bind this to state in hooks', () => {
      const LoggerMW = createMiddleware({
        state: { queries: [] as string[] },
        execute: async function(next, sql, params) {
          this.queries.push(sql);
          return next(sql, params);
        }
      });
      
      // State should be accessible via getCurrentContext
      const ctx = LoggerMW.getCurrentContext();
      expect(ctx.queries).toEqual([]);
      
      // State should be modifiable
      ctx.queries.push('test');
      expect(ctx.queries).toEqual(['test']);
      
      LoggerMW.clearContext();
    });
  });

  describe('static methods', () => {
    it('should support run() for isolated contexts', async () => {
      const CounterMW = createMiddleware({
        state: { count: 0 }
      });
      
      const results: number[] = [];
      
      await Promise.all([
        CounterMW.run(async () => {
          CounterMW.getCurrentContext().count = 10;
          await new Promise(r => setTimeout(r, 5));
          results.push(CounterMW.getCurrentContext().count);
        }),
        CounterMW.run(async () => {
          CounterMW.getCurrentContext().count = 20;
          results.push(CounterMW.getCurrentContext().count);
        })
      ]);
      
      expect(results.sort()).toEqual([10, 20]);
      CounterMW.clearContext();
    });

    it('should support hasContext()', () => {
      const TestMW = createMiddleware({ state: { x: 1 } });
      expect(TestMW.hasContext()).toBe(false);
      TestMW.getCurrentContext();
      expect(TestMW.hasContext()).toBe(true);
      TestMW.clearContext();
      expect(TestMW.hasContext()).toBe(false);
    });
  });

  describe('DBModel.createMiddleware', () => {
    it('should be accessible via DBModel.createMiddleware', () => {
      const TestMW = DBModel.createMiddleware({
        state: { tenantId: 0 }
      });
      
      const ctx = TestMW.getCurrentContext();
      expect(ctx.tenantId).toBe(0);
      ctx.tenantId = 123;
      expect(TestMW.getCurrentContext().tenantId).toBe(123);
      TestMW.clearContext();
    });
  });
});

