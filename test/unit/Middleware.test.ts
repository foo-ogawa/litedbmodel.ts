/**
 * Middleware Tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Middleware } from '../../src/Middleware';
import { StatisticsMiddleware } from '../../src/middlewares/statistics';

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
    it('should return false before any access', () => {
      expect(TestMiddleware.hasContext()).toBe(false);
    });

    it('should return true after access', () => {
      TestMiddleware.getCurrentContext();
      expect(TestMiddleware.hasContext()).toBe(true);
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
        count = 0;
      }

      await Promise.all([
        CounterMiddleware.run(async () => {
          const ctx = CounterMiddleware.getCurrentContext();
          ctx.count = 1;
          await new Promise(r => setTimeout(r, 10));
          results.push(ctx.count);
        }),
        CounterMiddleware.run(async () => {
          const ctx = CounterMiddleware.getCurrentContext();
          ctx.count = 2;
          await new Promise(r => setTimeout(r, 5));
          results.push(ctx.count);
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
      const mockNext = async () => null;

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
      const mockNext = async () => ({});

      await ctx.create({} as any, mockNext, []);

      expect(ctx.insert_counter).toBe(1);
    });

    it('createMany should increment insert_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

      await ctx.createMany({} as any, mockNext, []);

      expect(ctx.insert_counter).toBe(1);
    });

    it('update should increment update_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

      await ctx.update({} as any, mockNext, [], []);

      expect(ctx.update_counter).toBe(1);
    });

    it('delete should increment delete_counter', async () => {
      const ctx = StatisticsMiddleware.getCurrentContext();
      const mockNext = async () => [];

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

