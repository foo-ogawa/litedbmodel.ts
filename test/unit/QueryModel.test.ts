/**
 * Query-Based Model Tests
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import {
  DBModel,
  model,
  column,
  type ColumnsOf,
} from '../../src';
import { isColumn } from '../../src/Column';

describe('Query-Based Models', () => {
  describe('static QUERY property', () => {
    it('should recognize a model with QUERY as query-based', () => {
      @model('user_stats')
      class UserStats extends DBModel {
        @column() id?: number;
        @column() post_count?: number;

        static QUERY = `
          SELECT users.id, COUNT(posts.id) as post_count
          FROM users LEFT JOIN posts ON users.id = posts.user_id
          GROUP BY users.id
        `;
      }

      expect(UserStats.isQueryBased()).toBe(true);
      expect(UserStats.TABLE_NAME).toBe('user_stats');
    });

    it('should not be query-based without QUERY', () => {
      @model('users')
      class RegularModel extends DBModel {
        @column() id?: number;
      }

      expect(RegularModel.isQueryBased()).toBe(false);
      expect(RegularModel.getTableName()).toBe('users');
      expect(RegularModel.getUpdateTableName()).toBe('users');
    });

    it('should throw error on getUpdateTableName for query-based models', () => {
      @model('derived')
      class QueryModel extends DBModel {
        @column() id?: number;

        static QUERY = `SELECT id FROM users`;
      }

      expect(() => QueryModel.getUpdateTableName()).toThrow('Query-based models cannot be updated or deleted directly');
    });
  });

  describe('CTE SQL generation', () => {
    it('should generate CTE-based SQL for query-based models', () => {
      @model('user_stats')
      class UserStatsModel extends DBModel {
        @column() id?: number;
        @column() post_count?: number;

        static QUERY = `SELECT users.id, COUNT(posts.id) as post_count FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id`;
      }
      const UserStats = UserStatsModel as typeof UserStatsModel & ColumnsOf<UserStatsModel>;

      // Access protected method via any for testing
      const { sql, params } = (UserStats as any)._buildSelectSQL({ post_count: 10 });
      
      // Should use CTE (WITH clause)
      expect(sql).toContain('WITH user_stats AS');
      expect(sql).toContain('SELECT users.id, COUNT(posts.id) as post_count');
      expect(sql).toContain('SELECT * FROM user_stats');
      expect(sql).toContain('WHERE');
    });

    it('should not use CTE for regular models', () => {
      @model('users')
      class RegularModel extends DBModel {
        @column() id?: number;
        @column() name?: string;
      }

      const { sql } = (RegularModel as any)._buildSelectSQL({ name: 'John' });
      
      // Should NOT use CTE
      expect(sql).not.toContain('WITH');
      expect(sql).toContain('SELECT * FROM users');
    });

    it('should use custom alias from TABLE_NAME', () => {
      @model('my_custom_alias')
      class CustomAliasModel extends DBModel {
        @column() id?: number;

        static QUERY = `SELECT id FROM users`;
      }

      const { sql } = (CustomAliasModel as any)._buildSelectSQL({});
      
      expect(sql).toContain('WITH my_custom_alias AS');
      expect(sql).toContain('SELECT * FROM my_custom_alias');
    });
  });

  describe('withQuery() method', () => {
    it('should create a bound model with query parameters', () => {
      @model('sales_report')
      class SalesReportModel extends DBModel {
        @column() product_id?: number;
        @column() total_revenue?: number;

        static forPeriod(startDate: string, endDate: string) {
          return this.withQuery({
            sql: `
              SELECT p.id AS product_id, SUM(oi.price) AS total_revenue
              FROM products p
              JOIN order_items oi ON p.id = oi.product_id
              JOIN orders o ON oi.order_id = o.id
              WHERE o.created_at >= $1 AND o.created_at < $2
              GROUP BY p.id
            `,
            params: [startDate, endDate],
          });
        }
      }
      const SalesReport = SalesReportModel as typeof SalesReportModel & ColumnsOf<SalesReportModel>;

      const Q1Report = SalesReport.forPeriod('2024-01-01', '2024-04-01');

      // Check that the bound model has the query params
      expect(Q1Report.getQueryParams()).toEqual(['2024-01-01', '2024-04-01']);
      expect(Q1Report.isQueryBased()).toBe(true);
    });

    it('should preserve TABLE_NAME in bound model', () => {
      @model('my_model')
      class MyModel extends DBModel {
        @column() id?: number;

        static forDate(date: string) {
          return this.withQuery({
            sql: `SELECT id FROM users WHERE created_at > $1`,
            params: [date],
          });
        }
      }

      const BoundModel = MyModel.forDate('2024-01-01');

      expect(BoundModel.TABLE_NAME).toBe('my_model');
      expect(BoundModel.getCTEAlias()).toBe('my_model');
    });

    it('should preserve Column properties in bound model', () => {
      @model('test_model')
      class TestModel extends DBModel {
        @column() id?: number;
        @column() name?: string;

        static withDateFilter(date: string) {
          return this.withQuery({
            sql: `SELECT * FROM users WHERE date > $1`,
            params: [date],
          });
        }
      }
      const TestModelWithCols = TestModel as typeof TestModel & ColumnsOf<TestModel>;

      const Bound = TestModelWithCols.withDateFilter('2024-01-01');

      // Column properties should be preserved
      expect(isColumn((Bound as any).id)).toBe(true);
      expect(isColumn((Bound as any).name)).toBe(true);
    });

    it('should prepend query params to condition params', () => {
      @model('filtered')
      class FilteredModel extends DBModel {
        @column() id?: number;
        @column() status?: string;

        static QUERY = `SELECT * FROM users`;
        
        static forStatus(status: string) {
          return this.withQuery({
            sql: `SELECT * FROM users WHERE status = $1`,
            params: [status],
          });
        }
      }
      const Filtered = FilteredModel as typeof FilteredModel & ColumnsOf<FilteredModel>;

      const ActiveUsers = Filtered.forStatus('active');
      const { sql, params } = (ActiveUsers as any)._buildSelectSQL({ id: 1 });

      // Query params should come first, then condition params
      expect(params[0]).toBe('active');  // Query param
      expect(params[1]).toBe(1);          // Condition param
      
      // SQL should use CTE
      expect(sql).toContain('WITH filtered AS');
    });

    it('should create independent bound models', () => {
      @model('report')
      class ReportModel extends DBModel {
        @column() id?: number;

        static forYear(year: number) {
          return this.withQuery({
            sql: `SELECT * FROM data WHERE year = $1`,
            params: [year],
          });
        }
      }

      const Report2023 = ReportModel.forYear(2023);
      const Report2024 = ReportModel.forYear(2024);

      // Models should have different params
      expect(Report2023.getQueryParams()).toEqual([2023]);
      expect(Report2024.getQueryParams()).toEqual([2024]);
      
      // Original model should not have params
      expect(ReportModel.getQueryParams()).toEqual([]);
    });
  });

  describe('count with CTE', () => {
    it('should generate CTE-based COUNT SQL', () => {
      @model('stats')
      class StatsModel extends DBModel {
        @column() id?: number;
        @column() value?: number;

        static QUERY = `SELECT id, value FROM source_table`;
      }

      // Access protected _count method
      // Note: We can't easily test _count without mocking the handler,
      // but we can verify the model is properly configured
      expect(StatsModel.isQueryBased()).toBe(true);
      expect(StatsModel.QUERY).toContain('SELECT id, value');
    });
  });

  describe('Type-safe column references in QUERY', () => {
    it('should allow using Column references in QUERY string', () => {
      @model('users')
      class UserModel extends DBModel {
        @column() id?: number;
        @column() name?: string;
      }
      const User = UserModel as typeof UserModel & ColumnsOf<UserModel>;

      @model('posts')
      class PostModel extends DBModel {
        @column() id?: number;
        @column() user_id?: number;
      }
      const Post = PostModel as typeof PostModel & ColumnsOf<PostModel>;

      // Using Column references in QUERY
      @model('user_post_stats')
      class UserPostStatsModel extends DBModel {
        @column() user_id?: number;
        @column() post_count?: number;

        // Column.toString() returns the column name
        static QUERY = `
          SELECT ${User.id} AS user_id, COUNT(${Post.id}) AS post_count
          FROM users
          LEFT JOIN posts ON ${User.id} = ${Post.user_id}
          GROUP BY ${User.id}
        `;
      }

      // Verify the QUERY contains the expected column names
      expect(UserPostStatsModel.QUERY).toContain('id AS user_id');
      expect(UserPostStatsModel.QUERY).toContain('COUNT(id)');
    });
  });
});
