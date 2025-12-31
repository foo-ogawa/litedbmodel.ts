/**
 * TypeCast Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  castToDatetime,
  castToBoolean,
  castToIntegerArray,
  castToNumericArray,
  castToStringArray,
  castToBooleanArray,
  castToDatetimeArray,
  castToJson,
  getTypeCast,
  setTypeCastImpl,
  resetTypeCastImpl,
  type TypeCastFunctions,
} from '../../src/TypeCast';

describe('TypeCast', () => {
  beforeEach(() => {
    // Reset to default implementation before each test
    resetTypeCastImpl();
  });

  describe('castToDatetime', () => {
    it('should convert Date to Date', () => {
      const date = new Date('2024-01-01');
      expect(castToDatetime(date)).toEqual(date);
    });

    it('should convert ISO string to Date', () => {
      const result = castToDatetime('2024-01-01T12:00:00Z');
      expect(result).toBeInstanceOf(Date);
      expect(result?.getFullYear()).toBe(2024);
    });

    it('should convert timestamp to Date', () => {
      const timestamp = 1704067200000;
      const result = castToDatetime(timestamp);
      expect(result).toBeInstanceOf(Date);
    });

    it('should return null for null/undefined', () => {
      expect(castToDatetime(null)).toBeNull();
      expect(castToDatetime(undefined)).toBeNull();
    });

    it('should return null for invalid date', () => {
      expect(castToDatetime('invalid')).toBeNull();
    });

    it('should return null for other types', () => {
      expect(castToDatetime({})).toBeNull();
      expect(castToDatetime([])).toBeNull();
    });
  });

  describe('castToBoolean', () => {
    it('should convert boolean to boolean', () => {
      expect(castToBoolean(true)).toBe(true);
      expect(castToBoolean(false)).toBe(false);
    });

    it('should convert number to boolean', () => {
      expect(castToBoolean(1)).toBe(true);
      expect(castToBoolean(0)).toBe(false);
      expect(castToBoolean(-1)).toBe(true);
    });

    it('should convert string to boolean', () => {
      expect(castToBoolean('true')).toBe(true);
      expect(castToBoolean('TRUE')).toBe(true);
      expect(castToBoolean('t')).toBe(true);
      expect(castToBoolean('1')).toBe(true);
      expect(castToBoolean('false')).toBe(false);
      expect(castToBoolean('FALSE')).toBe(false);
      expect(castToBoolean('f')).toBe(false);
      expect(castToBoolean('0')).toBe(false);
    });

    it('should return null for null/undefined', () => {
      expect(castToBoolean(null)).toBeNull();
      expect(castToBoolean(undefined)).toBeNull();
    });

    it('should return null for invalid string', () => {
      expect(castToBoolean('yes')).toBeNull();
      expect(castToBoolean('no')).toBeNull();
    });
  });

  describe('castToIntegerArray', () => {
    it('should convert array of numbers', () => {
      expect(castToIntegerArray([1, 2, 3])).toEqual([1, 2, 3]);
    });

    it('should convert array of strings', () => {
      expect(castToIntegerArray(['1', '2', '3'])).toEqual([1, 2, 3]);
    });

    it('should convert JSON array string', () => {
      expect(castToIntegerArray('[1,2,3]')).toEqual([1, 2, 3]);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToIntegerArray('{1,2,3}')).toEqual([1, 2, 3]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToIntegerArray(null)).toEqual([]);
      expect(castToIntegerArray(undefined)).toEqual([]);
    });

    it('should return empty array for non-array JSON', () => {
      expect(castToIntegerArray('{"a":1}')).toEqual([]);
    });

    it('should handle non-numeric values in array', () => {
      expect(castToIntegerArray(['a', 'b'])).toEqual([0, 0]);
    });
  });

  describe('castToNumericArray', () => {
    it('should convert array with decimals', () => {
      expect(castToNumericArray([1.5, 2.5, 3.5])).toEqual([1.5, 2.5, 3.5]);
    });

    it('should handle null values in array', () => {
      expect(castToNumericArray([1, null, 3])).toEqual([1, null, 3]);
    });

    it('should convert JSON array string', () => {
      expect(castToNumericArray('[1.5,2.5]')).toEqual([1.5, 2.5]);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToNumericArray('{1.5,2.5,NULL}')).toEqual([1.5, 2.5, null]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToNumericArray(null)).toEqual([]);
      expect(castToNumericArray(undefined)).toEqual([]);
    });

    it('should return null for invalid values', () => {
      expect(castToNumericArray(['a'])).toEqual([null]);
    });
  });

  describe('castToStringArray', () => {
    it('should convert array of strings', () => {
      expect(castToStringArray(['a', 'b', 'c'])).toEqual(['a', 'b', 'c']);
    });

    it('should convert array of mixed types to strings', () => {
      expect(castToStringArray([1, 'b', true])).toEqual(['1', 'b', 'true']);
    });

    it('should convert JSON array string', () => {
      expect(castToStringArray('["a","b","c"]')).toEqual(['a', 'b', 'c']);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToStringArray('{a,b,c}')).toEqual(['a', 'b', 'c']);
    });

    it('should handle quoted strings in PostgreSQL format', () => {
      expect(castToStringArray('{"hello world","test"}')).toEqual(['hello world', 'test']);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToStringArray(null)).toEqual([]);
      expect(castToStringArray(undefined)).toEqual([]);
    });
  });

  describe('castToBooleanArray', () => {
    it('should convert array of booleans', () => {
      expect(castToBooleanArray([true, false, true])).toEqual([true, false, true]);
    });

    it('should convert array of mixed types', () => {
      expect(castToBooleanArray([1, 0, 'true', 'false'])).toEqual([true, false, true, false]);
    });

    it('should convert JSON array string', () => {
      expect(castToBooleanArray('[true,false,true]')).toEqual([true, false, true]);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToBooleanArray('{t,f,t}')).toEqual([true, false, true]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToBooleanArray(null)).toEqual([]);
      expect(castToBooleanArray(undefined)).toEqual([]);
    });
  });

  describe('castToDatetimeArray', () => {
    it('should convert array of dates', () => {
      const dates = [new Date('2024-01-01'), new Date('2024-01-02')];
      const result = castToDatetimeArray(dates);
      expect(result.length).toBe(2);
      expect(result[0]).toBeInstanceOf(Date);
    });

    it('should convert array of date strings', () => {
      const result = castToDatetimeArray(['2024-01-01', '2024-01-02']);
      expect(result.length).toBe(2);
      expect(result[0]).toBeInstanceOf(Date);
    });

    it('should convert JSON array string', () => {
      const result = castToDatetimeArray('["2024-01-01","2024-01-02"]');
      expect(result.length).toBe(2);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToDatetimeArray(null)).toEqual([]);
      expect(castToDatetimeArray(undefined)).toEqual([]);
    });
  });

  describe('castToJson', () => {
    it('should return object as-is', () => {
      const obj = { key: 'value' };
      expect(castToJson(obj)).toEqual(obj);
    });

    it('should return array as-is', () => {
      const arr = [1, 2, 3];
      expect(castToJson(arr)).toEqual(arr);
    });

    it('should parse JSON string to object', () => {
      expect(castToJson('{"key":"value"}')).toEqual({ key: 'value' });
    });

    it('should parse JSON string to array', () => {
      expect(castToJson('[1,2,3]')).toEqual([1, 2, 3]);
    });

    it('should return null for invalid JSON', () => {
      expect(castToJson('invalid')).toBeNull();
    });

    it('should return null for null/undefined', () => {
      expect(castToJson(null)).toBeNull();
      expect(castToJson(undefined)).toBeNull();
    });
  });

  describe('setTypeCastImpl / resetTypeCastImpl', () => {
    it('should allow custom implementation', () => {
      const customImpl: TypeCastFunctions = {
        castToDatetime: () => new Date('2000-01-01'),
        castToBoolean: () => true,
        castToIntegerArray: () => [999],
        castToNumericArray: () => [99.9],
        castToStringArray: () => ['custom'],
        castToBooleanArray: () => [true],
        castToDatetimeArray: () => [new Date()],
        castToJson: () => ({ custom: true }),
      };

      setTypeCastImpl(customImpl);

      expect(castToDatetime('anything')).toEqual(new Date('2000-01-01'));
      expect(castToBoolean(null)).toBe(true);
      expect(castToIntegerArray([])).toEqual([999]);

      resetTypeCastImpl();

      // After reset, should use default again
      expect(castToBoolean(null)).toBeNull();
    });

    it('getTypeCast should return current implementation', () => {
      const typeCast = getTypeCast();
      expect(typeof typeCast.castToDatetime).toBe('function');
      expect(typeof typeCast.castToBoolean).toBe('function');
    });
  });
});

