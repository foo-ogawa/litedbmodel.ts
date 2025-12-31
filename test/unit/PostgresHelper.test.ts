/**
 * PostgresHelper Tests
 */

import { describe, it, expect } from 'vitest';
import {
  castToDatetime,
  castToBoolean,
  castToIntegerArray,
  castToNumericArray,
  castToStringArray,
  castToBooleanArray,
  castToDatetimeArray,
  castToJson,
  pgArrayParse,
  makeLikeString,
  pgTypeToTsType,
  Now,
  Null,
  True,
  False,
  pgIntArray,
  pgNumericArray,
  pgStringArray,
  pgDateArray,
  TimeAfter,
  DayAfter,
  empty2null,
} from '../../src/drivers/PostgresHelper';

describe('PostgresHelper', () => {
  describe('castToDatetime', () => {
    it('should convert Date to Date', () => {
      const date = new Date('2024-01-01');
      expect(castToDatetime(date)).toEqual(date);
    });

    it('should convert string to Date', () => {
      const result = castToDatetime('2024-01-01T12:00:00Z');
      expect(result).toBeInstanceOf(Date);
      expect(result?.getFullYear()).toBe(2024);
    });

    it('should convert number (timestamp) to Date', () => {
      const timestamp = 1704067200000; // 2024-01-01
      const result = castToDatetime(timestamp);
      expect(result).toBeInstanceOf(Date);
    });

    it('should return null for null/undefined', () => {
      expect(castToDatetime(null)).toBeNull();
      expect(castToDatetime(undefined)).toBeNull();
    });

    it('should return null for invalid date string', () => {
      expect(castToDatetime('invalid')).toBeNull();
    });
  });

  describe('castToBoolean', () => {
    it('should convert boolean to boolean', () => {
      expect(castToBoolean(true)).toBe(true);
      expect(castToBoolean(false)).toBe(false);
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

    it('should convert number to boolean', () => {
      expect(castToBoolean(1)).toBe(true);
      expect(castToBoolean(0)).toBe(false);
      expect(castToBoolean(-1)).toBe(true);
    });

    it('should return null for null/undefined', () => {
      expect(castToBoolean(null)).toBeNull();
      expect(castToBoolean(undefined)).toBeNull();
    });
  });

  describe('castToIntegerArray', () => {
    it('should convert array of numbers', () => {
      expect(castToIntegerArray([1, 2, 3])).toEqual([1, 2, 3]);
    });

    it('should convert array of strings', () => {
      expect(castToIntegerArray(['1', '2', '3'])).toEqual([1, 2, 3]);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToIntegerArray('{1,2,3}')).toEqual([1, 2, 3]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToIntegerArray(null)).toEqual([]);
      expect(castToIntegerArray(undefined)).toEqual([]);
    });
  });

  describe('castToNumericArray', () => {
    it('should convert array with decimals', () => {
      expect(castToNumericArray([1.5, 2.5, 3.5])).toEqual([1.5, 2.5, 3.5]);
    });

    it('should handle null values', () => {
      expect(castToNumericArray([1, null, 3])).toEqual([1, null, 3]);
    });
  });

  describe('castToStringArray', () => {
    it('should convert array of strings', () => {
      expect(castToStringArray(['a', 'b', 'c'])).toEqual(['a', 'b', 'c']);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToStringArray('{a,b,c}')).toEqual(['a', 'b', 'c']);
    });

    it('should handle quoted strings', () => {
      expect(castToStringArray('{"hello world","test"}')).toEqual(['hello world', 'test']);
    });
  });

  describe('castToJson', () => {
    it('should return object as-is', () => {
      const obj = { key: 'value' };
      expect(castToJson(obj)).toEqual(obj);
    });

    it('should parse JSON string', () => {
      expect(castToJson('{"key":"value"}')).toEqual({ key: 'value' });
    });

    it('should return null for invalid JSON', () => {
      expect(castToJson('invalid')).toBeNull();
    });

    it('should return null for null/undefined', () => {
      expect(castToJson(null)).toBeNull();
      expect(castToJson(undefined)).toBeNull();
    });
  });

  describe('pgArrayParse', () => {
    it('should parse simple array', () => {
      expect(pgArrayParse('{1,2,3}')).toEqual(['1', '2', '3']);
    });

    it('should parse quoted string array', () => {
      expect(pgArrayParse('{"hello","world"}')).toEqual(['hello', 'world']);
    });

    it('should parse array with escaped quotes', () => {
      expect(pgArrayParse('{"say \\"hello\\""}')).toEqual(['say "hello"']);
    });

    it('should parse empty array', () => {
      expect(pgArrayParse('{}')).toEqual([]);
    });

    it('should handle null/empty input', () => {
      expect(pgArrayParse('')).toEqual([]);
    });
  });

  describe('makeLikeString', () => {
    it('should add wildcards on both sides by default', () => {
      expect(makeLikeString('test')).toBe('%test%');
    });

    it('should add wildcard only at front', () => {
      expect(makeLikeString('test', true, false)).toBe('%test');
    });

    it('should add wildcard only at back', () => {
      expect(makeLikeString('test', false, true)).toBe('test%');
    });

    it('should escape special characters', () => {
      expect(makeLikeString('100%')).toBe('%100\\%%');
      expect(makeLikeString('test_value')).toBe('%test\\_value%');
    });
  });

  describe('pgTypeToTsType', () => {
    it('should convert integer types', () => {
      expect(pgTypeToTsType('integer')).toBe('number');
      expect(pgTypeToTsType('bigint')).toBe('number');
      expect(pgTypeToTsType('smallint')).toBe('number');
    });

    it('should convert text types', () => {
      expect(pgTypeToTsType('text')).toBe('string');
      expect(pgTypeToTsType('varchar')).toBe('string');
    });

    it('should convert boolean', () => {
      expect(pgTypeToTsType('boolean')).toBe('boolean');
    });

    it('should convert date/time types', () => {
      expect(pgTypeToTsType('timestamp')).toBe('Date');
      expect(pgTypeToTsType('date')).toBe('Date');
    });

    it('should convert json types', () => {
      expect(pgTypeToTsType('jsonb')).toBe('Record<string, unknown>');
    });

    it('should return unknown for unsupported types', () => {
      expect(pgTypeToTsType('unknown_type')).toBe('unknown');
    });
  });

  describe('castToBooleanArray', () => {
    it('should convert array of booleans', () => {
      expect(castToBooleanArray([true, false, true])).toEqual([true, false, true]);
    });

    it('should convert PostgreSQL array literal', () => {
      expect(castToBooleanArray('{t,f,t}')).toEqual([true, false, true]);
    });

    it('should handle null values', () => {
      expect(castToBooleanArray([true, null, false])).toEqual([true, null, false]);
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

    it('should return empty array for null/undefined', () => {
      expect(castToDatetimeArray(null)).toEqual([]);
      expect(castToDatetimeArray(undefined)).toEqual([]);
    });
  });

  describe('Immediate value generators', () => {
    it('Now() should return NOW() SQL', () => {
      const now = Now();
      expect(now.value).toBe('NOW()');
    });

    it('Null() should return NULL SQL', () => {
      const nullVal = Null();
      expect(nullVal.value).toBe('NULL');
    });

    it('True() should return TRUE SQL', () => {
      const trueVal = True();
      expect(trueVal.value).toBe('TRUE');
    });

    it('False() should return FALSE SQL', () => {
      const falseVal = False();
      expect(falseVal.value).toBe('FALSE');
    });
  });

  describe('PostgreSQL array literal generators', () => {
    it('pgIntArray should generate integer array', () => {
      const result = pgIntArray([1, 2, 3]);
      expect(result.value).toBe('ARRAY[1,2,3]::INTEGER[]');
    });

    it('pgIntArray should handle empty array', () => {
      const result = pgIntArray([]);
      expect(result.value).toBe('ARRAY[]::INTEGER[]');
    });

    it('pgIntArray should support custom type', () => {
      const result = pgIntArray([1, 2], 'BIGINT');
      expect(result.value).toBe('ARRAY[1,2]::BIGINT[]');
    });

    it('pgNumericArray should generate numeric array', () => {
      const result = pgNumericArray([1.5, 2.5, null]);
      expect(result.value).toBe('ARRAY[1.5,2.5,NULL]::NUMERIC[]');
    });

    it('pgNumericArray should handle empty array', () => {
      const result = pgNumericArray([]);
      expect(result.value).toBe('ARRAY[]::NUMERIC[]');
    });

    it('pgStringArray should generate string array', () => {
      const result = pgStringArray(['a', 'b', 'c']);
      expect(result.value).toBe("ARRAY['a','b','c']::TEXT[]");
    });

    it('pgStringArray should escape single quotes', () => {
      const result = pgStringArray(["it's"]);
      expect(result.value).toBe("ARRAY['it''s']::TEXT[]");
    });

    it('pgStringArray should handle empty array', () => {
      const result = pgStringArray([]);
      expect(result.value).toBe('ARRAY[]::TEXT[]');
    });

    it('pgDateArray should generate timestamp array', () => {
      const dates = [new Date('2024-01-01T00:00:00Z'), new Date('2024-01-02T00:00:00Z')];
      const result = pgDateArray(dates);
      expect(result.value).toContain('ARRAY[');
      expect(result.value).toContain('::TIMESTAMP[]');
    });

    it('pgDateArray should handle empty array', () => {
      const result = pgDateArray([]);
      expect(result.value).toBe('ARRAY[]::TIMESTAMP[]');
    });
  });

  describe('Time calculation helpers', () => {
    it('TimeAfter should generate interval', () => {
      const result = TimeAfter(5, 'minute');
      expect(result.value).toBe("NOW() + INTERVAL '5 minute'");
    });

    it('DayAfter should generate day interval', () => {
      const result = DayAfter(7);
      expect(result.value).toBe("NOW() + INTERVAL '7 day'");
    });

    it('DayAfter should support custom interval', () => {
      const result = DayAfter(2, 'week');
      expect(result.value).toBe("NOW() + INTERVAL '2 week'");
    });
  });

  describe('empty2null', () => {
    it('should convert empty string to NULL', () => {
      const result = empty2null('');
      expect(result.value).toBe('NULL');
    });

    it('should convert undefined to NULL', () => {
      const result = empty2null(undefined);
      expect(result.value).toBe('NULL');
    });

    it('should pass through non-empty values', () => {
      expect(empty2null('test')).toBe('test');
      expect(empty2null(123)).toBe(123);
      expect(empty2null(null)).toBe(null);
    });
  });

  describe('castToBoolean edge cases', () => {
    it('should return null for non-boolean strings', () => {
      expect(castToBoolean('yes')).toBeNull();
      expect(castToBoolean('no')).toBeNull();
    });
  });

  describe('castToNumericArray edge cases', () => {
    it('should convert PostgreSQL array literal with NULLs', () => {
      expect(castToNumericArray('{1.5,NULL,3.5}')).toEqual([1.5, null, 3.5]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToNumericArray(null)).toEqual([]);
      expect(castToNumericArray(undefined)).toEqual([]);
    });
  });

  describe('castToJson edge cases', () => {
    it('should return array as-is', () => {
      const arr = [1, 2, 3];
      expect(castToJson(arr)).toEqual(arr);
    });
  });
});

