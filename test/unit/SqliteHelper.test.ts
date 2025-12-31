/**
 * SqliteHelper Tests
 */

import { describe, it, expect } from 'vitest';
import {
  castToDatetime,
  castToBoolean,
  castToIntegerArray,
  castToStringArray,
  castToJson,
  Now,
  Null,
  True,
  False,
  jsonIntArray,
  jsonStringArray,
  jsonObject,
  TimeAfter,
  DayAfter,
  empty2null,
  makeLikeString,
  sqliteTypeToTsType,
} from '../../src/drivers/SqliteHelper';

describe('SqliteHelper', () => {
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

    it('should convert Unix timestamp to Date', () => {
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

    it('should return null for non-date types', () => {
      expect(castToDatetime({})).toBeNull();
      expect(castToDatetime([])).toBeNull();
    });
  });

  describe('castToBoolean', () => {
    it('should convert boolean to boolean', () => {
      expect(castToBoolean(true)).toBe(true);
      expect(castToBoolean(false)).toBe(false);
    });

    it('should convert SQLite 0/1 to boolean', () => {
      expect(castToBoolean(1)).toBe(true);
      expect(castToBoolean(0)).toBe(false);
    });

    it('should convert other numbers to boolean', () => {
      expect(castToBoolean(-1)).toBe(true);
      expect(castToBoolean(42)).toBe(true);
    });

    it('should convert string "1"/"0" to boolean', () => {
      expect(castToBoolean('1')).toBe(true);
      expect(castToBoolean('0')).toBe(false);
    });

    it('should convert string "true"/"false" to boolean', () => {
      expect(castToBoolean('true')).toBe(true);
      expect(castToBoolean('false')).toBe(false);
      expect(castToBoolean('TRUE')).toBe(true);
      expect(castToBoolean('FALSE')).toBe(false);
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

    it('should convert JSON string', () => {
      expect(castToIntegerArray('[1,2,3]')).toEqual([1, 2, 3]);
    });

    it('should return empty array for invalid JSON', () => {
      expect(castToIntegerArray('invalid')).toEqual([]);
    });

    it('should return empty array for non-array JSON', () => {
      expect(castToIntegerArray('{"a":1}')).toEqual([]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToIntegerArray(null)).toEqual([]);
      expect(castToIntegerArray(undefined)).toEqual([]);
    });

    it('should handle non-numeric values in array', () => {
      expect(castToIntegerArray(['a', 'b'])).toEqual([0, 0]);
    });
  });

  describe('castToStringArray', () => {
    it('should convert array of strings', () => {
      expect(castToStringArray(['a', 'b', 'c'])).toEqual(['a', 'b', 'c']);
    });

    it('should convert array of mixed types', () => {
      expect(castToStringArray([1, 'b', true])).toEqual(['1', 'b', 'true']);
    });

    it('should convert JSON string', () => {
      expect(castToStringArray('["a","b","c"]')).toEqual(['a', 'b', 'c']);
    });

    it('should return empty array for invalid JSON', () => {
      expect(castToStringArray('invalid')).toEqual([]);
    });

    it('should return empty array for non-array JSON', () => {
      expect(castToStringArray('{"a":"b"}')).toEqual([]);
    });

    it('should return empty array for null/undefined', () => {
      expect(castToStringArray(null)).toEqual([]);
      expect(castToStringArray(undefined)).toEqual([]);
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

  describe('Immediate value generators', () => {
    it('Now() should return SQLite datetime function', () => {
      const now = Now();
      expect(now.value).toBe("datetime('now')");
    });

    it('Null() should return NULL', () => {
      const nullVal = Null();
      expect(nullVal.value).toBe('NULL');
    });

    it('True() should return 1 (SQLite boolean)', () => {
      const trueVal = True();
      expect(trueVal.value).toBe('1');
    });

    it('False() should return 0 (SQLite boolean)', () => {
      const falseVal = False();
      expect(falseVal.value).toBe('0');
    });
  });

  describe('JSON array/object generators', () => {
    it('jsonIntArray should generate JSON array', () => {
      const result = jsonIntArray([1, 2, 3]);
      expect(result.value).toBe("'[1,2,3]'");
    });

    it('jsonIntArray should handle empty array', () => {
      const result = jsonIntArray([]);
      expect(result.value).toBe("'[]'");
    });

    it('jsonStringArray should generate JSON array', () => {
      const result = jsonStringArray(['a', 'b', 'c']);
      expect(result.value).toBe("'[\"a\",\"b\",\"c\"]'");
    });

    it('jsonStringArray should handle empty array', () => {
      const result = jsonStringArray([]);
      expect(result.value).toBe("'[]'");
    });

    it('jsonObject should generate JSON object', () => {
      const result = jsonObject({ key: 'value', num: 123 });
      expect(result.value).toBe("'{\"key\":\"value\",\"num\":123}'");
    });

    it('jsonObject should handle empty object', () => {
      const result = jsonObject({});
      expect(result.value).toBe("'{}'");
    });
  });

  describe('Time calculation helpers', () => {
    it('TimeAfter should generate datetime modifier', () => {
      const result = TimeAfter(5, 'minutes');
      expect(result.value).toBe("datetime('now', '+5 minutes')");
    });

    it('TimeAfter should handle different intervals', () => {
      expect(TimeAfter(10, 'seconds').value).toBe("datetime('now', '+10 seconds')");
      expect(TimeAfter(2, 'hours').value).toBe("datetime('now', '+2 hours')");
      expect(TimeAfter(3, 'days').value).toBe("datetime('now', '+3 days')");
    });

    it('DayAfter should generate date modifier', () => {
      const result = DayAfter(7);
      expect(result.value).toBe("date('now', '+7 days')");
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
      expect(empty2null(0)).toBe(0);
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

    it('should add no wildcards', () => {
      expect(makeLikeString('test', false, false)).toBe('test');
    });

    it('should escape special characters', () => {
      expect(makeLikeString('100%')).toBe('%100\\%%');
      expect(makeLikeString('test_value')).toBe('%test\\_value%');
      expect(makeLikeString('back\\slash')).toBe('%back\\\\slash%');
    });
  });

  describe('sqliteTypeToTsType', () => {
    it('should convert integer types', () => {
      expect(sqliteTypeToTsType('integer')).toBe('number');
      expect(sqliteTypeToTsType('int')).toBe('number');
      expect(sqliteTypeToTsType('INTEGER')).toBe('number');
    });

    it('should convert real/float types', () => {
      expect(sqliteTypeToTsType('real')).toBe('number');
      expect(sqliteTypeToTsType('float')).toBe('number');
      expect(sqliteTypeToTsType('double')).toBe('number');
    });

    it('should convert text types', () => {
      expect(sqliteTypeToTsType('text')).toBe('string');
      expect(sqliteTypeToTsType('varchar')).toBe('string');
      expect(sqliteTypeToTsType('char')).toBe('string');
    });

    it('should convert blob type', () => {
      expect(sqliteTypeToTsType('blob')).toBe('Buffer');
    });

    it('should convert boolean', () => {
      expect(sqliteTypeToTsType('boolean')).toBe('boolean');
    });

    it('should convert date/time types', () => {
      expect(sqliteTypeToTsType('datetime')).toBe('Date');
      expect(sqliteTypeToTsType('date')).toBe('Date');
      expect(sqliteTypeToTsType('timestamp')).toBe('Date');
    });

    it('should convert json type', () => {
      expect(sqliteTypeToTsType('json')).toBe('Record<string, unknown>');
    });

    it('should handle type with length specifier', () => {
      expect(sqliteTypeToTsType('varchar(255)')).toBe('string');
      expect(sqliteTypeToTsType('char(10)')).toBe('string');
    });

    it('should return unknown for unsupported types', () => {
      expect(sqliteTypeToTsType('unknown_type')).toBe('unknown');
    });
  });
});

