/**
 * Connection error detection and execute-level retry tests
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { isConnectionError } from '../../src/connection-errors';
import { PostgresDriver } from '../../src/drivers/postgres';
import { MysqlDriver } from '../../src/drivers/mysql';
import type { Logger } from '../../src/drivers/types';

const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

function makeError(message: string, code?: string): Error {
  const err = new Error(message);
  if (code) {
    (err as NodeJS.ErrnoException).code = code;
  }
  return err;
}

describe('isConnectionError', () => {
  it('returns true for "Connection terminated unexpectedly"', () => {
    expect(isConnectionError(makeError('Connection terminated unexpectedly'))).toBe(true);
  });

  it('returns true for "Connection terminated"', () => {
    expect(isConnectionError(makeError('Connection terminated'))).toBe(true);
  });

  it('returns true for "Client has encountered a connection error"', () => {
    expect(isConnectionError(makeError('Client has encountered a connection error'))).toBe(true);
  });

  it('returns true for ECONNRESET error code', () => {
    expect(isConnectionError(makeError('read ECONNRESET', 'ECONNRESET'))).toBe(true);
  });

  it('returns true for ECONNREFUSED error code', () => {
    expect(isConnectionError(makeError('connect ECONNREFUSED', 'ECONNREFUSED'))).toBe(true);
  });

  it('returns true for EPIPE error code', () => {
    expect(isConnectionError(makeError('broken pipe', 'EPIPE'))).toBe(true);
  });

  it('returns true for EAI_AGAIN error code', () => {
    expect(isConnectionError(makeError('getaddrinfo EAI_AGAIN', 'EAI_AGAIN'))).toBe(true);
  });

  it('returns true for "Connection lost" (MySQL)', () => {
    expect(isConnectionError(makeError('Connection lost: The server closed the connection.'))).toBe(true);
  });

  it('returns true for PROTOCOL_CONNECTION_LOST (MySQL)', () => {
    expect(isConnectionError(makeError('PROTOCOL_CONNECTION_LOST', 'PROTOCOL_CONNECTION_LOST'))).toBe(true);
  });

  it('returns false for normal query errors', () => {
    expect(isConnectionError(makeError('syntax error at or near "SELCT"'))).toBe(false);
  });

  it('returns false for constraint violations', () => {
    expect(isConnectionError(makeError('duplicate key value violates unique constraint "users_pkey"'))).toBe(false);
  });

  it('returns true for ECONNRESET code without matching message', () => {
    expect(isConnectionError(makeError('query failed', 'ECONNRESET'))).toBe(true);
  });

  it('returns true for "read ECONNRESET" message without code', () => {
    expect(isConnectionError(makeError('read ECONNRESET'))).toBe(true);
  });

  it('returns true for "This socket has been ended by the other party"', () => {
    expect(isConnectionError(makeError('This socket has been ended by the other party'))).toBe(true);
  });
});

describe('PostgresDriver execute retry', () => {
  const testConfig = {
    host: 'retry-test-pg-host',
    port: 5432,
    database: 'retry_test_db',
    user: 'test',
    password: 'test',
  };

  let driver: PostgresDriver;

  beforeEach(() => {
    driver = new PostgresDriver({
      config: testConfig,
      logger: silentLogger,
    });
  });

  it('retries once on connection error in execute() and succeeds', async () => {
    const connectionError = makeError('Connection terminated unexpectedly');
    const mockRows = [{ id: 1 }];
    const mockQuery = vi
      .fn()
      .mockRejectedValueOnce(connectionError)
      .mockResolvedValueOnce({ rows: mockRows, rowCount: 1 });

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    const result = await driver.execute('SELECT * FROM users WHERE id = ?', [1]);

    expect(mockQuery).toHaveBeenCalledTimes(2);
    expect(result.rows).toEqual(mockRows);
    expect(result.rowCount).toBe(1);
  });

  it('throws when retry also fails in execute()', async () => {
    const connectionError = makeError('Connection terminated unexpectedly');
    const mockQuery = vi.fn().mockRejectedValue(connectionError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(driver.execute('SELECT 1')).rejects.toThrow('Connection terminated unexpectedly');
    expect(mockQuery).toHaveBeenCalledTimes(2);
  });

  it('does not retry on non-connection errors in execute()', async () => {
    const syntaxError = makeError('syntax error at or near "FOO"');
    const mockQuery = vi.fn().mockRejectedValue(syntaxError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(driver.execute('SELCT 1')).rejects.toThrow('syntax error');
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });

  it('retries once on connection error in executeWrite() and succeeds', async () => {
    const connectionError = makeError('Connection terminated', 'ECONNRESET');
    const mockQuery = vi
      .fn()
      .mockRejectedValueOnce(connectionError)
      .mockResolvedValueOnce({ rows: [], rowCount: 1 });

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    const result = await driver.executeWrite('UPDATE users SET name = ? WHERE id = ?', ['alice', 1]);

    expect(mockQuery).toHaveBeenCalledTimes(2);
    expect(result.rowCount).toBe(1);
  });

  it('throws when retry also fails in executeWrite()', async () => {
    const connectionError = makeError('Connection terminated unexpectedly');
    const mockQuery = vi.fn().mockRejectedValue(connectionError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(
      driver.executeWrite('UPDATE users SET name = ? WHERE id = ?', ['alice', 1])
    ).rejects.toThrow('Connection terminated unexpectedly');
    expect(mockQuery).toHaveBeenCalledTimes(2);
  });

  it('does not retry on non-connection errors in executeWrite()', async () => {
    const syntaxError = makeError('syntax error at or near "FOO"');
    const mockQuery = vi.fn().mockRejectedValue(syntaxError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(
      driver.executeWrite('UPDTE users SET name = ? WHERE id = ?', ['alice', 1])
    ).rejects.toThrow('syntax error');
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });
});

describe('MysqlDriver execute retry', () => {
  const testConfig = {
    host: 'retry-test-mysql-host',
    port: 3306,
    database: 'retry_test_db',
    user: 'test',
    password: 'test',
  };

  let driver: MysqlDriver;

  beforeEach(() => {
    driver = new MysqlDriver({
      config: testConfig,
      logger: silentLogger,
    });
  });

  it('retries once on connection error in execute() and succeeds', async () => {
    const connectionError = makeError('Connection lost: The server closed the connection.');
    const mockRows = [{ id: 1 }];
    const mockQuery = vi
      .fn()
      .mockRejectedValueOnce(connectionError)
      .mockResolvedValueOnce([mockRows, null]);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    const result = await driver.execute('SELECT * FROM users WHERE id = ?', [1]);

    expect(mockQuery).toHaveBeenCalledTimes(2);
    expect(result.rows).toEqual(mockRows);
    expect(result.rowCount).toBe(1);
  });

  it('throws when retry also fails in execute()', async () => {
    const connectionError = makeError('Connection lost', 'PROTOCOL_CONNECTION_LOST');
    const mockQuery = vi.fn().mockRejectedValue(connectionError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(driver.execute('SELECT 1')).rejects.toThrow('Connection lost');
    expect(mockQuery).toHaveBeenCalledTimes(2);
  });

  it('does not retry on non-connection errors in execute()', async () => {
    const syntaxError = makeError('You have an error in your SQL syntax');
    const mockQuery = vi.fn().mockRejectedValue(syntaxError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(driver.execute('SELCT 1')).rejects.toThrow('SQL syntax');
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });

  it('retries once on connection error in executeWrite() and succeeds', async () => {
    const connectionError = makeError('This socket has been ended by the other party');
    const mockQuery = vi
      .fn()
      .mockRejectedValueOnce(connectionError)
      .mockResolvedValueOnce([{ affectedRows: 1 }, null]);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    const result = await driver.executeWrite('UPDATE users SET name = ? WHERE id = ?', ['alice', 1]);

    expect(mockQuery).toHaveBeenCalledTimes(2);
    expect(result.rowCount).toBe(1);
  });

  it('throws when retry also fails in executeWrite()', async () => {
    const connectionError = makeError('Connection lost', 'PROTOCOL_CONNECTION_LOST');
    const mockQuery = vi.fn().mockRejectedValue(connectionError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(
      driver.executeWrite('UPDATE users SET name = ? WHERE id = ?', ['alice', 1])
    ).rejects.toThrow('Connection lost');
    expect(mockQuery).toHaveBeenCalledTimes(2);
  });

  it('does not retry on non-connection errors in executeWrite()', async () => {
    const syntaxError = makeError('You have an error in your SQL syntax');
    const mockQuery = vi.fn().mockRejectedValue(syntaxError);

    (driver as unknown as { pool: { query: typeof mockQuery } }).pool = { query: mockQuery };

    await expect(
      driver.executeWrite('UPDTE users SET name = ? WHERE id = ?', ['alice', 1])
    ).rejects.toThrow('SQL syntax');
    expect(mockQuery).toHaveBeenCalledTimes(1);
  });
});
