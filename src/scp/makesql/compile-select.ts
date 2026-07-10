/**
 * litedbmodel v2 SCP — SELECT compile → `makeSQL`, reproducing the ORIGINAL
 * `DBModel._buildSelectSQL` text byte-for-byte (the internal builder `find()` uses):
 *
 *   [WITH <cte> AS (…) ]SELECT <cols> FROM <t>[ <join>][ WHERE <cond>]
 *     [ GROUP BY <group>][ ORDER BY <order>][ LIMIT <n>][ OFFSET <n>][ FOR UPDATE][ <append>]
 *
 * LIMIT/OFFSET are INLINE literals (`LIMIT 10`), NOT parameters — the original inlines
 * them (`sql += \` LIMIT ${options.limit}\``); reproduced here. FOR UPDATE / GROUP BY /
 * raw `append` tail are the original's exact text. HAVING is carried through `append`
 * (v1 core has no dedicated HAVING; the .rs-only HAVING is not the PG anchor).
 *
 * Param order matches the original exactly: CTE params → JOIN params → WHERE params.
 */

import { DBConditions, type ConditionObject } from '../../DBConditions';
import { orderToString } from '../../Column';
import type { OrderSpec } from '../../Column';
import type { MakeSQL } from './makesql';
import { formatterFor } from './compile';
import type { Dialect } from './handler';

/** SELECT descriptor — mirrors the fields `_buildSelectSQL` reads from `SelectOptions`. */
export interface SelectDesc {
  dialect: Dialect;
  tableName: string;
  /** SELECT column list (default `*`). */
  select?: string;
  conditions?: ConditionObject;
  join?: string;
  joinParams?: unknown[];
  cte?: { name: string; sql: string; params: unknown[] };
  group?: string;
  order?: OrderSpec | string;
  limit?: number;
  offset?: number;
  forUpdate?: boolean;
  append?: string;
}

/**
 * Compile a SELECT to a `makeSQL` bundle, byte-identical to `_buildSelectSQL`.
 * Empty WHERE ⇒ no ` WHERE` (matches the original's `if (whereClause)` guard).
 */
export function compileSelect(desc: SelectDesc): MakeSQL {
  const params: unknown[] = [];
  const selectCols = desc.select ?? '*';
  const formatter = formatterFor(desc.dialect);

  // Param order (matches SQL order): CTE params → JOIN params → WHERE params.
  if (desc.cte?.params && desc.cte.params.length > 0) params.push(...desc.cte.params);
  if (desc.joinParams && desc.joinParams.length > 0) params.push(...desc.joinParams);

  const whereClause = new DBConditions(desc.conditions ?? {}).compile(params, formatter);

  let sql = '';
  if (desc.cte) sql = `WITH ${desc.cte.name} AS (${desc.cte.sql}) `;

  sql += `SELECT ${selectCols} FROM ${desc.tableName}`;
  if (desc.join) sql += ` ${desc.join}`;
  if (whereClause) sql += ` WHERE ${whereClause}`;
  if (desc.group) sql += ` GROUP BY ${desc.group}`;

  const orderClause =
    typeof desc.order === 'string' ? desc.order : orderToString(desc.order);
  if (orderClause) sql += ` ORDER BY ${orderClause}`;

  if (desc.limit !== undefined) sql += ` LIMIT ${desc.limit}`;
  if (desc.offset !== undefined) sql += ` OFFSET ${desc.offset}`;
  if (desc.forUpdate) sql += ' FOR UPDATE';
  if (desc.append) sql += ` ${desc.append}`;

  return { sql, params };
}
