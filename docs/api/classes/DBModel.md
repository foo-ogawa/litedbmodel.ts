[**litedbmodel v0.19.7**](../README.md)

***

[litedbmodel](../globals.md) / DBModel

# Abstract Class: DBModel

Defined in: DBModel.ts:110

Base class for all database models in litedbmodel.
Provides CRUD operations, relations, transactions, and middleware support.

DBModel is designed for explicit SQL control with type-safe operations:
- Condition tuples `[Column, value]` for compile-time validation
- Symbol-based columns for IDE refactoring support
- Transparent N+1 prevention via automatic batch loading
- Reader/writer separation for production deployments

## Example

```typescript
import { DBModel, model, column } from 'litedbmodel';

// 1. Define model
@model('users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() is_active?: boolean;
}
export const User = UserModel.asModel();

// 2. Configure database
DBModel.setConfig({
  host: 'localhost',
  database: 'mydb',
  user: 'user',
  password: 'pass',
});

// 3. CRUD operations (writes require transaction)
await DBModel.transaction(async () => {
  await User.create([
    [User.name, 'John'],
    [User.email, 'john@example.com'],
  ]);
  await User.update([[User.id, 1]], [[User.name, 'Jane']]);
  await User.delete([[User.is_active, false]]);
});

// 4. Read operations
const users = await User.find([[User.is_active, true]]);
const john = await User.findOne([[User.email, 'john@example.com']]);
```

## Constructors

### Constructor

```ts
new DBModel(): DBModel;
```

Defined in: DBModel.ts:1470

#### Returns

`DBModel`

## Properties

| Property | Modifier | Type | Default value | Description | Defined in |
| ------ | ------ | ------ | ------ | ------ | ------ |
| <a id="table_name"></a> `TABLE_NAME` | `static` | `string` | `''` | Table name | DBModel.ts:116 |
| <a id="update_table_name"></a> `UPDATE_TABLE_NAME` | `static` | `string` \| `null` | `null` | Table name for UPDATE/DELETE (if different from TABLE_NAME) | DBModel.ts:141 |
| <a id="select_column"></a> `SELECT_COLUMN` | `static` | `string` | `'*'` | Default SELECT columns | DBModel.ts:144 |
| <a id="default_order"></a> `DEFAULT_ORDER` | `static` | `OrderSpec` \| `null` | `null` | Default ORDER BY clause (type-safe OrderColumn or OrderColumn[]) | DBModel.ts:147 |
| <a id="default_group"></a> `DEFAULT_GROUP` | `static` | \| `string` \| [`Column`](../interfaces/Column.md)\<`unknown`, `unknown`\> \| [`Column`](../interfaces/Column.md)\<`unknown`, `unknown`\>[] \| `null` | `null` | Default GROUP BY clause (Column, Column[], or raw string) | DBModel.ts:150 |
| <a id="find_filter"></a> `FIND_FILTER` | `static` | `Conds` \| `null` | `null` | Default filter conditions applied to all queries (use tuple format like find()) | DBModel.ts:153 |
| <a id="query"></a> `QUERY` | `static` | `string` \| `null` | `null` | SQL query for query-based models (view models, aggregations, etc.) When defined, the model uses this query as a CTE instead of TABLE_NAME. **Example** `// Static query static QUERY = ` SELECT users.id, COUNT(posts.id) as post_count FROM users LEFT JOIN posts ON users.id = posts.user_id GROUP BY users.id `;` | DBModel.ts:169 |
| <a id="pkey_columns"></a> `PKEY_COLUMNS` | `static` | [`Column`](../interfaces/Column.md)\<`unknown`, `unknown`\>[] \| `null` | `null` | Primary key columns (use getter to reference Model.column) | DBModel.ts:178 |
| <a id="seq_name"></a> `SEQ_NAME` | `static` | `string` \| `null` | `null` | Sequence name for auto-increment (use getter if needed) | DBModel.ts:181 |
| <a id="id_type"></a> `ID_TYPE` | `static` | `"uuid"` \| `"serial"` \| `null` | `null` | ID type: 'serial' for auto-increment, 'uuid' for UUID generation | DBModel.ts:184 |
| <a id="_dbconfig"></a> `_dbConfig` | `static` | [`DBConfig`](../interfaces/DBConfig.md) \| `null` | `null` | Database config | DBModel.ts:191 |
| <a id="_limitconfig"></a> `_limitConfig` | `static` | [`LimitConfig`](../interfaces/LimitConfig.md) | `{}` | Limit config for safety guards | DBModel.ts:194 |
| <a id="_configoptions"></a> `_configOptions` | `static` | [`DBConfigOptions`](../interfaces/DBConfigOptions.md) | `undefined` | Configuration options for reader/writer separation | DBModel.ts:197 |
| <a id="_lasttransactiontime"></a> `_lastTransactionTime` | `static` | `number` | `0` | Last transaction completion time (for writer sticky) | DBModel.ts:203 |
| <a id="true"></a> `true` | `readonly` | `DBBoolValue` | `undefined` | Boolean TRUE value | DBModel.ts:1079 |
| <a id="false"></a> `false` | `readonly` | `DBBoolValue` | `undefined` | Boolean FALSE value | DBModel.ts:1082 |
| <a id="null"></a> `null` | `readonly` | `DBNullValue` | `undefined` | NULL value | DBModel.ts:1085 |
| <a id="notnull"></a> `notNull` | `readonly` | `DBNotNullValue` | `undefined` | NOT NULL value | DBModel.ts:1088 |
| <a id="now"></a> `now` | `readonly` | `DBImmediateValue` | `undefined` | NOW() value | DBModel.ts:1091 |
| <a id="_modelclass"></a> `_modelClass` | `protected` | *typeof* `DBModel` | `undefined` | Instance reference to the static class | DBModel.ts:1462 |
| <a id="_relationcache"></a> `_relationCache` | `protected` | `Map`\<`string`, `unknown`\> | `undefined` | Per-instance cache for loaded relations | DBModel.ts:1465 |

## Methods

### asModel()

```ts
static asModel<T>(this: T): T & ColumnsOf<InstanceType<T>>;
```

Defined in: DBModel.ts:136

Returns the model class with type-safe column properties.
Use this instead of manual casting with `ColumnsOf`.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `T` |

#### Returns

`T` & `ColumnsOf`\<`InstanceType`\<`T`\>\>

#### Example

```typescript
@model('users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
}
export const User = UserModel.asModel();
export type User = UserModel;

// Now you can use User.id, User.name as Column references
await User.find([[User.name, 'John']]);
```

***

### setConfig()

```ts
static setConfig(config: DBConfig, options?: DBConfigOptions): void;
```

Defined in: DBModel.ts:233

Initialize DBModel with database config.
Call this once at application startup.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `config` | [`DBConfig`](../interfaces/DBConfig.md) |
| `options?` | [`DBConfigOptions`](../interfaces/DBConfigOptions.md) |

#### Returns

`void`

#### Example

```typescript
import { DBModel } from 'litedbmodel';

// Basic configuration
DBModel.setConfig({
  host: 'localhost',
  port: 5432,
  database: 'mydb',
  user: 'user',
  password: 'pass',
});

// With reader/writer separation
DBModel.setConfig(
  { host: 'reader.db.example.com', database: 'mydb', ... },
  {
    writerConfig: { host: 'writer.db.example.com', database: 'mydb', ... },
    useWriterAfterTransaction: true,  // Keep using writer after transaction
    writerStickyDuration: 5000,       // Duration in ms
  }
);
```

***

### getLimitConfig()

```ts
static getLimitConfig(): LimitConfig;
```

Defined in: DBModel.ts:257

Get current limit configuration.

#### Returns

[`LimitConfig`](../interfaces/LimitConfig.md)

***

### setLimitConfig()

```ts
static setLimitConfig(config: LimitConfig): void;
```

Defined in: DBModel.ts:272

Update limit configuration.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `config` | [`LimitConfig`](../interfaces/LimitConfig.md) |

#### Returns

`void`

#### Example

```typescript
// Set limits after initial config
DBModel.setLimitConfig({ findHardLimit: 5000, hasManyHardLimit: 500 });

// Disable limits
DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: null });
```

***

### getDBConfig()

```ts
static getDBConfig(): DBConfig | null;
```

Defined in: DBModel.ts:279

Get database config

#### Returns

[`DBConfig`](../interfaces/DBConfig.md) \| `null`

***

### getDriverType()

```ts
static getDriverType(): "postgres" | "sqlite" | "mysql";
```

Defined in: DBModel.ts:287

Get the database driver type.
Returns 'postgres', 'mysql', or 'sqlite'.

#### Returns

`"postgres"` \| `"sqlite"` \| `"mysql"`

***

### getHandler()

```ts
protected static getHandler(): DBHandler;
```

Defined in: DBModel.ts:313

Get a DBHandler instance for this model.
Connection priority:
1. Transaction connection (if in transaction)
2. Writer context (if in withWriter())
3. Writer sticky (if within writerStickyDuration after transaction)
4. Default reader connection

#### Returns

[`DBHandler`](DBHandler.md)

***

### \_shouldUseWriterSticky()

```ts
protected static _shouldUseWriterSticky(): boolean;
```

Defined in: DBModel.ts:339

Check if we should use writer due to sticky after transaction.

#### Returns

`boolean`

***

### inWriterContext()

```ts
static inWriterContext(): boolean;
```

Defined in: DBModel.ts:350

Check if currently in a withWriter context.

#### Returns

`boolean`

***

### use()

```ts
static use(MiddlewareClass: MiddlewareClass): () => void;
```

Defined in: DBModel.ts:393

Register a middleware class to intercept DBModel methods.

Each request gets its own middleware instance via AsyncLocalStorage.

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `MiddlewareClass` | [`MiddlewareClass`](../globals.md#middlewareclass) | Middleware class to register |

#### Returns

Function to unregister the middleware

```ts
(): void;
```

##### Returns

`void`

#### Example

```typescript
class LoggerMiddleware extends Middleware {
  logs: string[] = [];
  
  async execute(next: NextExecute, sql: string, params?: unknown[]) {
    this.logs.push(sql);
    const start = Date.now();
    const result = await next(sql, params);
    console.log(`SQL: ${sql} (${Date.now() - start}ms)`);
    return result;
  }
  
  getLogs() {
    return this.logs;
  }
}

DBModel.use(LoggerMiddleware);

// After queries
console.log(LoggerMiddleware.getCurrentContext().getLogs());
```

***

### removeMiddleware()

```ts
static removeMiddleware(MiddlewareClass: MiddlewareClass): boolean;
```

Defined in: DBModel.ts:409

Remove a middleware class

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `MiddlewareClass` | [`MiddlewareClass`](../globals.md#middlewareclass) | Middleware class to remove |

#### Returns

`boolean`

true if middleware was found and removed

***

### clearMiddlewares()

```ts
static clearMiddlewares(): void;
```

Defined in: DBModel.ts:421

Clear all middlewares (useful for testing)

#### Returns

`void`

***

### getMiddlewares()

```ts
static getMiddlewares(): readonly MiddlewareClass[];
```

Defined in: DBModel.ts:428

Get registered middleware classes

#### Returns

readonly [`MiddlewareClass`](../globals.md#middlewareclass)[]

***

### buildSelectSQL()

```ts
static buildSelectSQL<T>(
   this: T, 
   conditions: ConditionObject, 
   options: SelectOptions, 
   params: unknown[]): {
  sql: string;
  params: unknown[];
};
```

Defined in: DBModel.ts:615

Build SELECT SQL without executing.
Useful for constructing CTE/subquery SQL fragments.
Returns SQL with ? placeholders and params array.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `this` | `T` | `undefined` | - |
| `conditions` | `ConditionObject` | `undefined` | WHERE conditions |
| `options` | [`SelectOptions`](../interfaces/SelectOptions.md) | `{}` | SELECT options (order, limit, select, etc.) |
| `params` | `unknown`[] | `[]` | Optional parameter array to append to (for joining with outer query) |

#### Returns

```ts
{
  sql: string;
  params: unknown[];
}
```

Object with sql and params

| Name | Type | Defined in |
| ------ | ------ | ------ |
| `sql` | `string` | DBModel.ts:620 |
| `params` | `unknown`[] | DBModel.ts:620 |

#### Example

```typescript
// Build a subquery SQL
const { sql, params } = User.buildSelectSQL(
  { status: 'active' },
  { select: 'id', order: 'created_at DESC', limit: 10 }
);
// sql: "SELECT id FROM users WHERE status = ? ORDER BY created_at DESC LIMIT 10"
// params: ['active']
```

***

### inSubquery()

```ts
static inSubquery<T, S>(
   this: T, 
   keyPairs: KeyPair | CompositeKeyPairs, 
   conditions: readonly [Column<any, S>, unknown][]): readonly [string, DBSubquery];
```

Defined in: DBModel.ts:1137

IN subquery condition.
Creates a condition like: column IN (SELECT selectColumn FROM targetModel WHERE ...)
Supports composite keys using key pairs (same format as relation decorators).
Type-safe: first column in pair must belong to caller model, second to target model.

#### Type Parameters

| Type Parameter | Description |
| ------ | ------ |
| `T` *extends* *typeof* `DBModel` | Parent model class type (inferred from this) |
| `S` | Target model class type |

#### Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `this` | `T` | `undefined` | - |
| `keyPairs` | `KeyPair` \| `CompositeKeyPairs` | `undefined` | Key pairs: [[parentCol, targetCol], ...] or single pair [parentCol, targetCol] |
| `conditions` | readonly \[[`Column`](../interfaces/Column.md)\<`any`, `S`\>, `unknown`\][] | `[]` | WHERE conditions for subquery (columns must belong to target model S) |

#### Returns

readonly \[`string`, `DBSubquery`\]

Condition tuple for use in find() conditions

#### Example

```typescript
import { parentRef } from 'litedbmodel';

// Single key: id IN (SELECT user_id FROM orders WHERE status = 'paid')
await User.find([
  User.inSubquery([[User.id, Order.user_id]], [
    [Order.status, 'paid']
  ])
]);

// Composite key: (id, group_id) IN (SELECT user_id, group_id FROM orders WHERE ...)
await User.find([
  User.inSubquery([
    [User.id, Order.user_id],
    [User.group_id, Order.group_id],
  ], [[Order.status, 'paid']])
]);

// Correlated subquery with parentRef
await User.find([
  User.inSubquery([[User.id, Order.user_id]], [
    [Order.tenant_id, parentRef(User.tenant_id)],
    [Order.status, 'paid']
  ])
]);
```

***

### notInSubquery()

```ts
static notInSubquery<T, S>(
   this: T, 
   keyPairs: KeyPair | CompositeKeyPairs, 
   conditions: readonly [Column<any, S>, unknown][]): readonly [string, DBSubquery];
```

Defined in: DBModel.ts:1185

NOT IN subquery condition.
Creates a condition like: table.column NOT IN (SELECT table.column FROM targetModel WHERE ...)
Supports composite keys using key pairs (same format as relation decorators).
Type-safe: first column in pair must belong to caller model, second to target model.

#### Type Parameters

| Type Parameter | Description |
| ------ | ------ |
| `T` *extends* *typeof* `DBModel` | Parent model class type (inferred from this) |
| `S` | Target model class type |

#### Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `this` | `T` | `undefined` | - |
| `keyPairs` | `KeyPair` \| `CompositeKeyPairs` | `undefined` | Key pairs: [[parentCol, targetCol], ...] or single pair [parentCol, targetCol] |
| `conditions` | readonly \[[`Column`](../interfaces/Column.md)\<`any`, `S`\>, `unknown`\][] | `[]` | WHERE conditions for subquery (columns must belong to target model S) |

#### Returns

readonly \[`string`, `DBSubquery`\]

Condition tuple for use in find() conditions

#### Example

```typescript
// users.id NOT IN (SELECT banned_users.user_id FROM banned_users)
await User.find([
  User.notInSubquery([[User.id, BannedUser.user_id]])
]);

// Composite key NOT IN
await User.find([
  User.notInSubquery([
    [User.id, BannedUser.user_id],
    [User.tenant_id, BannedUser.tenant_id],
  ])
]);
```

***

### exists()

```ts
static exists<S>(conditions: readonly [Column<any, S>, unknown][]): readonly [string, DBExists];
```

Defined in: DBModel.ts:1228

EXISTS subquery condition.
Creates a condition like: EXISTS (SELECT 1 FROM targetModel WHERE table.column = ...)
Uses table.column format for unambiguous references.
Type-safe: conditions columns must belong to the same target model.

#### Type Parameters

| Type Parameter | Description |
| ------ | ------ |
| `S` | Target model instance type |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `conditions` | readonly \[[`Column`](../interfaces/Column.md)\<`any`, `S`\>, `unknown`\][] | WHERE conditions for subquery (columns determine target table) |

#### Returns

readonly \[`string`, `DBExists`\]

Condition tuple for use in find() conditions

#### Example

```typescript
import { parentRef } from 'litedbmodel';

// EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
await User.find([
  [User.is_active, true],
  User.exists([
    [Order.user_id, parentRef(User.id)]
  ])
]);
```

***

### notExists()

```ts
static notExists<S>(conditions: readonly [Column<any, S>, unknown][]): readonly [string, DBExists];
```

Defined in: DBModel.ts:1263

NOT EXISTS subquery condition.
Creates a condition like: NOT EXISTS (SELECT 1 FROM targetModel WHERE table.column = ...)
Uses table.column format for unambiguous references.
Type-safe: conditions columns must belong to the same target model.

#### Type Parameters

| Type Parameter | Description |
| ------ | ------ |
| `S` | Target model instance type |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `conditions` | readonly \[[`Column`](../interfaces/Column.md)\<`any`, `S`\>, `unknown`\][] | WHERE conditions for subquery (columns determine target table) |

#### Returns

readonly \[`string`, `DBExists`\]

Condition tuple for use in find() conditions

#### Example

```typescript
import { parentRef } from 'litedbmodel';

// NOT EXISTS (SELECT 1 FROM banned_users WHERE banned_users.user_id = users.id)
await User.find([
  User.notExists([
    [BannedUser.user_id, parentRef(User.id)]
  ])
]);
```

***

### getTableName()

```ts
static getTableName(): string;
```

Defined in: DBModel.ts:1352

Get table name for SELECT queries.
For query-based models, returns the CTE alias (TABLE_NAME).

#### Returns

`string`

***

### isQueryBased()

```ts
static isQueryBased(): boolean;
```

Defined in: DBModel.ts:1375

Check if this model is query-based (uses QUERY instead of TABLE_NAME)

#### Returns

`boolean`

***

### withQuery()

```ts
static withQuery<T>(this: T, queryConfig: {
  sql: string;
  params?: unknown[];
}): T;
```

Defined in: DBModel.ts:1403

Create a new model class bound to specific query parameters.
Used for parameterized query-based models.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `queryConfig` | \{ `sql`: `string`; `params?`: `unknown`[]; \} | Query configuration with sql and params |
| `queryConfig.sql` | `string` | - |
| `queryConfig.params?` | `unknown`[] | - |

#### Returns

`T`

A new model class with bound parameters

#### Example

```typescript
class SalesReportModel extends DBModel {
  static QUERY = '...'; // Base query template
  
  static forPeriod(startDate: string, endDate: string) {
    return this.withQuery({
      sql: `SELECT ... WHERE date >= $1 AND date < $2 ...`,
      params: [startDate, endDate],
    });
  }
}

const Q1Report = SalesReport.forPeriod('2024-01-01', '2024-04-01');
const results = await Q1Report.find([...]);
```

***

### getUpdateTableName()

```ts
static getUpdateTableName(): string;
```

Defined in: DBModel.ts:1450

Get table name for UPDATE/DELETE queries.
Query-based models cannot be updated/deleted directly.

#### Returns

`string`

***

### clearRelationCache()

```ts
clearRelationCache(): void;
```

Defined in: DBModel.ts:1503

Clear the relation cache for this instance.
Also clears the context cache to force reload from DB.

#### Returns

`void`

***

### typeCastFromDB()

```ts
typeCastFromDB(): void;
```

Defined in: DBModel.ts:1640

Called after loading from DB to convert types
Override in derived class to implement type conversions

#### Returns

`void`

#### Example

```typescript
typeCastFromDB(): void {
  this.created_at = PostgresHelper.castToDatetime(this.created_at);
  this.is_active = PostgresHelper.castToBoolean(this.is_active);
}
```

***

### getPkey()

```ts
getPkey(): Record<string, unknown> | null;
```

Defined in: DBModel.ts:1652

Get primary key as object

#### Returns

`Record`\<`string`, `unknown`\> \| `null`

Object with primary key column names and values, or null if not set

***

### setPkey()

```ts
setPkey(key: unknown): void;
```

Defined in: DBModel.ts:1673

Set primary key value

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `key` | `unknown` | Single value for single-column PK, or object for composite PK |

#### Returns

`void`

***

### getPkeyString()

```ts
getPkeyString(): string;
```

Defined in: DBModel.ts:1691

Get primary key as string (for logging, caching, etc.)

#### Returns

`string`

***

### getSingleColId()

```ts
getSingleColId(): unknown;
```

Defined in: DBModel.ts:1703

Get single-column ID value

#### Returns

`unknown`

ID value or undefined

***

### clone()

```ts
clone<T>(this: T): T;
```

Defined in: DBModel.ts:1718

Create a shallow copy of the model instance

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `T` |

#### Returns

`T`

***

### assign()

```ts
assign(source: Partial<this>): this;
```

Defined in: DBModel.ts:1726

Copy properties from another object

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `source` | `Partial`\<`this`\> |

#### Returns

`this`

***

### toObject()

```ts
toObject(): Record<string, unknown>;
```

Defined in: DBModel.ts:1737

Convert to plain object

#### Returns

`Record`\<`string`, `unknown`\>

***

### toJSON()

```ts
toJSON(): Record<string, unknown>;
```

Defined in: DBModel.ts:1750

Convert to JSON-serializable object

#### Returns

`Record`\<`string`, `unknown`\>

***

### fromObject()

```ts
static fromObject<T>(this: () => T, obj: Record<string, unknown>): T;
```

Defined in: DBModel.ts:1761

Create an instance from a plain object

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | () => `T` |
| `obj` | `Record`\<`string`, `unknown`\> |

#### Returns

`T`

***

### fromObjects()

```ts
static fromObjects<T>(this: () => T, objs: Record<string, unknown>[]): T[];
```

Defined in: DBModel.ts:1774

Create multiple instances from an array of plain objects

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | () => `T` |
| `objs` | `Record`\<`string`, `unknown`\>[] |

#### Returns

`T`[]

***

### columnList()

```ts
static columnList<T>(records: T[], columnName: string): unknown[];
```

Defined in: DBModel.ts:1784

Get column values from an array of model instances

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `records` | `T`[] |
| `columnName` | `string` |

#### Returns

`unknown`[]

***

### hashByProperty()

```ts
static hashByProperty<T>(records: T[], propertyKey: string): Record<string, T>;
```

Defined in: DBModel.ts:1794

Create a hash map by property value

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `records` | `T`[] |
| `propertyKey` | `string` |

#### Returns

`Record`\<`string`, `T`\>

***

### groupByProperty()

```ts
static groupByProperty<T>(records: T[], propertyKey: string): Record<string, T[]>;
```

Defined in: DBModel.ts:1809

Group records by property value

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `records` | `T`[] |
| `propertyKey` | `string` |

#### Returns

`Record`\<`string`, `T`[]\>

***

### idList()

```ts
static idList<T>(records: T[], column?: string): unknown[];
```

Defined in: DBModel.ts:1827

Get ID list from records

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `DBModel` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `records` | `T`[] |
| `column?` | `string` |

#### Returns

`unknown`[]

***

### makeLikeString()

```ts
static makeLikeString(
   src: string, 
   front: boolean, 
   back: boolean): string;
```

Defined in: DBModel.ts:1835

Generate LIKE pattern string

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `src` | `string` | `undefined` |
| `front` | `boolean` | `true` |
| `back` | `boolean` | `true` |

#### Returns

`string`

***

### or()

```ts
static or<T>(this: T, ...condGroups: readonly CondsOf<T>[]): OrCondOf<T>;
```

Defined in: DBModel.ts:1872

Create a type-safe OR condition for this model.
All columns in the conditions must belong to this model.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| ...`condGroups` | readonly `CondsOf`\<`T`\>[] | Arrays of conditions to OR together |

#### Returns

`OrCondOf`\<`T`\>

OR condition that can be used in find(), findOne(), etc.

#### Example

```typescript
// (role = 'admin') OR (role = 'moderator')
const admins = await User.find([
  [User.deleted, false],
  User.or(
    [[User.role, 'admin']],
    [[User.role, 'moderator']],
  ),
]);

// This will cause a compile error:
User.or([[OtherModel.id, 1]]);  // Error: OtherModel.id is not a User column
```

***

### find()

```ts
static find<T>(
   this: T, 
   conditions: CondsOf<T>, 
options?: SelectOptions): Promise<InstanceType<T>[]>;
```

Defined in: DBModel.ts:1908

Find all records using type-safe condition tuples.
All columns in conditions must belong to this model.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `conditions` | `CondsOf`\<`T`\> | Array of condition tuples |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) | Query options (order, limit, offset, etc.) |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>

Array of model instances

#### Example

```typescript
const users = await User.find([
  [User.is_active, true],
  [`${User.age} >= ?`, 18],
]);

// With OR conditions (use Model.or() for type safety)
const admins = await User.find([
  [User.deleted, false],
  User.or(
    [[User.role, 'admin']],
    [[User.role, 'moderator']],
  ),
]);
```

***

### findOne()

```ts
static findOne<T>(
   this: T, 
   conditions: CondsOf<T>, 
options?: SelectOptions): Promise<InstanceType<T> | null>;
```

Defined in: DBModel.ts:1954

Find first record using type-safe condition tuples.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `conditions` | `CondsOf`\<`T`\> | Array of condition tuples |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) | Query options |

#### Returns

`Promise`\<`InstanceType`\<`T`\> \| `null`\>

Model instance or null

#### Example

```typescript
const user = await User.findOne([
  [User.email, 'test@example.com'],
]);
```

***

### findById()

```ts
static findById<T>(
   this: T, 
   pkeyResult: Pick<PkeyResult, "values">, 
options?: SelectOptions): Promise<InstanceType<T>[]>;
```

Defined in: DBModel.ts:1993

Find records by primary key using PkeyResult format.
Efficiently fetches multiple records by their primary keys.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `pkeyResult` | `Pick`\<[`PkeyResult`](../interfaces/PkeyResult.md), `"values"`\> | Object with `values` property containing 2D array of PK values |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) | Query options |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>

Array of model instances (empty array if no matches)

#### Example

```typescript
// Single record
const [user] = await User.findById({ values: [[1]] });

// Multiple records
const users = await User.findById({ values: [[1], [2], [3]] });

// Composite PK
const [entry] = await TenantUser.findById({
  values: [[1, 100]]  // [tenant_id, id]
});

// Use with write operation result
const result = await User.update(..., { returning: true });
const users = await User.findById(result);
```

***

### count()

```ts
static count<T>(this: T, conditions: CondsOf<T>): Promise<number>;
```

Defined in: DBModel.ts:2107

Count records using type-safe condition tuples.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `conditions` | `CondsOf`\<`T`\> | Array of condition tuples |

#### Returns

`Promise`\<`number`\>

Count

#### Example

```typescript
const count = await User.count([[User.is_active, true]]);
```

***

### create()

```ts
static create<T, P>(
   this: T, 
   pairs: P & CVs<P>, 
options?: InsertOptions<InstanceType<T>>): Promise<PkeyResult | null>;
```

Defined in: DBModel.ts:2142

Create a new record using type-safe column-value tuples.
Value types are validated at compile time.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |
| `P` *extends* readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][] |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `pairs` | `P` & `CVs`\<`P`\> | Array of [Column, value] tuples |
| `options?` | [`InsertOptions`](../interfaces/InsertOptions.md)\<`InstanceType`\<`T`\>\> | Insert options (returning: true to get PkeyResult) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

null by default, or PkeyResult if returning: true

#### Example

```typescript
// Default: returns null
await User.create([
  [User.name, 'John'],
  [User.email, 'john@test.com'],
]);

// With returning: true → PkeyResult
const result = await User.create([
  [User.name, 'John'],
  [User.email, 'john@test.com'],
], { returning: true });
const [user] = await User.findById(result);
```

***

### createMany()

```ts
static createMany<T>(
   this: T, 
   pairsArray: readonly readonly readonly [Column<any, any>, any][][], 
options?: InsertOptions<InstanceType<T>>): Promise<PkeyResult | null>;
```

Defined in: DBModel.ts:2212

Create multiple records using type-safe column-value tuples.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `pairsArray` | readonly readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][][] | Array of tuple arrays |
| `options?` | [`InsertOptions`](../interfaces/InsertOptions.md)\<`InstanceType`\<`T`\>\> | Insert options (returning: true to get PkeyResult) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

null by default, or PkeyResult if returning: true

#### Example

```typescript
// Default: returns null
await User.createMany([
  [[User.name, 'John'], [User.email, 'john@test.com']],
  [[User.name, 'Jane'], [User.email, 'jane@test.com']],
]);

// With returning: true → PkeyResult
const result = await User.createMany([
  [[User.name, 'John'], [User.email, 'john@test.com']],
  [[User.name, 'Jane'], [User.email, 'jane@test.com']],
], { returning: true });
const users = await User.findById(result);
```

***

### update()

```ts
static update<T, V>(
   this: T, 
   conditions: CondsOf<T>, 
   values: V & CVs<V>, 
options?: UpdateOptions): Promise<PkeyResult | null>;
```

Defined in: DBModel.ts:2282

Update records using type-safe column-value tuples.
Value types are validated at compile time.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |
| `V` *extends* readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][] |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `conditions` | `CondsOf`\<`T`\> | Array of condition tuples for WHERE clause |
| `values` | `V` & `CVs`\<`V`\> | Array of [Column, value] tuples for SET clause |
| `options?` | [`UpdateOptions`](../interfaces/UpdateOptions.md) | Update options (returning: true to get PkeyResult) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

null by default, or PkeyResult if returning: true

#### Example

```typescript
// Default: returns null
await User.update(
  [[User.id, 1]],
  [[User.name, 'Jane']],
);

// With returning: true → PkeyResult
const result = await User.update(
  [[User.status, 'pending']],
  [[User.status, 'active']],
  { returning: true }
);
const users = await User.findById(result);
```

***

### delete()

```ts
static delete<T>(
   this: T, 
   conditions: CondsOf<T>, 
options?: DeleteOptions): Promise<PkeyResult | null>;
```

Defined in: DBModel.ts:2360

Delete records matching conditions

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `conditions` | `CondsOf`\<`T`\> | Filter conditions |
| `options?` | [`DeleteOptions`](../interfaces/DeleteOptions.md) | Delete options (returning: true to get PkeyResult) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

null by default, or PkeyResult if returning: true

#### Example

```typescript
// Default: returns null
await User.delete([[User.is_active, false]]);

// With returning: true → PkeyResult
const result = await User.delete([[User.is_active, false]], { returning: true });
// result: { key: [User.id], values: [[4], [5]] }
```

***

### updateMany()

```ts
static updateMany<T>(
   this: T, 
   rows: readonly readonly readonly [Column<any, any>, any][][], 
options: UpdateManyOptions): Promise<PkeyResult | null>;
```

Defined in: DBModel.ts:2437

Update multiple records with different values per row.
Uses efficient bulk update strategies (UNNEST for PostgreSQL, VALUES for MySQL/SQLite).

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `rows` | readonly readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][][] | Array of [Column, value][] tuples, each representing one row's values |
| `options` | [`UpdateManyOptions`](../interfaces/UpdateManyOptions.md) | Options including keyColumns to identify rows |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

null by default, or PkeyResult if returning: true

#### Example

```typescript
// Default: returns null
await User.updateMany([
  [[User.id, 1], [User.name, 'John'], [User.email, 'john@example.com']],
  [[User.id, 2], [User.name, 'Jane'], [User.email, 'jane@example.com']],
], { keyColumns: [User.id] });

// With returning: true → PkeyResult
const result = await User.updateMany([
  [[User.id, 1], [User.name, 'John']],
  [[User.id, 2], [User.name, 'Jane']],
], { keyColumns: [User.id], returning: true });
const users = await User.findById(result);
```

***

### execute()

```ts
static execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;
```

Defined in: DBModel.ts:2640

Execute raw SQL query.

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `sql` | `string` | SQL query |
| `params?` | `unknown`[] | Query parameters |

#### Returns

`Promise`\<`ExecuteResult`\>

QueryResult with rows, rowCount

#### Example

```typescript
const result = await DBModel.execute(
  'SELECT * FROM users WHERE id = $1',
  [1]
);
console.log(result.rows);
console.log(result.rowCount);
```

***

### query()

```ts
static query<T>(
   this: T, 
   sql: string, 
params?: unknown[]): Promise<InstanceType<T>[]>;
```

Defined in: DBModel.ts:2684

Execute raw SQL and return model instances.
The SQL should return columns matching the model's properties.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* `DBModel` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `this` | `T` | - |
| `sql` | `string` | SQL query |
| `params?` | `unknown`[] | Query parameters |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>

Model instances

#### Example

```typescript
const users = await User.query(
  'SELECT * FROM users WHERE is_active = $1',
  [true]
);

// Complex join query
const posts = await Post.query(`
  SELECT p.* FROM posts p
  JOIN users u ON p.user_id = u.id
  WHERE u.email = $1
`, ['admin@example.com']);
```

***

### inTransaction()

```ts
static inTransaction(): boolean;
```

Defined in: DBModel.ts:2714

Check if currently in a transaction

#### Returns

`boolean`

***

### transaction()

```ts
static transaction<R>(func: () => Promise<R>, options: TransactionOptions): Promise<R>;
```

Defined in: DBModel.ts:2752

Execute a function within a transaction
All model operations inside the callback will use the same database connection.
Supports automatic retry on deadlock/serialization errors.

#### Type Parameters

| Type Parameter |
| ------ |
| `R` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `func` | () => `Promise`\<`R`\> | Function to execute within transaction |
| `options` | [`TransactionOptions`](../interfaces/TransactionOptions.md) | Transaction options (retry settings) |

#### Returns

`Promise`\<`R`\>

Result of the function

#### Example

```typescript
// Basic transaction
await DBModel.transaction(async () => {
  const user = await User.findFirst({ [User.def.id]: 1 });
  await Account.updateAll(
    { [Account.def.user_id]: user.id },
    { [Account.def.balance]: user.balance - 100 }
  );
  await Transaction.create({ user_id: user.id, amount: -100 });
});

// Transaction with return value
const result = await DBModel.transaction(async () => {
  const user = await User.create({ name: 'Alice' });
  return user;
});

// Transaction with options
await DBModel.transaction(
  async () => { ... },
  { retryLimit: 5, retryDuration: 100 }
);
```

***

### getCurrentConnection()

```ts
static getCurrentConnection(): DBConnection | null;
```

Defined in: DBModel.ts:2855

Get current transaction connection
Use this to execute raw SQL queries within a transaction

#### Returns

`DBConnection` \| `null`

Current DBConnection if in a transaction, null otherwise

#### Example

```typescript
await DBModel.transaction(async () => {
  const conn = DBModel.getCurrentConnection();
  if (conn) {
    await conn.query('SELECT * FROM some_table WHERE ...');
  }
});
```

***

### ~~getCurrentClient()~~

```ts
static getCurrentClient(): DBConnection | null;
```

Defined in: DBModel.ts:2863

#### Returns

`DBConnection` \| `null`

#### Deprecated

Use getCurrentConnection() instead

***

### withWriter()

```ts
static withWriter<R>(func: () => Promise<R>): Promise<R>;
```

Defined in: DBModel.ts:2888

Execute a function with explicit writer connection access.
Use this when you need to read from writer to avoid replication lag.
Write operations are NOT allowed in this context - use transaction() instead.

#### Type Parameters

| Type Parameter |
| ------ |
| `R` |

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `func` | () => `Promise`\<`R`\> | Function to execute with writer connection |

#### Returns

`Promise`\<`R`\>

Result of the function

#### Example

```typescript
// Read from writer to avoid replication lag
const user = await DBModel.withWriter(async () => {
  return await User.findOne([[User.id, 1]]);
});

// Write operations throw an error
await DBModel.withWriter(async () => {
  await User.create([[User.name, 'Error']]);  // Throws WriteInReadOnlyContextError
});
```

***

### createDBBase()

```ts
static createDBBase(config: DBConfig, options?: DBConfigOptions): typeof DBModel;
```

Defined in: DBModel.ts:2949

Create an independent database base class.
Use this to connect to multiple databases with isolated configurations.

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `config` | [`DBConfig`](../interfaces/DBConfig.md) | Database configuration |
| `options?` | [`DBConfigOptions`](../interfaces/DBConfigOptions.md) | Configuration options (writerConfig, limits, etc.) |

#### Returns

*typeof* `DBModel`

A new DBModel subclass with its own connection pool

#### Example

```typescript
// Create separate base classes for different databases
const BaseDB = DBModel.createDBBase({
  host: 'base-db.example.com',
  database: 'base_db',
}, {
  writerConfig: { host: 'base-db-writer.example.com', database: 'base_db' },
});

const CmsDB = DBModel.createDBBase({
  host: 'cms-db.example.com',
  database: 'cms_db',
});

// Define models using appropriate base class
@model('users')
class UserModel extends BaseDB {
  @column() id?: number;
}

@model('articles')
class ArticleModel extends CmsDB {
  @column() id?: number;
}

// Each base class has independent transactions
await BaseDB.transaction(async () => {
  await User.create([[User.name, 'John']]);
});
```

***

### reload()

```ts
reload(forUpdate: boolean): Promise<DBModel | null>;
```

Defined in: DBModel.ts:3125

Reload this instance from the database

#### Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `forUpdate` | `boolean` | `false` | If true, lock the row for update |

#### Returns

`Promise`\<`DBModel` \| `null`\>

Reloaded instance or null if not found

#### Example

```typescript
await user.reload();
```
