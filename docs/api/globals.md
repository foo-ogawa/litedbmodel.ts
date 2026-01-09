[**litedbmodel v0.19.7**](README.md)

***

# litedbmodel v0.19.7

litedbmodel - A lightweight TypeScript data access layer

Supports PostgreSQL and SQLite databases.

## Classes

### Core

- [DBModel](classes/DBModel.md)

### Column

- [Values](classes/Values.md)
- [Conditions](classes/Conditions.md)

### Middleware

- [Middleware](classes/Middleware.md)

### Errors

- [LimitExceededError](classes/LimitExceededError.md)
- [WriteOutsideTransactionError](classes/WriteOutsideTransactionError.md)
- [WriteInReadOnlyContextError](classes/WriteInReadOnlyContextError.md)

### Other

- [DBHandler](classes/DBHandler.md)

## Interfaces

### Types

- [PkeyResult](interfaces/PkeyResult.md)
- [ModelOptions](interfaces/ModelOptions.md)
- [DBConfigOptions](interfaces/DBConfigOptions.md)
- [LimitConfig](interfaces/LimitConfig.md)

### Other

- [Column](interfaces/Column.md)
- [DBConfig](interfaces/DBConfig.md)
- [ColumnOptions](interfaces/ColumnOptions.md)
- [SelectOptions](interfaces/SelectOptions.md)
- [InsertOptions](interfaces/InsertOptions.md)
- [UpdateOptions](interfaces/UpdateOptions.md)
- [DeleteOptions](interfaces/DeleteOptions.md)
- [UpdateManyOptions](interfaces/UpdateManyOptions.md)
- [TransactionOptions](interfaces/TransactionOptions.md)

## Type Aliases

### SkipType

```ts
type SkipType = typeof SKIP;
```

Defined in: Column.ts:757

***

### MiddlewareClass

```ts
type MiddlewareClass = typeof Middleware & () => Middleware;
```

Defined in: Middleware.ts:295

Type for middleware class (not instance)

## Variables

### Decorators

#### column

```ts
const column: (columnNameOrOptions?: string | ColumnOptions) => PropertyDecorator & {
  boolean: (columnName?: string) => PropertyDecorator;
  number: (columnName?: string) => PropertyDecorator;
  bigint: (columnName?: string) => PropertyDecorator;
  datetime: (columnName?: string) => PropertyDecorator;
  date: (columnName?: string) => PropertyDecorator;
  stringArray: (columnName?: string) => PropertyDecorator;
  intArray: (columnName?: string) => PropertyDecorator;
  numericArray: (columnName?: string) => PropertyDecorator;
  booleanArray: (columnName?: string) => PropertyDecorator;
  datetimeArray: (columnName?: string) => PropertyDecorator;
  json: <T>(columnName?: string) => PropertyDecorator;
  uuid: (columnName?: string) => PropertyDecorator;
  custom: <T>(castFn: (value: unknown) => T, serializeFn?: SerializeFn, columnName?: string) => PropertyDecorator;
};
```

Defined in: decorators.ts:356

Column decorator for defining model properties.

**Auto-inference**: For simple types (boolean, number, Date, bigint),
type conversion is automatically inferred from the TypeScript property type.
No need to use explicit variants like `@column.boolean()`.

Auto-inferred types:
```typescript
@column() id?: number;          // Auto: Number conversion
@column() name?: string;        // No conversion needed
@column() is_active?: boolean;  // Auto: Boolean conversion
@column() created_at?: Date;    // Auto: DateTime conversion
@column() large_id?: bigint;    // Auto: BigInt conversion
@column('custom_name') prop?: string;  // Custom column name
```

Explicit type conversion required (cannot be auto-inferred):
```typescript
@column.stringArray() tags?: string[];           // Array element type unknown
@column.intArray() scores?: number[];            // Array element type unknown
@column.json<MyType>() data?: MyType;            // Generic type unknown
@column.date() birth_date?: Date;                // date vs datetime distinction
```

Note: The explicit variants (`@column.boolean()`, `@column.datetime()`, etc.)
still work and can be used when you want to be explicit about the conversion.

##### Type Declaration

| Name | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| `boolean()` | (`columnName?`: `string`) => `PropertyDecorator` | Boolean type conversion Converts 't'/'f', 'true'/'false', 1/0 to boolean Preserves null for nullable columns, undefined stays undefined **Example** `@column.boolean() is_active?: boolean;` | decorators.ts:370 |
| `number()` | (`columnName?`: `string`) => `PropertyDecorator` | Number type conversion (from string) Preserves null for nullable columns, undefined stays undefined **Example** `@column.number() amount?: number;` | decorators.ts:381 |
| `bigint()` | (`columnName?`: `string`) => `PropertyDecorator` | BigInt type conversion Preserves null for nullable columns, undefined stays undefined **Example** `@column.bigint() large_id?: bigint;` | decorators.ts:394 |
| `datetime()` | (`columnName?`: `string`) => `PropertyDecorator` | DateTime type conversion (timestamp, timestamptz) Preserves null for nullable columns, undefined stays undefined **Example** `@column.datetime() created_at?: Date;` | decorators.ts:414 |
| `date()` | (`columnName?`: `string`) => `PropertyDecorator` | Date type conversion (date only, time set to 00:00:00) Preserves null for nullable columns, undefined stays undefined **Example** `@column.date() birth_date?: Date;` | decorators.ts:425 |
| `stringArray()` | (`columnName?`: `string`) => `PropertyDecorator` | String array type conversion (text[]) Preserves null for nullable columns, undefined stays undefined **Example** `@column.stringArray() tags?: string[];` | decorators.ts:445 |
| `intArray()` | (`columnName?`: `string`) => `PropertyDecorator` | Integer array type conversion (integer[]) Preserves null for nullable columns, undefined stays undefined **Example** `@column.intArray() scores?: number[];` | decorators.ts:460 |
| `numericArray()` | (`columnName?`: `string`) => `PropertyDecorator` | Numeric array type conversion (numeric[], allows null elements) Preserves null for nullable columns, undefined stays undefined **Example** `@column.numericArray() values?: (number | null)[];` | decorators.ts:475 |
| `booleanArray()` | (`columnName?`: `string`) => `PropertyDecorator` | Boolean array type conversion (boolean[]) Preserves null for nullable columns, undefined stays undefined **Example** `@column.booleanArray() flags?: (boolean | null)[];` | decorators.ts:490 |
| `datetimeArray()` | (`columnName?`: `string`) => `PropertyDecorator` | DateTime array type conversion (timestamp[]) Preserves null for nullable columns, undefined stays undefined **Example** `@column.datetimeArray() event_dates?: (Date | null)[];` | decorators.ts:505 |
| `json()` | \<`T`\>(`columnName?`: `string`) => `PropertyDecorator` | JSON/JSONB type conversion Preserves null for nullable columns, undefined stays undefined **Examples** `@column.json() metadata?: Record<string, unknown>;` `@column.json<UserSettings>() settings?: UserSettings;` | decorators.ts:531 |
| `uuid()` | (`columnName?`: `string`) => `PropertyDecorator` | UUID type with automatic casting for PostgreSQL. Automatically adds ::uuid cast to conditions and INSERT/UPDATE values. Preserves null for nullable columns, undefined stays undefined. **Example** `@column.uuid() id?: string; @column.uuid({ primaryKey: true }) id?: string; // Conditions automatically cast to UUID: await User.find([[User.id, 'uuid-string']]); // → WHERE id = ?::uuid // IN clauses also cast: await User.find([[User.id, ['uuid1', 'uuid2']]]); // → WHERE id IN (?::uuid, ?::uuid)` | decorators.ts:564 |
| `custom()` | \<`T`\>(`castFn`: (`value`: `unknown`) => `T`, `serializeFn?`: `SerializeFn`, `columnName?`: `string`) => `PropertyDecorator` | Custom type conversion with user-provided function **Examples** `@column.custom((v) => String(v).toUpperCase()) status?: string;` `@column.custom((v) => v, (v) => JSON.stringify(v)) data?: MyType; // with serializer` | decorators.ts:586 |

### Other

#### SKIP

```ts
const SKIP: typeof SKIP;
```

Defined in: Column.ts:756

Sentinel value to skip a field in create/update operations.
Use with conditional expressions to keep code as expressions instead of statements.

##### Example

```typescript
// Instead of:
const updates = new Values<User>();
if (body.name !== undefined) updates.add(User.name, body.name);
if (body.email !== undefined) updates.add(User.email, body.email);

// You can write:
await User.update(conds, [
  [User.name, body.name ?? SKIP],
  [User.email, body.email ?? SKIP],
]);
```

## Functions

### Decorators

- [hasMany](functions/hasMany.md)
- [belongsTo](functions/belongsTo.md)
- [hasOne](functions/hasOne.md)
- [model](functions/model.md)

### Other

- [initDBHandler](functions/initDBHandler.md)
- [getDBHandler](functions/getDBHandler.md)
- [getDBConfig](functions/getDBConfig.md)
- [closeAllPools](functions/closeAllPools.md)
- [getTransactionContext](functions/getTransactionContext.md)
- [getTransactionConnection](functions/getTransactionConnection.md)
- [dbNull](functions/dbNull.md)
- [dbNotNull](functions/dbNotNull.md)
- [dbTrue](functions/dbTrue.md)
- [dbFalse](functions/dbFalse.md)
- [dbNow](functions/dbNow.md)
- [dbIn](functions/dbIn.md)
- [dbDynamic](functions/dbDynamic.md)
- [dbRaw](functions/dbRaw.md)
- [dbImmediate](functions/dbImmediate.md)
- [dbCast](functions/dbCast.md)
- [dbUuid](functions/dbUuid.md)
- [dbCastIn](functions/dbCastIn.md)
- [dbUuidIn](functions/dbUuidIn.md)
- [parentRef](functions/parentRef.md)
- [createPostgresDriver](functions/createPostgresDriver.md)
- [createSqliteDriver](functions/createSqliteDriver.md)

## References

### LazyLoadingDBModel

Renames and re-exports [DBModel](classes/DBModel.md)
