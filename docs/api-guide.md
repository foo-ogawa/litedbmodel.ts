# litedbmodel API Guide

A lightweight TypeScript ORM for PostgreSQL, MySQL, and SQLite with Active Record pattern.

## Quick Start

### Database Configuration

```typescript
import { initDBHandler, createPostgresDriver } from 'litedbmodel';
import type { DBConfigOptions } from 'litedbmodel';

// PostgreSQL configuration
const config: DBConfigOptions = {
  config: {
    driver: 'postgres',
    host: 'localhost',
    port: 5432,
    database: 'mydb',
    user: 'user',
    password: 'pass',
    max: 10,  // connection pool size
  },
  // Optional: separate writer config for read replicas
  writerConfig: {
    driver: 'postgres',
    host: 'primary.db.example.com',
    database: 'mydb',
    user: 'writer',
    password: 'writerpass',
  },
};

// Initialize
await initDBHandler(config);

// Or use factory function for driver
const driver = createPostgresDriver({
  config: { host: 'localhost', database: 'mydb', user: 'user', password: 'pass' },
});
await initDBHandler({ driver });
```

See: [DBConfig](interfaces/DBConfig.md), [DBConfigOptions](interfaces/DBConfigOptions.md), [initDBHandler](functions/initDBHandler.md)

### Model Definition

```typescript
import { DBModel, model, column, hasMany, belongsTo, hasOne } from 'litedbmodel';

// Define model class
@model('users')
class UserModel extends DBModel {
  // Primary key (auto-increment)
  @column({ primaryKey: true }) id?: number;
  
  // UUID primary key (PostgreSQL: auto-casts to ::uuid)
  // @column.uuid({ primaryKey: true }) id?: string;

  // Basic columns (types auto-inferred)
  @column() name?: string;
  @column() email?: string;
  @column() is_active?: boolean;
  @column() created_at?: Date;

  // Array and JSON columns (explicit type required)
  @column.stringArray() tags?: string[];
  @column.json<{ theme: string }>() settings?: { theme: string };

  // Relations
  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;

  @hasOne(() => [User.id, Profile.user_id])
  declare profile: Promise<Profile | null>;
}

// Export with type-safe Column references using asModel()
export const User = UserModel.asModel();
export type User = UserModel;

// Now you can use User.id, User.name, etc. as type-safe Column references
// await User.find([[User.name, 'John']]);
// await User.find([[User.id, 1], [User.is_active, true]]);

@model('posts')
class PostModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() author_id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() published_at?: Date;

  @belongsTo(() => [Post.author_id, User.id])
  declare author: Promise<User | null>;
}
export const Post = PostModel.asModel();
export type Post = PostModel;

@model('profiles')
class ProfileModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() user_id?: number;
  @column() bio?: string;
}
export const Profile = ProfileModel.asModel();
export type Profile = ProfileModel;
```

**Note:** `asModel()` enables type-safe Column references like `User.id`, `User.name` for use in conditions, ordering, and other operations.

See: [model](functions/model.md), [column](globals.md#column), [hasMany](functions/hasMany.md), [belongsTo](functions/belongsTo.md), [hasOne](functions/hasOne.md), [DBModel.asModel](classes/DBModel.md#asmodel)

## Basic CRUD Operations

### Find Records

```typescript
// Find all with conditions
const users = await User.find([
  [User.is_active, true],
]);

// Find with multiple conditions
const users = await User.find([
  [User.is_active, true],
  [`${User.created_at} > ?`, new Date('2024-01-01')],
]);

// Find one record
const user = await User.findOne([
  [User.email, 'john@example.com'],
]);

// Find by primary key
const users = await User.findById(1);
const users = await User.findById([1, 2, 3]);

// Count records
const count = await User.count([
  [User.is_active, true],
]);
```

See: [DBModel.find](classes/DBModel.md#find), [DBModel.findOne](classes/DBModel.md#findone), [DBModel.findById](classes/DBModel.md#findbyid), [SelectOptions](interfaces/SelectOptions.md)

### Create Records

```typescript
// Create single record
const pkey = await User.create([
  [User.name, 'John'],
  [User.email, 'john@example.com'],
  [User.is_active, true],
]);
// pkey = { id: 1 }

// Create with SKIP for conditional fields
import { SKIP } from 'litedbmodel';

const pkey = await User.create([
  [User.name, body.name],
  [User.email, body.email ?? SKIP],  // Skip if undefined
]);

// Create multiple records
const pkeys = await User.createMany([
  [
    [User.name, 'Alice'],
    [User.email, 'alice@example.com'],
  ],
  [
    [User.name, 'Bob'],
    [User.email, 'bob@example.com'],
  ],
]);

// UPSERT: Insert or update on conflict
await User.create(
  [[User.name, 'John'], [User.email, 'john@example.com']],
  { onConflict: User.email, onConflictUpdate: [User.name] }
);

// Insert or ignore on conflict
await User.create(
  [[User.name, 'John'], [User.email, 'john@example.com']],
  { onConflict: User.email, onConflictIgnore: true }
);

// Composite unique constraint
await UserPref.createMany([
  [[UserPref.user_id, 1], [UserPref.key, 'theme'], [UserPref.value, 'dark']],
], {
  onConflict: [UserPref.user_id, UserPref.key],
  onConflictUpdate: [UserPref.value],
});
```

See: [DBModel.create](classes/DBModel.md#create), [DBModel.createMany](classes/DBModel.md#createmany), [PkeyResult](interfaces/PkeyResult.md), [InsertOptions](interfaces/InsertOptions.md)

### Update Records

```typescript
// Update with conditions
await User.update(
  [[User.id, 1]],                    // WHERE conditions
  [[User.name, 'John Updated']],     // SET values
);

// Update multiple fields
await User.update(
  [[User.id, 1]],
  [
    [User.name, 'New Name'],
    [User.email, 'new@example.com'],
    [User.is_active, false],
  ],
);

// Bulk update with updateMany
await User.updateMany([
  [
    [User.id, 1],
    [User.name, 'User 1'],
  ],
  [
    [User.id, 2],
    [User.name, 'User 2'],
  ],
]);

// Raw SQL update
import { dbRaw } from 'litedbmodel';

await User.update(
  [[User.id, 1]],
  [[User.login_count, dbRaw('login_count + 1')]],
);
```

See: [DBModel.update](classes/DBModel.md#update), [DBModel.updateMany](classes/DBModel.md#updatemany), [UpdateOptions](interfaces/UpdateOptions.md)

### Delete Records

```typescript
await User.delete([
  [User.id, 1],
]);

// Delete with multiple conditions
await User.delete([
  [User.is_active, false],
  [`${User.created_at} < ?`, new Date('2023-01-01')],
]);
```

See: [DBModel.delete](classes/DBModel.md#delete), [DeleteOptions](interfaces/DeleteOptions.md)

## Advanced Queries

### OR Conditions

```typescript
const users = await User.find([
  User.or(
    [[User.name, 'Alice']],
    [[User.name, 'Bob']],
  ),
]);
// WHERE (name = 'Alice' OR name = 'Bob')
```

### Special Values

```typescript
import { dbNull, dbNotNull, dbIn, dbRaw, dbNow, dbDynamic } from 'litedbmodel';

// NULL checks
await User.find([[User.deleted_at, dbNull()]]);     // IS NULL
await User.find([[User.email, dbNotNull()]]);       // IS NOT NULL

// IN clause (arrays work directly too)
await User.find([[User.status, ['active', 'pending']]]);
await User.find([[User.status, dbIn(['active', 'pending'])]]);

// Raw SQL expressions
await User.update([[User.id, 1]], [[User.count, dbRaw('count + 1')]]);

// NOW() function
await User.update([[User.id, 1]], [[User.updated_at, dbNow()]]);

// Dynamic SQL with parameters
await User.find([[User.search, dbDynamic("to_tsvector('english', ?)", [query])]]);
```

See: [dbNull](functions/dbNull.md), [dbNotNull](functions/dbNotNull.md), [dbIn](functions/dbIn.md), [dbRaw](functions/dbRaw.md), [dbNow](functions/dbNow.md), [dbDynamic](functions/dbDynamic.md)

### Transactions

```typescript
await User.transaction(async () => {
  const pkey = await User.create([[User.name, 'New User']]);
  await Profile.create([
    [Profile.user_id, pkey.id],
    [Profile.bio, 'Hello!'],
  ]);
  // Auto-commit on success, auto-rollback on error
});

// Nested transactions (savepoints)
await User.transaction(async () => {
  await User.create([[User.name, 'User 1']]);
  
  await User.transaction(async () => {
    await User.create([[User.name, 'User 2']]);
    // Inner transaction creates a savepoint
  });
});
```

See: [DBModel.transaction](classes/DBModel.md#transaction), [TransactionOptions](interfaces/TransactionOptions.md)

### Raw SQL

```typescript
// Execute raw SQL
const result = await User.execute('SELECT COUNT(*) as cnt FROM users WHERE is_active = ?', [true]);
// result.rows = [{ cnt: 42 }]

// Query with model instantiation
const users = await User.query('SELECT * FROM users WHERE name LIKE ?', ['%john%']);
// Returns User[] instances with type casting applied
```

See: [DBModel.execute](classes/DBModel.md#execute), [DBModel.query](classes/DBModel.md#query)

## Relations

### Defining Relations

```typescript
@model('users')
class User extends DBModel {
  @column({ primaryKey: true }) id?: number;

  // One-to-Many: User has many Posts
  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;

  // One-to-One: User has one Profile
  @hasOne(() => [User.id, Profile.user_id])
  declare profile: Promise<Profile | null>;
}

@model('posts')
class Post extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() author_id?: number;

  // Many-to-One: Post belongs to User
  @belongsTo(() => [Post.author_id, User.id])
  declare author: Promise<User | null>;
}
```

### Using Relations

```typescript
const user = await User.findOne([[User.id, 1]]);
if (user) {
  // Lazy loading (N+1 safe with batch loading)
  const posts = await user.posts;
  const profile = await user.profile;
}

// Batch loading for multiple records
const users = await User.find([[User.is_active, true]]);
for (const user of users) {
  // First access triggers batch load for ALL users
  const posts = await user.posts;
}
```

See: [hasMany](functions/hasMany.md), [belongsTo](functions/belongsTo.md), [hasOne](functions/hasOne.md)

## Middleware

```typescript
import { Middleware } from 'litedbmodel';

class LoggingMiddleware extends Middleware {
  async find<T extends typeof DBModel>(
    modelClass: T,
    conditions: Conds,
    options: SelectOptions | undefined,
    next: NextFind<T>
  ): Promise<InstanceType<T>[]> {
    console.log(`Finding ${modelClass.name}...`);
    const start = Date.now();
    const result = await next(conditions, options);
    console.log(`Found ${result.length} records in ${Date.now() - start}ms`);
    return result;
  }
}

// Register middleware
User.use(LoggingMiddleware);
```

See: [Middleware](classes/Middleware.md)

## Dynamic Conditions

```typescript
import { Conditions, Values, SKIP } from 'litedbmodel';

// Dynamic conditions builder
const where = new Conditions<User>();
where.add(User.is_active, true);
if (query.name) {
  where.addRaw(`${User.name} LIKE ?`, `%${query.name}%`);
}
const users = await User.find(where.build());

// Dynamic values builder
const values = new Values<User>();
if (body.name) values.add(User.name, body.name);
if (body.email) values.add(User.email, body.email);
await User.update([[User.id, id]], values.build());

// Or use SKIP for inline conditionals
await User.update([[User.id, id]], [
  [User.name, body.name ?? SKIP],
  [User.email, body.email ?? SKIP],
]);
```

### SKIP Behavior by Operation

| Operation | SKIP Behavior |
|-----------|---------------|
| `find` / `findOne` / `count` | Condition excluded from WHERE |
| `create` / `update` | Column excluded from INSERT/UPDATE |
| `createMany` | Column excluded → DB DEFAULT applied |
| `updateMany` | Column excluded → existing value retained |

```typescript
// createMany - records grouped by SKIP pattern, each group inserted separately
await User.createMany([
  [[User.name, 'John'], [User.email, 'john@test.com']],           // Pattern A
  [[User.name, 'Jane'], [User.email, SKIP]],                       // Pattern B (email = DEFAULT)
  [[User.name, 'Bob'], [User.email, SKIP]],                        // Pattern B (batched with Jane)
]);

// updateMany - SKIPped columns retain existing values
await User.updateMany([
  [[User.id, 1], [User.email, 'new@test.com'], [User.status, SKIP]],  // status unchanged
  [[User.id, 2], [User.email, SKIP], [User.status, 'active']],        // email unchanged
], { keyColumns: User.id });
```

### SQL Generation Strategy (Complete Matrix)

| DB | Operation | SKIP | SQL Strategy | SQL Example | Test |
|----|-----------|------|--------------|-------------|------|
| PostgreSQL | create | No | VALUES | `INSERT INTO t (name, arr, json) VALUES ($1, '{1,2}', '{"a":1}')` | ✅ |
| PostgreSQL | create | Yes | VALUES (column excluded) | `INSERT INTO t (name, json) VALUES ($1, '{"a":1}')` - arr excluded | ✅ |
| PostgreSQL | createMany | No | UNNEST | `INSERT INTO t SELECT v.name, (jsonb->arr), v.json::jsonb FROM UNNEST($1::text[], $2::text[], $3::text[]) AS v` | ✅ |
| PostgreSQL | createMany | Yes | Group by SKIP pattern -> UNNEST per group | Same as above, SKIP columns excluded from INSERT -> DB DEFAULT | ✅ |
| PostgreSQL | update | No | SET | `UPDATE t SET name=$1, arr='{1,2}', json='{"a":1}' WHERE id=$2` | ✅ |
| PostgreSQL | update | Yes | SET (column excluded) | `UPDATE t SET name=$1, json='{"a":1}' WHERE id=$2` - arr excluded | ✅ |
| PostgreSQL | updateMany | No | UNNEST | `UPDATE t SET arr=(jsonb->arr), json=v.json::jsonb FROM UNNEST(...) AS v WHERE t.id=v.id` | ✅ |
| PostgreSQL | updateMany | Yes | UNNEST + skip_flag column | `SET arr=CASE WHEN v._skip_arr THEN t.arr ELSE (jsonb->arr) END` | ✅ |
| MySQL | create | No | VALUES | `INSERT INTO t (name, status) VALUES (?, ?)` | ✅ |
| MySQL | create | Yes | VALUES (column excluded) | `INSERT INTO t (name) VALUES (?)` - status excluded -> DB DEFAULT | ✅ |
| MySQL | createMany | No | VALUES ROW | `INSERT INTO t (name, status) VALUES ROW(?,?), ROW(?,?)` | ✅ |
| MySQL | createMany | Yes | Group by SKIP pattern -> VALUES per group | Same as above, SKIP columns excluded from INSERT -> DB DEFAULT | ✅ |
| MySQL | update | No | SET | `UPDATE t SET name=?, status=? WHERE id=?` | ✅ |
| MySQL | update | Yes | SET (column excluded) | `UPDATE t SET name=? WHERE id=?` - status excluded | ✅ |
| MySQL | updateMany | No | JOIN VALUES ROW | `UPDATE t JOIN (VALUES ROW(?,?)) AS v ON t.id=v.id SET t.col=v.col` | ✅ |
| MySQL | updateMany | Yes | JOIN VALUES + IF(skip_flag) | `SET t.status=IF(v._skip_status, t.status, v.status)` | ✅ |
| SQLite | create | No | VALUES | `INSERT INTO t (name, status) VALUES (?, ?)` | ✅ |
| SQLite | create | Yes | VALUES (column excluded) | `INSERT INTO t (name) VALUES (?)` - status excluded -> DB DEFAULT | ✅ |
| SQLite | createMany | No | VALUES | `INSERT INTO t (name, status) VALUES (?,?), (?,?)` | ✅ |
| SQLite | createMany | Yes | Group by SKIP pattern -> VALUES per group | Same as above, SKIP columns excluded from INSERT -> DB DEFAULT | ✅ |
| SQLite | update | No | SET | `UPDATE t SET name=?, status=? WHERE id=?` | ✅ |
| SQLite | update | Yes | SET (column excluded) | `UPDATE t SET name=? WHERE id=?` - status excluded | ✅ |
| SQLite | updateMany | No | CASE WHEN | `UPDATE t SET status=CASE WHEN id=? THEN ? WHEN id=? THEN ? END WHERE id IN (?,?)` | ✅ |
| SQLite | updateMany | Yes | CASE WHEN + column ref | `SET status=CASE WHEN id=1 THEN ? WHEN id=2 THEN t.status END` - SKIP uses column ref | ✅ |

**Notes:**
- PostgreSQL array types: In createMany/updateMany, `JSON.stringify()` -> `text[]` -> `jsonb_array_elements_text()` to deserialize
- PostgreSQL JSON types: In createMany/updateMany, `JSON.stringify()` -> `text[]` -> `::jsonb` cast
- MySQL/SQLite have no native array types, so array tests are PostgreSQL-only
- `(jsonb->arr)` = `COALESCE((SELECT array_agg(elem::int) FROM jsonb_array_elements_text(v.col::jsonb) AS elem), ARRAY[]::int[])`

See: [Conditions](classes/Conditions.md), [Values](classes/Values.md), [SKIP](variables/SKIP.md)

---

## API Reference

For complete API documentation, see the [API Reference](globals.md).

### Classes

| Class | Description |
|-------|-------------|
| [DBModel](classes/DBModel.md) | Base class for all models - CRUD operations, relations, transactions |
| [Middleware](classes/Middleware.md) | Base class for creating custom middleware |
| [Values](classes/Values.md) | Dynamic builder for update/create values |
| [Conditions](classes/Conditions.md) | Dynamic builder for query conditions |
| [DBHandler](classes/DBHandler.md) | Database connection handler |

### Interfaces

| Interface | Description |
|-----------|-------------|
| [Column](interfaces/Column.md) | Type-safe column reference |
| [DBConfig](interfaces/DBConfig.md) | Database connection settings |
| [DBConfigOptions](interfaces/DBConfigOptions.md) | Options for initDBHandler |
| [PkeyResult](interfaces/PkeyResult.md) | Primary key result from create/update |
| [SelectOptions](interfaces/SelectOptions.md) | Options for find/query operations |
| [InsertOptions](interfaces/InsertOptions.md) | Options for create operations |
| [UpdateOptions](interfaces/UpdateOptions.md) | Options for update operations |
| [DeleteOptions](interfaces/DeleteOptions.md) | Options for delete operations |
| [TransactionOptions](interfaces/TransactionOptions.md) | Options for transactions |
| [ModelOptions](interfaces/ModelOptions.md) | Options for @model decorator |
| [ColumnOptions](interfaces/ColumnOptions.md) | Options for @column decorator |
| [LimitConfig](interfaces/LimitConfig.md) | Query result limit configuration |

### Decorators

| Decorator | Description |
|-----------|-------------|
| [model](functions/model.md) | Class decorator for defining table name |
| [column](globals.md#column) | Property decorator for column definition |
| [hasMany](functions/hasMany.md) | One-to-many relation decorator |
| [belongsTo](functions/belongsTo.md) | Many-to-one relation decorator |
| [hasOne](functions/hasOne.md) | One-to-one relation decorator |

### Functions

| Function | Description |
|----------|-------------|
| [initDBHandler](functions/initDBHandler.md) | Initialize database connection |
| [getDBHandler](functions/getDBHandler.md) | Get current database handler |
| [closeAllPools](functions/closeAllPools.md) | Close all connection pools |
| [createPostgresDriver](functions/createPostgresDriver.md) | Create PostgreSQL driver |
| [createSqliteDriver](functions/createSqliteDriver.md) | Create SQLite driver |
| [dbNull](functions/dbNull.md) | IS NULL condition |
| [dbNotNull](functions/dbNotNull.md) | IS NOT NULL condition |
| [dbIn](functions/dbIn.md) | IN clause helper |
| [dbRaw](functions/dbRaw.md) | Raw SQL expression |
| [dbNow](functions/dbNow.md) | NOW() function |
| [dbDynamic](functions/dbDynamic.md) | Dynamic SQL with parameters |
| [parentRef](functions/parentRef.md) | Parent column reference for subqueries |

### Variables

| Variable | Description |
|----------|-------------|
| [SKIP](globals.md#skip) | Skip field in create/update |


