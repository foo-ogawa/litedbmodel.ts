[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / model

# Function: model()

## Call Signature

```ts
function model<T>(constructor: T): T;
```

Defined in: decorators.ts:784

Model class decorator.

Can be used with or without table name:
- `@model` - uses class name as table name (via TABLE_NAME)
- `@model('users')` - sets TABLE_NAME to 'users'

Automatically:
1. Sets static TABLE_NAME property (if table name provided)
2. Creates static Column properties for each

### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* (...`args`: `unknown`[]) => `object` |

### Parameters

| Parameter | Type |
| ------ | ------ |
| `constructor` | `T` |

### Returns

`T`

### Column

decorated property
3. Generates typeCastFromDB() method from

### Column

type conversion settings
4. Creates relation getters from @hasMany, @belongsTo,

### Has One

decorators

### Example

```typescript
@model('users')
class User extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column.boolean() is_active?: boolean;
  @column.datetime() created_at?: Date;

  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;
}

// Usage - call column to get name as string for computed property key
await User.findAll({ [User.id()]: 1 });

// Or use condition builders with spread
await User.findAll({ ...User.is_active.eq(true) });

// Access relations
const user = await User.findOne([[User.id, 1]]);
const posts = await user.posts;  // Batch loads with other users in context
```

## Call Signature

```ts
function model(tableName: string): <T>(constructor: T) => T;
```

Defined in: decorators.ts:788

Model class decorator.

Can be used with or without table name:
- `@model` - uses class name as table name (via TABLE_NAME)
- `@model('users')` - sets TABLE_NAME to 'users'

Automatically:
1. Sets static TABLE_NAME property (if table name provided)
2. Creates static Column properties for each

### Parameters

| Parameter | Type |
| ------ | ------ |
| `tableName` | `string` |

### Returns

```ts
<T>(constructor: T): T;
```

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* (...`args`: `unknown`[]) => `object` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `constructor` | `T` |

#### Returns

`T`

### Column

decorated property
3. Generates typeCastFromDB() method from

### Column

type conversion settings
4. Creates relation getters from @hasMany, @belongsTo,

### Has One

decorators

### Example

```typescript
@model('users')
class User extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column.boolean() is_active?: boolean;
  @column.datetime() created_at?: Date;

  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;
}

// Usage - call column to get name as string for computed property key
await User.findAll({ [User.id()]: 1 });

// Or use condition builders with spread
await User.findAll({ ...User.is_active.eq(true) });

// Access relations
const user = await User.findOne([[User.id, 1]]);
const posts = await user.posts;  // Batch loads with other users in context
```

## Call Signature

```ts
function model(tableName: string, options: ModelOptions): <T>(constructor: T) => T;
```

Defined in: decorators.ts:792

Model class decorator.

Can be used with or without table name:
- `@model` - uses class name as table name (via TABLE_NAME)
- `@model('users')` - sets TABLE_NAME to 'users'

Automatically:
1. Sets static TABLE_NAME property (if table name provided)
2. Creates static Column properties for each

### Parameters

| Parameter | Type |
| ------ | ------ |
| `tableName` | `string` |
| `options` | [`ModelOptions`](../interfaces/ModelOptions.md) |

### Returns

```ts
<T>(constructor: T): T;
```

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* (...`args`: `unknown`[]) => `object` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `constructor` | `T` |

#### Returns

`T`

### Column

decorated property
3. Generates typeCastFromDB() method from

### Column

type conversion settings
4. Creates relation getters from @hasMany, @belongsTo,

### Has One

decorators

### Example

```typescript
@model('users')
class User extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column.boolean() is_active?: boolean;
  @column.datetime() created_at?: Date;

  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;
}

// Usage - call column to get name as string for computed property key
await User.findAll({ [User.id()]: 1 });

// Or use condition builders with spread
await User.findAll({ ...User.is_active.eq(true) });

// Access relations
const user = await User.findOne([[User.id, 1]]);
const posts = await user.posts;  // Batch loads with other users in context
```
