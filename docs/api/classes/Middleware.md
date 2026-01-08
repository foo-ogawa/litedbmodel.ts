[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / Middleware

# Abstract Class: Middleware

Defined in: Middleware.ts:134

Base class for middlewares.

Middlewares use AsyncLocalStorage to maintain per-request instances.
On first access within a request, a new instance is created automatically.

Middleware hooks are called in the following flow:
- Method-level: `find`, `findOne`, `findById`, `count`, `create`, `createMany`, `update`, `updateMany`, `delete`
- Instantiation-level: `query` — returns model instances from raw SQL
- SQL-level: `execute` — intercepts ALL SQL queries

## Example

```typescript
class LoggerMiddleware extends Middleware {
  logs: string[] = [];
  
  async execute(next: NextExecute, sql: string, params?: unknown[]) {
    this.logs.push(sql);
    return next(sql, params);
  }
  
  getLogs() {
    return this.logs;
  }
}

// Register
DBModel.use(LoggerMiddleware);

// After request
console.log(LoggerMiddleware.getCurrentContext().getLogs());
```

## Constructors

### Constructor

```ts
new Middleware(): Middleware;
```

#### Returns

`Middleware`

## Methods

### getCurrentContext()

```ts
static getCurrentContext<T>(this: () => T): T;
```

Defined in: Middleware.ts:151

Get current request's instance.
Creates a new instance on first access within a request.

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `Middleware` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | () => `T` |

#### Returns

`T`

***

### run()

```ts
static run<T, R>(this: () => T, fn: () => R): R;
```

Defined in: Middleware.ts:165

Run a function with a fresh middleware context.
Useful for explicit context boundaries (e.g., in tests).

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* `Middleware` |
| `R` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | () => `T` |
| `fn` | () => `R` |

#### Returns

`R`

***

### hasContext()

```ts
static hasContext(): boolean;
```

Defined in: Middleware.ts:174

Check if currently in a context

#### Returns

`boolean`

***

### clearContext()

```ts
static clearContext(): void;
```

Defined in: Middleware.ts:181

Clear current context (for testing)

#### Returns

`void`

***

### init()?

```ts
optional init(): void;
```

Defined in: Middleware.ts:193

Called when instance is created

#### Returns

`void`

***

### find()?

```ts
optional find<T>(
   this: Middleware, 
   model: T, 
   next: NextFind<T>, 
   conditions: Conds, 
options?: SelectOptions): Promise<InstanceType<T>[]>;
```

Defined in: Middleware.ts:196

Intercept find()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextFind`\<`T`\> |
| `conditions` | `Conds` |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>

***

### findOne()?

```ts
optional findOne<T>(
   this: Middleware, 
   model: T, 
   next: NextFindOne<T>, 
   conditions: Conds, 
options?: SelectOptions): Promise<InstanceType<T> | null>;
```

Defined in: Middleware.ts:205

Intercept findOne()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextFindOne`\<`T`\> |
| `conditions` | `Conds` |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) |

#### Returns

`Promise`\<`InstanceType`\<`T`\> \| `null`\>

***

### findById()?

```ts
optional findById<T>(
   this: Middleware, 
   model: T, 
   next: NextFindById<T>, 
   id: unknown, 
options?: SelectOptions): Promise<InstanceType<T>[]>;
```

Defined in: Middleware.ts:214

Intercept findById()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextFindById`\<`T`\> |
| `id` | `unknown` |
| `options?` | [`SelectOptions`](../interfaces/SelectOptions.md) |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>

***

### count()?

```ts
optional count<T>(
   this: Middleware, 
   model: T, 
   next: NextCount, 
conditions: Conds): Promise<number>;
```

Defined in: Middleware.ts:223

Intercept count()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextCount` |
| `conditions` | `Conds` |

#### Returns

`Promise`\<`number`\>

***

### create()?

```ts
optional create<T>(
   this: Middleware, 
   model: T, 
   next: NextCreate, 
   pairs: readonly readonly [Column<any, any>, any][], 
options?: InsertOptions<unknown>): Promise<PkeyResult | null>;
```

Defined in: Middleware.ts:231

Intercept create()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextCreate` |
| `pairs` | readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][] |
| `options?` | [`InsertOptions`](../interfaces/InsertOptions.md)\<`unknown`\> |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

***

### createMany()?

```ts
optional createMany<T>(
   this: Middleware, 
   model: T, 
   next: NextCreateMany, 
   pairsArray: readonly readonly readonly [Column<any, any>, any][][], 
options?: InsertOptions<unknown>): Promise<PkeyResult | null>;
```

Defined in: Middleware.ts:240

Intercept createMany()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextCreateMany` |
| `pairsArray` | readonly readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][][] |
| `options?` | [`InsertOptions`](../interfaces/InsertOptions.md)\<`unknown`\> |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

***

### update()?

```ts
optional update<T>(
   this: Middleware, 
   model: T, 
   next: NextUpdate, 
   conditions: Conds, 
   values: readonly readonly [Column<any, any>, any][], 
options?: UpdateOptions): Promise<PkeyResult | null>;
```

Defined in: Middleware.ts:249

Intercept update()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextUpdate` |
| `conditions` | `Conds` |
| `values` | readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][] |
| `options?` | [`UpdateOptions`](../interfaces/UpdateOptions.md) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

***

### updateMany()?

```ts
optional updateMany<T>(
   this: Middleware, 
   model: T, 
   next: NextUpdateMany, 
   records: readonly readonly readonly [Column<any, any>, any][][], 
options?: UpdateManyOptions): Promise<PkeyResult | null>;
```

Defined in: Middleware.ts:259

Intercept updateMany()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextUpdateMany` |
| `records` | readonly readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `any`\][][] |
| `options?` | [`UpdateManyOptions`](../interfaces/UpdateManyOptions.md) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

***

### delete()?

```ts
optional delete<T>(
   this: Middleware, 
   model: T, 
   next: NextDelete, 
   conditions: Conds, 
options?: DeleteOptions): Promise<PkeyResult | null>;
```

Defined in: Middleware.ts:268

Intercept delete()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextDelete` |
| `conditions` | `Conds` |
| `options?` | [`DeleteOptions`](../interfaces/DeleteOptions.md) |

#### Returns

`Promise`\<[`PkeyResult`](../interfaces/PkeyResult.md) \| `null`\>

***

### execute()?

```ts
optional execute(
   this: Middleware, 
   next: NextExecute, 
   sql: string, 
params?: unknown[]): Promise<ExecuteResult>;
```

Defined in: Middleware.ts:277

Intercept execute()

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `next` | `NextExecute` |
| `sql` | `string` |
| `params?` | `unknown`[] |

#### Returns

`Promise`\<`ExecuteResult`\>

***

### query()?

```ts
optional query<T>(
   this: Middleware, 
   model: T, 
   next: NextQuery<T>, 
   sql: string, 
params?: unknown[]): Promise<InstanceType<T>[]>;
```

Defined in: Middleware.ts:285

Intercept query()

#### Type Parameters

| Type Parameter |
| ------ |
| `T` *extends* *typeof* [`DBModel`](DBModel.md) |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `this` | `Middleware` |
| `model` | `T` |
| `next` | `NextQuery`\<`T`\> |
| `sql` | `string` |
| `params?` | `unknown`[] |

#### Returns

`Promise`\<`InstanceType`\<`T`\>[]\>
