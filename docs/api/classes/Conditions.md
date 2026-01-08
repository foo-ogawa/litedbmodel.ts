[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / Conditions

# Class: Conditions\<Model\>

Defined in: Column.ts:601

Type-safe builder for query conditions.
Use array literals for static conditions, builder for dynamic construction.

## Example

```typescript
// Static conditions - use array literal directly
const users = await User.find([
  [User.deleted, false],
  [User.is_active, true],
]);

// Dynamic construction - use Conditions builder
const where = new Conditions<User>();
where.add(User.deleted, false);
if (query.name) where.addRaw(`${User.name} LIKE ?`, `%${query.name}%`);
const users = await User.find(where.build());

// Mixed: initial conditions + dynamic additions
const where = new Conditions<User>([
  [User.deleted, false],
]);
if (query.active) where.add(User.is_active, true);
```

## Type Parameters

| Type Parameter |
| ------ |
| `Model` |

## Constructors

### Constructor

```ts
new Conditions<Model>(initial?: Conds): Conditions<Model>;
```

Defined in: Column.ts:607

Create a Conditions builder, optionally with initial conditions.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `initial?` | `Conds` |

#### Returns

`Conditions`\<`Model`\>

## Accessors

### length

#### Get Signature

```ts
get length(): number;
```

Defined in: Column.ts:651

Get the number of conditions.

##### Returns

`number`

## Methods

### add()

```ts
add<V>(column: Column<V, Model>, value: V | null | undefined): this;
```

Defined in: Column.ts:616

Add a type-safe column equality condition.

#### Type Parameters

| Type Parameter |
| ------ |
| `V` |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `column` | [`Column`](../interfaces/Column.md)\<`V`, `Model`\> |
| `value` | `V` \| `null` \| `undefined` |

#### Returns

`this`

***

### addRaw()

```ts
addRaw(condition: string, value?: unknown): this;
```

Defined in: Column.ts:624

Add a raw condition with template literal (e.g., `${User.age} > ?`).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `condition` | `string` |
| `value?` | `unknown` |

#### Returns

`this`

***

### or()

```ts
or(...condGroups: readonly Conds[]): this;
```

Defined in: Column.ts:636

Add an OR condition group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| ...`condGroups` | readonly `Conds`[] |

#### Returns

`this`

***

### build()

```ts
build(): Conds;
```

Defined in: Column.ts:644

Build the final array for use with find/count/delete.

#### Returns

`Conds`
