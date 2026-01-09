[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / Values

# Class: Values\<Model\>

Defined in: Column.ts:565

Type-safe builder for update/create value pairs.
Use array literals for static values, builder for dynamic construction.

## Example

```typescript
// Static values - use array literal directly (type-checked)
await User.create([
  [User.name, 'John'],
  [User.email, 'john@example.com'],
]);

// Dynamic construction - use Values builder
const updates = new Values<User>();
if (body.name) updates.add(User.name, body.name);
if (body.email) updates.add(User.email, body.email);
await User.update([[User.id, id]], updates.build());

// Mixed: initial values + dynamic additions
const values = new Values<User>([
  [User.created_at, new Date()],
]);
if (body.name) values.add(User.name, body.name);
await User.create(values.build());
```

## Type Parameters

| Type Parameter |
| ------ |
| `Model` |

## Constructors

### Constructor

```ts
new Values<Model>(initial?: readonly readonly [Column<any, Model>, unknown][]): Values<Model>;
```

Defined in: Column.ts:571

Create a Values builder, optionally with initial pairs.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `initial?` | readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `Model`\>, `unknown`\][] |

#### Returns

`Values`\<`Model`\>

## Accessors

### length

#### Get Signature

```ts
get length(): number;
```

Defined in: Column.ts:595

Get the number of pairs.

##### Returns

`number`

## Methods

### add()

```ts
add<V>(column: Column<V, Model>, value: V | null | undefined): this;
```

Defined in: Column.ts:580

Add a type-safe column-value pair.

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

### build()

```ts
build(): readonly readonly [Column<any, any>, unknown][];
```

Defined in: Column.ts:588

Build the final array for use with update/create.

#### Returns

readonly readonly \[[`Column`](../interfaces/Column.md)\<`any`, `any`\>, `unknown`\][]
