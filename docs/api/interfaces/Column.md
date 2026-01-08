[**litedbmodel v0.19.4**](../README.md)

***

[litedbmodel](../globals.md) / Column

# Interface: Column()\<ValueType, ModelType\>

Defined in: Column.ts:143

Type-safe column reference as a callable function.

## Type Parameters

| Type Parameter | Default type | Description |
| ------ | ------ | ------ |
| `ValueType` | `unknown` | The TypeScript type of the column value |
| `ModelType` | `unknown` | The model class this column belongs to (for relation type safety) - Call `User.id()` to get the column name as a string (for computed property keys) - Use methods like `User.id.eq(1)` for condition builders - Use in template literals: `${User.id}` (calls toString()) |

```ts
Column(): string;
```

Defined in: Column.ts:145

Call to get column name as string (for computed property keys)

## Returns

`string`

## Properties

| Property | Modifier | Type | Description | Defined in |
| ------ | ------ | ------ | ------ | ------ |
| <a id="columnname"></a> `columnName` | `readonly` | `string` | The database column name | Column.ts:148 |
| <a id="propertyname"></a> `propertyName` | `readonly` | `string` | The property name on the model class (may differ from columnName) | Column.ts:151 |
| <a id="tablename"></a> `tableName` | `readonly` | `string` | The database table name | Column.ts:154 |
| <a id="modelname"></a> `modelName` | `readonly` | `string` | The model class name (for debugging and static analysis) | Column.ts:157 |
| <a id="_brand"></a> `_brand` | `readonly` | `"Column"` | Brand for type discrimination - enables static analysis to distinguish from regular variables | Column.ts:160 |
| <a id="__model"></a> `__model?` | `readonly` | `ModelType` | Phantom type for model association (compile-time only, not used at runtime) | Column.ts:163 |

## Methods

### eq()

```ts
eq(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:173

Equal condition (column = value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.id.eq(1) → { id: 1 }
```

***

### ne()

```ts
ne(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:179

Not equal condition (column != value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.status.ne('deleted') → { 'status != ?': 'deleted' }
```

***

### gt()

```ts
gt(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:185

Greater than condition (column > value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.age.gt(18) → { 'age > ?': 18 }
```

***

### gte()

```ts
gte(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:191

Greater than or equal condition (column >= value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.age.gte(18) → { 'age >= ?': 18 }
```

***

### lt()

```ts
lt(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:197

Less than condition (column < value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.age.lt(65) → { 'age < ?': 65 }
```

***

### lte()

```ts
lte(value: ValueType): Record<string, ValueType>;
```

Defined in: Column.ts:203

Less than or equal condition (column <= value)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `ValueType` |

#### Returns

`Record`\<`string`, `ValueType`\>

#### Example

```ts
User.age.lte(65) → { 'age <= ?': 65 }
```

***

### like()

```ts
like(pattern: string): Record<string, string>;
```

Defined in: Column.ts:209

LIKE condition (column LIKE pattern)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `pattern` | `string` |

#### Returns

`Record`\<`string`, `string`\>

#### Example

```ts
User.name.like('%test%') → { 'name LIKE ?': '%test%' }
```

***

### notLike()

```ts
notLike(pattern: string): Record<string, string>;
```

Defined in: Column.ts:215

NOT LIKE condition (column NOT LIKE pattern)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `pattern` | `string` |

#### Returns

`Record`\<`string`, `string`\>

#### Example

```ts
User.name.notLike('%test%') → { 'name NOT LIKE ?': '%test%' }
```

***

### ilike()

```ts
ilike(pattern: string): Record<string, string>;
```

Defined in: Column.ts:221

ILIKE condition (case-insensitive LIKE, PostgreSQL specific)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `pattern` | `string` |

#### Returns

`Record`\<`string`, `string`\>

#### Example

```ts
User.name.ilike('%TEST%') → { 'name ILIKE ?': '%TEST%' }
```

***

### between()

```ts
between(from: ValueType, to: ValueType): Record<string, [ValueType, ValueType]>;
```

Defined in: Column.ts:227

BETWEEN condition (column BETWEEN from AND to)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `from` | `ValueType` |
| `to` | `ValueType` |

#### Returns

`Record`\<`string`, \[`ValueType`, `ValueType`\]\>

#### Example

```ts
User.age.between(18, 65) → { 'age BETWEEN ? AND ?': [18, 65] }
```

***

### in()

```ts
in(values: ValueType[]): Record<string, ValueType[]>;
```

Defined in: Column.ts:234

IN condition (column IN (values))
Note: Arrays are automatically converted to IN clause by litedbmodel

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `values` | `ValueType`[] |

#### Returns

`Record`\<`string`, `ValueType`[]\>

#### Example

```ts
User.status.in(['active', 'pending']) → { status: ['active', 'pending'] }
```

***

### notIn()

```ts
notIn(values: ValueType[]): Record<string, ValueType[]>;
```

Defined in: Column.ts:240

NOT IN condition (column NOT IN (values))

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `values` | `ValueType`[] |

#### Returns

`Record`\<`string`, `ValueType`[]\>

#### Example

```ts
User.status.notIn(['deleted', 'banned'])
```

***

### isNull()

```ts
isNull(): Record<string, null>;
```

Defined in: Column.ts:246

IS NULL condition

#### Returns

`Record`\<`string`, `null`\>

#### Example

```ts
User.deleted_at.isNull() → { deleted_at: null }
```

***

### isNotNull()

```ts
isNotNull(): Record<string, DBNotNullValue>;
```

Defined in: Column.ts:252

IS NOT NULL condition

#### Returns

`Record`\<`string`, `DBNotNullValue`\>

#### Example

```ts
User.email.isNotNull() → { email: DBNotNullValue }
```

***

### asc()

```ts
asc(): OrderColumn<ModelType>;
```

Defined in: Column.ts:262

Ascending order

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.created_at.asc() → OrderColumn('created_at', 'ASC')
```

***

### desc()

```ts
desc(): OrderColumn<ModelType>;
```

Defined in: Column.ts:268

Descending order

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.created_at.desc() → OrderColumn('created_at', 'DESC')
```

***

### ascNullsFirst()

```ts
ascNullsFirst(): OrderColumn<ModelType>;
```

Defined in: Column.ts:274

Ascending order with NULLS FIRST

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.updated_at.ascNullsFirst() → OrderColumn with NULLS FIRST
```

***

### ascNullsLast()

```ts
ascNullsLast(): OrderColumn<ModelType>;
```

Defined in: Column.ts:280

Ascending order with NULLS LAST

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.updated_at.ascNullsLast() → OrderColumn with NULLS LAST
```

***

### descNullsFirst()

```ts
descNullsFirst(): OrderColumn<ModelType>;
```

Defined in: Column.ts:286

Descending order with NULLS FIRST

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.updated_at.descNullsFirst() → OrderColumn with NULLS FIRST
```

***

### descNullsLast()

```ts
descNullsLast(): OrderColumn<ModelType>;
```

Defined in: Column.ts:292

Descending order with NULLS LAST

#### Returns

`OrderColumn`\<`ModelType`\>

#### Example

```ts
User.updated_at.descNullsLast() → OrderColumn with NULLS LAST
```

***

### toString()

```ts
toString(): string;
```

Defined in: Column.ts:302

Returns column name (for template literals)

#### Returns

`string`

#### Example

```ts
`${User.id}` → 'id'
```
