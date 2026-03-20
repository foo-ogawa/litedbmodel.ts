[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / sql

# Function: sql()

## Call Signature

```ts
function sql<V, M>(strings: TemplateStringsArray, col: Column<V, M>): SqlTypedFragment<V, M>;
```

Defined in: SqlFragment.ts:234

Single Column interpolation → SqlTypedFragment (for Pattern A tuples or IS NULL conditions).

### Type Parameters

| Type Parameter |
| ------ |
| `V` |
| `M` |

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| `col` | [`Column`](../interfaces/Column.md)\<`V`, `M`\> |

### Returns

[`SqlTypedFragment`](../interfaces/SqlTypedFragment.md)\<`V`, `M`\>

## Call Signature

```ts
function sql<V, M>(
   strings: TemplateStringsArray, 
   col: Column<V, M>, 
value: V): SqlCondition<M>;
```

Defined in: SqlFragment.ts:242

Single Column + 1 value → SqlCondition (Pattern B: `sql\`${Col} > ${value}\``).

### Type Parameters

| Type Parameter |
| ------ |
| `V` |
| `M` |

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| `col` | [`Column`](../interfaces/Column.md)\<`V`, `M`\> |
| `value` | `V` |

### Returns

[`SqlCondition`](../interfaces/SqlCondition.md)\<`M`\>

## Call Signature

```ts
function sql<V, M>(
   strings: TemplateStringsArray, 
   col: Column<V, M>, 
   v1: V, 
v2: V): SqlCondition<M>;
```

Defined in: SqlFragment.ts:251

Single Column + 2 values → SqlCondition (Pattern B: BETWEEN).

### Type Parameters

| Type Parameter |
| ------ |
| `V` |
| `M` |

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| `col` | [`Column`](../interfaces/Column.md)\<`V`, `M`\> |
| `v1` | `V` |
| `v2` | `V` |

### Returns

[`SqlCondition`](../interfaces/SqlCondition.md)\<`M`\>

## Call Signature

```ts
function sql<V, M>(
   strings: TemplateStringsArray, 
   col: Column<V, M>, 
values: readonly V[]): SqlCondition<M>;
```

Defined in: SqlFragment.ts:261

Single Column + array → SqlCondition (Pattern B: IN).

### Type Parameters

| Type Parameter |
| ------ |
| `V` |
| `M` |

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| `col` | [`Column`](../interfaces/Column.md)\<`V`, `M`\> |
| `values` | readonly `V`[] |

### Returns

[`SqlCondition`](../interfaces/SqlCondition.md)\<`M`\>

## Call Signature

```ts
function sql(strings: TemplateStringsArray, ...values: (
  | DBParentRef
  | SqlRaw
  | SqlRef
  | Column<any, any>
  | {
  TABLE_NAME: string;
})[]): SqlFragment;
```

Defined in: SqlFragment.ts:270

Multiple Columns / TABLE_NAME only → SqlFragment (for QUERY).

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| ...`values` | ( \| `DBParentRef` \| [`SqlRaw`](../classes/SqlRaw.md) \| [`SqlRef`](../classes/SqlRef.md) \| [`Column`](../interfaces/Column.md)\<`any`, `any`\> \| \{ `TABLE_NAME`: `string`; \})[] |

### Returns

[`SqlFragment`](../interfaces/SqlFragment.md)

## Call Signature

```ts
function sql(strings: TemplateStringsArray, ...values: SqlInterpolation[]): SqlFragment;
```

Defined in: SqlFragment.ts:278

General: mixed interpolation → SqlFragment (for withQuery / execute).

### Parameters

| Parameter | Type |
| ------ | ------ |
| `strings` | `TemplateStringsArray` |
| ...`values` | [`SqlInterpolation`](../globals.md#sqlinterpolation)[] |

### Returns

[`SqlFragment`](../interfaces/SqlFragment.md)
