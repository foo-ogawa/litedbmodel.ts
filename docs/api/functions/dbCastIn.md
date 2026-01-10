[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / dbCastIn

# Function: dbCastIn()

```ts
function dbCastIn(values: unknown[], sqlType: string): DBCastArray;
```

Defined in: DBValues.ts:529

Create a type-cast array for IN clause

## Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `values` | `unknown`[] | Array of values |
| `sqlType` | `string` | The SQL type to cast each value to |

## Returns

`DBCastArray`
