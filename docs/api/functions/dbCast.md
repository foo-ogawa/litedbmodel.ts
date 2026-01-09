[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / dbCast

# Function: dbCast()

```ts
function dbCast(
   value: unknown, 
   sqlType: string, 
   operator: string): DBCast;
```

Defined in: DBValues.ts:503

Create a type-cast value

## Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `value` | `unknown` | `undefined` | The value to cast |
| `sqlType` | `string` | `undefined` | The SQL type to cast to (e.g., 'uuid', 'jsonb') |
| `operator` | `string` | `'='` | The comparison operator (default: '=') |

## Returns

`DBCast`
