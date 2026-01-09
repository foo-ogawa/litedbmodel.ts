[**litedbmodel v0.19.7**](../README.md)

***

[litedbmodel](../globals.md) / dbUuid

# Function: dbUuid()

```ts
function dbUuid(value: string, operator: string): DBCast;
```

Defined in: DBValues.ts:519

Create a type-cast UUID value

## Parameters

| Parameter | Type | Default value | Description |
| ------ | ------ | ------ | ------ |
| `value` | `string` | `undefined` | The UUID string value |
| `operator` | `string` | `'='` | The comparison operator (default: '=') |

## Returns

`DBCast`

## Example

```typescript
await User.find([[User.id, dbUuid('123e4567-e89b-12d3-a456-426614174000')]]);
// → WHERE id = ?::uuid
```
