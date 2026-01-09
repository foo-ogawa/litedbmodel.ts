[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / dbUuidIn

# Function: dbUuidIn()

```ts
function dbUuidIn(values: string[]): DBCastArray;
```

Defined in: DBValues.ts:544

Create a type-cast UUID array for IN clause

## Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `values` | `string`[] | Array of UUID strings |

## Returns

`DBCastArray`

## Example

```typescript
await User.find([[User.id, dbUuidIn(['uuid1', 'uuid2'])]]);
// → WHERE id IN (?::uuid, ?::uuid)
```
