[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / parentRef

# Function: parentRef()

```ts
function parentRef(column: ColumnRef): DBParentRef;
```

Defined in: DBValues.ts:643

Create a parent table column reference for correlated subqueries.

## Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `column` | `ColumnRef` | The column from the parent (outer) query (type-safe Column reference) |

## Returns

`DBParentRef`

DBParentRef instance

## Example

```typescript
// Reference parent's id column in subquery
await User.find([
  User.inSubquery([User.id], Order, [Order.user_id], [
    [Order.user_id, parentRef(User.id)]
  ])
]);
```
