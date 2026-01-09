[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / belongsTo

# Function: belongsTo()

```ts
function belongsTo(keys: KeysFactory, options?: RelationDecoratorOptions): PropertyDecorator;
```

Defined in: decorators.ts:687

BelongsTo relation decorator (N:1).
Defines a many-to-one relationship where this model belongs to a parent record.

## Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `keys` | `KeysFactory` | Factory function returning [sourceKey, targetKey] or composite key pairs |
| `options?` | `RelationDecoratorOptions` | Optional order and where clauses |

## Returns

`PropertyDecorator`

## Example

```typescript
// Single key relation
@belongsTo(() => [Post.author_id, User.id])
declare author: Promise<User | null>;

// Composite key relation
@belongsTo(() => [
  [TenantPost.tenant_id, TenantUser.tenant_id],
  [TenantPost.author_id, TenantUser.id],
])
declare author: Promise<TenantUser | null>;
```
