[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / hasMany

# Function: hasMany()

```ts
function hasMany(keys: KeysFactory, options?: RelationDecoratorOptions): PropertyDecorator;
```

Defined in: decorators.ts:611

HasMany relation decorator (1:N).
Defines a one-to-many relationship where this model has many related records.

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
@hasMany(() => [User.id, Post.author_id])
declare posts: Promise<Post[]>;

// With options
@hasMany(() => [User.id, Post.author_id], {
  order: () => Post.created_at.desc(),
  where: () => [[Post.is_deleted, false]],
})
declare activePosts: Promise<Post[]>;

// Composite key relation
@hasMany(() => [
  [TenantUser.tenant_id, TenantPost.tenant_id],
  [TenantUser.id, TenantPost.author_id],
])
declare posts: Promise<TenantPost[]>;
```
