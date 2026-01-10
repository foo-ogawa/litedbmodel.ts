[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / PkeyResult

# Interface: PkeyResult

Defined in: types.ts:26

Result of write operations when returning: true is specified.
Contains primary key column(s) and their values for affected rows.

## Example

```typescript
// Single PK
{ key: [User.id], values: [[1], [2], [3]] }

// Composite PK
{ key: [TenantUser.tenant_id, TenantUser.id], values: [[1, 100], [1, 101]] }
```

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="key"></a> `key` | [`Column`](Column.md)\<`unknown`, `unknown`\>[] | Primary key column(s) | types.ts:28 |
| <a id="values"></a> `values` | `unknown`[][] | 2D array of primary key values (each inner array is one row's PK values) | types.ts:30 |
