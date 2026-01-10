[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / ModelOptions

# Interface: ModelOptions

Defined in: types.ts:43

Options for the

## Model

decorator.
All options use lazy evaluation (functions) to support forward references.

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="order"></a> `order?` | () => `OrderSpec` | DEFAULT_ORDER: Returns OrderSpec for default ordering | types.ts:45 |
| <a id="filter"></a> `filter?` | () => `Conds` | FIND_FILTER: Returns Conds for automatic filtering in find() | types.ts:48 |
| <a id="select"></a> `select?` | `string` | SELECT_COLUMN: Column selection string (default: '*') | types.ts:51 |
| <a id="updatetable"></a> `updateTable?` | `string` | UPDATE_TABLE_NAME: Table name for INSERT/UPDATE operations | types.ts:54 |
| <a id="group"></a> `group?` | () => \| `string` \| [`Column`](Column.md)\<`unknown`, `unknown`\> \| [`Column`](Column.md)\<`unknown`, `unknown`\>[] | DEFAULT_GROUP: Returns Column(s) or string for default grouping | types.ts:57 |
