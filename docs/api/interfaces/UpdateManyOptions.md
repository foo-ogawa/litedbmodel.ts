[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / UpdateManyOptions

# Interface: UpdateManyOptions

Defined in: types.ts:210

Options for updateMany operation.

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="keycolumns"></a> `keyColumns` | \| [`Column`](Column.md)\<`unknown`, `unknown`\> \| [`Column`](Column.md)\<`unknown`, `unknown`\>[] | Columns that identify each row (used in WHERE/JOIN clause). Must uniquely identify rows (primary key or unique constraint). | types.ts:215 |
| <a id="returning"></a> `returning?` | `boolean` | If true, return PkeyResult with affected primary keys. If false (default), return null for better performance. | types.ts:220 |
