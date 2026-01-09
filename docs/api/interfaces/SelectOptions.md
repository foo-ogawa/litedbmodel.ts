[**litedbmodel v0.19.7**](../README.md)

***

[litedbmodel](../globals.md) / SelectOptions

# Interface: SelectOptions

Defined in: types.ts:108

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="order"></a> `order?` | `string` \| `OrderSpec` | Order by clause. Accepts OrderSpec (Column.asc()/desc()) or raw string. | types.ts:110 |
| <a id="limit"></a> `limit?` | `number` | - | types.ts:111 |
| <a id="offset"></a> `offset?` | `number` | - | types.ts:112 |
| <a id="select"></a> `select?` | `string` | - | types.ts:113 |
| <a id="group"></a> `group?` | `string` | - | types.ts:114 |
| <a id="tablename"></a> `tableName?` | `string` | - | types.ts:115 |
| <a id="append"></a> `append?` | `string` | - | types.ts:116 |
| <a id="forupdate"></a> `forUpdate?` | `boolean` | - | types.ts:117 |
| <a id="join"></a> `join?` | `string` | JOIN clause to add to the query. Can include parameters using ? placeholders. **Example** `join: 'JOIN unnest(?::int[]) AS _keys(id) ON t.id = _keys.id'` | types.ts:124 |
| <a id="joinparams"></a> `joinParams?` | `unknown`[] | Parameters for the JOIN clause (prepended to condition params). | types.ts:128 |
| <a id="cte"></a> `cte?` | \{ `name`: `string`; `sql`: `string`; `params`: `unknown`[]; \} | CTE (Common Table Expression) to prepend to the query. Used for window functions like ROW_NUMBER() or complex subqueries. The SQL should use ? placeholders for parameters. **Example** `cte: { name: 'ranked', sql: 'SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id) AS _rn FROM posts WHERE user_id IN (?, ?)', params: [1, 2] }` | types.ts:140 |
| `cte.name` | `string` | - | types.ts:141 |
| `cte.sql` | `string` | - | types.ts:142 |
| `cte.params` | `unknown`[] | - | types.ts:143 |
