[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / SqlTypedFragment

# Interface: SqlTypedFragment\<V, M\>

Defined in: SqlFragment.ts:34

A typed SQL fragment that preserves the Column's value type.
Used as the first element of a condition tuple (Pattern A).

## Type Parameters

| Type Parameter | Default type | Description |
| ------ | ------ | ------ |
| `V` | `unknown` | The value type of the referenced Column |
| `M` | `unknown` | The model type the Column belongs to |

## Properties

| Property | Modifier | Type | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="_tag"></a> `_tag` | `readonly` | `"SqlTypedFragment"` | SqlFragment.ts:35 |
| <a id="sql"></a> `sql` | `readonly` | `string` | SqlFragment.ts:36 |
| <a id="params"></a> `params` | `readonly` | readonly `unknown`[] | SqlFragment.ts:37 |
| <a id="__valuetype"></a> `__valueType?` | `readonly` | `V` | SqlFragment.ts:38 |
| <a id="__modeltype"></a> `__modelType?` | `readonly` | `M` | SqlFragment.ts:39 |
