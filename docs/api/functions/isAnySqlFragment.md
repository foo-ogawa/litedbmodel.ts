[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / isAnySqlFragment

# Function: isAnySqlFragment()

```ts
function isAnySqlFragment(value: unknown): value is SqlFragment | SqlTypedFragment<unknown, unknown> | SqlCondition<unknown>;
```

Defined in: SqlFragment.ts:148

Matches any of SqlFragment, SqlTypedFragment, or SqlCondition.

## Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `unknown` |

## Returns

value is SqlFragment \| SqlTypedFragment\<unknown, unknown\> \| SqlCondition\<unknown\>
