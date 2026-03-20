[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / SqlRaw

# Class: SqlRaw

Defined in: SqlFragment.ts:59

Raw SQL string that bypasses parameterization.
Created via `sql.raw()`. Only usable inside `sql` tagged templates.

## Constructors

### Constructor

```ts
new SqlRaw(value: string): SqlRaw;
```

Defined in: SqlFragment.ts:61

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `value` | `string` |

#### Returns

`SqlRaw`

## Properties

| Property | Modifier | Type | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="_tag"></a> `_tag` | `readonly` | `"SqlRaw"` | SqlFragment.ts:60 |
| <a id="value"></a> `value` | `readonly` | `string` | SqlFragment.ts:61 |

## Methods

### toString()

```ts
toString(): string;
```

Defined in: SqlFragment.ts:62

#### Returns

`string`
