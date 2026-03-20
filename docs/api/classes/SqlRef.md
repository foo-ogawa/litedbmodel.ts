[**litedbmodel v1.0.1**](../README.md)

***

[litedbmodel](../globals.md) / SqlRef

# Class: SqlRef

Defined in: SqlFragment.ts:71

Table-qualified column reference (e.g., `users.id`).
Created via `sql.ref()`. Only usable inside `sql` tagged templates.

## Constructors

### Constructor

```ts
new SqlRef(column: {
  tableName: string;
  columnName: string;
}): SqlRef;
```

Defined in: SqlFragment.ts:76

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `column` | \{ `tableName`: `string`; `columnName`: `string`; \} |
| `column.tableName` | `string` |
| `column.columnName` | `string` |

#### Returns

`SqlRef`

## Properties

| Property | Modifier | Type | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="_tag"></a> `_tag` | `readonly` | `"SqlRef"` | SqlFragment.ts:72 |
| <a id="tablename"></a> `tableName` | `readonly` | `string` | SqlFragment.ts:73 |
| <a id="columnname"></a> `columnName` | `readonly` | `string` | SqlFragment.ts:74 |

## Methods

### toString()

```ts
toString(): string;
```

Defined in: SqlFragment.ts:81

#### Returns

`string`
