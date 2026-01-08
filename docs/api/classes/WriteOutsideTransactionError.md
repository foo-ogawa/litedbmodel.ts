[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / WriteOutsideTransactionError

# Class: WriteOutsideTransactionError

Defined in: types.ts:377

Error thrown when attempting write operations outside a transaction.

## Extends

- `Error`

## Constructors

### Constructor

```ts
new WriteOutsideTransactionError(operation: string, modelName?: string): WriteOutsideTransactionError;
```

Defined in: types.ts:378

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `operation` | `string` |
| `modelName?` | `string` |

#### Returns

`WriteOutsideTransactionError`

#### Overrides

```ts
Error.constructor
```

## Properties

| Property | Modifier | Type | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="operation"></a> `operation` | `readonly` | `string` | types.ts:379 |
| <a id="modelname"></a> `modelName?` | `readonly` | `string` | types.ts:380 |
