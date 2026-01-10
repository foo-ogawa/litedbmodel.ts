[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / WriteInReadOnlyContextError

# Class: WriteInReadOnlyContextError

Defined in: types.ts:395

Error thrown when attempting write operations inside withWriter() context.

## Extends

- `Error`

## Constructors

### Constructor

```ts
new WriteInReadOnlyContextError(operation: string, modelName?: string): WriteInReadOnlyContextError;
```

Defined in: types.ts:396

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `operation` | `string` |
| `modelName?` | `string` |

#### Returns

`WriteInReadOnlyContextError`

#### Overrides

```ts
Error.constructor
```

## Properties

| Property | Modifier | Type | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="operation"></a> `operation` | `readonly` | `string` | types.ts:397 |
| <a id="modelname"></a> `modelName?` | `readonly` | `string` | types.ts:398 |
