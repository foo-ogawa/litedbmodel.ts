[**litedbmodel v0.19.6**](../README.md)

***

[litedbmodel](../globals.md) / LimitExceededError

# Class: LimitExceededError

Defined in: types.ts:345

Error thrown when a query exceeds the configured limit.

## Extends

- `Error`

## Constructors

### Constructor

```ts
new LimitExceededError(
   limit: number, 
   actualCount: number, 
   context: "find" | "relation", 
   modelName?: string, 
   relationName?: string): LimitExceededError;
```

Defined in: types.ts:346

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `limit` | `number` | - |
| `actualCount` | `number` | Number of records returned. For find() with findHardLimit, this is limit+1 (actual total may be higher). For relation loading, this is the exact count. |
| `context` | `"find"` \| `"relation"` | - |
| `modelName?` | `string` | - |
| `relationName?` | `string` | - |

#### Returns

`LimitExceededError`

#### Overrides

```ts
Error.constructor
```

## Properties

| Property | Modifier | Type | Description | Defined in |
| ------ | ------ | ------ | ------ | ------ |
| <a id="limit"></a> `limit` | `readonly` | `number` | - | types.ts:347 |
| <a id="actualcount"></a> `actualCount` | `readonly` | `number` | Number of records returned. For find() with findHardLimit, this is limit+1 (actual total may be higher). For relation loading, this is the exact count. | types.ts:352 |
| <a id="context"></a> `context` | `readonly` | `"find"` \| `"relation"` | - | types.ts:353 |
| <a id="modelname"></a> `modelName?` | `readonly` | `string` | - | types.ts:354 |
| <a id="relationname"></a> `relationName?` | `readonly` | `string` | - | types.ts:355 |
