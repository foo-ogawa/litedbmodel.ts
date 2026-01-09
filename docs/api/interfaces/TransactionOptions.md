[**litedbmodel v0.19.7**](../README.md)

***

[litedbmodel](../globals.md) / TransactionOptions

# Interface: TransactionOptions

Defined in: types.ts:242

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="retryonerror"></a> `retryOnError?` | `boolean` | - | types.ts:243 |
| <a id="retrylimit"></a> `retryLimit?` | `number` | - | types.ts:244 |
| <a id="retryduration"></a> `retryDuration?` | `number` | - | types.ts:245 |
| <a id="rollbackonly"></a> `rollbackOnly?` | `boolean` | If true, always rollback instead of commit (useful for preview/dry-run) | types.ts:247 |
| <a id="usewriteraftertransaction"></a> `useWriterAfterTransaction?` | `boolean` | Override global useWriterAfterTransaction for this transaction. If true, subsequent reads will use writer connection for writerStickyDuration. **Default** `Uses global setting (true by default)` | types.ts:253 |
