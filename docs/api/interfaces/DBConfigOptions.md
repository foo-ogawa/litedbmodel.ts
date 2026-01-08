[**litedbmodel v0.19.5**](../README.md)

***

[litedbmodel](../globals.md) / DBConfigOptions

# Interface: DBConfigOptions

Defined in: types.ts:266

Options for database configuration.
Used with DBModel.setConfig() and DBModel.createDBBase().

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="writerconfig"></a> `writerConfig?` | `DBConfig` | Writer database configuration for reader/writer separation | types.ts:268 |
| <a id="logger"></a> `logger?` | `Logger` | Logger instance | types.ts:270 |
| <a id="findhardlimit"></a> `findHardLimit?` | `number` \| `null` | Hard limit for find() - throws if exceeded | types.ts:272 |
| <a id="hasmanyhardlimit"></a> `hasManyHardLimit?` | `number` \| `null` | Hard limit for hasMany lazy loading - throws if exceeded | types.ts:274 |
| <a id="usewriteraftertransaction"></a> `useWriterAfterTransaction?` | `boolean` | Keep using writer connection after transaction completes. Helps avoid stale reads due to replication lag. **Default** `true` | types.ts:280 |
| <a id="writerstickyduration"></a> `writerStickyDuration?` | `number` | Duration (in milliseconds) to keep using writer after transaction. Only applies when useWriterAfterTransaction is true. **Default** `5000` | types.ts:286 |
