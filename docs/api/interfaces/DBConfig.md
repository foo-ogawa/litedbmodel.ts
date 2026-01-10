[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / DBConfig

# Interface: DBConfig

Defined in: DBHandler.ts:40

Database configuration

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="host"></a> `host?` | `string` | Database host (for server-based DBs) | DBHandler.ts:42 |
| <a id="port"></a> `port?` | `number` | Database port | DBHandler.ts:44 |
| <a id="database"></a> `database` | `string` | Database name or file path | DBHandler.ts:46 |
| <a id="user"></a> `user?` | `string` | Username | DBHandler.ts:48 |
| <a id="password"></a> `password?` | `string` | Password | DBHandler.ts:50 |
| <a id="max"></a> `max?` | `number` | Maximum pool size | DBHandler.ts:52 |
| <a id="timeout"></a> `timeout?` | `number` | Connection timeout in seconds | DBHandler.ts:54 |
| <a id="querytimeout"></a> `queryTimeout?` | `number` | Query timeout in seconds | DBHandler.ts:56 |
| <a id="driver"></a> `driver?` | `"postgres"` \| `"sqlite"` \| `"mysql"` | Driver type: 'postgres' (default), 'sqlite', or 'mysql' | DBHandler.ts:58 |
