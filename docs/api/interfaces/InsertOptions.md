[**litedbmodel v0.20.1**](../README.md)

***

[litedbmodel](../globals.md) / InsertOptions

# Interface: InsertOptions\<Model\>

Defined in: types.ts:151

Insert options with type-safe column references.

## Type Parameters

| Type Parameter | Default type | Description |
| ------ | ------ | ------ |
| `Model` | `unknown` | The model class for type-safe column constraints |

## Properties

| Property | Type | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="tablename"></a> `tableName?` | `string` | - | types.ts:152 |
| <a id="returning"></a> `returning?` | `boolean` | If true, return PkeyResult with affected primary keys. If false (default), return null for better performance. | types.ts:157 |
| <a id="conflict"></a> ~~`conflict?`~~ | `string` | **Deprecated** Use onConflict instead | types.ts:159 |
| <a id="onconflict"></a> `onConflict?` | \| [`Column`](Column.md)\<`unknown`, `Model`\> \| [`Column`](Column.md)\<`unknown`, `Model`\>[] | Columns for ON CONFLICT clause (unique constraint columns). Must be Column symbols from the same model for type safety. **Example** `// Single column onConflict: User.email // Multiple columns (composite unique constraint) onConflict: [UserPref.user_id, UserPref.key]` | types.ts:169 |
| <a id="onconflictupdate"></a> `onConflictUpdate?` | [`Column`](Column.md)\<`unknown`, `Model`\>[] \| `"all"` | Columns to update on conflict. Can be: - 'all': Update all inserted columns - Array of Column symbols from the same model **Example** `// Update all columns onConflictUpdate: 'all' // Update specific columns onConflictUpdate: [User.name, User.updated_at]` | types.ts:181 |
| <a id="onconflictignore"></a> `onConflictIgnore?` | `boolean` | If true, ignore the insert on conflict (DO NOTHING). Cannot be used with onConflictUpdate. | types.ts:186 |
