# `sql` Tagged Template Literal

The `sql` tagged template provides type-safe, parameterized SQL fragments for conditions, `withQuery`, `QUERY`, and `execute`.

```typescript
import { sql } from 'litedbmodel';
```

---

## Conditions

### Column Tuples with `sql` Tag (Pattern A)

Use `[sql\`...\`, value]` tuples for operator conditions. The `sql` tag extracts the Column reference and preserves its value type, so the tuple's second element is type-checked:

```typescript
await User.find([
  [sql`${User.age} > ?`, 18],                          // ✅ number
  [sql`${User.age} BETWEEN ? AND ?`, [18, 65]],        // ✅ number[]
  [sql`${User.name} LIKE ?`, '%test%'],                 // ✅ string
  [sql`${User.status} IN (?)`, ['active', 'pending']],  // ✅ string[]
  sql`${User.deleted_at} IS NULL`,                       // ✅ no value needed
]);
```

Type errors are caught at compile time:

```typescript
[sql`${User.age} > ?`, 'hello']   // ❌ Type error: string is not assignable to number
[sql`${User.name} LIKE ?`, 42]    // ❌ Type error: number is not assignable to string
```

### Embedded Values (Pattern B)

Values can be interpolated directly in the template. The `sql` tag automatically converts them to `?` placeholders and extracts them as parameters:

```typescript
await User.find([
  sql`${User.age} > ${18}`,
  sql`${User.age} BETWEEN ${18} AND ${65}`,
  sql`${User.name} LIKE ${'%test%'}`,
  sql`${User.status} IN ${['active', 'pending']}`,
  sql`${User.deleted_at} IS NULL`,
]);
```

Both patterns produce the same internal representation and can be mixed freely.

### SKIP Pattern

Both patterns work with `SKIP` for conditional conditions:

```typescript
import { SKIP } from 'litedbmodel';

await User.find([
  [User.deleted, false],
  query.name ? [sql`${User.name} LIKE ?`, `%${query.name}%`] : SKIP,
  [User.status, query.status ?? SKIP],
]);
```

### OR Conditions

```typescript
await User.find([
  [sql`${User.age} >= ?`, 18],
  User.or(
    [[sql`${User.role} = ?`, 'admin']],
    [[sql`${User.role} = ?`, 'moderator']],
  ),
]);
```

### Conditions Builder

The `Conditions` class provides an `addSql()` method for dynamic condition building:

```typescript
const where = new Conditions<User>();
where.add(User.is_active, true);
where.addSql(sql`${User.age} > ?`, 18);
where.addSql(sql`${User.deleted_at} IS NULL`);  // value-free
const users = await User.find(where.build());
```

---

## `withQuery` — Parameterized Query-Based Models

The `sql` tag eliminates manual `$1`/`$2` placeholder numbering and DB dialect differences. Parameters are co-located with their SQL usage:

```typescript
@model('sales_report')
class SalesReportModel extends DBModel {
  @column() product_id?: number;
  @column() total_revenue?: number;

  static forPeriod(startDate: string, endDate: string) {
    return this.withQuery(sql`
      SELECT p.id AS product_id, SUM(oi.price) AS total_revenue
      FROM products p
      INNER JOIN order_items oi ON p.id = oi.product_id
      INNER JOIN orders o ON oi.order_id = o.id
      WHERE o.status = 'completed'
        AND o.created_at >= ${startDate}
        AND o.created_at < ${endDate}
      GROUP BY p.id
    `);
  }
}

const Q1Report = SalesReport.forPeriod('2024-01-01', '2024-04-01');
const results = await Q1Report.find([
  [sql`${SalesReport.total_revenue} > ?`, 10000],
]);
```

The `sql` tag always uses `?` placeholders internally. The dialect conversion (`?` → `$1`/`$2` for PostgreSQL) is handled transparently at execution time.

The `{ sql, params }` object form is still supported for backward compatibility.

---

## `QUERY` — Static Query-Based Models

Use the `sql` tag for `QUERY` to get Column and table name references that are refactoring-safe:

```typescript
@model('user_activity')
class UserActivityModel extends DBModel {
  @column() user_id?: number;
  @column() user_name?: string;
  @column() total_posts?: number;

  static QUERY = sql`
    SELECT 
      ${User.id} AS user_id,
      ${User.name} AS user_name,
      COUNT(${Post.id}) AS total_posts
    FROM ${User.TABLE_NAME}
    LEFT JOIN ${Post.TABLE_NAME} ON ${User.id} = ${Post.user_id}
    GROUP BY ${User.id}, ${User.name}
  `;
}
```

When only Column references and `TABLE_NAME` objects are interpolated (no runtime values), the `sql` tag embeds them directly in the SQL string with no parameters.

String `QUERY` is still supported for backward compatibility.

---

## `execute` — Raw SQL Execution

`DBModel.execute()` accepts `sql` tagged templates:

```typescript
await DBModel.execute(sql`SELECT process_daily_aggregates(${targetDate})`);
```

String + params form is still supported:

```typescript
await DBModel.execute('SELECT process_daily_aggregates($1)', [targetDate]);
```

---

## Interpolation Rules

The `sql` tag processes interpolated values according to their type:

| Interpolated Value | Processing | Example Output |
|---|---|---|
| `Column` | Embedded as column name | `age`, `name` |
| `{ TABLE_NAME }` | Embedded as table name | `users`, `posts` |
| `sql.raw(str)` | Embedded as raw SQL (no parameterization) | `ASC`, `DISTINCT` |
| `sql.ref(col)` | Embedded as `table.column` | `users.id` |
| `parentRef(col)` | Embedded as `table.column` | `users.id` |
| Nested `sql` fragment | SQL expanded, params merged | See below |
| Array | Expanded to `?, ?, ?` + params extracted | `?, ?, ?` |
| Primitive / Date | Replaced with `?` + param extracted | `?` |
| `null` / `undefined` | Embedded as `NULL` literal | `NULL` |

### `sql.raw()` — Raw SQL Embedding

Use for dynamic SQL keywords that should not be parameterized:

```typescript
const direction = sql.raw(isAsc ? 'ASC' : 'DESC');
const results = await Ranking.query(
  sql`SELECT * FROM rankings ORDER BY score ${direction} LIMIT ${limit}`
);
```

### `sql.ref()` — Table-Qualified Column References

Use for JOINs where table disambiguation is needed:

```typescript
sql`SELECT ${sql.ref(User.id)}, ${sql.ref(Post.title)}
    FROM users 
    JOIN posts ON ${sql.ref(User.id)} = ${sql.ref(Post.user_id)}`
```

| Interpolation | Output |
|---|---|
| `${User.id}` | `id` |
| `${sql.ref(User.id)}` | `users.id` |

### Nested `sql` Fragments

A `sql` fragment can be interpolated inside another. The inner SQL is expanded in place and parameters are merged:

```typescript
const sub = sql`SELECT user_id FROM orders WHERE status = ${'paid'}`;
// → { sql: 'SELECT user_id FROM orders WHERE status = ?', params: ['paid'] }

sql`SELECT * FROM users WHERE id IN (${sub}) AND age > ${18}`;
// → {
//     sql: 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = ?) AND age > ?',
//     params: ['paid', 18]
//   }
```

---

## Return Types

The `sql` tag returns different types based on the interpolated values, enabling type-safe usage in different contexts:

| Interpolation Pattern | Return Type | Usage |
|---|---|---|
| Single `Column` only | `SqlTypedFragment<V, M>` | Condition tuple first element, or value-free condition (IS NULL) |
| Single `Column` + value(s) | `SqlCondition<M>` | Direct condition element (Pattern B) |
| Multiple Columns / TABLE_NAMEs | `SqlFragment` | `QUERY`, `withQuery`, `execute` |
| Mixed (Columns + values) | `SqlFragment` | `withQuery`, `execute` |

- `SqlTypedFragment<V, M>` carries the Column's value type `V` and model type `M`, enabling type checking on the tuple's second element.
- `SqlCondition<M>` carries the model type `M` for model validation.
- `SqlFragment` is the general type for arbitrary SQL with parameters.

All three types have `sql: string` and `params: readonly unknown[]` properties.

---

## Backward Compatibility

The `sql` tag is a purely additive feature. All existing patterns continue to work:

| Existing Pattern | Status |
|---|---|
| `[Column, value]` tuples | Recommended for equality conditions |
| `[string, value]` tuples (template literals) | Supported (backward compatible) |
| `{ sql, params }` for `withQuery` | Supported (backward compatible) |
| `string` for `QUERY` | Supported (backward compatible) |
| `execute(sql, params)` | Supported (backward compatible) |
