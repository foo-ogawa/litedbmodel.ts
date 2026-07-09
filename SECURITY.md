# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in litedbmodel, please report it through one of these channels:

- **GitHub Security Advisories** (preferred): [Report a vulnerability](https://github.com/foo-ogawa/litedbmodel/security/advisories/new)
- **Email**: security@foo-ogawa.dev

Please do **not** open a public GitHub issue for security vulnerabilities.

We will acknowledge receipt within 48 hours and aim to provide a fix within 7 days for critical issues.

## Trust Boundaries

litedbmodel has clear boundaries between safe (parameterized) and unsafe (raw) SQL construction.

### Safe by default: `sql` tagged template

Values interpolated into the `sql` tagged template are automatically parameterized. User input passed through this path is safe from SQL injection.

```typescript
// SAFE: parameterized — userId becomes a bind parameter ($1 / ?)
const users = await User.find([sql`${User.id} = ${userId}`]);
```

### Unsafe escape hatches

The following APIs embed values directly into SQL strings **without parameterization**. They exist for legitimate use cases (dynamic SQL keywords, trusted expressions) but must never receive user input.

| API | Purpose | Risk if misused |
|-----|---------|-----------------|
| `sql.raw(value)` | Dynamic SQL keywords (ASC/DESC, DISTINCT) | SQL injection |
| `dbRaw(expr)` | Raw SQL expressions in conditions | SQL injection |
| `dbImmediate(value)` | Inline literal values | SQL injection |
| `dbDynamic(fragment)` | Dynamic SQL fragments | SQL injection |
| `execute(sqlString)` | Direct SQL execution (string form) | SQL injection |

### Trusted code

The following are considered trusted and are not parameterized by design:

- **Model definitions** — table names, column names, decorator arguments
- **`@model()` table name** — embedded directly in generated SQL
- **Column references** (`User.id`, `Post.title`) — resolved to column name strings at decoration time
- **Relation definitions** (`@hasMany`, `@belongsTo`) — column pair references

These are part of application source code and are not influenced by runtime input.

## Unsafe vs Safe Patterns

```typescript
// ✅ SAFE: sql tagged template parameterizes values
await User.find([sql`${User.name} LIKE ${`%${searchTerm}%`}`]);

// ✅ SAFE: tuple conditions parameterize the value
await User.find([[User.email, userEmail]]);

// ✅ SAFE: sql.raw with hardcoded trusted value
const direction = sql.raw(sortOrder === 'asc' ? 'ASC' : 'DESC');

// ❌ UNSAFE: sql.raw with user input
const direction = sql.raw(req.query.sort); // SQL INJECTION

// ❌ UNSAFE: string concatenation in execute
await DBModel.execute(`SELECT * FROM users WHERE name = '${userName}'`); // SQL INJECTION

// ✅ SAFE: use sql tag with execute
await DBModel.execute(sql`SELECT * FROM users WHERE name = ${userName}`);
```

## Recommendations

1. **Use the `sql` tagged template** for all dynamic values — it handles parameterization across all supported databases.
2. **Restrict `sql.raw()`** to hardcoded or validated-enum values only (e.g., sort direction, column aliases from a known set).
3. **Never pass request parameters** (`req.query`, `req.body`, `req.params`, `req.headers`) to any raw/immediate/dynamic API.
4. **Treat LLM output as untrusted** — AI-generated values should go through parameterized queries, not raw SQL escape hatches.
5. **Review `execute()` calls** — when using the string overload, ensure all values are parameterized or hardcoded.
