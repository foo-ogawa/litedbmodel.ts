# ORM Comparison: litedbmodel vs Others

A detailed comparison of litedbmodel with popular TypeScript/JavaScript ORMs.

## Quick Comparison Matrix

> Legend: ✅ built-in / automatic, ⚠️ possible but manual / opt-in / partial, ❌ not provided / not typical

| Category | Feature | litedbmodel | Kysely | Drizzle | TypeORM | Prisma | MikroORM | Sequelize | Objection.js |
|----------|---------|-------------|--------|---------|---------|--------|----------|-----------|--------------|
| **Modeling** | Schema definition | Decorators | TS types | TS schema objects | Decorators | Prisma schema | Decorators | JS objects | Knex + Model |
|  | Migrations | Manual | Manual | Kit (optional) | Built-in | Built-in | Built-in | Built-in | Knex |
| **Query API** | Primary query style | Tuple array | Fluent QB | Fluent QB | QB / Find | Fluent client | QB / EM | Fluent | Fluent |
|  | Raw SQL escape hatch† | ✅ `query()` | ⚠️ `sql` | ⚠️ `sql` | ⚠️ `query()` | ⚠️ `$queryRaw` | ⚠️ `execute()` | ⚠️ `query()` | ⚠️ `raw()` |
| **Type Safety** | Compile-time type safety | ✅ | ✅ | ✅ | ⚠️ partial | ✅ | ⚠️ partial | ❌ runtime-heavy | ⚠️ partial |
| **Column Refs**‡ | How columns are referenced | **Symbols** | String literals | Column objects | Strings/decorators | Object keys | Strings | Strings | Strings |
| **IDE Support**§ | Refactoring safety (IDE) | ✅ Full | ❌ | ⚠️ Partial | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Updates** | Declarative partial update | ✅ **SKIP** | ❌ manual | ❌ manual | ❌ manual | ⚠️ `undefined` | ❌ manual | ❌ manual | ❌ manual |
| **Relations** | Relation definition (built-in) | ✅ getters | ❌ | ❌ | ✅ decorators | ✅ schema | ✅ decorators | ✅ associations | ✅ mappings |
|  | Default loading style | Lazy + **auto batch** | Manual joins | Manual joins | Eager/Lazy | Explicit include | UoW + populate | Include/eager | Graph fetch |
|  | N+1 prevention behavior∥ | ✅ Auto | ❌ manual | ❌ manual | ⚠️ manual | ⚠️ if included | ⚠️ identity map | ⚠️ if included | ⚠️ if graphed |
|  | Composite key relations | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ⚠️ Partial | ✅ Full |
| **SQL Quality** | Stable SQL fingerprints (PG)¶ | ✅ `ANY`/`unnest` | ⚠️ possible | ⚠️ possible | ⚠️ possible | ⚠️ possible | ⚠️ possible | ⚠️ possible | ⚠️ possible |
|  | Standard API: 1-query nested# | ❌ (by design) | ⚠️ manual SQL | ⚠️ manual SQL | ⚠️ possible | ⚠️ depends | ⚠️ possible | ⚠️ possible | ⚠️ possible |
|  | SQL readability (typical) | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Extension** | Cross-cutting hooks | ✅ middleware | ⚠️ plugins | ⚠️ manual wrappers | ⚠️ subscribers | ⚠️ extensions | ⚠️ hooks | ⚠️ hooks | ⚠️ plugins |
| **DB Support** | Supported DBs | PG/MySQL/SQLite | PG/MySQL/SQLite | PG/MySQL/SQLite | Many | Many | Many | Many | PG/MySQL/SQLite |
|  | DB switching cost (portability)** | ✅ config-only | ⚠️ dialect | ❌ schema rewrite | ⚠️ type/driver | ❌ regenerate | ⚠️ driver | ⚠️ dialect | ⚠️ knex |

**† Raw SQL escape hatch:**  
litedbmodel `query()` returns typed **model instances** (with methods, relations, middleware context). Others return typed records but not model instances.

**‡ Column references:**  
- **Symbols** = unique identifier per column (best for "find references")  
- **Column objects** = schema object properties (IDE can track usage, but DB renames are separate)  
- **String literals / object keys** = plain strings/keys (IDE cannot reliably distinguish DB usage)

**§ IDE refactoring:**  
- **Full** = IDE tracks all column usages via symbol references across the codebase  
- **Partial** = IDE tracks *some* references (e.g., schema objects), but DB column renaming requires separate migration handling

**∥ N+1 prevention:**  
- **Auto** = batching happens transparently when accessing relations over a result set  
- **If included/graphed** = requires explicit include/graph configuration to avoid N+1  
- **Identity map** = reduces duplicate loads within a request, but does not automatically batch relation queries

**¶ Stable SQL fingerprints (PG):**  
Built-in patterns that keep SQL text/param shapes stable (e.g., `ANY($1::int[])`, `unnest` joins). Other tools may achieve similar shapes via driver/SQL patterns, but it is not typically a built-in default.

**# Standard API: 1-query nested:**  
Whether the *tool's standard relation API* fetches nested relations in a single DB round-trip. Raw SQL can achieve this in any tool; this row evaluates the default/idiomatic approach only.

**\*\* DB portability:**  
- **Multi-DB** = databases the tool can talk to  
- **Portability** = code changes required when switching databases (schema + queries + build/runtime coupling)

---

## Why Use an ORM? (Beyond Raw SQL)

**The real value of an ORM is maintainability, safety, and observability while simplifying 80% of common tasks.**

```typescript
// 80% of operations: Simple, type-safe, trackable via column symbols
const users = await User.find([[User.is_active, true]]);

// 20% of operations: DB-specific optimization in raw SQL
const stats = await User.query(`
  SELECT department, COUNT(*) FROM users
  GROUP BY GROUPING SETS ((department), ())  -- PostgreSQL-specific
`, []);
```

| Value | Raw SQL | Other ORMs | litedbmodel | Details |
|-------|---------|------------|-------------|---------|
| **80% Simplification** — Schema, CRUD, Queries | ❌ Verbose | ✅ Prisma, TypeORM, Drizzle, Kysely | ✅ + SKIP pattern | [§1](#1-schema--migrations), [§2](#2-query-expressiveness), [§3](#3-declarative-updates-skip-pattern) |
| **SQL Escape Hatch** — Complex queries in raw SQL | ✅ Native | ⚠️ Prisma `$queryRaw`, Drizzle `sql` | ✅ `query()` | [§4](#4-sql-friendly-design) |
| **Type Safety** — Catch errors at compile time | ❌ Strings | ✅ Prisma, Drizzle, Kysely | ✅ Yes | [§5](#5-type-safety) |
| **Refactoring Safety** — IDE tracks column usage | ❌ No | ⚠️ Drizzle (partial) | ✅ Column symbols | [§6](#6-column-references--ide-support) |
| **Model-Centric** — Active Record + lazy relations | ❌ N/A | ⚠️ TypeORM, Sequelize (partial) | ✅ Auto N+1 prevention | [§7](#7-design-patterns), [§8](#8-relations) |
| **Observability** — Logging, SQL quality, policies | ❌ Manual | ⚠️ Prisma extensions, Kysely plugins | ✅ Middleware | [§9](#9-extensibility), [§10](#10-sql-quality--debuggability) |

> See [Detailed Analysis](#detailed-analysis) for in-depth comparisons of each feature.

---

## Detailed Analysis

---

## 1. Schema & Migrations

### litedbmodel

```typescript
// Schema: TypeScript decorators
@model('users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() created_at?: Date;  // Auto-inferred from Date type
}

// Migrations: Manual SQL files
```

### Prisma

```prisma
// Schema: .prisma file (DSL)
model User {
  id        Int      @id @default(autoincrement())
  name      String
  createdAt DateTime @default(now())
}

// Migrations: `prisma migrate dev` generates SQL
```

### TypeORM

```typescript
// Schema: Decorators
@Entity()
class User {
  @PrimaryGeneratedColumn() id: number;
  @Column() name: string;
  @CreateDateColumn() createdAt: Date;
}

// Migrations: Auto-generated or manual
typeorm migration:generate -n CreateUser
```

### Drizzle

```typescript
// Schema: TypeScript
export const users = pgTable('users', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 256 }),
  createdAt: timestamp('created_at').defaultNow(),
});

// Migrations: drizzle-kit (optional)
drizzle-kit generate:pg
```

### Migration Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Schema Format** | TS Decorators | Prisma DSL | TS Decorators | TS Schema | TS Interface |
| **Migration Tool** | Manual | Built-in | Built-in | drizzle-kit | Manual |
| **Auto-Generate** | ❌ | ✅ | ✅ | ✅ | ❌ |
| **Reversible** | Manual | ✅ | ✅ | ⚠️ | Manual |

---

## 2. Query Expressiveness

### Complex Query Example

Find active users who either:
- Have admin role, OR
- Have moderator role AND level >= 5

```typescript
// litedbmodel
const users = await User.find([
  [User.is_active, true],
  User.or(
    [[User.role, 'admin']],
    [[User.role, 'moderator'], [`${User.level} >= ?`, 5]],
  ),
]);

// Prisma
const users = await prisma.user.findMany({
  where: {
    isActive: true,
    OR: [
      { role: 'admin' },
      { role: 'moderator', level: { gte: 5 } },
    ],
  },
});

// TypeORM (QueryBuilder)
const users = await userRepo.createQueryBuilder('u')
  .where('u.is_active = :active', { active: true })
  .andWhere(new Brackets(qb => {
    qb.where('u.role = :admin', { admin: 'admin' })
      .orWhere('u.role = :mod AND u.level >= :level', { mod: 'moderator', level: 5 });
  }))
  .getMany();

// Kysely
const users = await db.selectFrom('users')
  .where('is_active', '=', true)
  .where(eb => eb.or([
    eb('role', '=', 'admin'),
    eb.and([
      eb('role', '=', 'moderator'),
      eb('level', '>=', 5)
    ])
  ]))
  .selectAll()
  .execute();

// Drizzle
const users = await db.select().from(usersTable)
  .where(and(
    eq(usersTable.isActive, true),
    or(
      eq(usersTable.role, 'admin'),
      and(eq(usersTable.role, 'moderator'), gte(usersTable.level, 5))
    )
  ));
```

---

## 3. Declarative Updates (SKIP Pattern)

litedbmodel's unique feature for declarative conditional fields:

### litedbmodel - SKIP Sentinel

```typescript
import { SKIP } from 'litedbmodel';

// All fields visible, SKIP omits undefined
await User.update(
  [[User.id, id]],
  [
    [User.name, body.name ?? SKIP],
    [User.email, body.email ?? SKIP],
    [User.phone, body.phone ?? SKIP],
    [User.updated_at, new Date()],
  ]
);
```

### Other ORMs - Manual Building

```typescript
// Prisma - spread undefined values
await prisma.user.update({
  where: { id },
  data: {
    name: body.name,      // undefined = no update (Prisma specific)
    email: body.email,
    phone: body.phone,
    updatedAt: new Date(),
  }
});

// TypeORM - must filter manually
const updateData: Partial<User> = { updatedAt: new Date() };
if (body.name !== undefined) updateData.name = body.name;
if (body.email !== undefined) updateData.email = body.email;
await userRepo.update(id, updateData);

// Kysely - manual object building
const updates: Updateable<UsersTable> = {};
if (body.name !== undefined) updates.name = body.name;
if (body.email !== undefined) updates.email = body.email;
await db.updateTable('users').set(updates).where('id', '=', id).execute();
```

### Why SKIP Matters

| Approach | Readability | All Fields Visible | Mutations | Type-Safe |
|----------|-------------|-------------------|-----------|-----------|
| **SKIP sentinel** | ✅ Declarative | ✅ Yes | ❌ None | ✅ |
| **Prisma undefined** | ✅ Clean | ✅ Yes | ❌ None | ✅ |
| **Manual if/spread** | ❌ Imperative | ❌ Scattered | ✅ Mutable | ⚠️ |

---

## 4. SQL-Friendly Design

### The Problem with Query Builders

Most ORMs hide SQL behind proprietary DSLs or query builders:

| Approach | Problem |
|----------|---------|
| **Query Builder** | Generated SQL is unpredictable; debugging requires mental translation |
| **DSL/HQL** | Learning another language; still need SQL for complex queries |
| **Magic Methods** | `findByNameAndStatusOrderByCreatedAtDesc()` — unreadable, limited |

### litedbmodel's Approach

litedbmodel is **SQL-friendly**: it doesn't hide SQL, but wraps it safely with type information.

```typescript
// Other ORMs: Complex query builder syntax
const users = await queryBuilder
  .select('u')
  .from(User, 'u')
  .innerJoin('u.orders', 'o')
  .where('o.createdAt >= :date', { date: lastMonth })
  .groupBy('u.id')
  .having('COUNT(o.id) >= :min', { min: 5 })
  .orderBy('COUNT(o.id)', 'DESC')
  .getMany();  // What SQL does this generate? 🤔

// litedbmodel: Just write the SQL you want
const users = await User.query(`
  SELECT u.* FROM users u
  INNER JOIN (
    SELECT user_id FROM orders
    WHERE created_at >= $1
    GROUP BY user_id HAVING COUNT(*) >= $2
  ) active ON u.id = active.user_id
  ORDER BY ...
`, [lastMonth, minOrders]);  // Exactly what you wrote ✅
```

### Query Approaches Comparison

| Scenario | litedbmodel | Query Builder ORMs |
|----------|-------------|-------------------|
| **Simple CRUD** | Tuple API: `[[User.name, 'John']]` | Object: `{ name: 'John' }` |
| **Complex WHERE** | SQL operators: `` [`${User.age} > ?`, 18] `` | DSL methods: `.where('age', '>', 18)` |
| **JOINs, CTEs** | Real SQL via `query()` or Query-Based Models | Limited DSL or raw escape |
| **Debugging** | See actual SQL | Translate DSL → SQL mentally |
| **Learning** | Know SQL = Ready | Learn DSL + SQL |

### Three Layers of SQL Access

| Layer | Method | Use Case |
|-------|--------|----------|
| **1. Tuple API** | `Model.find()`, `create()`, `update()` | Simple CRUD, conditions |
| **2. Raw SQL** | `Model.query()`, `DBModel.execute()` | One-off complex queries |
| **3. Query-Based Models** | `static QUERY` + `find()` | Reusable complex queries (aggregations, analytics) |

---

## 5. Type Safety

### litedbmodel - Compile-time (Column Symbols)

Type derived from decorators, checked via Column symbol references.

```typescript
// Compile-time validation via Column symbols
await User.create([
  [User.name, 'John'],       // ✅ string
  [User.is_active, true],    // ✅ boolean
  // [User.name, 123],       // ❌ Compile error (type mismatch)
]);

// Conditions also type-checked
await User.find([
  [User.id, 1],              // ✅ number
  [`${User.age} > ?`, 18],   // ✅ operator with value
]);
```

### Prisma - Compile-time (Generated)

Types auto-generated from `.prisma` schema file.

```typescript
// Types generated from .prisma schema
const user = await prisma.user.create({
  data: {
    name: 'John',       // ✅ Typed
    isActive: true,     // ✅ Typed
    // age: 'wrong',    // ❌ Compile error
  }
});

// Select/include typed
const userWithPosts = await prisma.user.findFirst({
  include: { posts: true }  // ✅ Relation typed
});
```

### TypeORM - Partial Compile-time (Decorators)

Types from decorators, but QueryBuilder uses string parameters (untyped).

```typescript
// Types from decorators, but QueryBuilder loses type safety
@Entity()
class User {
  @Column() name: string;
  @Column() isActive: boolean;
}

// Query builder loses some type safety
const users = await userRepo
  .createQueryBuilder('user')
  .where('user.name = :name', { name: 'John' })  // Parameters untyped
  .getMany();
```

### Drizzle - Compile-time (Schema Inferred)

Types inferred from TypeScript schema definition.

```typescript
// Types inferred from TS schema
const users = pgTable('users', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 256 }),
  isActive: boolean('is_active'),
});

// Fully typed queries
const result = await db
  .select({ name: users.name })
  .from(users)
  .where(eq(users.isActive, true));
// result: { name: string }[]  ✅
```

### Kysely - Compile-time (Interface Inferred)

Types inferred from TypeScript interface definition.

```typescript
// Types from Database interface
interface Database {
  users: {
    id: number;
    name: string;
    is_active: boolean;
  };
}

// Type-safe queries
const users = await db
  .selectFrom('users')
  .select(['name', 'is_active'])
  .where('is_active', '=', true)
  .execute();
// users: { name: string; is_active: boolean }[]  ✅
```

### Type Safety Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Type Source** | Decorators → Symbols | .prisma → Generated | Decorators | TS Schema | TS Interface |
| **Condition Check** | ✅ Full | ✅ Full | ⚠️ Partial | ✅ Full | ✅ Full |
| **Result Types** | ✅ Model types | ✅ Generated | ✅ Entity types | ✅ Inferred | ✅ Inferred |
| **Compile-Time** | ✅ | ✅ | ⚠️ | ✅ | ✅ |

---

## 6. Column References & IDE Support

### Column Reference Types

| Type | Example | IDE Tracking | ORMs |
|------|---------|--------------|------|
| **Column Symbols** | `User.email` | ✅ Full codebase | litedbmodel |
| **Column Objects** | `users.email` | ⚠️ Partial | Drizzle |
| **Object Keys** | `{ email: ... }` | ❌ | Prisma, TypeORM |
| **String Literals** | `'email'` | ❌ | Kysely |

### What "IDE Refactoring" Means

- **✅ Full**: IDE tracks all usages of a column across the entire codebase via symbol references. Renaming `User.email` updates all call sites.
- **⚠️ Partial**: Property references (e.g., `users.email`) are tracked by IDE, but DB column renaming requires separate migration handling. The schema object and its usages are connected, but the actual DB column name is a separate concern.
- **❌ None**: Column names are strings or object keys; IDE cannot distinguish them from other strings/keys.

```typescript
// litedbmodel - Column symbols enable IDE "Find All References"
await User.find([[User.email, 'test@example.com']]);
await User.update([[User.id, 1]], [[User.email, 'new@example.com']]);
// Right-click User.email → "Find All References" → shows both usages

// Drizzle - Column objects (property references are trackable)
db.select().from(users).where(eq(users.email, 'test@example.com'));
// users.email is a property reference; IDE can track usages, but DB column name is separate

// Kysely - String literals (no tracking)
db.selectFrom('users').where('email', '=', 'test@example.com');
// 'email' is just a string; IDE cannot track

// Prisma - Object keys (no tracking)
prisma.user.findMany({ where: { email: 'test@example.com' } });
// { email: ... } is an object key; IDE cannot track
```

### Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Column Reference** | `User.email` (Symbol) | `{ email: ... }` | `{ email: ... }` | `users.email` (Object) | `'email'` (String) |
| **IDE "Find References"** | ✅ Full | ❌ | ❌ | ⚠️ Partial | ❌ |
| **IDE "Rename Symbol"** | ✅ Full | ❌ | ❌ | ⚠️ Partial | ❌ |

---

## 7. Design Patterns

### Active Record (litedbmodel, Sequelize, Objection.js)

Model instances have methods to persist themselves:

```typescript
// litedbmodel
const user = await User.create([[User.name, 'John']]);
const users = await User.find([[User.is_active, true]]);

// Sequelize
const user = await User.create({ name: 'John' });
const users = await User.findAll({ where: { is_active: true } });

// Objection.js
const user = await User.query().insert({ name: 'John' });
const users = await User.query().where('is_active', true);
```

**Pros**: Simple, intuitive, less boilerplate  
**Cons**: Tight coupling between domain and persistence

### Data Mapper (Prisma, MikroORM)

Separate repository/client handles persistence:

```typescript
// Prisma
const user = await prisma.user.create({ data: { name: 'John' } });
const users = await prisma.user.findMany({ where: { isActive: true } });

// MikroORM
const user = em.create(User, { name: 'John' });
await em.persistAndFlush(user);
const users = await em.find(User, { isActive: true });
```

**Pros**: Clean separation, testable, flexible  
**Cons**: More boilerplate, learning curve

### Query Builder (Kysely, Drizzle)

SQL-like fluent API:

```typescript
// Kysely
const users = await db
  .selectFrom('users')
  .where('is_active', '=', true)
  .selectAll()
  .execute();

// Drizzle
const users = await db
  .select()
  .from(users)
  .where(eq(users.isActive, true));
```

**Pros**: Full SQL control, composable  
**Cons**: Verbose, no model abstraction

### Hybrid (TypeORM)

Supports both patterns:

```typescript
// Active Record
const user = new User();
user.name = 'John';
await user.save();

// Data Mapper (Repository)
const user = userRepository.create({ name: 'John' });
await userRepository.save(user);
```

---

## 8. Relations

### litedbmodel - Auto Batch Loading

```typescript
@model('posts')
class PostModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;

  get author(): Promise<User | null> {
    return this._belongsTo(User, {
      targetKey: User.id,
      sourceKey: Post.user_id,
    });
  }

  get comments(): Promise<Comment[]> {
    return this._hasMany(Comment, {
      targetKey: Comment.post_id,
      order: Comment.created_at.desc(),
    });
  }
}

// Auto batch loading - prevents N+1 automatically
const posts = await Post.find([]);    // Returns multiple posts
for (const post of posts) {
  const author = await post.author;   // First access batch loads ALL authors
}
// Total: 2 queries (posts + authors) instead of N+1!

// Composite key relations also supported
get author(): Promise<User | null> {
  return this._belongsTo(User, {
    targetKeys: [User.tenant_id, User.id],
    sourceKeys: [Post.tenant_id, Post.user_id],
  });
}
```

### Prisma - Include/Select

```typescript
// Eager loading with include
const post = await prisma.post.findUnique({
  where: { id: 1 },
  include: {
    author: true,
    comments: { orderBy: { createdAt: 'desc' } }
  }
});

// Fluent API
const author = await prisma.post.findUnique({ where: { id: 1 } }).author();
```

### TypeORM - Decorators + Relations

```typescript
@Entity()
class Post {
  @ManyToOne(() => User, user => user.posts)
  author: User;

  @OneToMany(() => Comment, comment => comment.post)
  comments: Comment[];
}

// Eager/Lazy loading
const post = await postRepo.findOne({
  where: { id: 1 },
  relations: ['author', 'comments']
});
```

### Objection.js - Relation Mappings

```typescript
class Post extends Model {
  static relationMappings = {
    author: {
      relation: Model.BelongsToOneRelation,
      modelClass: User,
      join: { from: 'posts.user_id', to: 'users.id' }
    }
  };
}

// Graph fetching
const post = await Post.query()
  .findById(1)
  .withGraphFetched('author');
```

### Relation Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Definition Style** | Getter methods | Schema relations | Decorators | No built-in | No built-in |
| **Loading Strategy** | Auto batch | Eager (include) | Eager/Lazy | Manual joins | Manual joins |
| **N+1 Prevention** | ✅ Auto | ⚠️ If included | ⚠️ Manual | ❌ N/A | ❌ N/A |
| **Composite Keys** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full |

*Note: "Auto" = batch loading happens transparently when accessing relations. "If included" = N+1 prevented only when `include` is specified; forgetting it causes N+1.*

---

## 9. Extensibility

How each ORM allows developers to add cross-cutting concerns (logging, authentication, tenant isolation, soft deletes, etc.).

### litedbmodel — Class-Based Middleware

```typescript
import { Middleware, NextExecute, ExecuteResult, NextFind } from 'litedbmodel';

class AuditMiddleware extends Middleware {
  logs: string[] = [];

  // Intercept all SQL executions
  async execute(next: NextExecute, sql: string, params?: unknown[]): Promise<ExecuteResult> {
    const start = Date.now();
    const result = await next(sql, params);
    this.logs.push(`${sql} (${Date.now() - start}ms)`);
    return result;
  }

  // Intercept find() calls per model
  async find<T extends typeof DBModel>(model: T, next: NextFind<T>, conditions: Conds) {
    // Add tenant isolation automatically
    const tenantCol = (model as any).tenant_id;
    if (tenantCol) {
      conditions = [[tenantCol, this.tenantId], ...conditions];
    }
    return next(conditions);
  }
}

DBModel.use(AuditMiddleware);

// Per-request context access
const ctx = AuditMiddleware.getCurrentContext();
ctx.tenantId = req.user.tenantId;
```

**Features:**
- Class-based with instance state (per-request context)
- Intercept `execute()`, `find()`, `create()`, `update()`, `delete()`
- AsyncLocalStorage for request-scoped data
- Type-safe with full model access

### Prisma — Client Extensions (v4.16+)

```typescript
const prisma = new PrismaClient().$extends({
  query: {
    $allModels: {
      async findMany({ model, operation, args, query }) {
        console.log(`Query: ${model}.${operation}`);
        return query(args);
      },
    },
    user: {
      async create({ args, query }) {
        args.data.createdAt = new Date();
        return query(args);
      },
    },
  },
  model: {
    user: {
      async softDelete(id: number) {
        return prisma.user.update({ where: { id }, data: { deletedAt: new Date() } });
      },
    },
  },
});
```

**Features:**
- Query extensions (intercept operations)
- Model extensions (add custom methods)
- Result extensions (transform output)
- Client extensions (add properties)
- Replaces deprecated `$use()` middleware

### TypeORM — Entity Subscribers

```typescript
@EventSubscriber()
class AuditSubscriber implements EntitySubscriberInterface<User> {
  listenTo() {
    return User;
  }

  beforeInsert(event: InsertEvent<User>) {
    event.entity.createdAt = new Date();
  }

  afterLoad(entity: User) {
    console.log(`Loaded user: ${entity.id}`);
  }

  beforeUpdate(event: UpdateEvent<User>) {
    event.entity.updatedAt = new Date();
  }
}

// Register in data source
const dataSource = new DataSource({
  subscribers: [AuditSubscriber],
  // ...
});
```

**Features:**
- Entity lifecycle events (beforeInsert, afterLoad, etc.)
- Per-entity or global subscribers
- Decorator-based (`@BeforeInsert()`, `@AfterLoad()`)
- Transaction-aware

### Kysely — Plugins

```typescript
import { KyselyPlugin, PluginTransformQueryArgs, PluginTransformResultArgs } from 'kysely';

class LoggingPlugin implements KyselyPlugin {
  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    console.log('Query:', args.node);
    return args.node;
  }

  async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<unknown>> {
    console.log('Result rows:', args.result.rows.length);
    return args.result;
  }
}

const db = new Kysely<Database>({
  dialect: new PostgresDialect({ pool }),
  plugins: [new LoggingPlugin()],
});
```

**Features:**
- Query transformation (AST manipulation)
- Result transformation
- Composable plugin chain
- No per-model hooks (query-level only)

### Drizzle — No Built-in Extension System

```typescript
// Manual wrapper approach
async function auditedQuery<T>(queryFn: () => Promise<T>, context: string): Promise<T> {
  const start = Date.now();
  const result = await queryFn();
  console.log(`${context} took ${Date.now() - start}ms`);
  return result;
}

// Must wrap each call manually
const users = await auditedQuery(
  () => db.select().from(usersTable).where(eq(usersTable.isActive, true)),
  'findActiveUsers'
);
```

**Features:**
- No built-in middleware/plugin system
- Requires manual wrapper functions
- Full control but more boilerplate
- Relies on external logging libraries

### Sequelize — Hooks

```typescript
User.addHook('beforeCreate', (user, options) => {
  user.createdAt = new Date();
});

User.addHook('afterFind', (users, options) => {
  console.log(`Found ${users.length} users`);
});

// Or via model definition
@Table
class User extends Model {
  @BeforeCreate
  static addTimestamp(instance: User) {
    instance.createdAt = new Date();
  }
}
```

**Features:**
- Lifecycle hooks (beforeCreate, afterFind, etc.)
- Global and per-model hooks
- Decorator support (@BeforeCreate)
- Options passed through hooks

### Extensibility Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Mechanism** | Class Middleware | Client Extensions | Subscribers | ⚠️ Manual wrappers | Plugins |
| **Scope** | All operations | Query/Model/Result | Entity lifecycle | Manual wrappers | Query/Result |
| **Per-Request State** | ✅ AsyncLocalStorage | ⚠️ Manual | ⚠️ Via DI | ⚠️ Manual | ❌ None |
| **Type-Safe** | ✅ Full | ✅ Generated | ✅ Decorators | ⚠️ Manual | ⚠️ AST level |

### Use Cases

| Use Case | litedbmodel | Prisma | TypeORM | Kysely |
|----------|-------------|--------|---------|--------|
| **Logging** | execute() middleware | Query extension | Global subscriber | Plugin |
| **Tenant Isolation** | find() middleware | Query extension | Repository pattern | Manual |
| **Soft Deletes** | find()/delete() middleware | Model extension | Subscriber | Manual |
| **Audit Trail** | create()/update() middleware | Query extension | BeforeInsert/Update | Plugin |
| **Auth Context** | AsyncLocalStorage | Manual passing | DI container | Manual |

---

## 10. SQL Quality & Debuggability

### Parameter Count Comparison

litedbmodel uses PostgreSQL's `ANY()` with array parameters, resulting in **fixed parameter counts** regardless of data size:

> **Note:** `ANY()` and `unnest()` are PostgreSQL-specific features. On MySQL/SQLite, litedbmodel falls back to standard `IN (...)` syntax.

```sql
-- litedbmodel: Always 1 parameter (array)
SELECT * FROM posts WHERE author_id = ANY($1::int[])

-- Other ORMs: Variable parameters (grows with data)
SELECT * FROM posts WHERE author_id IN ($1, $2, $3, ..., $1000)
```

| Feature | litedbmodel | Prisma | TypeORM | Kysely |
|---------|-------------|--------|---------|--------|
| **100 records** | **`$1`** | `$1`~`$100`* | `$1`~`$100`* | `$1`~`$100`* |
| **1000 records** | **`$1`** | `$1`~`$1000`* | `$1`~`$1000`* | `$1`~`$1000`* |
| **Parameter Style** | `ANY($1::int[])` | `IN ($1,...,$N)` | `IN ($1,...,$N)` | `IN ($1,...,$N)` |

*\* Typical behavior. Some ORMs may use array parameters in certain configurations or drivers.*

### Benefits of Fixed Parameters

1. **SQL Log Analysis** - Same query pattern makes grep/analysis easier
2. **Stable SQL Fingerprints** - Consistent SQL text makes monitoring, profiling, and prepared statements more predictable
3. **Readability** - Understand query intent without expanding 1000 parameters

### Composite Key Handling

For composite keys, litedbmodel uses `unnest + JOIN`:

```sql
-- litedbmodel: Always 2 parameters (2 arrays)
SELECT * FROM posts 
JOIN unnest($1::int[], $2::int[]) AS _keys(tenant_id, user_id)
ON posts.tenant_id = _keys.tenant_id AND posts.user_id = _keys.user_id

-- Other ORMs (typical): Variable parameters for composite keys
WHERE (tenant_id, user_id) IN (($1,$2),($3,$4),...,($1999,$2000))
```

### SQL Readability Comparison

| Feature | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|---------|-------------|--------|---------|---------|--------|
| **Readability** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| **Sample SQL** | `...WHERE id = ANY($1)` | `"public"."table"."col"...` | Hash aliases | LATERAL JOIN | Quoted identifiers |

---

## 11. Database Support

| Database | litedbmodel | Prisma | TypeORM | Drizzle | Kysely |
|----------|-------------|--------|---------|---------|--------|
| **PostgreSQL** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **MySQL** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **SQLite** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **MSSQL** | ❌ | ✅ | ✅ | ❌ | ✅ |
| **MongoDB** | ❌ | ✅ | ✅ | ❌ | ❌ |

**litedbmodel**: Install only the driver you need
```bash
npm install litedbmodel pg        # PostgreSQL
npm install litedbmodel mysql2    # MySQL
npm install litedbmodel better-sqlite3  # SQLite
```

### DB Portability (When It Matters)

**Note:** Most teams never switch databases in production. DB portability is valuable primarily in these scenarios:
- Dev/Test uses SQLite, Production uses PostgreSQL
- Multi-tenant SaaS with customer-specific DB requirements
- Migration from legacy database planned

| What Changes | litedbmodel | Kysely | Drizzle | TypeORM | Prisma |
|--------------|-------------|--------|---------|---------|--------|
| **Config** | 1 file | 1 file | 1 file | 1 file | 1 file |
| **Imports** | ✅ None | 1 file (dialect) | ❌ All schemas | ✅ None | ✅ None |
| **Schema** | ✅ None | ✅ None | ❌ All schemas | ✅ None | Regenerate |
| **Build step** | ✅ None | ✅ None | ✅ None | ✅ None | ❌ Required |

Drizzle requires different packages per DB (`pgTable` vs `mysqlTable` vs `sqliteTable`), meaning all schema files must be rewritten when switching databases. This is a design choice prioritizing DB-specific type safety over portability.

---

## 12. Other Characteristics

| Feature | litedbmodel | Kysely | Drizzle | TypeORM | Prisma | MikroORM | Sequelize | Objection.js |
|---------|-------------|--------|---------|---------|--------|----------|-----------|--------------|
| **Install Size** | ~1MB | ~6MB | ~11MB | ~28MB | ~78MB | ~300KB | ~200KB | ~100KB |
| **Learning Curve** | Low | Low | Low | High | Medium | Medium | Medium | Medium |
| **Cold Start** | Fast | Fast | Fast | Medium | Slow (engine) | Fast | Medium | Fast |
| **Query Overhead** | Minimal | Minimal | Minimal | Some | Binary protocol | Some | Some | Minimal |

### ORM-Specific Notes

- **Prisma** — Uses a Rust query engine binary, adding cold start latency (~1s on serverless)
- **MikroORM** — Uses identity map for caching, reducing duplicate queries within a request
- **Query Builders** (Kysely, Drizzle) — Minimal abstraction overhead, closest to raw SQL performance

### Nested Relation Loading Strategies

Different ORMs use different approaches for loading nested relations. See [Nested Benchmark](./BENCHMARK-NESTED.md) for detailed SQL analysis.

**Drizzle — LATERAL JOIN (Single Query)**

Uses PostgreSQL's `LATERAL JOIN` with `json_agg()` to fetch all nested data in one query:
- ✅ 1 round-trip (fastest for network latency)
- ❌ Complex SQL (difficult to debug), heavy DB-side JSON processing
- ❌ Query must be pre-defined with all relations upfront

**litedbmodel — Transparent Lazy Loading (Batch Queries)**

Relations are statically defined but dynamically loaded only when accessed:
- ✅ Same model works for list/detail views (no query duplication)
- ✅ Only fetches what's accessed (efficient for list views)
- ✅ Automatic N+1 prevention via batch loading
- ❌ Multiple round-trips (2-3 queries for nested relations)

```typescript
// One model, multiple use cases
const users = await User.find([], { limit: 100 });

// List view: No additional queries
for (const user of users) console.log(user.name);

// Detail view: Batch loads all posts in ONE query
for (const user of users) {
  const posts = await user.posts;  // Not N+1!
}
```

---

## 13. Performance Benchmark

Benchmark comparing litedbmodel with Prisma, Kysely, Drizzle, and TypeORM on PostgreSQL.

Based on [Prisma's official orm-benchmarks](https://github.com/prisma/orm-benchmarks) methodology.  
Reference: [Kysely performance comparison article](https://izanami.dev/post/1e3fa298-252c-4f6e-8bcc-b225d53c95fb)

**Test Environment:**
- PostgreSQL 16 (Docker, local - no network latency)
- Node.js v24
- **10 rounds × 100 iterations = 1,000 total per ORM**
- Interleaved execution to reduce environmental variance
- 1,000 users, 5,000 posts seed data
- Metrics: **Median** (primary), IQR, StdDev

### Visual Comparison

![ORM Benchmark Chart](../docs/benchmark-chart.svg)

### Results Table (Median of 1,000 iterations)

| Operation | litedbmodel | Kysely | Drizzle | TypeORM | Prisma |
|-----------|-------------|--------|---------|---------|--------|
| Find all (limit 100) | 0.62ms | 0.62ms | **0.57ms** | 0.71ms | 1.36ms |
| Filter, paginate & sort | **0.68ms** 🏆 | 0.76ms | 0.88ms | 0.95ms | 1.11ms |
| Nested find all | **2.31ms** 🏆 | 2.69ms | 3.51ms | 5.00ms | 7.39ms |
| Find first | 0.34ms | 0.31ms | **0.29ms** | 0.31ms | 0.61ms |
| Nested find first | 0.57ms | 0.56ms | 0.59ms | **0.50ms** | 0.97ms |
| Find unique (by email) | **0.28ms** 🏆 | **0.28ms** | 0.30ms | 0.32ms | 0.54ms |
| Nested find unique | 0.58ms | **0.57ms** | 0.61ms | 0.98ms | 0.94ms |
| Create | **0.40ms** 🏆 | 0.41ms | 0.42ms | 0.90ms | 0.60ms |
| Nested create | **0.80ms** 🏆 | 0.84ms | 0.89ms | 2.09ms | 1.67ms |
| Update | **0.44ms** 🏆 | 0.45ms | 0.47ms | 0.48ms | 0.71ms |
| Nested update | 1.01ms | **0.91ms** | 1.04ms | 1.09ms | 2.40ms |
| Upsert | 0.50ms | **0.46ms** | 0.49ms | 0.58ms | 2.01ms |
| Nested upsert | 0.99ms | **0.97ms** | 1.06ms | **0.97ms** | 2.27ms |
| Delete | **0.96ms** 🏆 | 1.00ms | 1.13ms | 1.85ms | 1.35ms |

### Deep Nested Relations (10,000 records)

Separate benchmark for large-scale nested relation queries (100 users → 1000 posts → 10000 comments).  
**5 rounds × 20 iterations = 100 total per ORM**

![Deep Nested Benchmark Chart](./benchmark-nested-chart.svg)

| Operation | litedbmodel | Kysely | Drizzle | TypeORM | Prisma |
|-----------|-------------|--------|---------|---------|--------|
| Single Key (10K) | 28.05ms | 28.84ms | **23.23ms** 🏆 | 32.50ms | 81.73ms |
| Composite Key (10K) | 24.63ms | **13.19ms** 🏆 | 17.84ms | 35.52ms | 101.94ms |

> See [Nested Benchmark](./BENCHMARK-NESTED.md) for detailed SQL analysis.

### Analysis

1. **litedbmodel** - **Fastest in 7 operations!** 🏆
   - **#1 in Filter/paginate/sort, Nested find all, Find unique, Create, Nested create, Update, Delete**
   - Excellent auto N+1 prevention (Nested find all: 3.2x faster than Prisma)
   - Competitive in all other operations (within 15% of fastest)
   - Best for applications with complex queries and relations

2. **Kysely** - Fastest in upsert and nested update operations
   - Best for Nested update, Upsert, Nested upsert, Nested find unique
   - Minimal abstraction overhead
   - Best for write-heavy apps needing raw SQL control

3. **Drizzle** - Strong all-around performance
   - Fastest in Find all, Find first
   - Consistent performance across operations
   - Good balance of features and speed

4. **TypeORM** - Variable performance
   - Fastest in Nested find first (JOIN-based approach)
   - **Slow Create** (~2.3x slower than fastest)
   - Higher overhead for nested operations

5. **Prisma** - Convenience over speed
   - **Slowest in most operations** (1.4x - 4.4x slower)
   - Nested find all: 7.39ms vs litedbmodel's 2.31ms (3.2x slower)
   - Trade-off: Rich DX features (Prisma Studio, migrations, etc.)

### Deep Nested Analysis

For large-scale nested queries (10K+ records), see [Nested Benchmark](./BENCHMARK-NESTED.md):

- **Single Key:** Drizzle wins with LATERAL JOIN (23ms), litedbmodel close (28ms), Prisma slowest (82ms)
- **Composite Key:** Kysely wins (13ms), litedbmodel good (25ms), Prisma slowest (102ms)
- **litedbmodel advantage:** Readable SQL with fixed parameter count (ideal for log analysis)


### Conclusion

**litedbmodel excels at:**
- **Nested/relation queries** (auto N+1 prevention)
- **Complex filtering with pagination**
- **CRUD operations** (Create, Update, Delete)
- **Unique lookups**

**vs Prisma:** litedbmodel is **1.5x - 3.2x faster** across all operations  
**vs Query Builders:** litedbmodel is competitive (within 15%), with better DX

> **litedbmodel provides best-in-class performance while offering:**
> - Type-safe column symbols (IDE refactoring)
> - SKIP declarative pattern for partial updates
> - Automatic N+1 prevention without explicit includes
> - Middleware support for cross-cutting concerns
> - Active Record pattern simplicity

For detailed SQL analysis with 10,000+ records, see [Nested Benchmark](./BENCHMARK-NESTED.md).

---

## 14. Use Case Recommendations

### Choose litedbmodel when:

- ✅ **IDE refactoring support** matters (large codebase, frequent schema changes)
- ✅ **PostgreSQL, MySQL, or SQLite** project
- ✅ **Active Record pattern** preference
- ✅ **Declarative conditional fields** (SKIP pattern)
- ✅ **Minimal bundle size** requirements
- ✅ **Cross-cutting concerns** (middleware for logging, auth, metrics)
- ✅ **Auto N+1 prevention** without explicit includes/joins
- ✅ **Composite key relations** (multi-tenant systems)
- ✅ **SQL log analysis** (fixed param count = consistent patterns)
- ✅ **Readable SQL output** for debugging and performance tuning

### Choose Prisma when:

- ✅ **Rapid prototyping** with auto-generated CRUD
- ✅ **Multi-database** support needed
- ✅ **Built-in migrations** preferred
- ✅ **Serverless deployments** (Data Proxy)
- ✅ **Team familiarity** with Prisma

### Choose TypeORM when:

- ✅ **Flexible patterns** (Active Record + Data Mapper)
- ✅ **Complex enterprise** applications
- ✅ **Multi-database** with feature parity
- ✅ **Decorator-based** schema definition

### Choose MikroORM when:

- ✅ **Unit of Work pattern** (batch changes, identity map)
- ✅ **Clean architecture** with Data Mapper
- ✅ **MongoDB support** with same API

### Choose Drizzle when:

- ✅ **SQL-like API** preference
- ✅ **Edge runtime** compatibility (small bundle)
- ✅ **Full type inference** without code generation
- ✅ **Schema as code** approach

### Choose Kysely when:

- ✅ **Raw SQL control** with type safety
- ✅ **Minimal abstraction** over SQL
- ✅ **Composable query building**
- ✅ **No schema file** needed

### Choose Sequelize when:

- ✅ **Legacy JavaScript** projects
- ✅ **Migration from** other JS ORMs
- ✅ **Mature ecosystem** requirements

### Choose Objection.js when:

- ✅ **Knex.js** already in use
- ✅ **Graph operations** (insertGraph, fetchGraph)
- ✅ **JSON schema** validation

---

## 15. Summary

### litedbmodel's Characteristics

1. **Symbol-Based Columns**: IDE "Find References" and "Rename Symbol" work
2. **SKIP Sentinel**: Declarative conditional fields without if-statements
3. **Minimal Footprint**: 60KB minified, no binary dependencies
4. **Middleware**: Cross-cutting concern support (logging, auth, metrics)
5. **Auto N+1 Prevention**: Batch loading enabled automatically for multiple records
6. **Composite Key Support**: Full support for composite keys in relations
7. **Fixed SQL Parameters**: `ANY()` and `unnest()` keep parameter count constant
8. **Readable SQL**: Simple, debuggable queries (no hash aliases or deep nesting)

### Trade-offs

| litedbmodel Pros | litedbmodel Cons |
|------------------|------------------|
| IDE refactoring works | No MSSQL/Oracle support |
| SKIP declarative pattern | Manual migrations |
| Small bundle size | Smaller community |
| Middleware support | Less documentation |
| Auto N+1 prevention | |
| Composite key relations | |
| Fixed SQL params (log analysis) | |
| Readable SQL output | |

### When NOT to use litedbmodel

- ❌ Need MSSQL, Oracle, or other databases
- ❌ Want auto-generated migrations
- ❌ Prefer Query Builder pattern
- ❌ Need large community/ecosystem

---

## Appendix: Code Examples

### A. Basic CRUD Comparison

<details>
<summary>Create</summary>

```typescript
// litedbmodel
await User.create([
  [User.name, 'John'],
  [User.email, 'john@test.com'],
]);

// Prisma
await prisma.user.create({
  data: { name: 'John', email: 'john@test.com' }
});

// TypeORM
await userRepo.save({ name: 'John', email: 'john@test.com' });

// Drizzle
await db.insert(users).values({ name: 'John', email: 'john@test.com' });

// Kysely
await db.insertInto('users').values({ name: 'John', email: 'john@test.com' }).execute();
```

</details>

<details>
<summary>Read</summary>

```typescript
// litedbmodel
await User.find([[User.is_active, true]]);

// Prisma
await prisma.user.findMany({ where: { isActive: true } });

// TypeORM
await userRepo.find({ where: { isActive: true } });

// Drizzle
await db.select().from(users).where(eq(users.isActive, true));

// Kysely
await db.selectFrom('users').where('is_active', '=', true).selectAll().execute();
```

</details>

<details>
<summary>Update</summary>

```typescript
// litedbmodel
await User.update([[User.id, 1]], [[User.name, 'Jane']]);

// Prisma
await prisma.user.update({ where: { id: 1 }, data: { name: 'Jane' } });

// TypeORM
await userRepo.update(1, { name: 'Jane' });

// Drizzle
await db.update(users).set({ name: 'Jane' }).where(eq(users.id, 1));

// Kysely
await db.updateTable('users').set({ name: 'Jane' }).where('id', '=', 1).execute();
```

</details>

<details>
<summary>Delete</summary>

```typescript
// litedbmodel
await User.delete([[User.is_active, false]]);

// Prisma
await prisma.user.deleteMany({ where: { isActive: false } });

// TypeORM
await userRepo.delete({ isActive: false });

// Drizzle
await db.delete(users).where(eq(users.isActive, false));

// Kysely
await db.deleteFrom('users').where('is_active', '=', false).execute();
```

</details>

### B. Transaction Comparison

<details>
<summary>Transactions</summary>

```typescript
// litedbmodel
await DBModel.transaction(async () => {
  const user = await User.create([[User.name, 'Alice']]);
  await Profile.create([[Profile.user_id, user.id]]);
});

// Prisma
await prisma.$transaction(async (tx) => {
  const user = await tx.user.create({ data: { name: 'Alice' } });
  await tx.profile.create({ data: { userId: user.id } });
});

// TypeORM
await dataSource.transaction(async (manager) => {
  const user = await manager.save(User, { name: 'Alice' });
  await manager.save(Profile, { userId: user.id });
});

// Kysely
await db.transaction().execute(async (trx) => {
  const user = await trx.insertInto('users').values({ name: 'Alice' }).returningAll().executeTakeFirst();
  await trx.insertInto('profiles').values({ user_id: user.id }).execute();
});
```

</details>

---

*Last updated: December 2025*  
*Benchmark methodology: Based on [Prisma orm-benchmarks](https://github.com/prisma/orm-benchmarks)*
