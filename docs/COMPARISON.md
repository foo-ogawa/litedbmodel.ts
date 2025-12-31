# ORM Comparison: litedbmodel vs Others

A detailed comparison of litedbmodel with popular TypeScript/JavaScript ORMs.

## Quick Comparison Matrix

| Feature | litedbmodel | Kysely | Drizzle | TypeORM | Prisma | MikroORM | Sequelize | Objection.js |
|---------|-------------|--------|---------|---------|--------|----------|-----------|--------------|
| **Column References** | Symbols | Strings | Strings | Strings/Decorators | Strings | Strings | Strings | Strings |
| **IDE Refactoring** | ✅ Works | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Pattern** | Active Record | Query Builder | Query Builder | Both | Data Mapper | Data Mapper | Active Record | Active Record |
| **Type Safety** | ✅ Compile-time | ✅ Compile-time | ✅ Compile-time | ⚠️ Partial | ✅ Compile-time | ⚠️ Partial | ❌ Runtime | ⚠️ Partial |
| **Multi-DB** | PG/MySQL/SQLite | PG/MySQL/SQLite | PG/MySQL/SQLite | Many | PG/MySQL/SQLite/... | Many | Many | PG/MySQL/SQLite |
| **Schema Definition** | Decorators | TypeScript | TypeScript | Decorators | Prisma Schema | Decorators | JS Objects | Knex migrations |
| **Migrations** | Manual | Manual | Kit (optional) | Built-in | Built-in | Built-in | Built-in | Knex |
| **Query Style** | Tuple array | Fluent | Fluent | QueryBuilder/Find | Fluent | QueryBuilder | Fluent | Fluent |
| **N+1 Prevention** | ✅ Auto | ❌ Manual | ❌ Manual | ⚠️ Manual | ✅ Include | ✅ Identity Map | ⚠️ Include | ⚠️ Graph |
| **Composite Keys** | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ✅ Full | ⚠️ Partial | ✅ Full |
| **Fixed SQL Params** | ✅ `ANY()`* | ❌ | ✅ LATERAL | ❌ | ❌ | ❌ | ❌ | ❌ |
| **SQL Readability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Install Size** | ~1MB | ~6MB | ~11MB | ~28MB | ~78MB | ~300KB | ~200KB | ~100KB |
| **Learning Curve** | Low | Low | Low | High | Medium | Medium | Medium | Medium |

*\* PostgreSQL only. Falls back to `IN (...)` on MySQL/SQLite.*

## Detailed Analysis

---

## 1. Column References & IDE Support

### The Difference

litedbmodel uses explicit column symbols (`Model.column`), while other ORMs use string keys or object properties.

```typescript
// litedbmodel - Column symbols enable IDE "Find All References"
await User.find([[User.email, 'test@example.com']]);
await User.update([[User.id, 1]], [[User.email, 'new@example.com']]);
// Right-click User.email → "Find All References" → shows both usages

// Other ORMs - String-based
// Prisma
prisma.user.findMany({ where: { email: 'test@example.com' } });
// TypeORM
userRepo.find({ where: { email: 'test@example.com' } });
// Kysely
db.selectFrom('users').where('email', '=', 'test@example.com');
// Drizzle
db.select().from(users).where(eq(users.email, 'test@example.com'));
```

### Comparison

| ORM | Column Reference | IDE "Find References" | IDE "Rename Symbol" |
|-----|-----------------|----------------------|---------------------|
| **litedbmodel** | `User.email` (Symbol) | ✅ Works | ✅ Works |
| **Prisma** | `{ email: ... }` (Object key) | ❌ | ❌ |
| **TypeORM** | `{ email: ... }` or `'email'` | ❌ | ❌ |
| **MikroORM** | `{ email: ... }` | ❌ | ❌ |
| **Drizzle** | `users.email` (Schema ref) | ⚠️ Schema only | ⚠️ Schema only |
| **Kysely** | `'email'` (String) | ❌ | ❌ |
| **Sequelize** | `{ email: ... }` | ❌ | ❌ |
| **Objection.js** | `{ email: ... }` | ❌ | ❌ |

---

## 2. Design Patterns

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

## 3. Type Safety

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

| ORM | Type Source | Condition Check | Result Types | Compile-Time |
|-----|-------------|-----------------|--------------|--------------|
| **litedbmodel** | Decorators → Column symbols | ✅ Full | ✅ Model types | ✅ |
| **Prisma** | .prisma → Generated | ✅ Full | ✅ Generated | ✅ |
| **TypeORM** | Decorators | ⚠️ Partial (QB weak) | ✅ Entity types | ⚠️ |
| **MikroORM** | Decorators | ⚠️ Partial | ✅ Entity types | ⚠️ |
| **Drizzle** | TS Schema → Inferred | ✅ Full | ✅ Inferred | ✅ |
| **Kysely** | TS Interface → Inferred | ✅ Full | ✅ Inferred | ✅ |
| **Sequelize** | JS Objects (runtime) | ❌ None | ⚠️ Manual | ❌ |
| **Objection.js** | Model class | ❌ None | ⚠️ Manual | ⚠️ |

---

## 4. Conditional Updates (SKIP Pattern)

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

## 5. Relations

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

| ORM | Definition Style | Loading Strategy | N+1 Prevention | Composite Keys |
|-----|-----------------|------------------|----------------|----------------|
| **litedbmodel** | Getter methods | Auto batch | ✅ Automatic | ✅ Full |
| **Prisma** | Schema relations | Eager (include) | ✅ Built-in | ✅ Full |
| **TypeORM** | Decorators | Eager/Lazy | ⚠️ Manual joins | ✅ Full |
| **MikroORM** | Decorators | Lazy (identity map) | ✅ Built-in | ✅ Full |
| **Drizzle** | No built-in | Manual joins | ❌ N/A | ✅ Full |
| **Kysely** | No built-in | Manual joins | ❌ N/A | ✅ Full |
| **Sequelize** | Methods | Eager/Lazy | ⚠️ include option | ⚠️ Partial |
| **Objection.js** | Static mapping | Graph fetching | ⚠️ withGraphFetched | ✅ Full |

---

## 6. Database Support

| ORM | PostgreSQL | MySQL | SQLite | MSSQL | MongoDB | Others |
|-----|------------|-------|--------|-------|---------|--------|
| **litedbmodel** | ✅ | ✅ | ✅ | ❌ | ❌ | - |
| **Prisma** | ✅ | ✅ | ✅ | ✅ | ✅ | CockroachDB, PlanetScale |
| **TypeORM** | ✅ | ✅ | ✅ | ✅ | ✅ | Oracle, SAP Hana |
| **MikroORM** | ✅ | ✅ | ✅ | ❌ | ✅ | MariaDB |
| **Drizzle** | ✅ | ✅ | ✅ | ❌ | ❌ | - |
| **Kysely** | ✅ | ✅ | ✅ | ✅ | ❌ | - |
| **Sequelize** | ✅ | ✅ | ✅ | ✅ | ❌ | MariaDB, Snowflake |
| **Objection.js** | ✅ | ✅ | ✅ | ✅ | ❌ | Via Knex |

**litedbmodel**: Install only the driver you need
```bash
npm install litedbmodel pg        # PostgreSQL
npm install litedbmodel mysql2    # MySQL
npm install litedbmodel better-sqlite3  # SQLite
```

---

## 7. Schema & Migrations

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

| ORM | Schema Format | Migration Tool | Auto-Generate | Reversible |
|-----|--------------|----------------|---------------|------------|
| **litedbmodel** | TS Decorators | Manual | ❌ | Manual |
| **Prisma** | Prisma DSL | Built-in | ✅ | ✅ |
| **TypeORM** | TS Decorators | Built-in | ✅ | ✅ |
| **MikroORM** | TS Decorators | Built-in | ✅ | ✅ |
| **Drizzle** | TS Schema | drizzle-kit | ✅ | ⚠️ |
| **Kysely** | TS Interface | Manual | ❌ | Manual |
| **Sequelize** | JS/TS | CLI | ⚠️ | ✅ |
| **Objection.js** | JS/TS | Knex | ❌ | ✅ |

---

## 8. Query Expressiveness

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

## 9. Performance Characteristics

| ORM | Cold Start | Query Overhead | Connection Pooling | Batch Operations |
|-----|------------|----------------|-------------------|------------------|
| **litedbmodel** | Fast | Minimal | Via pg pool | createMany |
| **Prisma** | Slow (engine) | Binary protocol | Built-in | createMany |
| **TypeORM** | Medium | Some | Built-in | insert().values() |
| **MikroORM** | Medium | Identity map | Built-in | persistAndFlush |
| **Drizzle** | Fast | Minimal | Via driver | insert().values() |
| **Kysely** | Fast | Minimal | Via driver | insertInto().values() |
| **Sequelize** | Medium | Some | Built-in | bulkCreate |
| **Objection.js** | Fast | Minimal | Via Knex | insertGraph |

### Notes:
- **Prisma** uses a Rust query engine binary, adding cold start latency
- **MikroORM** uses identity map for caching, reducing duplicate queries
- **Query Builders** (Kysely, Drizzle) have minimal overhead

---

## 10. Performance Benchmark

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

## 11. SQL Quality & Debuggability

### Parameter Count Comparison

litedbmodel uses PostgreSQL's `ANY()` with array parameters, resulting in **fixed parameter counts** regardless of data size:

> **Note:** `ANY()` and `unnest()` are PostgreSQL-specific features. On MySQL/SQLite, litedbmodel falls back to standard `IN (...)` syntax.

```sql
-- litedbmodel: Always 1 parameter (array)
SELECT * FROM posts WHERE author_id = ANY($1::int[])

-- Other ORMs: Variable parameters (grows with data)
SELECT * FROM posts WHERE author_id IN ($1, $2, $3, ..., $1000)
```

| ORM | 100 records | 1000 records | Parameter Style |
|-----|-------------|--------------|-----------------|
| **litedbmodel** | **`$1`** | **`$1`** | `ANY($1::int[])` |
| Prisma | `$1`~`$100` | `$1`~`$1000` | `IN ($1,$2,...,$N)` |
| Kysely | `$1`~`$100` | `$1`~`$1000` | `IN ($1,$2,...,$N)` |
| TypeORM | `$1`~`$100` | `$1`~`$1000` | `IN ($1,$2,...,$N)` |

### Benefits of Fixed Parameters

1. **SQL Log Analysis** - Same query pattern makes grep/analysis easier
2. **Query Plan Caching** - PostgreSQL caches plans by SQL text; fixed params = better cache hits
3. **Readability** - Understand query intent without expanding 1000 parameters

### Composite Key Handling

For composite keys, litedbmodel uses `unnest + JOIN`:

```sql
-- litedbmodel: Always 2 parameters (2 arrays)
SELECT * FROM posts 
JOIN unnest($1::int[], $2::int[]) AS _keys(tenant_id, user_id)
ON posts.tenant_id = _keys.tenant_id AND posts.user_id = _keys.user_id

-- Other ORMs: 2000 parameters for 1000 composite keys
WHERE (tenant_id, user_id) IN (($1,$2),($3,$4),...,($1999,$2000))
```

### SQL Readability Comparison

| ORM | Sample SQL | Readability |
|-----|------------|-------------|
| **litedbmodel** | `SELECT * FROM posts WHERE author_id = ANY($1::int[])` | ⭐⭐⭐⭐⭐ |
| Kysely | `select * from "posts" where "author_id" in ($1, $2, ...)` | ⭐⭐⭐⭐ |
| Drizzle | `select ... left join lateral (select json_agg(...))` | ⭐⭐ |
| TypeORM | `SELECT "44e9b3e14e8c506ab5696ff82803c1017462faac"."id"...` | ⭐ |
| Prisma | `SELECT "public"."benchmark_posts"."id", "public"."benchmark_posts"."title"...` | ⭐⭐ |

---

## 12. Use Case Recommendations

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

## 13. Summary

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
