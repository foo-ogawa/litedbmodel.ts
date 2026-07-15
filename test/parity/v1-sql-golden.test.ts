/**
 * #64 — v1 ORM SQL parity golden capture harness.
 *
 * Captures the ACTUAL SQL that the litedbmodel v1 ORM path emits for every op in
 * the ORM-comparison bench (benchmark/benchmark.ts -> testCategories), across all
 * three dialects (sqlite in-proc / mysql :3307 / postgres :5433), by running the
 * real litedbmodel `fn` of each op against a real seeded database and capturing what
 * the driver receives via a SqlLoggerMiddleware hook (see
 * test/integration/LazyRelation.test.ts:38 for the middleware pattern).
 *
 * NO FABRICATION: every SQL string in the golden comes from an actual execution.
 * The litedbmodel models + each op's `fn` are copied verbatim from benchmark.ts so
 * the captured SQL is exactly what the bench exercises.
 *
 * The middleware hook sits ABOVE the driver's placeholder rewrite, so it captures
 * the portable `?`-style SQL that litedbmodel emits (the parity SSoT). Notes:
 *   - Postgres driver converts `?` -> `$N` INSIDE the driver (post-middleware); that
 *     is a mechanical driver detail, not part of the litedbmodel SQL contract.
 *   - MySQL driver strips `RETURNING` and issues an internal follow-up SELECT below
 *     the middleware to simulate RETURNING; that internal SELECT is a driver detail.
 *   - Transaction BEGIN/COMMIT are raw connection.query() below the middleware and
 *     are therefore NOT captured here (they are dialect-standard and identical).
 *
 * Run:  see benchmark/parity/README (regeneration) or the npm script `parity:capture`.
 */
import 'reflect-metadata';
import { describe, it } from 'vitest';
import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

import {
  DBModel,
  model,
  column,
  ColumnsOf,
  hasMany,
  belongsTo,
  closeAllPools,
  Middleware,
} from '../../src';
import type { DBConfig, ExecuteResult } from '../../src';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Golden files live under benchmark/parity/ (repo-root relative).
const OUT_DIR = path.resolve(__dirname, '..', '..', 'benchmark', 'parity');

// ============================================
// SQL Logger Middleware (driver-reach capture)
// Mirrors test/integration/LazyRelation.test.ts:38
// ============================================

interface QueryLogEntry {
  sql: string;
  params: unknown[];
}

class SqlLoggerMiddleware extends Middleware {
  private static queries: QueryLogEntry[] = [];

  async execute(
    next: (sql: string, params?: unknown[]) => Promise<ExecuteResult>,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    SqlLoggerMiddleware.queries.push({ sql, params: params ? [...params] : [] });
    return next(sql, params);
  }

  static reset(): void {
    SqlLoggerMiddleware.queries = [];
  }

  static drain(): QueryLogEntry[] {
    const out = SqlLoggerMiddleware.queries;
    SqlLoggerMiddleware.queries = [];
    return out;
  }
}

// ============================================
// litedbmodel v1 Models — copied verbatim from benchmark.ts (~57-135)
// ============================================

@model('benchmark_users')
class LiteUserModel extends DBModel {
  @column() id?: number;
  @column() email?: string;
  @column() name?: string;
  @column() created_at?: Date;
  @column() updated_at?: Date;

  @hasMany(() => [LiteUser.id, LitePost.author_id])
  declare posts: Promise<LitePostModel[]>;
}
const LiteUser = LiteUserModel as typeof LiteUserModel & ColumnsOf<LiteUserModel>;

@model('benchmark_posts')
class LitePostModel extends DBModel {
  @column() id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() published?: boolean;
  @column() author_id?: number;
  @column() created_at?: Date;

  @belongsTo(() => [LitePost.author_id, LiteUser.id])
  declare author: Promise<LiteUserModel | null>;

  @hasMany(() => [LitePost.id, LiteComment.post_id])
  declare comments: Promise<LiteCommentModel[]>;
}
const LitePost = LitePostModel as typeof LitePostModel & ColumnsOf<LitePostModel>;

@model('benchmark_comments')
class LiteCommentModel extends DBModel {
  @column() id?: number;
  @column() body?: string;
  @column() post_id?: number;
  @column() created_at?: Date;

  @belongsTo(() => [LiteComment.post_id, LitePost.id])
  declare post: Promise<LitePostModel | null>;
}
const LiteComment = LiteCommentModel as typeof LiteCommentModel & ColumnsOf<LiteCommentModel>;

// Composite key models (multi-tenant)
@model('benchmark_tenant_users')
class LiteTenantUserModel extends DBModel {
  @column({ primaryKey: true }) tenant_id?: number;
  @column({ primaryKey: true }) user_id?: number;
  @column() name?: string;

  @hasMany(() => [[LiteTenantUser.tenant_id, LiteTenantPost.tenant_id], [LiteTenantUser.user_id, LiteTenantPost.user_id]])
  declare posts: Promise<LiteTenantPostModel[]>;
}
const LiteTenantUser = LiteTenantUserModel as typeof LiteTenantUserModel & ColumnsOf<LiteTenantUserModel>;

@model('benchmark_tenant_posts')
class LiteTenantPostModel extends DBModel {
  @column({ primaryKey: true }) tenant_id?: number;
  @column({ primaryKey: true }) post_id?: number;
  @column() user_id?: number;
  @column() title?: string;

  @belongsTo(() => [[LiteTenantPost.tenant_id, LiteTenantUser.tenant_id], [LiteTenantPost.user_id, LiteTenantUser.user_id]])
  declare user: Promise<LiteTenantUserModel | null>;

  @hasMany(() => [[LiteTenantPost.tenant_id, LiteTenantComment.tenant_id], [LiteTenantPost.post_id, LiteTenantComment.post_id]])
  declare comments: Promise<LiteTenantCommentModel[]>;
}
const LiteTenantPost = LiteTenantPostModel as typeof LiteTenantPostModel & ColumnsOf<LiteTenantPostModel>;

@model('benchmark_tenant_comments')
class LiteTenantCommentModel extends DBModel {
  @column({ primaryKey: true }) tenant_id?: number;
  @column({ primaryKey: true }) comment_id?: number;
  @column() post_id?: number;
  @column() body?: string;

  @belongsTo(() => [[LiteTenantComment.tenant_id, LiteTenantPost.tenant_id], [LiteTenantComment.post_id, LiteTenantPost.post_id]])
  declare post: Promise<LiteTenantPostModel | null>;
}
const LiteTenantComment = LiteTenantCommentModel as typeof LiteTenantCommentModel & ColumnsOf<LiteTenantCommentModel>;

// ============================================
// Ops — the litedbmodel `fn` of every testCategories entry (verbatim behavior).
// Mutating counters mirror benchmark.ts. Each op is run ONCE per dialect.
// ============================================

let createCounter = 10000;
let upsertCounter = 20000;

interface OpDef {
  name: string;
  fn: () => Promise<unknown>;
}

function buildOps(): OpDef[] {
  return [
    {
      name: 'Find all (limit 100)',
      fn: () => LiteUser.find([], { limit: 100 }),
    },
    {
      name: 'Filter, paginate & sort',
      fn: () => LitePost.find([[LitePost.published, true]], {
        order: LitePost.created_at.desc(),
        limit: 20,
        offset: 10,
      }),
    },
    {
      name: 'Nested find all (include posts)',
      fn: async () => {
        const users = await LiteUser.find([], { limit: 100 });
        for (const user of users) {
          await user.posts;
        }
        return users;
      },
    },
    {
      name: 'Find first',
      fn: () => LiteUser.findOne([[`${LiteUser.name} LIKE ?`, 'User%']]),
    },
    {
      name: 'Nested find first (include posts)',
      fn: async () => {
        const user = await LiteUser.findOne([[`${LiteUser.name} LIKE ?`, 'User%']]);
        if (user) {
          await user.posts;
        }
        return user;
      },
    },
    {
      name: 'Find unique (by email)',
      fn: () => LiteUser.findOne([[LiteUser.email, 'user500@example.com']]),
    },
    {
      name: 'Nested find unique (include posts)',
      fn: async () => {
        const user = await LiteUser.findOne([[LiteUser.email, 'user500@example.com']]);
        if (user) {
          await user.posts;
        }
        return user;
      },
    },
    {
      name: 'Create',
      fn: () => LiteUser.transaction(async () => LiteUser.create([
        [LiteUser.email, `bench${createCounter++}@example.com`],
        [LiteUser.name, `Benchmark User`],
      ])),
    },
    {
      name: 'Nested create (with post)',
      fn: () => LiteUser.transaction(async () => {
        const result = await LiteUser.create([
          [LiteUser.email, `nested${createCounter++}@example.com`],
          [LiteUser.name, `Nested User`],
        ], { returning: true });
        await LitePost.create([
          [LitePost.title, 'Nested Post'],
          [LitePost.content, 'Content'],
          [LitePost.author_id, result!.values[0][0] as number],
        ]);
        return result;
      }),
    },
    {
      name: 'Update',
      fn: () => LiteUser.transaction(async () => LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Updated User']])),
    },
    {
      name: 'Nested update (update user + post)',
      fn: () => LiteUser.transaction(async () => {
        await LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Nested Updated']]);
        await LitePost.update([[LitePost.author_id, 100]], [[LitePost.title, 'Updated Post']]);
      }),
    },
    {
      name: 'Upsert',
      fn: () => LiteUser.transaction(async () => LiteUser.create(
        [
          [LiteUser.email, `upsert${upsertCounter++}@example.com`],
          [LiteUser.name, 'Upsert User'],
        ],
        { onConflict: LiteUser.email, onConflictUpdate: [LiteUser.name], returning: true }
      )),
    },
    {
      name: 'Nested upsert (user + post)',
      fn: () => LiteUser.transaction(async () => {
        const result = await LiteUser.create(
          [
            [LiteUser.email, `nupsert${upsertCounter++}@example.com`],
            [LiteUser.name, 'Nested Upsert'],
          ],
          { onConflict: LiteUser.email, onConflictUpdate: [LiteUser.name], returning: true }
        );
        await LitePost.create([
          [LitePost.title, 'Upsert Post'],
          [LitePost.author_id, result!.values[0][0] as number],
        ]);
        return result;
      }),
    },
    {
      name: 'Delete',
      fn: () => LiteUser.transaction(async () => {
        const result = await LiteUser.create([
          [LiteUser.email, `del${createCounter++}@example.com`],
          [LiteUser.name, 'Delete User'],
        ], { returning: true });
        return LiteUser.delete([[LiteUser.id, result!.values[0][0] as number]]);
      }),
    },
    {
      name: 'Create Many (10 records)',
      fn: () => LiteUser.transaction(async () => {
        const records = Array.from({ length: 10 }, (_, i) => [
          [LiteUser.email, `bulk${createCounter++}@example.com`],
          [LiteUser.name, `Bulk User ${i}`],
        ] as [[typeof LiteUser.email, string], [typeof LiteUser.name, string]]);
        return LiteUser.createMany(records);
      }),
    },
    {
      name: 'Upsert Many (10 records)',
      fn: () => LiteUser.transaction(async () => {
        const records = Array.from({ length: 10 }, (_, i) => [
          [LiteUser.email, `upsertbulk${upsertCounter++}@example.com`],
          [LiteUser.name, `Upsert Bulk ${i}`],
        ] as [[typeof LiteUser.email, string], [typeof LiteUser.name, string]]);
        return LiteUser.createMany(records, {
          onConflict: LiteUser.email,
          onConflictUpdate: [LiteUser.name],
        });
      }),
    },
    {
      name: 'Update Many (10 different values)',
      fn: () => LiteUser.transaction(async () => {
        return LiteUser.updateMany(
          Array.from({ length: 10 }, (_, i) => [
            [LiteUser.id, 100 + i],
            [LiteUser.name, `Updated Different ${i}`],
          ] as const),
          { keyColumns: [LiteUser.id] }
        );
      }),
    },
    {
      name: 'Nested relations (100->1000->10000)',
      fn: async () => {
        const users = await LiteUser.find([], { limit: 100, order: LiteUser.id.asc() });
        let commentCount = 0;
        for (const user of users) {
          const posts = await user.posts;
          for (const post of posts) {
            const comments = await post.comments;
            for (const _comment of comments) {
              commentCount++;
            }
          }
        }
        return users;
      },
    },
    {
      name: 'Nested relations (composite key, 5 tenants)',
      fn: async () => {
        const users = await LiteTenantUser.find(
          [[LiteTenantUser.tenant_id, [1, 2, 3, 4, 5]]],
          { limit: 100 }
        );
        let commentCount = 0;
        for (const user of users) {
          const posts = await user.posts;
          for (const post of posts) {
            const comments = await post.comments;
            for (const _comment of comments) {
              commentCount++;
            }
          }
        }
        return users;
      },
    },
  ];
}

// ============================================
// Per-dialect schema + seed (minimal, deterministic)
// ============================================

type Dialect = 'sqlite' | 'mysql' | 'postgres';

const SEED = {
  // Base users. `user500@example.com` and names `User <n>` are required by
  // Find unique / Find first. IDs 100-109 are required by Update / Update Many.
  users: 110, // ids 1..110 (covers id=100..109 for updateMany, plus a pool)
  extraUniqueUserId: 500, // ensures user500@example.com exists (find unique)
  postsPerUser: 2, // small: authors 1..110 each get 2 posts -> Nested find all has rows
  commentsPerPost: 2,
  tenants: 5,
  usersPerTenant: 4,
  postsPerTenantUser: 2,
  commentsPerTenantPost: 2,
};

function ddl(dialect: Dialect): string[] {
  if (dialect === 'sqlite') {
    return [
      `CREATE TABLE benchmark_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        name TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      )`,
      `CREATE TABLE benchmark_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT,
        published INTEGER DEFAULT 0,
        author_id INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
      )`,
      `CREATE TABLE benchmark_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        body TEXT NOT NULL,
        post_id INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
      )`,
      `CREATE TABLE benchmark_tenant_users (
        tenant_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (tenant_id, user_id)
      )`,
      `CREATE TABLE benchmark_tenant_posts (
        tenant_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        PRIMARY KEY (tenant_id, post_id)
      )`,
      `CREATE TABLE benchmark_tenant_comments (
        tenant_id INTEGER NOT NULL,
        comment_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        PRIMARY KEY (tenant_id, comment_id)
      )`,
    ];
  }
  if (dialect === 'mysql') {
    return [
      `CREATE TABLE benchmark_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE benchmark_posts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        published TINYINT(1) DEFAULT 0,
        author_id INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE benchmark_comments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        body TEXT NOT NULL,
        post_id INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE benchmark_tenant_users (
        tenant_id INT NOT NULL,
        user_id INT NOT NULL,
        name VARCHAR(255),
        PRIMARY KEY (tenant_id, user_id)
      )`,
      `CREATE TABLE benchmark_tenant_posts (
        tenant_id INT NOT NULL,
        post_id INT NOT NULL,
        user_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        PRIMARY KEY (tenant_id, post_id)
      )`,
      `CREATE TABLE benchmark_tenant_comments (
        tenant_id INT NOT NULL,
        comment_id INT NOT NULL,
        post_id INT NOT NULL,
        body TEXT NOT NULL,
        PRIMARY KEY (tenant_id, comment_id)
      )`,
    ];
  }
  // postgres
  return [
    `CREATE TABLE benchmark_users (
      id SERIAL PRIMARY KEY,
      email VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255),
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE benchmark_posts (
      id SERIAL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      content TEXT,
      published BOOLEAN DEFAULT false,
      author_id INTEGER,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE benchmark_comments (
      id SERIAL PRIMARY KEY,
      body TEXT NOT NULL,
      post_id INTEGER,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE benchmark_tenant_users (
      tenant_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      name VARCHAR(255),
      PRIMARY KEY (tenant_id, user_id)
    )`,
    `CREATE TABLE benchmark_tenant_posts (
      tenant_id INTEGER NOT NULL,
      post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      title VARCHAR(255) NOT NULL,
      PRIMARY KEY (tenant_id, post_id)
    )`,
    `CREATE TABLE benchmark_tenant_comments (
      tenant_id INTEGER NOT NULL,
      comment_id INTEGER NOT NULL,
      post_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      PRIMARY KEY (tenant_id, comment_id)
    )`,
  ];
}

const DROP_TABLES = [
  'benchmark_tenant_comments',
  'benchmark_tenant_posts',
  'benchmark_tenant_users',
  'benchmark_comments',
  'benchmark_posts',
  'benchmark_users',
];

async function dropAll(dialect: Dialect): Promise<void> {
  for (const t of DROP_TABLES) {
    if (dialect === 'mysql') {
      await DBModel.execute(`DROP TABLE IF EXISTS ${t}`);
    } else if (dialect === 'postgres') {
      await DBModel.execute(`DROP TABLE IF EXISTS ${t} CASCADE`);
    } else {
      await DBModel.execute(`DROP TABLE IF EXISTS ${t}`);
    }
  }
}

async function createSchema(dialect: Dialect): Promise<void> {
  for (const stmt of ddl(dialect)) {
    await DBModel.execute(stmt);
  }
}

/** Seed via parameterised inserts so it is deterministic across dialects. */
async function seed(dialect: Dialect): Promise<void> {
  const bool = (b: boolean) => (dialect === 'postgres' ? b : b ? 1 : 0);

  // Users 1..N. Force id explicitly so id=100..109 are guaranteed present.
  for (let id = 1; id <= SEED.users; id++) {
    await DBModel.execute(
      'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)',
      [id, `user${id}@example.com`, `User ${id}`]
    );
  }
  // Ensure the find-unique target exists at a fixed id (500).
  await DBModel.execute(
    'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)',
    [SEED.extraUniqueUserId, `user${SEED.extraUniqueUserId}@example.com`, `User ${SEED.extraUniqueUserId}`]
  );

  // Posts: authors 1..N, `postsPerUser` each. Fixed ids for reproducibility.
  let postId = 1;
  for (let uid = 1; uid <= SEED.users; uid++) {
    for (let p = 0; p < SEED.postsPerUser; p++) {
      const published = postId % 3 === 0;
      await DBModel.execute(
        'INSERT INTO benchmark_posts (id, title, content, published, author_id) VALUES (?, ?, ?, ?, ?)',
        [postId, `Post ${postId}`, `Content ${postId}`, bool(published), uid]
      );
      postId++;
    }
  }
  const maxPostId = postId - 1;

  // Comments: `commentsPerPost` per post.
  let commentId = 1;
  for (let pid = 1; pid <= maxPostId; pid++) {
    for (let c = 0; c < SEED.commentsPerPost; c++) {
      await DBModel.execute(
        'INSERT INTO benchmark_comments (id, body, post_id) VALUES (?, ?, ?)',
        [commentId, `Comment ${commentId} for post ${pid}`, pid]
      );
      commentId++;
    }
  }

  // Composite-key tenant data (tenants 1..5). post_id/comment_id repeat per tenant.
  for (let t = 1; t <= SEED.tenants; t++) {
    for (let u = 1; u <= SEED.usersPerTenant; u++) {
      await DBModel.execute(
        'INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES (?, ?, ?)',
        [t, u, `Tenant${t} User${u}`]
      );
    }
    let localPostId = 1;
    for (let u = 1; u <= SEED.usersPerTenant; u++) {
      for (let p = 0; p < SEED.postsPerTenantUser; p++) {
        await DBModel.execute(
          'INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES (?, ?, ?, ?)',
          [t, localPostId, u, `T${t}Post ${localPostId}`]
        );
        localPostId++;
      }
    }
    const tenantPosts = localPostId - 1;
    let localCommentId = 1;
    for (let lp = 1; lp <= tenantPosts; lp++) {
      for (let c = 0; c < SEED.commentsPerTenantPost; c++) {
        await DBModel.execute(
          'INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES (?, ?, ?, ?)',
          [t, localCommentId, lp, `T${t}Comment ${localCommentId}`]
        );
        localCommentId++;
      }
    }
  }

  // We seeded explicit ids, so the auto-increment counter must be advanced past
  // the seeded max, otherwise the first INSERT-without-id (Create) collides on PK.
  if (dialect === 'postgres') {
    // SERIAL sequences track their own counter independent of inserted values.
    await DBModel.execute(
      `SELECT setval(pg_get_serial_sequence('benchmark_users', 'id'), (SELECT MAX(id) FROM benchmark_users))`
    );
    await DBModel.execute(
      `SELECT setval(pg_get_serial_sequence('benchmark_posts', 'id'), (SELECT MAX(id) FROM benchmark_posts))`
    );
    await DBModel.execute(
      `SELECT setval(pg_get_serial_sequence('benchmark_comments', 'id'), (SELECT MAX(id) FROM benchmark_comments))`
    );
  }
  // sqlite AUTOINCREMENT and mysql AUTO_INCREMENT both derive the next id from the
  // current MAX(id), so no fixup is needed there.
}

// ============================================
// Capture driver
// ============================================

function dialectConfig(dialect: Dialect): DBConfig {
  if (dialect === 'sqlite') {
    return { database: ':memory:', driver: 'sqlite' } as DBConfig;
  }
  if (dialect === 'mysql') {
    return {
      host: process.env.MYSQL_HOST || 'localhost',
      port: parseInt(process.env.MYSQL_PORT || '3307'),
      database: process.env.MYSQL_DB || 'testdb',
      user: process.env.MYSQL_USER || 'testuser',
      password: process.env.MYSQL_PASSWORD || 'testpass',
      driver: 'mysql',
    };
  }
  return {
    host: process.env.TEST_DB_HOST || 'localhost',
    port: parseInt(process.env.TEST_DB_PORT || '5433'),
    database: process.env.TEST_DB_NAME || 'testdb',
    user: process.env.TEST_DB_USER || 'testuser',
    password: process.env.TEST_DB_PASSWORD || 'testpass',
    driver: 'postgres',
  };
}

type CapturedOp =
  | { statements: QueryLogEntry[] }
  | { error: string };

async function captureDialect(dialect: Dialect): Promise<Record<string, CapturedOp>> {
  DBModel.setConfig(dialectConfig(dialect));

  // Fresh schema + seed. sqlite :memory: is per-connection; keep the pool alive
  // for the whole dialect run.
  await dropAll(dialect);
  await createSchema(dialect);
  await seed(dialect);

  const unregister = DBModel.use(SqlLoggerMiddleware);

  // Reset the mutating counters so ids in captured params are stable per dialect.
  createCounter = 10000;
  upsertCounter = 20000;

  const ops = buildOps();
  const out: Record<string, CapturedOp> = {};

  for (const op of ops) {
    SqlLoggerMiddleware.reset();
    try {
      await op.fn();
      out[op.name] = { statements: SqlLoggerMiddleware.drain() };
    } catch (err) {
      // Drain whatever fired before the error too, but record the failure clearly.
      const partial = SqlLoggerMiddleware.drain();
      out[op.name] = {
        error: `${(err as Error).message}` + (partial.length ? ` | partial(${partial.length}): ${JSON.stringify(partial)}` : ''),
      };
    }
  }

  unregister();
  DBModel.clearMiddlewares();
  await closeAllPools();
  return out;
}

async function main(): Promise<void> {
  const dialects: Dialect[] = ['sqlite', 'mysql', 'postgres'];
  const perDialect: Record<Dialect, Record<string, CapturedOp>> = {} as any;

  for (const d of dialects) {
    process.stderr.write(`\n=== Capturing dialect: ${d} ===\n`);
    perDialect[d] = await captureDialect(d);
    const errs = Object.entries(perDialect[d]).filter(([, v]) => 'error' in v);
    for (const [name, v] of errs) {
      process.stderr.write(`  [ERROR] ${name}: ${(v as { error: string }).error}\n`);
    }
  }

  // Re-key: op -> dialect -> [...]
  const opNames = buildOps().map((o) => o.name);
  const golden: Record<string, Record<string, CapturedOp>> = {};
  for (const name of opNames) {
    golden[name] = {};
    for (const d of dialects) {
      golden[name][d] = perDialect[d][name];
    }
  }

  const doc = {
    $schema: 'litedbmodel v1 ORM-path SQL parity golden (#64)',
    generatedBy: 'benchmark/parity/capture-v1-sql.ts',
    note:
      'Every SQL string was captured live via SqlLoggerMiddleware.execute (driver-reach hook) ' +
      'while running each op once against a real seeded DB. `?` placeholders are the litedbmodel ' +
      'portable form; the postgres driver rewrites `?`->`$N` internally (post-middleware). ' +
      'MySQL strips RETURNING and issues an internal follow-up SELECT below the middleware. ' +
      'Transaction BEGIN/COMMIT run as raw connection.query() below the middleware and are not captured.',
    seed: SEED,
    dialects,
    ops: golden,
  };

  const outDir = OUT_DIR;
  await fs.mkdir(outDir, { recursive: true });
  const jsonPath = path.join(outDir, 'v1-sql.golden.json');
  await fs.writeFile(jsonPath, JSON.stringify(doc, null, 2) + '\n');
  process.stderr.write(`\nWrote ${jsonPath}\n`);

  // Human-readable markdown
  const md: string[] = [];
  md.push('# litedbmodel v1 ORM-path SQL parity golden (#64)');
  md.push('');
  md.push('Captured live via `SqlLoggerMiddleware.execute` (driver-reach hook) by running each');
  md.push('bench op once against a real seeded DB, for all three dialects. No SQL is hand-written.');
  md.push('');
  md.push('- `?` placeholders are the litedbmodel portable form. The postgres driver rewrites');
  md.push('  `?`->`$N` **inside** the driver (post-middleware), so it is not shown here.');
  md.push('- MySQL strips `RETURNING` and issues an internal follow-up `SELECT` below the');
  md.push('  middleware to simulate it; that internal SELECT is a driver detail, not captured.');
  md.push('- Transaction `BEGIN`/`COMMIT` are raw `connection.query()` below the middleware; not captured.');
  md.push('');
  md.push('Regenerate: `npm run parity:capture` (see `benchmark/parity/README.md`).');
  md.push('');
  md.push('## Statement counts (op x dialect)');
  md.push('');
  md.push('| Op | sqlite | mysql | postgres |');
  md.push('| --- | ---: | ---: | ---: |');
  for (const name of opNames) {
    const cell = (d: Dialect) => {
      const v = golden[name][d];
      return 'error' in v ? 'ERR' : String(v.statements.length);
    };
    md.push(`| ${name} | ${cell('sqlite')} | ${cell('mysql')} | ${cell('postgres')} |`);
  }
  md.push('');
  md.push('## Full captured SQL');
  md.push('');
  for (const name of opNames) {
    md.push(`### ${name}`);
    md.push('');
    for (const d of dialects) {
      const v = golden[name][d];
      md.push(`**${d}**`);
      md.push('');
      if ('error' in v) {
        md.push('```');
        md.push(`ERROR: ${v.error}`);
        md.push('```');
        md.push('');
        continue;
      }
      if (v.statements.length === 0) {
        md.push('_(no statements captured)_');
        md.push('');
        continue;
      }
      md.push('```sql');
      v.statements.forEach((s, i) => {
        md.push(`-- [${i + 1}] params: ${JSON.stringify(s.params)}`);
        md.push(s.sql.trim());
      });
      md.push('```');
      md.push('');
    }
  }
  const mdPath = path.join(outDir, 'v1-sql.golden.md');
  await fs.writeFile(mdPath, md.join('\n') + '\n');
  process.stderr.write(`Wrote ${mdPath}\n`);
}

// Run as a vitest test so it inherits the working experimentalDecorators transform
// and the built-in DB driver resolution. Generous timeout: it seeds three DBs.
describe('#64 v1 ORM SQL parity golden capture', () => {
  it('captures every bench op x 3 dialects into benchmark/parity/', async () => {
    await main();
  }, 300_000);
});
