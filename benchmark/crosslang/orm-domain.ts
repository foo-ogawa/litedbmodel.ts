// ════════════════════════════════════════════════════════════════════════════
// Unified ORM-bench DOMAIN (epic #63) — schema + seed for the 19-op cross-lang bench.
// ════════════════════════════════════════════════════════════════════════════
//
// The SAME tables + seed the #64 v1 SQL golden captured against (test/parity/
// v1-sql-golden.test.ts): benchmark_users / benchmark_posts / benchmark_comments +
// the composite-key tenant_* tables. Every language's live leg creates these in an
// ISOLATED per-bench namespace (PG schema `scp_ts_bench` via search_path, MySQL
// database `scp_ts_bench`, sqlite fresh :memory:) so the bench never collides with the
// conformance/integration fixtures.
//
// The seed is deterministic parameterised INSERTs (identical across dialects), matching
// the golden's SEED so the 19 ops read exactly the rows the ORM-bench litedbmodel column
// reads (rows/op parity → TS cross-lang == ORM column).

export type OrmDialect = 'sqlite' | 'mysql' | 'postgres';

// Matches test/parity/v1-sql-golden.test.ts SEED.
export const SEED = {
  users: 110, // ids 1..110 (covers id=100..109 for update/updateMany)
  extraUniqueUserId: 500, // user500@example.com (find unique)
  postsPerUser: 2,
  commentsPerPost: 2,
  tenants: 5,
  usersPerTenant: 4,
  postsPerTenantUser: 2,
  commentsPerTenantPost: 2,
} as const;

const DROP_ORDER = [
  'benchmark_tenant_comments',
  'benchmark_tenant_posts',
  'benchmark_tenant_users',
  'benchmark_comments',
  'benchmark_posts',
  'benchmark_users',
] as const;

export function dropStatements(dialect: OrmDialect): string[] {
  const cascade = dialect === 'postgres' ? ' CASCADE' : '';
  return DROP_ORDER.map((t) => `DROP TABLE IF EXISTS ${t}${cascade}`);
}

export function ddl(dialect: OrmDialect): string[] {
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

// A seed statement as a portable `?`-placeholder SQL + params (each language binds via its driver;
// PG rewrites `?`→`$N`). Deterministic + identical across dialects (booleans as 1/0 for mysql/sqlite,
// true/false for pg).
export interface SeedStmt {
  readonly sql: string;
  readonly params: readonly unknown[];
}

export function seedStatements(dialect: OrmDialect): SeedStmt[] {
  const bool = (b: boolean) => (dialect === 'postgres' ? b : b ? 1 : 0);
  const out: SeedStmt[] = [];

  for (let id = 1; id <= SEED.users; id++) {
    out.push({ sql: 'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)', params: [id, `user${id}@example.com`, `User ${id}`] });
  }
  out.push({
    sql: 'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)',
    params: [SEED.extraUniqueUserId, `user${SEED.extraUniqueUserId}@example.com`, `User ${SEED.extraUniqueUserId}`],
  });

  let postId = 1;
  for (let uid = 1; uid <= SEED.users; uid++) {
    for (let p = 0; p < SEED.postsPerUser; p++) {
      const published = postId % 3 === 0;
      out.push({
        sql: 'INSERT INTO benchmark_posts (id, title, content, published, author_id) VALUES (?, ?, ?, ?, ?)',
        params: [postId, `Post ${postId}`, `Content ${postId}`, bool(published), uid],
      });
      postId++;
    }
  }
  const maxPostId = postId - 1;

  let commentId = 1;
  for (let pid = 1; pid <= maxPostId; pid++) {
    for (let c = 0; c < SEED.commentsPerPost; c++) {
      out.push({ sql: 'INSERT INTO benchmark_comments (id, body, post_id) VALUES (?, ?, ?)', params: [commentId, `Comment ${commentId} for post ${pid}`, pid] });
      commentId++;
    }
  }

  for (let t = 1; t <= SEED.tenants; t++) {
    for (let u = 1; u <= SEED.usersPerTenant; u++) {
      out.push({ sql: 'INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES (?, ?, ?)', params: [t, u, `Tenant${t} User${u}`] });
    }
    let localPostId = 1;
    for (let u = 1; u <= SEED.usersPerTenant; u++) {
      for (let p = 0; p < SEED.postsPerTenantUser; p++) {
        out.push({ sql: 'INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES (?, ?, ?, ?)', params: [t, localPostId, u, `T${t}Post ${localPostId}`] });
        localPostId++;
      }
    }
    const tenantPosts = localPostId - 1;
    let localCommentId = 1;
    for (let lp = 1; lp <= tenantPosts; lp++) {
      for (let c = 0; c < SEED.commentsPerTenantPost; c++) {
        out.push({ sql: 'INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES (?, ?, ?, ?)', params: [t, localCommentId, lp, `T${t}Comment ${localCommentId}`] });
        localCommentId++;
      }
    }
  }
  return out;
}

// After the explicit-id seed, advance the PG SERIAL sequences past MAX(id) so the first Create
// (INSERT without id) does not collide. sqlite AUTOINCREMENT + mysql AUTO_INCREMENT derive next
// id from MAX(id) automatically (no fixup).
export function pgSeqResetStatements(): string[] {
  return [
    `SELECT setval(pg_get_serial_sequence('benchmark_users', 'id'), (SELECT MAX(id) FROM benchmark_users))`,
    `SELECT setval(pg_get_serial_sequence('benchmark_posts', 'id'), (SELECT MAX(id) FROM benchmark_posts))`,
    `SELECT setval(pg_get_serial_sequence('benchmark_comments', 'id'), (SELECT MAX(id) FROM benchmark_comments))`,
  ];
}
