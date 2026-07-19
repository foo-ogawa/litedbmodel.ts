// Native-codegen proof — LIVE DB seeder (epic #123 / #124 commit 2/3).
//
// Brings docker Postgres (:5433) / MySQL (:3307) to the SAME seeded state the sqlite proof DBs use, so
// the rust native-codegen cell can run against a REAL pg/mysql and compare BYTE-FOR-BYTE to the SAME
// dialect-independent mode-2 oracle (oracles*.json). Three states mirror the sqlite files:
//   read  = the full orm-domain seed (== proof.db)         — reads/relations
//   write = 3 users                                        (== write_seed.db) — write ops
//   tx    = 3 users + 1 post (id 1, "Post B", author 2)    (== tx_seed.db)     — tx ops
//
// The schema is INT-TYPED to match the E1 MODEL's declared column types (RESOLVE = ddl('sqlite'), so
// `published` is int, not the orm-domain pg BOOLEAN): the DB matches the model the codegen compiled
// against, keeping the oracle dialect-independent. AUTO-increment sequences are reset so the next id is
// max+1 (pg SERIAL needs setval; mysql AUTO_INCREMENT continues from max automatically).
//
//   node livedb-seed.mjs <postgres|mysql> <read|write|tx>

import { seedStatements } from '../../benchmark/crosslang/orm-domain.ts';

const PG = { host: 'localhost', port: 5433, user: 'testuser', password: 'testpass', database: 'testdb' };
const MY = { host: 'localhost', port: 3307, user: 'testuser', password: 'testpass', database: 'testdb' };
export const PG_CONN_STR = `host=${PG.host} port=${PG.port} user=${PG.user} password=${PG.password} dbname=${PG.database}`;
export const MYSQL_URL = `mysql://${MY.user}:${MY.password}@${MY.host}:${MY.port}/${MY.database}`;

const toPg = (sql) => { let i = 0; return sql.replace(/\?/g, () => `$${++i}`); };

/** INT-typed schema (mirrors ddl('sqlite') types; `published` INTEGER) for the given dialect. */
function schema(dialect) {
  const auto = dialect === 'postgres' ? 'SERIAL PRIMARY KEY' : 'INT AUTO_INCREMENT PRIMARY KEY';
  const txt = dialect === 'postgres' ? 'TEXT' : 'VARCHAR(255)';
  const intt = dialect === 'postgres' ? 'INTEGER' : 'INT';
  return [
    `CREATE TABLE benchmark_users (id ${auto}, email ${txt} NOT NULL UNIQUE, name ${txt})`,
    `CREATE TABLE benchmark_posts (id ${auto}, title ${txt} NOT NULL, content TEXT, published ${intt} DEFAULT 0, author_id ${intt}, created_at ${txt})`,
    `CREATE TABLE benchmark_comments (id ${auto}, body TEXT NOT NULL, post_id ${intt})`,
    `CREATE TABLE benchmark_tenant_users (tenant_id ${intt} NOT NULL, user_id ${intt} NOT NULL, name ${txt}, PRIMARY KEY (tenant_id, user_id))`,
    `CREATE TABLE benchmark_tenant_posts (tenant_id ${intt} NOT NULL, post_id ${intt} NOT NULL, user_id ${intt} NOT NULL, title ${txt} NOT NULL, PRIMARY KEY (tenant_id, post_id))`,
    `CREATE TABLE benchmark_tenant_comments (tenant_id ${intt} NOT NULL, comment_id ${intt} NOT NULL, post_id ${intt} NOT NULL, body TEXT NOT NULL, PRIMARY KEY (tenant_id, comment_id))`,
  ];
}

const DROPS = [
  'benchmark_tenant_comments', 'benchmark_tenant_posts', 'benchmark_tenant_users',
  'benchmark_comments', 'benchmark_posts', 'benchmark_users',
].map((t) => `DROP TABLE IF EXISTS ${t}`);

/** The seed rows for a state, as portable `?`-SQL + params (published as 0/1 → the int column). */
function seedRows(state) {
  if (state === 'read') return seedStatements('sqlite'); // full deterministic orm-domain seed
  if (state === 'write') {
    return [1, 2, 3].map((id) => ({ sql: 'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)', params: [id, `user${id}@example.com`, `User ${id}`] }));
  }
  if (state === 'tx') {
    const rows = [1, 2, 3].map((id) => ({ sql: 'INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)', params: [id, `user${id}@example.com`, `User ${id}`] }));
    rows.push({ sql: 'INSERT INTO benchmark_posts (id, title, author_id) VALUES (?, ?, ?)', params: [1, 'Post B', 2] });
    return rows;
  }
  throw new Error(`unknown state '${state}'`);
}

/** pg: reset each SERIAL sequence so the next insert id is max(id)+1 (explicit-id seed leaves it at 1). */
const PG_SEQ_RESET = [
  `SELECT setval(pg_get_serial_sequence('benchmark_users','id'), COALESCE((SELECT MAX(id) FROM benchmark_users),1))`,
  `SELECT setval(pg_get_serial_sequence('benchmark_posts','id'), COALESCE((SELECT MAX(id) FROM benchmark_posts),1))`,
  `SELECT setval(pg_get_serial_sequence('benchmark_comments','id'), COALESCE((SELECT MAX(id) FROM benchmark_comments),1))`,
];

async function seedPostgres(state) {
  const pg = (await import('pg')).default;
  const client = new pg.Client(PG);
  await client.connect();
  try {
    for (const s of DROPS) await client.query(`${s} CASCADE`);
    for (const s of schema('postgres')) await client.query(s);
    for (const s of seedRows(state)) await client.query(toPg(s.sql), s.params);
    for (const s of PG_SEQ_RESET) await client.query(s);
  } finally {
    await client.end();
  }
}

async function seedMysql(state) {
  const mysql = (await import('mysql2/promise')).default;
  const conn = await mysql.createConnection(MY);
  try {
    await conn.query('SET FOREIGN_KEY_CHECKS=0');
    for (const s of DROPS) await conn.query(s);
    for (const s of schema('mysql')) await conn.query(s);
    for (const s of seedRows(state)) await conn.query(s.sql, s.params);
    await conn.query('SET FOREIGN_KEY_CHECKS=1');
  } finally {
    await conn.end();
  }
}

export async function seedE1(dialect, state) {
  if (dialect === 'postgres') return seedPostgres(state);
  if (dialect === 'mysql') return seedMysql(state);
  throw new Error(`seedE1: '${dialect}' is not a live DB (sqlite is file-seeded)`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const [dialect, state] = process.argv.slice(2);
  seedE1(dialect, state ?? 'read')
    .then(() => process.stderr.write(`livedb-seed: ${dialect} ${state ?? 'read'} seeded\n`))
    .catch((e) => { process.stderr.write(String(e?.stack ?? e) + '\n'); process.exit(1); });
}
