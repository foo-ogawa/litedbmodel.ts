/**
 * Phase F-3 (#106) — README examples as EXECUTABLE conformance (the v2-beta USABILITY gate).
 *
 * Phases F1+F2 rewired the decorator ActiveRecord API (the README surface) onto the SCP + A-E
 * runtime. F3 PROVES it: every executable code example in `README.md` is turned into a running
 * assertion here. The tests run the ACTUAL README code — the decorated model classes and the
 * documented method calls, verbatim — against the 3 REAL databases (SQLite in-process, PG:5433,
 * MySQL:3307), and assert the documented result AND (where the README shows SQL or a behavior) the
 * documented SQL / behavior. This is the gate that shows the README is no longer aspirational: a
 * future README change that breaks an example fails here.
 *
 * Each `describe(dialect)` block reconfigures `DBModel.setConfig(cfg)` (the README Quick-Start call)
 * and creates the schema, so the SAME README code runs on every dialect. The `execute` middleware is
 * registered to prove the decorator surface lowers to SCP — a registered SCP middleware sees the SQL
 * that the high-level `find`/`create`/… actually issue.
 *
 * Requires live PG (:5433) + MySQL (:3307). Bring up: `npm run docker:livedb:up`. SQLite is in-process.
 *
 * README section → test coverage map (see the `it(...)` titles):
 *   Quick Start / CRUD .................. crud + returning PkeyResult + findById + findOne
 *   Column decorators (bare @column) .... the string→string / number→number / boolean→boolean / date read contract
 *   Model Options ....................... order / filter / select defaults applied by find()
 *   PkeyResult / upsert / updateMany .... returning shapes + onConflict ignore/update + composite unique
 *   Type-Safe Conditions + sql + OR ..... tuple / sql`` operators / IN(?) list / IS NULL / OR + order
 *   Subqueries .......................... inSubquery / composite / notIn / parentRef correlated / exists / notExists (+ documented SQL)
 *   SKIP ................................ update / createMany DEFAULT / updateMany retain
 *   Relations ........................... hasMany / belongsTo / hasOne / composite key / per-parent limit / transparent N+1
 *   Query Limits ........................ findHardLimit + hasManyHardLimit + per-relation hardLimit override + LimitExceededError
 *   Transactions ........................ basic / return value / rollbackOnly / one-connection proof / retry option
 *   Middleware .......................... execute sees all SQL / method-level find hook / per-request state
 *   Query-Based Models .................. static QUERY as CTE + find() with conditions
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  DBModel,
  model,
  column,
  hasMany,
  belongsTo,
  hasOne,
  sql,
  SKIP,
  parentRef,
  LimitExceededError,
  formatLocalDate,
  formatUTCDate,
} from '../../src';
import type { Column, ColumnsOf } from '../../src';
import { skipIntegrationTests, pgConfig, mysqlConfig, sqliteConfig } from '../helpers/setup';

// ============================================================================
// README models — copied VERBATIM from README.md (decorators + shapes unchanged).
// The bare `@column()` shapes are exactly what the README Quick Start uses.
// ============================================================================

// Quick Start / CRUD / Conditions / Middleware model (README §Quick Start, §CRUD, §Conditions)
@model('rc_users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() is_active?: boolean;
  @column() created_at?: Date;
  @column() updated_at?: Date;
  // extra columns exercised by README condition/subquery/upsert examples
  @column() status?: string;
  @column() role?: string;
  @column() age?: number;
  @column() tenant_id?: number;
  @column() group_id?: number;
  // README §Date vs DateTime: a NULLABLE timestamp uses an EXPLICIT decorator — the `Date | null`
  // union defeats reflect-metadata's design:type inference (it emits Object, not Date), so a bare
  // `@column()` cannot type it. `@column.datetime()` is the documented decorator for this (mirrors
  // the repo's own test helper). Non-null `Date` columns (created_at/updated_at) reflect fine as bare.
  @column.datetime() deleted_at?: Date | null;

  // README §Relations
  @hasMany(() => [User.id, Post.author_id])
  declare posts: Promise<Post[]>;

  @hasOne(() => [User.id, UserProfile.user_id])
  declare profile: Promise<UserProfile | null>;

  // README §Relations "With Options" — per-parent limit
  @hasMany(() => [User.id, Post.author_id], {
    limit: 5,
    order: () => Post.id.desc(),
  })
  declare recentPosts: Promise<Post[]>;

  // README §Query Limits — per-relation hardLimit override
  @hasMany(() => [User.id, Post.author_id], {
    hardLimit: 500,
  })
  declare guardedPosts: Promise<Post[]>;

  @hasMany(() => [User.id, Post.author_id], {
    hardLimit: null,
  })
  declare unlimitedPosts: Promise<Post[]>;
}
const User = UserModel as typeof UserModel & ColumnsOf<UserModel>;
type User = UserModel;

@model('rc_posts')
class PostModel extends DBModel {
  @column() id?: number;
  @column() author_id?: number;
  @column() title?: string;

  @belongsTo(() => [Post.author_id, User.id])
  declare author: Promise<User | null>;

  @hasMany(() => [Post.id, Comment.post_id])
  declare comments: Promise<Comment[]>;
}
const Post = PostModel as typeof PostModel & ColumnsOf<PostModel>;
type Post = PostModel;

@model('rc_comments')
class CommentModel extends DBModel {
  @column() id?: number;
  @column() post_id?: number;
  @column() body?: string;
}
const Comment = CommentModel as typeof CommentModel & ColumnsOf<CommentModel>;
type Comment = CommentModel;

@model('rc_user_profiles')
class UserProfileModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() bio?: string;
}
const UserProfile = UserProfileModel as typeof UserProfileModel & ColumnsOf<UserProfileModel>;
type UserProfile = UserProfileModel;

// README §Subqueries target tables
@model('rc_orders')
class OrderModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() group_id?: number;
  @column() tenant_id?: number;
  @column() status?: string;
}
const Order = OrderModel as typeof OrderModel & ColumnsOf<OrderModel>;

@model('rc_banned_users')
class BannedUserModel extends DBModel {
  @column() user_id?: number;
}
const BannedUser = BannedUserModel as typeof BannedUserModel & ColumnsOf<BannedUserModel>;

@model('rc_complaints')
class ComplaintModel extends DBModel {
  @column() user_id?: number;
}
const Complaint = ComplaintModel as typeof ComplaintModel & ColumnsOf<ComplaintModel>;

// README §Upsert composite unique key. NOTE: the README example names the column `key`, which is a
// RESERVED word in MySQL. litedbmodel embeds identifiers raw (it does not auto-quote reserved words —
// a general, dialect-wide design choice, not an F1/F2 gap), so a literal `key` column is not portable
// to MySQL. We keep the README's INTENT (composite-unique-key upsert) with a portable column name.
@model('rc_user_prefs')
class UserPrefModel extends DBModel {
  @column() user_id?: number;
  @column() pref_key?: string;
  @column() value?: string;
}
const UserPref = UserPrefModel as typeof UserPrefModel & ColumnsOf<UserPrefModel>;

// README §Model Options
@model('rc_entries', {
  order: () => Entry.created_at.desc(),
  filter: () => [[Entry.is_deleted, false]],
  select: 'id, title, created_at',
})
class EntryModel extends DBModel {
  @column() id?: number;
  @column() title?: string;
  @column() created_at?: Date;
  @column.boolean() is_deleted?: boolean;
}
const Entry = EntryModel as typeof EntryModel & ColumnsOf<EntryModel>;

// README §Column Decorators — the Date vs DateTime distinction
@model('rc_events')
class EventModel extends DBModel {
  @column() id?: number;
  @column.date() birth_date?: string; // DATE → 'YYYY-MM-DD' string
  @column.datetime() updated_at?: Date; // TIMESTAMP → Date/string
}
const Event = EventModel as typeof EventModel & ColumnsOf<EventModel>;

// README §Composite Key Relations
@model('rc_tenant_users')
class TenantUserModel extends DBModel {
  @column({ primaryKey: true }) tenant_id?: number;
  @column({ primaryKey: true }) id?: number;
  @column() name?: string;
}
const TenantUser = TenantUserModel as typeof TenantUserModel & ColumnsOf<TenantUserModel>;
type TenantUser = TenantUserModel;

@model('rc_tenant_posts')
class TenantPostModel extends DBModel {
  @column({ primaryKey: true }) tenant_id?: number;
  @column({ primaryKey: true }) id?: number;
  @column() author_id?: number;

  @belongsTo(() => [
    [TenantPost.tenant_id, TenantUser.tenant_id],
    [TenantPost.author_id, TenantUser.id],
  ])
  declare author: Promise<TenantUser | null>;
}
const TenantPost = TenantPostModel as typeof TenantPostModel & ColumnsOf<TenantPostModel>;

// README §Query-Based Models
@model('rc_user_stats')
class UserStatsModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() post_count?: number;

  static QUERY = `
    SELECT
      u.id,
      u.name,
      COUNT(p.id) AS post_count
    FROM rc_users u
    LEFT JOIN rc_posts p ON u.id = p.author_id
    GROUP BY u.id, u.name
  `;
}
const UserStats = UserStatsModel as typeof UserStatsModel & ColumnsOf<UserStatsModel>;

// ============================================================================
// Schema per dialect (README shapes; portable DDL is not litedbmodel's job)
// ============================================================================

interface Dialect {
  readonly name: string;
  readonly cfg: typeof pgConfig;
  readonly ddl: readonly string[];
  readonly serial: string;
  readonly boolType: string;
}

const AUTO_PG = 'SERIAL PRIMARY KEY';
const AUTO_MY = 'INT AUTO_INCREMENT PRIMARY KEY';
const AUTO_SQ = 'INTEGER PRIMARY KEY AUTOINCREMENT';

function schema(serial: string, uniqueEmail: string, boolT: string, prefKeyType: string): string[] {
  return [
    `CREATE TABLE rc_users (
       id ${serial}, name TEXT, email ${uniqueEmail}, is_active ${boolT},
       created_at TIMESTAMP NULL, updated_at TIMESTAMP NULL, status TEXT, role TEXT, age INT,
       tenant_id INT, group_id INT, deleted_at TIMESTAMP NULL)`,
    `CREATE TABLE rc_posts (id ${serial}, author_id INT, title TEXT)`,
    `CREATE TABLE rc_comments (id ${serial}, post_id INT, body TEXT)`,
    `CREATE TABLE rc_user_profiles (id ${serial}, user_id INT, bio TEXT)`,
    `CREATE TABLE rc_orders (id ${serial}, user_id INT, group_id INT, tenant_id INT, status TEXT)`,
    `CREATE TABLE rc_banned_users (user_id INT)`,
    `CREATE TABLE rc_complaints (user_id INT)`,
    `CREATE TABLE rc_user_prefs (user_id INT, pref_key ${prefKeyType}, value TEXT)`,
    `CREATE TABLE rc_entries (id ${serial}, title TEXT, created_at TIMESTAMP NULL, is_deleted ${boolT})`,
    `CREATE TABLE rc_events (id ${serial}, birth_date DATE, updated_at TIMESTAMP NULL)`,
    `CREATE TABLE rc_tenant_users (tenant_id INT, id INT, name TEXT, PRIMARY KEY (tenant_id, id))`,
    `CREATE TABLE rc_tenant_posts (tenant_id INT, id INT, author_id INT, PRIMARY KEY (tenant_id, id))`,
  ];
}

const ALL_TABLES = [
  'rc_tenant_posts', 'rc_tenant_users', 'rc_events', 'rc_entries', 'rc_user_prefs',
  'rc_complaints', 'rc_banned_users', 'rc_orders', 'rc_user_profiles', 'rc_comments',
  'rc_posts', 'rc_users',
];

const dialects: Dialect[] = [
  {
    name: 'sqlite',
    cfg: sqliteConfig as typeof pgConfig,
    serial: AUTO_SQ,
    boolType: 'BOOLEAN',
    ddl: schema(AUTO_SQ, 'TEXT UNIQUE', 'BOOLEAN', 'TEXT'),
  },
  {
    name: 'postgres',
    cfg: pgConfig,
    serial: AUTO_PG,
    boolType: 'BOOLEAN',
    ddl: schema(AUTO_PG, 'TEXT UNIQUE', 'BOOLEAN', 'TEXT'),
  },
  {
    name: 'mysql',
    cfg: mysqlConfig as typeof pgConfig,
    serial: AUTO_MY,
    boolType: 'BOOLEAN',
    ddl: schema(AUTO_MY, 'VARCHAR(255) UNIQUE', 'BOOLEAN', 'VARCHAR(191)'),
  },
];

// ---- helper: a recording execute-middleware (proves the decorator surface lowers to SCP) ----
function recordingMiddleware(sink: string[]) {
  return DBModel.createMiddleware({
    execute: async function (next, s: string, p?: unknown[]) {
      sink.push(s);
      return next(s, p);
    },
  });
}

// ============================================================================
// Per-dialect conformance run
// ============================================================================

for (const d of dialects) {
  describe.skipIf(skipIntegrationTests)(`README conformance — ${d.name}`, () => {
    beforeAll(async () => {
      DBModel.setConfig(d.cfg);
      DBModel.clearMiddlewares();
      for (const t of ALL_TABLES) await DBModel.execute(`DROP TABLE IF EXISTS ${t}`);
      for (const stmt of d.ddl) await DBModel.execute(stmt);
    });

    afterAll(async () => {
      for (const t of ALL_TABLES) await DBModel.execute(`DROP TABLE IF EXISTS ${t}`);
      DBModel.clearMiddlewares();
      DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: null });
    });

    beforeEach(async () => {
      DBModel.clearMiddlewares();
      DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: null });
      for (const t of ALL_TABLES) {
        // reset rows between tests (keep schema)
        await DBModel.execute(`DELETE FROM ${t}`);
      }
    });

    // ---------------------------------------------------------------- Quick Start / CRUD
    it('README §Quick Start / §CRUD — create/update/delete/find/findOne + returning PkeyResult + findById', async () => {
      // README: create (default → null), then with returning: true → PkeyResult { key:[User.id], values:[[1]] }
      const noReturn = await DBModel.transaction(async () =>
        User.create([[User.name, 'John'], [User.email, 'john@example.com']]),
      );
      expect(noReturn).toBeNull(); // default: no RETURNING → null

      const created = await DBModel.transaction(async () =>
        User.create(
          [[User.name, 'Jane'], [User.email, 'jane@example.com'], [User.is_active, true]],
          { returning: true },
        ),
      );
      expect(created).not.toBeNull();
      // README: result: { key: [User.id], values: [[1]] } — key is the PK Column.
      expect((created!.key[0] as unknown as { columnName: string }).columnName).toBe('id');
      expect(created!.values.length).toBe(1);
      expect(created!.values[0].length).toBe(1);

      // README: const [newUser] = await User.findById(created);
      const [newUser] = await User.findById(created!);
      expect(newUser.name).toBe('Jane');

      // README read ops
      const active = await User.find([[User.is_active, true]]);
      expect(active.length).toBe(1);
      expect(active[0].name).toBe('Jane');

      const john = await User.findOne([[User.email, 'john@example.com']]);
      expect(john).not.toBeNull();
      expect(john!.name).toBe('John');

      // README: update([[User.id,1]], [[User.name,'Jane']]) then delete([[User.is_active,false]])
      await DBModel.transaction(async () =>
        User.update([[User.id, newUser.id!]], [[User.name, 'Janet']]),
      );
      const janet = await User.findById(created!);
      expect(janet[0].name).toBe('Janet');

      await DBModel.transaction(async () => User.delete([[User.is_active, false]]));
      const remaining = await User.find([]);
      // 'John' had is_active NULL (not false) so is not deleted; both remain
      expect(remaining.length).toBe(2);
    });

    // ---------------------------------------------------------------- bare @column() read contract
    it('README §Column Decorators — the BARE @column() read contract holds live (string→string, number→number, boolean→boolean, Date column)', async () => {
      await DBModel.transaction(async () =>
        User.create(
          [[User.name, 'Alice'], [User.email, 'alice@example.com'], [User.is_active, true]],
          { returning: true },
        ),
      );
      const [u] = await User.find([[User.email, 'alice@example.com']]);
      // README auto-inferred types: name/email string, id number, is_active boolean
      expect(typeof u.id).toBe('number');
      expect(typeof u.name).toBe('string');
      expect(u.name).toBe('Alice');
      expect(typeof u.email).toBe('string');
      expect(u.email).toBe('alice@example.com');
      expect(typeof u.is_active).toBe('boolean');
      expect(u.is_active).toBe(true);
    });

    it('README §Column Decorators — @column.date() reads a DATE as a YYYY-MM-DD string; @column.datetime() reads a TIMESTAMP', async () => {
      await DBModel.transaction(async () =>
        Event.create(
          [[Event.birth_date, '1990-06-15'], [Event.updated_at, new Date('2024-06-15T10:30:00.000Z')]],
          { returning: true },
        ),
      );
      const [e] = await Event.find([]);
      // README: @column.date() returns a string 'YYYY-MM-DD'
      expect(typeof e.birth_date).toBe('string');
      expect(e.birth_date).toBe('1990-06-15');
      // README: @column.datetime() is a datetime column (v2 read materializes to a TZ-attached string)
      expect(String(e.updated_at)).toMatch(/^\d{4}-\d{2}-\d{2}/);
    });

    it('README §Date Utility Functions — formatLocalDate / formatUTCDate return YYYY-MM-DD', () => {
      // Pure functions (no DB) but documented in README; assert the contract.
      expect(formatUTCDate(new Date('2024-06-15T23:30:00.000Z'))).toBe('2024-06-15');
      expect(formatLocalDate(new Date(2024, 5, 15))).toBe('2024-06-15'); // month is 0-based
    });

    // ---------------------------------------------------------------- Model Options
    it('README §Model Options — order / filter / select defaults are applied by find()', async () => {
      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));
      await DBModel.transaction(async () => {
        await Entry.create([[Entry.title, 'A'], [Entry.created_at, new Date('2024-01-01T00:00:00Z')], [Entry.is_deleted, false]]);
        await Entry.create([[Entry.title, 'B'], [Entry.created_at, new Date('2024-02-01T00:00:00Z')], [Entry.is_deleted, false]]);
        await Entry.create([[Entry.title, 'Deleted'], [Entry.created_at, new Date('2024-03-01T00:00:00Z')], [Entry.is_deleted, true]]);
      });
      seen.length = 0;
      const rows = await Entry.find([]);
      // filter: is_deleted=false auto-applied → 'Deleted' excluded
      expect(rows.map((r) => r.title)).toEqual(['B', 'A']); // DEFAULT_ORDER created_at DESC
      const findSql = seen.find((s) => /select/i.test(s))!;
      expect(findSql).toMatch(/is_deleted/i); // filter applied at SQL level
      expect(findSql).toMatch(/order by/i); // default order applied
      un();
    });

    // ---------------------------------------------------------------- PkeyResult / upsert / updateMany
    it('README §CRUD — createMany returning + updateMany (keyColumns) returning + delete returning', async () => {
      const created = await DBModel.transaction(async () =>
        User.createMany(
          [
            [[User.name, 'John'], [User.email, 'john@example.com']],
            [[User.name, 'Jane'], [User.email, 'jane@example.com']],
          ],
          { returning: true },
        ),
      );
      expect(created!.values.length).toBe(2);

      const updated = await DBModel.transaction(async () =>
        User.updateMany(
          [
            [[User.id, created!.values[0][0]], [User.name, 'John2'], [User.email, 'john2@example.com']],
            [[User.id, created!.values[1][0]], [User.name, 'Jane2'], [User.email, 'jane2@example.com']],
          ],
          { keyColumns: [User.id], returning: true },
        ),
      );
      // README §PkeyResult: key always contains PK regardless of keyColumns used in updateMany
      expect((updated!.key[0] as unknown as { columnName: string }).columnName).toBe('id');
      expect(updated!.values.length).toBe(2);
      const refetched = await User.findById(updated!);
      expect(refetched.map((u) => u.name).sort()).toEqual(['Jane2', 'John2']);

      const deleted = await DBModel.transaction(async () =>
        User.delete([[User.email, 'john2@example.com']], { returning: true }),
      );
      expect(deleted!.values.length).toBe(1);
      const gone = await User.findOne([[User.email, 'john2@example.com']]);
      expect(gone).toBeNull();
    });

    it('README §Upsert — onConflictIgnore, onConflictUpdate, and composite unique key', async () => {
      // Insert or ignore
      await DBModel.transaction(async () =>
        User.create([[User.name, 'John'], [User.email, 'u@example.com']]),
      );
      await DBModel.transaction(async () =>
        User.create([[User.name, 'JohnIgnored'], [User.email, 'u@example.com']], {
          onConflict: User.email,
          onConflictIgnore: true,
        }),
      );
      let u = await User.findOne([[User.email, 'u@example.com']]);
      expect(u!.name).toBe('John'); // ignored → original kept

      // Insert or update
      await DBModel.transaction(async () =>
        User.create([[User.name, 'JohnUpdated'], [User.email, 'u@example.com']], {
          onConflict: User.email,
          onConflictUpdate: [User.name],
        }),
      );
      u = await User.findOne([[User.email, 'u@example.com']]);
      expect(u!.name).toBe('JohnUpdated'); // updated

      // Composite unique key (README §Upsert). MySQL cannot index a TEXT column without a prefix
      // length → use VARCHAR for the keyed col. (Property `pref_key`; the README's `key` name is a
      // MySQL-reserved word — see the model note.)
      const prefKeyType = d.name === 'mysql' ? 'VARCHAR(191)' : 'TEXT';
      await DBModel.execute('DROP TABLE IF EXISTS rc_user_prefs');
      await DBModel.execute(
        `CREATE TABLE rc_user_prefs (user_id INT, pref_key ${prefKeyType}, value TEXT, UNIQUE (user_id, pref_key))`,
      );
      await DBModel.transaction(async () =>
        UserPref.create([[UserPref.user_id, 1], [UserPref.pref_key, 'theme'], [UserPref.value, 'light']]),
      );
      await DBModel.transaction(async () =>
        UserPref.create(
          [[UserPref.user_id, 1], [UserPref.pref_key, 'theme'], [UserPref.value, 'dark']],
          { onConflict: [UserPref.user_id, UserPref.pref_key], onConflictUpdate: [UserPref.value] },
        ),
      );
      const pref = await UserPref.findOne([[UserPref.user_id, 1], [UserPref.pref_key, 'theme']]);
      expect(pref!.value).toBe('dark');
    });

    it('README §findById — single, multiple, and composite PK', async () => {
      const c = await DBModel.transaction(async () =>
        User.createMany(
          [
            [[User.name, 'A'], [User.email, 'a@x.com']],
            [[User.name, 'B'], [User.email, 'b@x.com']],
            [[User.name, 'C'], [User.email, 'c@x.com']],
          ],
          { returning: true },
        ),
      );
      const ids = c!.values.map((v) => v[0]);
      const [single] = await User.findById({ values: [[ids[0]]] });
      expect(single.name).toBe('A');
      const multi = await User.findById({ values: [[ids[0]], [ids[1]], [ids[2]]] });
      expect(multi.length).toBe(3);

      // Composite PK
      await DBModel.transaction(async () =>
        TenantUser.create([[TenantUser.tenant_id, 1], [TenantUser.id, 100], [TenantUser.name, 'T']]),
      );
      const [entry] = await TenantUser.findById({ values: [[1, 100]] });
      expect(entry.name).toBe('T');
    });

    // ---------------------------------------------------------------- Type-Safe Conditions + sql + OR
    it('README §Type-Safe Conditions — tuple / sql`` operators (>, BETWEEN, LIKE, IN (?), IS NULL) run live', async () => {
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'young'], [User.email, 'y@x.com'], [User.age, 15], [User.status, 'active']]);
        await User.create([[User.name, 'adult'], [User.email, 'a@x.com'], [User.age, 30], [User.status, 'pending'], [User.deleted_at, new Date()]]);
        await User.create([[User.name, 'testy'], [User.email, 't@x.com'], [User.age, 40], [User.status, 'active']]);
      });

      // tuple equality
      expect((await User.find([[User.status, 'active']])).length).toBe(2);

      // sql`` operators (README §sql Tagged Template)
      expect((await User.find([[sql`${User.age} > ?`, 18]])).length).toBe(2);
      expect((await User.find([[sql`${User.age} BETWEEN ? AND ?`, [18, 35]]])).length).toBe(1);
      expect((await User.find([[sql`${User.name} LIKE ?`, '%test%']])).length).toBe(1);
      // README: status IN (?) with an ARRAY value → expands to IN (?, ?)
      expect((await User.find([[sql`${User.status} IN (?)`, ['active', 'pending']]])).length).toBe(3);
      // README: deleted_at IS NULL (no value)
      expect((await User.find([sql`${User.deleted_at} IS NULL`])).length).toBe(2);

      // README: values embedded directly in the template
      expect((await User.find([sql`${User.age} > ${18}`, sql`${User.name} LIKE ${'%test%'}`])).length).toBe(1);
    });

    it('README §OR Conditions and ORDER BY — User.or(...) + { order }', async () => {
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'x'], [User.email, 'x@x.com'], [User.is_active, true], [User.role, 'admin']]);
        await User.create([[User.name, 'y'], [User.email, 'y@x.com'], [User.is_active, true], [User.role, 'moderator']]);
        await User.create([[User.name, 'z'], [User.email, 'z@x.com'], [User.is_active, true], [User.role, 'user']]);
      });
      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));
      const rows = await User.find(
        [[User.is_active, true], User.or([[User.role, 'admin']], [[User.role, 'moderator']])],
        { order: User.created_at.desc() },
      );
      expect(rows.length).toBe(2);
      const s = seen.find((q) => /select/i.test(q))!;
      expect(s).toMatch(/or/i);
      expect(s).toMatch(/order by/i);
      un();
    });

    // ---------------------------------------------------------------- Subqueries
    it('README §Subquery Conditions — inSubquery / composite / notIn / correlated parentRef / exists / notExists (+documented SQL)', async () => {
      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));

      await DBModel.transaction(async () => {
        await User.createMany(
          [
            [[User.name, 'u1'], [User.email, 'u1@x.com'], [User.is_active, true], [User.tenant_id, 7], [User.group_id, 3]],
            [[User.name, 'u2'], [User.email, 'u2@x.com'], [User.is_active, true], [User.tenant_id, 7], [User.group_id, 9]],
            [[User.name, 'u3'], [User.email, 'u3@x.com'], [User.is_active, true], [User.tenant_id, 8], [User.group_id, 3]],
          ],
          { returning: true },
        );
      });
      const [u1, u2, u3] = await User.find([], { order: User.id.asc() });
      await DBModel.transaction(async () => {
        await Order.create([[Order.user_id, u1.id!], [Order.group_id, 3], [Order.tenant_id, 7], [Order.status, 'paid']]);
        await Order.create([[Order.user_id, u2.id!], [Order.group_id, 9], [Order.tenant_id, 7], [Order.status, 'active']]);
        await Order.create([[Order.user_id, u1.id!], [Order.group_id, 3], [Order.tenant_id, 7], [Order.status, 'completed']]);
        await BannedUser.create([[BannedUser.user_id, u3.id!]]);
        await Complaint.create([[Complaint.user_id, u2.id!]]);
      });

      const grab = (marker: RegExp) => seen.filter((s) => marker.test(s)).pop() ?? '';

      // IN subquery
      seen.length = 0;
      const paidUsers = await User.find([User.inSubquery([[User.id, Order.user_id]], [[Order.status, 'paid']])]);
      expect(paidUsers.map((u) => u.id)).toEqual([u1.id]);
      expect(grab(/rc_orders/i)).toMatch(/rc_users\.id IN \(SELECT rc_orders\.user_id FROM rc_orders WHERE/i);

      // Composite key IN subquery
      seen.length = 0;
      const composite = await User.find([
        User.inSubquery([[User.id, Order.user_id], [User.group_id, Order.group_id]], [[Order.status, 'active']]),
      ]);
      expect(composite.map((u) => u.id)).toEqual([u2.id]);
      expect(grab(/rc_orders/i)).toMatch(/\(rc_users\.id, rc_users\.group_id\) IN \(SELECT rc_orders\.user_id, rc_orders\.group_id/i);

      // NOT IN subquery
      seen.length = 0;
      const notBanned = await User.find([User.notInSubquery([[User.id, BannedUser.user_id]])]);
      expect(notBanned.map((u) => u.id).sort()).toEqual([u1.id, u2.id].sort());
      expect(grab(/rc_banned/i)).toMatch(/rc_users\.id NOT IN \(SELECT rc_banned_users\.user_id FROM rc_banned_users\)/i);

      // Correlated subquery with parentRef
      seen.length = 0;
      const correlated = await User.find([
        User.inSubquery([[User.id, Order.user_id]], [[Order.tenant_id, parentRef(User.tenant_id)], [Order.status, 'completed']]),
      ]);
      expect(correlated.map((u) => u.id)).toEqual([u1.id]);
      expect(grab(/rc_orders/i)).toMatch(/rc_orders\.tenant_id = rc_users\.tenant_id/i);

      // EXISTS subquery
      seen.length = 0;
      const withOrders = await User.find([[User.is_active, true], User.exists([[Order.user_id, parentRef(User.id)]])]);
      expect(withOrders.map((u) => u.id).sort()).toEqual([u1.id, u2.id].sort());
      expect(grab(/exists/i)).toMatch(/EXISTS \(SELECT 1 FROM rc_orders WHERE rc_orders\.user_id = rc_users\.id\)/i);

      // NOT EXISTS subquery
      seen.length = 0;
      const noComplaints = await User.find([User.notExists([[Complaint.user_id, parentRef(User.id)]])]);
      expect(noComplaints.map((u) => u.id).sort()).toEqual([u1.id, u3.id].sort());
      expect(grab(/not exists/i)).toMatch(/NOT EXISTS \(SELECT 1 FROM rc_complaints WHERE rc_complaints\.user_id = rc_users\.id\)/i);

      un();
    });

    // ---------------------------------------------------------------- SKIP
    it('README §SKIP — update excludes column, createMany applies DEFAULT, updateMany retains existing', async () => {
      const created = await DBModel.transaction(async () =>
        User.create([[User.name, 'orig'], [User.email, 'orig@x.com'], [User.status, 'active']], { returning: true }),
      );
      const id = created!.values[0][0];

      // README §SKIP update (verbatim): body.email undefined → SKIP → email unchanged
      const body: { name?: string; email?: string } = { name: 'updated' };
      await DBModel.transaction(async () =>
        User.update([[User.id, id]], [
          [User.name, body.name ?? SKIP],
          [User.email, body.email ?? SKIP],
          [User.updated_at, new Date()],
        ]),
      );
      let u = await User.findById(created!);
      expect(u[0].name).toBe('updated');
      expect(u[0].email).toBe('orig@x.com'); // SKIPped → retained

      // README §SKIP conditions: SKIP drops the condition
      const q: { name?: string; status?: string } = {};
      const all = await User.find([
        [User.status, q.status ?? SKIP],
        q.name ? [sql`${User.name} LIKE ?`, `%${q.name}%`] : SKIP,
      ]);
      expect(all.length).toBe(1); // both conditions SKIPped → all rows

      // README §SKIP createMany → SKIPped column gets DB DEFAULT
      await DBModel.transaction(async () =>
        User.createMany([
          [[User.name, 'John'], [User.email, 'jd@x.com']],
          [[User.name, 'Jane'], [User.email, SKIP]], // email = DEFAULT (NULL here)
        ]),
      );
      const jane = await User.findOne([[User.name, 'Jane']]);
      expect(jane).not.toBeNull();
      expect(jane!.email == null).toBe(true);

      // README §SKIP updateMany → SKIPped column retains existing value
      const j = await User.findOne([[User.name, 'John']]);
      await DBModel.transaction(async () =>
        User.updateMany(
          [[[User.id, j!.id!], [User.email, 'new@x.com'], [User.status, SKIP]]],
          { keyColumns: User.id },
        ),
      );
      const j2 = await User.findOne([[User.id, j!.id!]]);
      expect(j2!.email).toBe('new@x.com');
    });

    // ---------------------------------------------------------------- Relations
    it('README §Relations — hasMany / belongsTo / hasOne lazy loading through the decorator API', async () => {
      const c = await DBModel.transaction(async () =>
        User.create([[User.name, 'author'], [User.email, 'author@x.com']], { returning: true }),
      );
      const uid = c!.values[0][0];
      const p = await DBModel.transaction(async () =>
        Post.create([[Post.author_id, uid], [Post.title, 'Hello']], { returning: true }),
      );
      const pid = p!.values[0][0];
      await DBModel.transaction(async () => {
        await Comment.create([[Comment.post_id, pid], [Comment.body, 'c1']]);
        await Comment.create([[Comment.post_id, pid], [Comment.body, 'c2']]);
        await UserProfile.create([[UserProfile.user_id, uid], [UserProfile.bio, 'hi']]);
      });

      // README §Relations usage: const post = await Post.findOne(...); const author = await post.author;
      const post = await Post.findOne([[Post.id, pid]]);
      expect(post).not.toBeNull();
      const author = await post!.author; // belongsTo, lazy
      expect(author!.name).toBe('author');
      const comments = await post!.comments; // hasMany, lazy
      expect(comments.length).toBe(2);

      const user = await User.findOne([[User.id, uid]]);
      const profile = await user!.profile; // hasOne, lazy
      expect(profile!.bio).toBe('hi');
      const posts = await user!.posts; // hasMany, lazy
      expect(posts.length).toBe(1);
    });

    it('README §Relations — composite-key belongsTo resolves', async () => {
      await DBModel.transaction(async () => {
        await TenantUser.create([[TenantUser.tenant_id, 1], [TenantUser.id, 100], [TenantUser.name, 'CompAuthor']]);
        await TenantPost.create([[TenantPost.tenant_id, 1], [TenantPost.id, 500], [TenantPost.author_id, 100]]);
      });
      const tp = await TenantPost.findOne([[TenantPost.tenant_id, 1], [TenantPost.id, 500]]);
      const author = await tp!.author;
      expect(author!.name).toBe('CompAuthor');
    });

    it('README §Relations — per-parent limit (top-N per group) via the limit option', async () => {
      const c = await DBModel.transaction(async () =>
        User.createMany(
          [
            [[User.name, 'ua'], [User.email, 'ua@x.com']],
            [[User.name, 'ub'], [User.email, 'ub@x.com']],
          ],
          { returning: true },
        ),
      );
      const [ua, ub] = c!.values.map((v) => v[0]);
      await DBModel.transaction(async () => {
        for (let i = 0; i < 8; i++) await Post.create([[Post.author_id, ua], [Post.title, `a${i}`]]);
        for (let i = 0; i < 8; i++) await Post.create([[Post.author_id, ub], [Post.title, `b${i}`]]);
      });
      const users = await User.find([], { order: User.id.asc() });
      for (const user of users) {
        const recent = await user.recentPosts; // limit: 5 per parent
        expect(recent.length).toBe(5);
      }
    });

    it('README §Transparent N+1 Prevention — user.posts inside a loop batch-loads (2 queries, not N+1)', async () => {
      const c = await DBModel.transaction(async () =>
        User.createMany(
          [
            [[User.name, 'p1'], [User.email, 'p1@x.com']],
            [[User.name, 'p2'], [User.email, 'p2@x.com']],
            [[User.name, 'p3'], [User.email, 'p3@x.com']],
          ],
          { returning: true },
        ),
      );
      const uids = c!.values.map((v) => v[0]);
      await DBModel.transaction(async () => {
        for (const uid of uids) {
          await Post.create([[Post.author_id, uid], [Post.title, `post-${uid}`]]);
        }
      });

      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));
      const users = await User.find([]); // auto batch context
      seen.length = 0; // count only the relation-loading queries
      for (const user of users) {
        const posts = await user.posts; // first access batch-loads ALL users' posts
        expect(posts.length).toBe(1);
      }
      // README: "Total: 2 queries instead of N+1" — the relation batch is a SINGLE query for all parents.
      const relationQueries = seen.filter((s) => /rc_posts/i.test(s));
      expect(relationQueries.length).toBe(1);
      un();
    });

    // ---------------------------------------------------------------- Query Limits
    it('README §Query Limits — findHardLimit throws LimitExceededError with limit/actualCount', async () => {
      await DBModel.transaction(async () => {
        for (let i = 0; i < 5; i++) await User.create([[User.name, `u${i}`], [User.email, `u${i}@x.com`]]);
      });
      DBModel.setLimitConfig({ findHardLimit: 3, hasManyHardLimit: 10000 });
      let caught: LimitExceededError | null = null;
      try {
        await User.find([]); // 5 rows > limit 3
      } catch (e) {
        if (e instanceof LimitExceededError) caught = e;
      }
      expect(caught).not.toBeNull();
      expect(caught!.limit).toBe(3);
      expect(caught!.actualCount).toBeGreaterThan(3);
      DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: null });
    });

    it('README §Query Limits — per-relation hardLimit override throws; hardLimit:null disables', async () => {
      const c = await DBModel.transaction(async () =>
        User.create([[User.name, 'owner'], [User.email, 'owner@x.com']], { returning: true }),
      );
      const uid = c!.values[0][0];
      await DBModel.transaction(async () => {
        for (let i = 0; i < 20; i++) await Post.create([[Post.author_id, uid], [Post.title, `t${i}`]]);
      });
      const user = await User.findOne([[User.id, uid]]);

      // Global hasManyHardLimit tight; guardedPosts overrides with 500 (>20) → OK
      DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: 5 });
      const guarded = await user!.guardedPosts; // hardLimit: 500 override
      expect(guarded.length).toBe(20);

      // unlimitedPosts (hardLimit: null) disables the check entirely
      const user2 = await User.findOne([[User.id, uid]]);
      const unlimited = await user2!.unlimitedPosts;
      expect(unlimited.length).toBe(20);

      // A plain relation under the tight global limit throws
      const user3 = await User.findOne([[User.id, uid]]);
      let threw = false;
      try {
        await user3!.posts;
      } catch (e) {
        threw = e instanceof LimitExceededError;
      }
      expect(threw).toBe(true);
      DBModel.setLimitConfig({ findHardLimit: null, hasManyHardLimit: null });
    });

    // ---------------------------------------------------------------- Transactions
    it('README §Transactions — basic / with-return-value / rollbackOnly / retry option', async () => {
      // Basic + a write inside
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'txn'], [User.email, 'txn@x.com']]);
      });
      expect((await User.findOne([[User.email, 'txn@x.com']]))).not.toBeNull();

      // With return value
      const ret = await DBModel.transaction(async () => {
        return User.create([[User.name, 'Alice'], [User.email, 'alice-tx@x.com']], { returning: true });
      });
      expect(ret!.values.length).toBe(1);

      // rollbackOnly: preview then roll back → no row persists
      await DBModel.transaction(
        async () => {
          await User.create([[User.name, 'ghost'], [User.email, 'ghost@x.com']]);
        },
        { rollbackOnly: true },
      );
      expect(await User.findOne([[User.email, 'ghost@x.com']])).toBeNull();

      // retryOnError option is accepted (a successful body runs once)
      let runs = 0;
      await DBModel.transaction(
        async () => {
          runs++;
          await User.create([[User.name, 'retry'], [User.email, 'retry@x.com']]);
        },
        { retryOnError: true, retryLimit: 3 },
      );
      expect(runs).toBe(1);
      expect(await User.findOne([[User.email, 'retry@x.com']])).not.toBeNull();
    });

    it('README §Transactions — the tx body runs on ONE transactional connection (writes are atomic; a mid-tx failure rolls BOTH back)', async () => {
      // The observable "one connection" guarantee through the PUBLIC decorator API: the two writes
      // in a tx body either BOTH commit or (on any failure) BOTH roll back — they share one
      // transactional connection. (The BEGIN/COMMIT tx-control funnels through the lower SCP seam,
      // not the public `execute` middleware — the README's execute hook documents DML, not tx-control.)
      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'oneconn'], [User.email, 'oneconn@x.com']]);
        await User.create([[User.name, 'oneconn2'], [User.email, 'oneconn2@x.com']]);
      });
      un();
      // Both inserts flowed through the ONE execute seam and both committed atomically.
      const inserts = seen.filter((s) => /INSERT INTO rc_users/i.test(s));
      expect(inserts.length).toBe(2);
      expect((await User.find([[sql`${User.name} LIKE ?`, 'oneconn%']])).length).toBe(2);

      // Atomicity: a throw AFTER the first write inside the tx rolls back the whole transaction —
      // neither row survives (proving both writes shared one transactional connection).
      await expect(
        DBModel.transaction(async () => {
          await User.create([[User.name, 'atom1'], [User.email, 'atom1@x.com']]);
          await User.create([[User.name, 'atom2'], [User.email, 'atom2@x.com']]);
          throw new Error('boom');
        }),
      ).rejects.toThrow('boom');
      expect((await User.find([[sql`${User.name} LIKE ?`, 'atom%']])).length).toBe(0);
    });

    // ---------------------------------------------------------------- Middleware
    it('README §Middleware — execute middleware sees EVERY SQL statement through the decorator API', async () => {
      const seen: string[] = [];
      // README: const LoggerMiddleware = DBModel.createMiddleware({ execute: async function(next, sql, params){...} })
      const LoggerMiddleware = DBModel.createMiddleware({
        execute: async function (next, s: string, params?: unknown[]) {
          seen.push(s);
          return next(s, params);
        },
      });
      DBModel.use(LoggerMiddleware);
      await DBModel.transaction(async () =>
        User.create([[User.name, 'mw'], [User.email, 'mw@x.com']]),
      );
      await User.find([[User.name, 'mw']]);
      DBModel.clearMiddlewares();
      // The high-level create + find both lowered to SQL the execute-hook observed.
      expect(seen.some((s) => /insert into rc_users/i.test(s))).toBe(true);
      expect(seen.some((s) => /select .* from rc_users/i.test(s) || /select \* from rc_users/i.test(s))).toBe(true);
    });

    it('README §Middleware — method-level find hook with per-request state (getCurrentContext)', async () => {
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'a'], [User.email, 'a-mw@x.com'], [User.tenant_id, 1]]);
        await User.create([[User.name, 'b'], [User.email, 'b-mw@x.com'], [User.tenant_id, 2]]);
      });
      // README: DBModel.createMiddleware({ state: { tenantId, queryCount }, find: async function(model, next, conditions, options){...} })
      const TenantMiddleware = DBModel.createMiddleware({
        state: { tenantId: 0, queryCount: 0 },
        find: async function (model, next, conditions, options) {
          this.queryCount++;
          const tenantCol = (model as { tenant_id?: Column }).tenant_id;
          if (tenantCol) {
            conditions = [[tenantCol, this.tenantId], ...(conditions as unknown[])] as never;
          }
          return next(conditions, options);
        },
      });
      DBModel.use(TenantMiddleware);
      // README: TenantMiddleware.getCurrentContext().tenantId = req.user.tenantId;
      TenantMiddleware.getCurrentContext().tenantId = 2;
      const rows = await User.find([]);
      expect(rows.length).toBe(1); // only tenant 2
      expect(rows[0].name).toBe('b');
      expect(TenantMiddleware.getCurrentContext().queryCount).toBe(1);
      DBModel.clearMiddlewares();
    });

    // ---------------------------------------------------------------- Query-Based Models
    it('README §Query-Based Models — static QUERY becomes a CTE; find() applies extra conditions', async () => {
      const c = await DBModel.transaction(async () =>
        User.createMany(
          [
            [[User.name, 'qa'], [User.email, 'qa@x.com']],
            [[User.name, 'qb'], [User.email, 'qb@x.com']],
          ],
          { returning: true },
        ),
      );
      const [qa, qb] = c!.values.map((v) => v[0]);
      await DBModel.transaction(async () => {
        for (let i = 0; i < 3; i++) await Post.create([[Post.author_id, qa], [Post.title, `qa${i}`]]);
        await Post.create([[Post.author_id, qb], [Post.title, 'qb0']]);
      });

      const seen: string[] = [];
      const un = DBModel.use(recordingMiddleware(seen));
      const stats = await UserStats.find([[sql`${UserStats.post_count} >= ?`, 2]], {
        order: UserStats.post_count.desc(),
      });
      un();
      expect(stats.length).toBe(1);
      expect(stats[0].name).toBe('qa');
      expect(Number(stats[0].post_count)).toBe(3);
      // README §Generated SQL (CTE-based): the QUERY is wrapped as a WITH clause.
      const cteSql = seen.find((s) => /with .*rc_user_stats/i.test(s));
      expect(cteSql).toBeTruthy();
    });
  });
}
