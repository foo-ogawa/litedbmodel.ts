/**
 * ORM Benchmark: litedbmodel vs Prisma vs Kysely vs Drizzle vs TypeORM
 * 
 * Based on Prisma's official orm-benchmarks:
 * https://github.com/prisma/orm-benchmarks
 * 
 * Reference article:
 * https://izanami.dev/post/1e3fa298-252c-4f6e-8bcc-b225d53c95fb
 * 
 * Test operations:
 * - Find all
 * - Filter, paginate & sort
 * - Nested find all (1-level nesting)
 * - Find first
 * - Nested find first
 * - Find unique
 * - Nested find unique
 * - Create
 * - Nested create
 * - Update
 * - Nested update
 * - Upsert
 * - Nested upsert
 * - Delete
 */
import 'reflect-metadata';
import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============================================
// Configuration
// ============================================

const config = {
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  database: process.env.DB_NAME || 'testdb',
  user: process.env.DB_USER || 'testuser',
  password: process.env.DB_PASSWORD || 'testpass',
};

const ROUNDS = 5;              // Number of rounds (each round runs all ORMs)
const ITERATIONS = 50;         // Each test runs 50 times per round
const WARMUP_ITERATIONS = 5;

// ============================================
// litedbmodel Setup
// ============================================

import { DBModel, model, column, ColumnsOf, closeAllPools, hasMany, belongsTo } from 'litedbmodel';

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
// Prisma Setup
// ============================================

import { PrismaClient } from '@prisma/client';

// ============================================
// Kysely Setup
// ============================================

import { Kysely, PostgresDialect, Generated, sql } from 'kysely';
import pg from 'pg';

interface KyselyDB {
  benchmark_users: {
    id: Generated<number>;
    email: string;
    name: string | null;
    created_at: Generated<Date>;
    updated_at: Generated<Date>;
  };
  benchmark_posts: {
    id: Generated<number>;
    title: string;
    content: string | null;
    published: Generated<boolean>;
    author_id: number;
    created_at: Generated<Date>;
  };
  benchmark_comments: {
    id: Generated<number>;
    body: string;
    post_id: number;
    created_at: Generated<Date>;
  };
}

// ============================================
// Drizzle Setup
// ============================================

import { drizzle } from 'drizzle-orm/node-postgres';
import { pgTable, serial, varchar, integer, boolean, timestamp, text, primaryKey } from 'drizzle-orm/pg-core';
import { eq, desc, and, asc, sql as drizzleSql, relations, inArray } from 'drizzle-orm';

const drizzleUsers = pgTable('benchmark_users', {
  id: serial('id').primaryKey(),
  email: varchar('email', { length: 255 }).notNull(),
  name: varchar('name', { length: 255 }),
  created_at: timestamp('created_at').defaultNow(),
  updated_at: timestamp('updated_at').defaultNow(),
});

const drizzlePosts = pgTable('benchmark_posts', {
  id: serial('id').primaryKey(),
  title: varchar('title', { length: 255 }).notNull(),
  content: text('content'),
  published: boolean('published').default(false),
  author_id: integer('author_id').notNull(),
  created_at: timestamp('created_at').defaultNow(),
});

const drizzleComments = pgTable('benchmark_comments', {
  id: serial('id').primaryKey(),
  body: text('body').notNull(),
  post_id: integer('post_id').notNull(),
  created_at: timestamp('created_at').defaultNow(),
});

// Composite key tables
const drizzleTenantUsers = pgTable('benchmark_tenant_users', {
  tenant_id: integer('tenant_id').notNull(),
  user_id: integer('user_id').notNull(),
  name: varchar('name', { length: 255 }),
});

const drizzleTenantPosts = pgTable('benchmark_tenant_posts', {
  tenant_id: integer('tenant_id').notNull(),
  post_id: integer('post_id').notNull(),
  user_id: integer('user_id').notNull(),
  title: varchar('title', { length: 255 }).notNull(),
});

const drizzleTenantComments = pgTable('benchmark_tenant_comments', {
  tenant_id: integer('tenant_id').notNull(),
  comment_id: integer('comment_id').notNull(),
  post_id: integer('post_id').notNull(),
  body: text('body'),
}, (table) => [primaryKey({ columns: [table.tenant_id, table.comment_id] })]);

// Drizzle Relations - Single Key
const usersRelations = relations(drizzleUsers, ({ many }) => ({
  posts: many(drizzlePosts),
}));

const postsRelations = relations(drizzlePosts, ({ one, many }) => ({
  author: one(drizzleUsers, { fields: [drizzlePosts.author_id], references: [drizzleUsers.id] }),
  comments: many(drizzleComments),
}));

const commentsRelations = relations(drizzleComments, ({ one }) => ({
  post: one(drizzlePosts, { fields: [drizzleComments.post_id], references: [drizzlePosts.id] }),
}));

// Drizzle Relations - Composite Key
const tenantUsersRelations = relations(drizzleTenantUsers, ({ many }) => ({
  posts: many(drizzleTenantPosts),
}));

const tenantPostsRelations = relations(drizzleTenantPosts, ({ one, many }) => ({
  user: one(drizzleTenantUsers, {
    fields: [drizzleTenantPosts.tenant_id, drizzleTenantPosts.user_id],
    references: [drizzleTenantUsers.tenant_id, drizzleTenantUsers.user_id],
  }),
  comments: many(drizzleTenantComments),
}));

const tenantCommentsRelations = relations(drizzleTenantComments, ({ one }) => ({
  post: one(drizzleTenantPosts, {
    fields: [drizzleTenantComments.tenant_id, drizzleTenantComments.post_id],
    references: [drizzleTenantPosts.tenant_id, drizzleTenantPosts.post_id],
  }),
}));

// Drizzle Schema (needed for query API with relations)
const drizzleSchema = {
  users: drizzleUsers,
  posts: drizzlePosts,
  comments: drizzleComments,
  tenantUsers: drizzleTenantUsers,
  tenantPosts: drizzleTenantPosts,
  tenantComments: drizzleTenantComments,
  usersRelations,
  postsRelations,
  commentsRelations,
  tenantUsersRelations,
  tenantPostsRelations,
  tenantCommentsRelations,
};

// ============================================
// TypeORM Setup
// ============================================

import { DataSource, Entity, PrimaryGeneratedColumn, PrimaryColumn, Column as TypeORMColumn, Repository, ManyToOne, OneToMany, JoinColumn, In } from 'typeorm';

@Entity('benchmark_users')
class TypeORMUser {
  @PrimaryGeneratedColumn()
  id!: number;
  
  @TypeORMColumn({ type: 'varchar', length: 255 })
  email!: string;
  
  @TypeORMColumn({ type: 'varchar', length: 255, nullable: true })
  name!: string | null;
  
  @TypeORMColumn({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
  
  @TypeORMColumn({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  updated_at!: Date;
  
  @OneToMany(() => TypeORMPost, post => post.author)
  posts!: TypeORMPost[];
}

@Entity('benchmark_posts')
class TypeORMPost {
  @PrimaryGeneratedColumn()
  id!: number;
  
  @TypeORMColumn({ type: 'varchar', length: 255 })
  title!: string;
  
  @TypeORMColumn({ type: 'text', nullable: true })
  content!: string | null;
  
  @TypeORMColumn({ type: 'boolean', default: false })
  published!: boolean;
  
  @TypeORMColumn({ type: 'int' })
  author_id!: number;
  
  @TypeORMColumn({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
  
  @ManyToOne(() => TypeORMUser, user => user.posts)
  @JoinColumn({ name: 'author_id' })
  author!: TypeORMUser;
  
  @OneToMany(() => TypeORMComment, comment => comment.post)
  comments!: TypeORMComment[];
}

@Entity('benchmark_comments')
class TypeORMComment {
  @PrimaryGeneratedColumn()
  id!: number;
  
  @TypeORMColumn({ type: 'text' })
  body!: string;
  
  @TypeORMColumn({ type: 'int' })
  post_id!: number;
  
  @TypeORMColumn({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  created_at!: Date;
  
  @ManyToOne(() => TypeORMPost, post => post.comments)
  @JoinColumn({ name: 'post_id' })
  post!: TypeORMPost;
}

// Composite key entities
@Entity('benchmark_tenant_users')
class TypeORMTenantUser {
  @PrimaryColumn({ type: 'int' })
  tenant_id!: number;
  
  @PrimaryColumn({ type: 'int' })
  user_id!: number;
  
  @TypeORMColumn({ type: 'varchar', length: 255, nullable: true })
  name!: string | null;
  
  @OneToMany(() => TypeORMTenantPost, post => post.user)
  posts!: TypeORMTenantPost[];
}

@Entity('benchmark_tenant_posts')
class TypeORMTenantPost {
  @PrimaryColumn({ type: 'int' })
  tenant_id!: number;
  
  @PrimaryColumn({ type: 'int' })
  post_id!: number;
  
  @TypeORMColumn({ type: 'int' })
  user_id!: number;
  
  @TypeORMColumn({ type: 'varchar', length: 255 })
  title!: string;
  
  @ManyToOne(() => TypeORMTenantUser, user => user.posts)
  @JoinColumn([{ name: 'tenant_id', referencedColumnName: 'tenant_id' }, { name: 'user_id', referencedColumnName: 'user_id' }])
  user!: TypeORMTenantUser;
  
  @OneToMany(() => TypeORMTenantComment, comment => comment.post)
  comments!: TypeORMTenantComment[];
}

@Entity('benchmark_tenant_comments')
class TypeORMTenantComment {
  @PrimaryColumn({ type: 'int' })
  tenant_id!: number;
  
  @PrimaryColumn({ type: 'int' })
  comment_id!: number;
  
  @TypeORMColumn({ type: 'int' })
  post_id!: number;
  
  @TypeORMColumn({ type: 'text', nullable: true })
  body!: string | null;
  
  @ManyToOne(() => TypeORMTenantPost, post => post.comments)
  @JoinColumn([{ name: 'tenant_id', referencedColumnName: 'tenant_id' }, { name: 'post_id', referencedColumnName: 'post_id' }])
  post!: TypeORMTenantPost;
}

// ============================================
// Types
// ============================================

interface BenchmarkResult {
  name: string;
  median: number;
  iqr: number;
  stdDev: number;
  min: number;
  max: number;
}

// ============================================
// Benchmark Utilities
// ============================================

/**
 * Run a single benchmark iteration
 */
async function runIteration(fn: () => Promise<unknown>): Promise<number> {
  const start = performance.now();
  await fn();
  const end = performance.now();
  return end - start;
}

/**
 * Warmup a function
 */
async function warmup(fn: () => Promise<unknown>, iterations: number): Promise<void> {
  for (let i = 0; i < iterations; i++) {
    await fn();
  }
}

/**
 * Compute statistics from times (based on Prisma benchmark methodology)
 */
function computeStats(allTimes: number[]): BenchmarkResult {
  const sorted = [...allTimes].sort((a, b) => a - b);
  const len = sorted.length;
  
  // Median
  const median = len % 2 === 0
    ? (sorted[len / 2 - 1] + sorted[len / 2]) / 2
    : sorted[Math.floor(len / 2)];
  
  // IQR (Interquartile Range)
  const q1Index = Math.floor(len * 0.25);
  const q3Index = Math.floor(len * 0.75);
  const q1 = sorted[q1Index];
  const q3 = sorted[q3Index];
  const iqr = q3 - q1;
  
  // Standard Deviation
  const avg = allTimes.reduce((a, b) => a + b, 0) / len;
  const variance = allTimes.reduce((sum, t) => sum + Math.pow(t - avg, 2), 0) / len;
  const stdDev = Math.sqrt(variance);
  
  return {
    name: '',
    median: Math.round(median * 100) / 100,
    iqr: Math.round(iqr * 100) / 100,
    stdDev: Math.round(stdDev * 100) / 100,
    min: Math.round(sorted[0] * 100) / 100,
    max: Math.round(sorted[len - 1] * 100) / 100,
  };
}

function printResults(category: string, results: BenchmarkResult[]) {
  console.log(`\n${'='.repeat(90)}`);
  console.log(`📊 ${category}`);
  console.log('='.repeat(90));
  console.log('| ORM          | Median (ms) | IQR (ms) | StdDev (ms) | Min (ms) | Max (ms) |');
  console.log('|--------------|-------------|----------|-------------|----------|----------|');
  
  // Sort by median time
  results.sort((a, b) => a.median - b.median);
  
  const fastest = results[0];
  
  for (const r of results) {
    const isFastest = r === fastest;
    const name = (isFastest ? `${r.name} 🏆` : r.name).padEnd(12);
    const median = r.median.toFixed(2).padStart(11);
    const iqr = r.iqr.toFixed(2).padStart(8);
    const stdDev = r.stdDev.toFixed(2).padStart(11);
    const min = r.min.toFixed(2).padStart(8);
    const max = r.max.toFixed(2).padStart(8);
    console.log(`| ${name} | ${median} | ${iqr} | ${stdDev} | ${min} | ${max} |`);
  }
  
  // Show relative performance
  console.log('\nRelative performance (vs fastest):');
  for (const r of results) {
    const relative = (r.median / fastest.median).toFixed(2);
    console.log(`  ${r.name}: ${relative}x`);
  }
}

// ============================================
// Test Definitions
// ============================================

interface TestDefinition {
  name: string;
  tests: {
    orm: string;
    fn: () => Promise<unknown>;
  }[];
}

// ============================================
// Main Benchmark
// ============================================

async function main() {
  console.log('🚀 ORM Benchmark: litedbmodel vs Prisma vs Kysely vs Drizzle vs TypeORM');
  console.log(`   Based on Prisma orm-benchmarks methodology`);
  console.log(`   Rounds: ${ROUNDS}`);
  console.log(`   Iterations per ORM per round: ${ITERATIONS}`);
  console.log(`   Total iterations per ORM: ${ROUNDS * ITERATIONS}`);
  console.log(`   Warmup iterations: ${WARMUP_ITERATIONS}`);
  console.log(`   Database: PostgreSQL @ ${config.host}:${config.port}/${config.database}`);
  
  // Initialize ORMs
  console.log('\n⏳ Initializing ORMs...');
  
  // litedbmodel
  DBModel.setConfig(config);
  
  // Prisma
  const prisma = new PrismaClient();
  await prisma.$connect();
  
  // Kysely
  const kyselyPool = new pg.Pool(config);
  const kysely = new Kysely<KyselyDB>({
    dialect: new PostgresDialect({ pool: kyselyPool }),
  });
  
  // Drizzle (with schema for query API / relation loading)
  const drizzlePool = new pg.Pool(config);
  const drizzleDb = drizzle(drizzlePool, { schema: drizzleSchema });
  
  // TypeORM
  const typeormDS = new DataSource({
    type: 'postgres',
    host: config.host,
    port: config.port,
    database: config.database,
    username: config.user,
    password: config.password,
    entities: [TypeORMUser, TypeORMPost, TypeORMComment, TypeORMTenantUser, TypeORMTenantPost, TypeORMTenantComment],
    synchronize: false,
    logging: false,
  });
  await typeormDS.initialize();
  const typeormUserRepo = typeormDS.getRepository(TypeORMUser);
  const typeormPostRepo = typeormDS.getRepository(TypeORMPost);
  
  console.log('✅ All ORMs initialized\n');
  
  // Counters for INSERT/UPDATE tests
  let createCounter = 10000;
  let upsertCounter = 20000;
  
  // Define all test categories (based on Prisma orm-benchmarks)
  const testCategories: TestDefinition[] = [
    // ============================================
    // Find All
    // ============================================
    {
      name: 'Find all (limit 100)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.find([], { limit: 100 }) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findMany({ take: 100 }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.selectFrom('benchmark_users').selectAll().limit(100).execute() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.select().from(drizzleUsers).limit(100) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.find({ take: 100 }) 
        },
      ],
    },
    
    // ============================================
    // Filter, paginate & sort
    // ============================================
    {
      name: 'Filter, paginate & sort',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LitePost.find([[LitePost.published, true]], { 
            order: LitePost.created_at.desc(), 
            limit: 20, 
            offset: 10 
          }) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.post.findMany({
            where: { published: true },
            orderBy: { createdAt: 'desc' },
            skip: 10,
            take: 20,
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.selectFrom('benchmark_posts')
            .where('published', '=', true)
            .orderBy('created_at', 'desc')
            .offset(10)
            .limit(20)
            .selectAll()
            .execute() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.select().from(drizzlePosts)
            .where(eq(drizzlePosts.published, true))
            .orderBy(desc(drizzlePosts.created_at))
            .offset(10)
            .limit(20) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormPostRepo.find({
            where: { published: true },
            order: { created_at: 'DESC' },
            skip: 10,
            take: 20,
          }) 
        },
      ],
    },
    
    // ============================================
    // Nested find all (1-level nesting) - Auto batch loading
    // ============================================
    {
      name: 'Nested find all (include posts)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            // Auto batch loading via relation
            const users = await LiteUser.find([], { limit: 100 });
            // First access triggers batch load for ALL users' posts
            for (const user of users) {
              await user.posts;
            }
            return users;
          }
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findMany({
            take: 100,
            include: { posts: true },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            // Two queries approach
            const users = await kysely.selectFrom('benchmark_users').selectAll().limit(100).execute();
            if (users.length > 0) {
              const userIds = users.map(u => u.id);
              await kysely.selectFrom('benchmark_posts')
                .where('author_id', 'in', userIds)
                .selectAll()
                .execute();
            }
            return users;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            // Two queries approach
            const users = await drizzleDb.select().from(drizzleUsers).limit(100);
            if (users.length > 0) {
              const userIds = users.map(u => u.id);
              await drizzleDb.select().from(drizzlePosts)
                .where(drizzleSql`${drizzlePosts.author_id} IN ${userIds}`);
            }
            return users;
          }
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.find({
            take: 100,
            relations: ['posts'],
          }) 
        },
      ],
    },
    
    // ============================================
    // Find first
    // ============================================
    {
      name: 'Find first',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.findOne([[`${LiteUser.name} LIKE ?`, 'User%']]) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findFirst({
            where: { name: { startsWith: 'User' } },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.selectFrom('benchmark_users')
            .where('name', 'like', 'User%')
            .selectAll()
            .limit(1)
            .executeTakeFirst() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.select().from(drizzleUsers)
            .where(drizzleSql`${drizzleUsers.name} LIKE 'User%'`)
            .limit(1) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.createQueryBuilder('user')
            .where('user.name LIKE :name', { name: 'User%' })
            .limit(1)
            .getOne() 
        },
      ],
    },
    
    // ============================================
    // Nested find first
    // ============================================
    {
      name: 'Nested find first (include posts)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            const user = await LiteUser.findOne([[`${LiteUser.name} LIKE ?`, 'User%']]);
            if (user) {
              await user.posts;
            }
            return user;
          }
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findFirst({
            where: { name: { startsWith: 'User' } },
            include: { posts: true },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            const user = await kysely.selectFrom('benchmark_users')
              .where('name', 'like', 'User%')
              .selectAll()
              .limit(1)
              .executeTakeFirst();
            if (user) {
              await kysely.selectFrom('benchmark_posts')
                .where('author_id', '=', user.id)
                .selectAll()
                .execute();
            }
            return user;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            const users = await drizzleDb.select().from(drizzleUsers)
              .where(drizzleSql`${drizzleUsers.name} LIKE 'User%'`)
              .limit(1);
            if (users.length > 0) {
              await drizzleDb.select().from(drizzlePosts)
                .where(eq(drizzlePosts.author_id, users[0].id));
            }
            return users[0];
          }
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.createQueryBuilder('user')
            .leftJoinAndSelect('user.posts', 'posts')
            .where('user.name LIKE :name', { name: 'User%' })
            .limit(1)
            .getOne() 
        },
      ],
    },
    
    // ============================================
    // Find unique
    // ============================================
    {
      name: 'Find unique (by email)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.findOne([[LiteUser.email, 'user500@example.com']]) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findUnique({
            where: { email: 'user500@example.com' },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.selectFrom('benchmark_users')
            .where('email', '=', 'user500@example.com')
            .selectAll()
            .executeTakeFirst() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.select().from(drizzleUsers)
            .where(eq(drizzleUsers.email, 'user500@example.com'))
            .limit(1) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.findOneBy({ email: 'user500@example.com' }) 
        },
      ],
    },
    
    // ============================================
    // Nested find unique
    // ============================================
    {
      name: 'Nested find unique (include posts)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            const user = await LiteUser.findOne([[LiteUser.email, 'user500@example.com']]);
            if (user) {
              await user.posts;
            }
            return user;
          }
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.findUnique({
            where: { email: 'user500@example.com' },
            include: { posts: true },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            const user = await kysely.selectFrom('benchmark_users')
              .where('email', '=', 'user500@example.com')
              .selectAll()
              .executeTakeFirst();
            if (user) {
              await kysely.selectFrom('benchmark_posts')
                .where('author_id', '=', user.id)
                .selectAll()
                .execute();
            }
            return user;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            const users = await drizzleDb.select().from(drizzleUsers)
              .where(eq(drizzleUsers.email, 'user500@example.com'))
              .limit(1);
            if (users.length > 0) {
              await drizzleDb.select().from(drizzlePosts)
                .where(eq(drizzlePosts.author_id, users[0].id));
            }
            return users[0];
          }
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.findOne({
            where: { email: 'user500@example.com' },
            relations: ['posts'],
          }) 
        },
      ],
    },
    
    // ============================================
    // Create (all ORMs use transaction for fair comparison)
    // ============================================
    {
      name: 'Create',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => LiteUser.create([
            [LiteUser.email, `bench${createCounter++}@example.com`],
            [LiteUser.name, `Benchmark User`],
          ])) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => tx.user.create({
            data: {
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            },
          }))
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => 
            trx.insertInto('benchmark_users')
              .values({
                email: `bench${createCounter++}@example.com`,
                name: 'Benchmark User',
              })
              .returningAll()
              .executeTakeFirst()
          )
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) =>
            tx.insert(drizzleUsers)
              .values({
                email: `bench${createCounter++}@example.com`,
                name: 'Benchmark User',
              })
              .returning()
          )
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            const user = em.create(TypeORMUser, {
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            });
            return em.save(user);
          })
        },
      ],
    },
    
    // ============================================
    // Nested create (all ORMs use transaction)
    // ============================================
    {
      name: 'Nested create (with post)',
      tests: [
        { 
          orm: 'litedbmodel', 
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
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => tx.user.create({
            data: {
              email: `nested${createCounter++}@example.com`,
              name: 'Nested User',
              posts: {
                create: { title: 'Nested Post', content: 'Content' },
              },
            },
          }))
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            const user = await trx.insertInto('benchmark_users')
              .values({
                email: `nested${createCounter++}@example.com`,
                name: 'Nested User',
              })
              .returningAll()
              .executeTakeFirstOrThrow();
            await trx.insertInto('benchmark_posts')
              .values({
                title: 'Nested Post',
                content: 'Content',
                author_id: user.id,
              })
              .execute();
            return user;
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            const [user] = await tx.insert(drizzleUsers)
              .values({
                email: `nested${createCounter++}@example.com`,
                name: 'Nested User',
              })
              .returning();
            await tx.insert(drizzlePosts)
              .values({
                title: 'Nested Post',
                content: 'Content',
                author_id: user.id,
              });
            return user;
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            const user = em.create(TypeORMUser, {
              email: `nested${createCounter++}@example.com`,
              name: 'Nested User',
            });
            const savedUser = await em.save(user);
            const post = em.create(TypeORMPost, {
              title: 'Nested Post',
              content: 'Content',
              author_id: savedUser.id,
            });
            await em.save(post);
            return savedUser;
          })
        },
      ],
    },
    
    // ============================================
    // Update (all ORMs use transaction)
    // ============================================
    {
      name: 'Update',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Updated User']])) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => tx.user.update({
            where: { id: 100 },
            data: { name: 'Updated User' },
          }))
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) =>
            trx.updateTable('benchmark_users')
              .set({ name: 'Updated User' })
              .where('id', '=', 100)
              .execute()
          )
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) =>
            tx.update(drizzleUsers)
              .set({ name: 'Updated User' })
              .where(eq(drizzleUsers.id, 100))
          )
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) =>
            em.update(TypeORMUser, { id: 100 }, { name: 'Updated User' })
          )
        },
      ],
    },
    
    // ============================================
    // Nested update (all ORMs use transaction)
    // ============================================
    {
      name: 'Nested update (update user + post)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => {
            await LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Nested Updated']]);
            await LitePost.update([[LitePost.author_id, 100]], [[LitePost.title, 'Updated Post']]);
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => tx.user.update({
            where: { id: 100 },
            data: {
              name: 'Nested Updated',
              posts: {
                updateMany: {
                  where: {},
                  data: { title: 'Updated Post' },
                },
              },
            },
          }))
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            await trx.updateTable('benchmark_users')
              .set({ name: 'Nested Updated' })
              .where('id', '=', 100)
              .execute();
            await trx.updateTable('benchmark_posts')
              .set({ title: 'Updated Post' })
              .where('author_id', '=', 100)
              .execute();
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            await tx.update(drizzleUsers)
              .set({ name: 'Nested Updated' })
              .where(eq(drizzleUsers.id, 100));
            await tx.update(drizzlePosts)
              .set({ title: 'Updated Post' })
              .where(eq(drizzlePosts.author_id, 100));
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            await em.update(TypeORMUser, { id: 100 }, { name: 'Nested Updated' });
            await em.update(TypeORMPost, { author_id: 100 }, { title: 'Updated Post' });
          })
        },
      ],
    },
    
    // ============================================
    // Upsert (all ORMs use transaction)
    // ============================================
    {
      name: 'Upsert',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => LiteUser.create(
            [
              [LiteUser.email, `upsert${upsertCounter++}@example.com`],
              [LiteUser.name, 'Upsert User'],
            ],
            { onConflict: LiteUser.email, onConflictUpdate: [LiteUser.name], returning: true }
          )) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => tx.user.upsert({
            where: { email: `upsert${upsertCounter++}@example.com` },
            update: { name: 'Upsert User' },
            create: { email: `upsert${upsertCounter}@example.com`, name: 'Upsert User' },
          }))
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) =>
            trx.insertInto('benchmark_users')
              .values({
                email: `upsert${upsertCounter++}@example.com`,
                name: 'Upsert User',
              })
              .onConflict(oc => oc.column('email').doUpdateSet({ name: 'Upsert User' }))
              .execute()
          )
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) =>
            tx.insert(drizzleUsers)
              .values({
                email: `upsert${upsertCounter++}@example.com`,
                name: 'Upsert User',
              })
              .onConflictDoUpdate({
                target: drizzleUsers.email,
                set: { name: 'Upsert User' },
              })
          )
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) =>
            em.upsert(TypeORMUser,
              { email: `upsert${upsertCounter++}@example.com`, name: 'Upsert User' },
              ['email']
            )
          )
        },
      ],
    },
    
    // ============================================
    // Nested upsert (all ORMs use transaction)
    // ============================================
    {
      name: 'Nested upsert (user + post)',
      tests: [
        { 
          orm: 'litedbmodel', 
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
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => {
            const user = await tx.user.upsert({
              where: { email: `nupsert${upsertCounter++}@example.com` },
              update: { name: 'Nested Upsert' },
              create: {
                email: `nupsert${upsertCounter}@example.com`,
                name: 'Nested Upsert',
                posts: { create: { title: 'Upsert Post' } },
              },
            });
            return user;
          })
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            const user = await trx.insertInto('benchmark_users')
              .values({
                email: `nupsert${upsertCounter++}@example.com`,
                name: 'Nested Upsert',
              })
              .onConflict(oc => oc.column('email').doUpdateSet({ name: 'Nested Upsert' }))
              .returningAll()
              .executeTakeFirstOrThrow();
            await trx.insertInto('benchmark_posts')
              .values({ title: 'Upsert Post', author_id: user.id })
              .execute();
            return user;
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            const [user] = await tx.insert(drizzleUsers)
              .values({
                email: `nupsert${upsertCounter++}@example.com`,
                name: 'Nested Upsert',
              })
              .onConflictDoUpdate({
                target: drizzleUsers.email,
                set: { name: 'Nested Upsert' },
              })
              .returning();
            await tx.insert(drizzlePosts)
              .values({ title: 'Upsert Post', author_id: user.id });
            return user;
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            const result = await em.upsert(TypeORMUser,
              { email: `nupsert${upsertCounter++}@example.com`, name: 'Nested Upsert' },
              ['email']
            );
            const user = await em.findOneBy(TypeORMUser, { email: `nupsert${upsertCounter}@example.com` });
            if (user) {
              const post = em.create(TypeORMPost, { title: 'Upsert Post', author_id: user.id });
              await em.save(post);
            }
            return result;
          })
        },
      ],
    },
    
    // ============================================
    // Delete (all ORMs use transaction)
    // ============================================
    {
      name: 'Delete',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => {
            // First create then delete
            const result = await LiteUser.create([
              [LiteUser.email, `del${createCounter++}@example.com`],
              [LiteUser.name, 'Delete User'],
            ], { returning: true });
            return LiteUser.delete([[LiteUser.id, result!.values[0][0] as number]]);
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => {
            const user = await tx.user.create({
              data: { email: `del${createCounter++}@example.com`, name: 'Delete User' },
            });
            return tx.user.delete({ where: { id: user.id } });
          })
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            const user = await trx.insertInto('benchmark_users')
              .values({ email: `del${createCounter++}@example.com`, name: 'Delete User' })
              .returningAll()
              .executeTakeFirstOrThrow();
            return trx.deleteFrom('benchmark_users').where('id', '=', user.id).execute();
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            const [user] = await tx.insert(drizzleUsers)
              .values({ email: `del${createCounter++}@example.com`, name: 'Delete User' })
              .returning();
            return tx.delete(drizzleUsers).where(eq(drizzleUsers.id, user.id));
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            const user = em.create(TypeORMUser, { email: `del${createCounter++}@example.com`, name: 'Delete User' });
            const saved = await em.save(user);
            return em.delete(TypeORMUser, { id: saved.id });
          })
        },
      ],
    },
    
    // ============================================
    // Create Many (bulk insert)
    // ============================================
    {
      name: 'Create Many (10 records)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => {
            const records = Array.from({ length: 10 }, (_, i) => [
              [LiteUser.email, `bulk${createCounter++}@example.com`],
              [LiteUser.name, `Bulk User ${i}`],
            ] as [[typeof LiteUser.email, string], [typeof LiteUser.name, string]]);
            return LiteUser.createMany(records);
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => {
            return tx.user.createMany({
              data: Array.from({ length: 10 }, (_, i) => ({
                email: `bulk${createCounter++}@example.com`,
                name: `Bulk User ${i}`,
              })),
            });
          })
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            return trx.insertInto('benchmark_users')
              .values(Array.from({ length: 10 }, (_, i) => ({
                email: `bulk${createCounter++}@example.com`,
                name: `Bulk User ${i}`,
              })))
              .execute();
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            return tx.insert(drizzleUsers)
              .values(Array.from({ length: 10 }, (_, i) => ({
                email: `bulk${createCounter++}@example.com`,
                name: `Bulk User ${i}`,
              })));
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            return em.insert(TypeORMUser, Array.from({ length: 10 }, (_, i) => ({
              email: `bulk${createCounter++}@example.com`,
              name: `Bulk User ${i}`,
            })));
          })
        },
      ],
    },
    
    // ============================================
    // Upsert Many (bulk upsert)
    // ============================================
    {
      name: 'Upsert Many (10 records)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => {
            const records = Array.from({ length: 10 }, (_, i) => [
              [LiteUser.email, `upsertbulk${upsertCounter++}@example.com`],
              [LiteUser.name, `Upsert Bulk ${i}`],
            ] as [[typeof LiteUser.email, string], [typeof LiteUser.name, string]]);
            return LiteUser.createMany(records, { 
              onConflict: LiteUser.email, 
              onConflictUpdate: [LiteUser.name] 
            });
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => {
            // Prisma createMany doesn't support onConflict update
            // Must use individual upserts
            return Promise.all(Array.from({ length: 10 }, (_, i) => 
              tx.user.upsert({
                where: { email: `upsertbulk${upsertCounter++}@example.com` },
                update: { name: `Upsert Bulk ${i}` },
                create: { email: `upsertbulk${upsertCounter}@example.com`, name: `Upsert Bulk ${i}` },
              })
            ));
          })
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            return trx.insertInto('benchmark_users')
              .values(Array.from({ length: 10 }, (_, i) => ({
                email: `upsertbulk${upsertCounter++}@example.com`,
                name: `Upsert Bulk ${i}`,
              })))
              .onConflict(oc => oc.column('email').doUpdateSet({ name: 'Upsert Bulk' }))
              .execute();
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            return tx.insert(drizzleUsers)
              .values(Array.from({ length: 10 }, (_, i) => ({
                email: `upsertbulk${upsertCounter++}@example.com`,
                name: `Upsert Bulk ${i}`,
              })))
              .onConflictDoUpdate({
                target: drizzleUsers.email,
                set: { name: drizzleSql`excluded.name` },
              });
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            return em.upsert(TypeORMUser, 
              Array.from({ length: 10 }, (_, i) => ({
                email: `upsertbulk${upsertCounter++}@example.com`,
                name: `Upsert Bulk ${i}`,
              })),
              ['email']
            );
          })
        },
      ],
    },
    
    // ============================================
    // Update Many (different values per row)
    // Only litedbmodel supports this natively
    // ============================================
    {
      name: 'Update Many (10 different values)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.transaction(async () => {
            // Update 10 users with different names in a single query
            return LiteUser.updateMany(
              Array.from({ length: 10 }, (_, i) => [
                [LiteUser.id, 100 + i],
                [LiteUser.name, `Updated Different ${i}`],
              ] as const),
              { keyColumns: [LiteUser.id] }
            );
          })
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.$transaction(async (tx) => {
            // Prisma requires individual updates - N queries
            return Promise.all(Array.from({ length: 10 }, (_, i) => 
              tx.user.update({
                where: { id: 100 + i },
                data: { name: `Updated Different ${i}` },
              })
            ));
          })
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.transaction().execute(async (trx) => {
            // Kysely requires individual updates - N queries
            return Promise.all(Array.from({ length: 10 }, (_, i) => 
              trx.updateTable('benchmark_users')
                .set({ name: `Updated Different ${i}` })
                .where('id', '=', 100 + i)
                .execute()
            ));
          })
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.transaction(async (tx) => {
            // Drizzle requires individual updates - N queries
            return Promise.all(Array.from({ length: 10 }, (_, i) => 
              tx.update(drizzleUsers)
                .set({ name: `Updated Different ${i}` })
                .where(eq(drizzleUsers.id, 100 + i))
            ));
          })
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormDS.transaction(async (em) => {
            // TypeORM requires individual updates - N queries
            return Promise.all(Array.from({ length: 10 }, (_, i) => 
              em.update(TypeORMUser, { id: 100 + i }, { name: `Updated Different ${i}` })
            ));
          })
        },
      ],
    },
    
    // ============================================
    // Nested Relations (100 users → 1000 posts → 10000 comments)
    // Simulates real-world deep relation traversal
    // ============================================
    {
      name: 'Nested relations (100→1000→10000)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            // Fetch first 100 users by ID (they have 10 posts each = 1000 posts)
            const users = await LiteUser.find([], { limit: 100, order: LiteUser.id.asc() });
            let commentCount = 0;
            // Access all posts via lazy loading (triggers batch load)
            for (const user of users) {
              const posts = await user.posts;
              for (const post of posts) {
                // Access all comments via lazy loading (triggers batch load)
                const comments = await post.comments;
                for (const _comment of comments) {
                  commentCount++;
                }
              }
            }
            // Verify we accessed all 10000 comments
            if (commentCount !== 10000) {
              console.warn(`litedbmodel: Expected 10000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
        { 
          orm: 'Prisma', 
          fn: async () => {
            const users = await prisma.user.findMany({
              take: 100,
              orderBy: { id: 'asc' },
              include: { 
                posts: {
                  include: { comments: true }
                }
              },
            });
            let commentCount = 0;
            for (const user of users) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            if (commentCount !== 10000) {
              console.warn(`Prisma: Expected 10000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            // Load users
            const users = await kysely.selectFrom('benchmark_users').selectAll().orderBy('id').limit(100).execute();
            const userIds = users.map(u => u.id);
            
            // Load posts for these users
            const posts = await kysely.selectFrom('benchmark_posts')
              .where('author_id', 'in', userIds)
              .selectAll()
              .execute();
            const postIds = posts.map(p => p.id);
            
            // Load comments for these posts
            const comments = await kysely.selectFrom('benchmark_comments')
              .where('post_id', 'in', postIds)
              .selectAll()
              .execute();
            
            // Group posts by user
            const postsByUser = new Map<number, typeof posts>();
            for (const post of posts) {
              if (!postsByUser.has(post.author_id)) postsByUser.set(post.author_id, []);
              postsByUser.get(post.author_id)!.push(post);
            }
            
            // Group comments by post
            const commentsByPost = new Map<number, typeof comments>();
            for (const comment of comments) {
              if (!commentsByPost.has(comment.post_id)) commentsByPost.set(comment.post_id, []);
              commentsByPost.get(comment.post_id)!.push(comment);
            }
            
            // Iterate through all
            let commentCount = 0;
            for (const user of users) {
              const userPosts = postsByUser.get(user.id) || [];
              for (const post of userPosts) {
                const postComments = commentsByPost.get(post.id) || [];
                for (const _comment of postComments) {
                  commentCount++;
                }
              }
            }
            if (commentCount !== 10000) {
              console.warn(`Kysely: Expected 10000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            // Use Drizzle's query API with relations (LATERAL JOIN internally)
            const users = await drizzleDb.query.users.findMany({
              limit: 100,
              orderBy: asc(drizzleUsers.id),
              with: { posts: { with: { comments: true } } }
            });
            let commentCount = 0;
            for (const user of users) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            if (commentCount !== 10000) {
              console.warn(`Drizzle: Expected 10000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const users = await typeormUserRepo.find({
              take: 100,
              order: { id: 'ASC' },
              relations: ['posts', 'posts.comments'],
            });
            let commentCount = 0;
            for (const user of users) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            if (commentCount !== 10000) {
              console.warn(`TypeORM: Expected 10000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
      ],
    },
    
    // ============================================
    // Nested Relations - Composite Key (5 tenants)
    // 100 users across 5 tenants → 1000 posts → 5000 comments
    // Tests proper multi-tenant batch loading with composite foreign keys
    // ============================================
    {
      name: 'Nested relations (composite key, 5 tenants)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            // Fetch users from 5 tenants (20 users per tenant = 100 users total)
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
            // 100 users × 10 posts × 10 comments = 10000, but only 5 tenants have comments
            // 100 users × 10 posts = 1000 posts, each post has 10 comments = 10000 comments
            // But comments exist only for tenants 1-5, so: 5 tenants × 100 users/tenant × 10 posts × 10 comments
            // Actually: 5 tenants × 20 users × 10 posts × 10 comments = 10000
            if (commentCount < 5000) {
              console.warn(`litedbmodel (composite): Expected >= 5000 comments, got ${commentCount}`);
            }
            return users;
          }
        },
        { 
          orm: 'Prisma', 
          fn: async () => {
            const users = await prisma.tenantUser.findMany({
              where: { tenant_id: { in: [1, 2, 3, 4, 5] } },
              take: 100,
              include: { 
                posts: {
                  include: { comments: true }
                }
              },
            });
            let commentCount = 0;
            for (const user of users) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            return users;
          }
        },
        // Kysely: N/A - Cannot properly batch load composite FK (would need manual tuple matching)
        { 
          orm: 'Drizzle', 
          fn: async () => {
            // Use Drizzle's query API with relations (LATERAL JOIN internally)
            const users = await drizzleDb.query.tenantUsers.findMany({
              where: inArray(drizzleTenantUsers.tenant_id, [1, 2, 3, 4, 5]),
              limit: 100,
              with: { posts: { with: { comments: true } } }
            });
            let commentCount = 0;
            for (const user of users) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            return users;
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const users = await typeormDS.getRepository(TypeORMTenantUser).find({
              where: { tenant_id: In([1, 2, 3, 4, 5]) },
              take: 100,
              relations: ['posts', 'posts.comments'],
            });
            let commentCount = 0;
            for (const user of users as any[]) {
              for (const post of user.posts) {
                for (const _comment of post.comments) {
                  commentCount++;
                }
              }
            }
            return users;
          }
        },
      ],
    },
  ];
  
  // Store all results
  const allResults: Map<string, Map<string, number[]>> = new Map();
  
  // Initialize result storage
  for (const category of testCategories) {
    const categoryMap = new Map<string, number[]>();
    for (const test of category.tests) {
      categoryMap.set(test.orm, []);
    }
    allResults.set(category.name, categoryMap);
  }
  
  // Warmup all tests
  console.log('⏳ Warming up...');
  for (const category of testCategories) {
    for (const test of category.tests) {
      await warmup(test.fn, WARMUP_ITERATIONS);
    }
  }
  
  // Cleanup warmup data (keep seed data: 1000 users, 5500 posts)
  await DBModel.execute('DELETE FROM benchmark_posts WHERE id > 5500');
  await DBModel.execute('DELETE FROM benchmark_users WHERE id > 1000');
  
  // Reset counters
  createCounter = 10000;
  upsertCounter = 20000;
  
  console.log('✅ Warmup complete\n');
  
  // Run benchmark rounds
  for (let round = 1; round <= ROUNDS; round++) {
    console.log(`🔄 Round ${round}/${ROUNDS}...`);
    
    for (const category of testCategories) {
      const categoryResults = allResults.get(category.name)!;
      
      // Run each ORM's iterations for this category
      for (const test of category.tests) {
        const times = categoryResults.get(test.orm)!;
        
        for (let i = 0; i < ITERATIONS; i++) {
          const time = await runIteration(test.fn);
          times.push(time);
        }
      }
    }
    
    // Cleanup inserted data after each round (keep seed data: 1000 users, 5500 posts)
    if (round < ROUNDS) {
      await DBModel.execute('DELETE FROM benchmark_posts WHERE id > 5500');
      await DBModel.execute('DELETE FROM benchmark_users WHERE id > 1000');
    }
  }
  
  console.log('\n✅ All rounds complete!\n');
  
  // Collect all results for CSV export
  const csvRows: string[] = ['Operation,ORM,Median,IQR,StdDev,Min,Max,Iterations'];
  
  // Print results for each category
  for (const category of testCategories) {
    const categoryResults = allResults.get(category.name)!;
    const results: BenchmarkResult[] = [];
    
    for (const [orm, times] of categoryResults) {
      const stats = computeStats(times);
      stats.name = orm;
      results.push(stats);
      
      // Add to CSV
      csvRows.push(`"${category.name}","${orm}",${stats.median.toFixed(4)},${stats.iqr.toFixed(4)},${stats.stdDev.toFixed(4)},${stats.min.toFixed(4)},${stats.max.toFixed(4)},${times.length}`);
    }
    
    printResults(category.name, results);
  }
  
  // Save CSV to file
  const csvPath = path.join(__dirname, 'results', 'benchmark-results.csv');
  await fs.mkdir(path.join(__dirname, 'results'), { recursive: true });
  await fs.writeFile(csvPath, csvRows.join('\n'));
  console.log(`\n📊 Results saved to: ${csvPath}`);
  
  // ============================================
  // Summary Table (Median comparison)
  // ============================================
  
  console.log('\n' + '='.repeat(100));
  console.log('📋 SUMMARY - Median (ms)');
  console.log('='.repeat(100));
  console.log('| Operation                       | litedbmodel | Prisma    | Kysely    | Drizzle   | TypeORM   |');
  console.log('|---------------------------------|-------------|-----------|-----------|-----------|-----------|');
  
  for (const category of testCategories) {
    const categoryResults = allResults.get(category.name)!;
    const row: string[] = [category.name.padEnd(31)];
    
    const medians: { orm: string; median: number }[] = [];
    for (const [orm, times] of categoryResults) {
      const stats = computeStats(times);
      medians.push({ orm, median: stats.median });
    }
    
    const fastest = Math.min(...medians.map(m => m.median));
    
    for (const orm of ['litedbmodel', 'Prisma', 'Kysely', 'Drizzle', 'TypeORM']) {
      const entry = medians.find(m => m.orm === orm);
      if (entry) {
        const isFastest = entry.median === fastest;
        const val = isFastest ? `**${entry.median.toFixed(2)}ms**` : `${entry.median.toFixed(2)}ms`;
        row.push(val.padStart(11));
      } else {
        row.push('-'.padStart(11));
      }
    }
    
    console.log(`| ${row.join(' | ')} |`);
  }
  
  // ============================================
  // Cleanup
  // ============================================
  
  console.log('\n⏳ Cleaning up...');
  
  // Delete benchmark-inserted data (keep seed data: 1000 users, 5500 posts)
  await DBModel.execute('DELETE FROM benchmark_posts WHERE id > 5500');
  await DBModel.execute('DELETE FROM benchmark_users WHERE id > 1000');
  
  await closeAllPools();
  await prisma.$disconnect();
  await kyselyPool.end();
  await drizzlePool.end();
  await typeormDS.destroy();
  
  console.log('✅ Benchmark complete!\n');
  
  // Methodology
  console.log('='.repeat(80));
  console.log('📋 METHODOLOGY');
  console.log('='.repeat(80));
  console.log(`
Based on Prisma's official orm-benchmarks:
https://github.com/prisma/orm-benchmarks

Reference article:
https://izanami.dev/post/1e3fa298-252c-4f6e-8bcc-b225d53c95fb

Test Conditions:
- ${ROUNDS} rounds × ${ITERATIONS} iterations = ${ROUNDS * ITERATIONS} total iterations per ORM
- Interleaved execution to reduce environmental variance
- PostgreSQL running locally (Docker) to eliminate network latency
- Warmup: ${WARMUP_ITERATIONS} iterations before measurement
- Metrics: Median, IQR (Interquartile Range), StdDev (Standard Deviation)

Operations tested:
- Find all (limit 100)
- Filter, paginate & sort
- Nested find all (1-level nesting with posts)
- Find first
- Nested find first
- Find unique (by email)
- Nested find unique
- Create
- Nested create (user + post)
- Update
- Nested update
- Upsert
- Nested upsert
- Delete

Lower Median = better performance
Lower IQR/StdDev = more consistent results
`);
}

main().catch(console.error);
