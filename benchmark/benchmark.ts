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

const ROUNDS = 10;             // Number of rounds (each round runs all ORMs)
const ITERATIONS = 100;        // Each test runs 100 times per round
const WARMUP_ITERATIONS = 10;

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
    created_at: Date;
    updated_at: Date;
  };
  benchmark_posts: {
    id: Generated<number>;
    title: string;
    content: string | null;
    published: boolean;
    author_id: number;
    created_at: Date;
  };
  benchmark_comments: {
    id: Generated<number>;
    body: string;
    post_id: number;
    created_at: Date;
  };
}

// ============================================
// Drizzle Setup
// ============================================

import { drizzle } from 'drizzle-orm/node-postgres';
import { pgTable, serial, varchar, integer, boolean, timestamp, text } from 'drizzle-orm/pg-core';
import { eq, desc, and, asc, sql as drizzleSql } from 'drizzle-orm';

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

// ============================================
// TypeORM Setup
// ============================================

import { DataSource, Entity, PrimaryGeneratedColumn, Column as TypeORMColumn, Repository, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

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
  
  // Drizzle
  const drizzlePool = new pg.Pool(config);
  const drizzleDb = drizzle(drizzlePool);
  
  // TypeORM
  const typeormDS = new DataSource({
    type: 'postgres',
    host: config.host,
    port: config.port,
    database: config.database,
    username: config.user,
    password: config.password,
    entities: [TypeORMUser, TypeORMPost, TypeORMComment],
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
    // Create
    // ============================================
    {
      name: 'Create',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.create([
            [LiteUser.email, `bench${createCounter++}@example.com`],
            [LiteUser.name, `Benchmark User`],
          ]) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.create({
            data: {
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.insertInto('benchmark_users')
            .values({
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            })
            .returningAll()
            .executeTakeFirst() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.insert(drizzleUsers)
            .values({
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            })
            .returning() 
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const user = typeormUserRepo.create({
              email: `bench${createCounter++}@example.com`,
              name: 'Benchmark User',
            });
            return typeormUserRepo.save(user);
          }
        },
      ],
    },
    
    // ============================================
    // Nested create
    // ============================================
    {
      name: 'Nested create (with post)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            const user = await LiteUser.create([
              [LiteUser.email, `nested${createCounter++}@example.com`],
              [LiteUser.name, `Nested User`],
            ]);
            await LitePost.create([
              [LitePost.title, 'Nested Post'],
              [LitePost.content, 'Content'],
              [LitePost.author_id, user.id!],
            ]);
            return user;
          }
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.create({
            data: {
              email: `nested${createCounter++}@example.com`,
              name: 'Nested User',
              posts: {
                create: { title: 'Nested Post', content: 'Content' },
              },
            },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            const user = await kysely.insertInto('benchmark_users')
              .values({
                email: `nested${createCounter++}@example.com`,
                name: 'Nested User',
              })
              .returningAll()
              .executeTakeFirstOrThrow();
            await kysely.insertInto('benchmark_posts')
              .values({
                title: 'Nested Post',
                content: 'Content',
                author_id: user.id,
              })
              .execute();
            return user;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            const [user] = await drizzleDb.insert(drizzleUsers)
              .values({
                email: `nested${createCounter++}@example.com`,
                name: 'Nested User',
              })
              .returning();
            await drizzleDb.insert(drizzlePosts)
              .values({
                title: 'Nested Post',
                content: 'Content',
                author_id: user.id,
              });
            return user;
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const user = typeormUserRepo.create({
              email: `nested${createCounter++}@example.com`,
              name: 'Nested User',
            });
            const savedUser = await typeormUserRepo.save(user);
            const post = typeormPostRepo.create({
              title: 'Nested Post',
              content: 'Content',
              author_id: savedUser.id,
            });
            await typeormPostRepo.save(post);
            return savedUser;
          }
        },
      ],
    },
    
    // ============================================
    // Update
    // ============================================
    {
      name: 'Update',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Updated User']]) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.update({
            where: { id: 100 },
            data: { name: 'Updated User' },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.updateTable('benchmark_users')
            .set({ name: 'Updated User' })
            .where('id', '=', 100)
            .execute() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.update(drizzleUsers)
            .set({ name: 'Updated User' })
            .where(eq(drizzleUsers.id, 100)) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.update({ id: 100 }, { name: 'Updated User' }) 
        },
      ],
    },
    
    // ============================================
    // Nested update
    // ============================================
    {
      name: 'Nested update (update user + post)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            await LiteUser.update([[LiteUser.id, 100]], [[LiteUser.name, 'Nested Updated']]);
            await LitePost.update([[LitePost.author_id, 100]], [[LitePost.title, 'Updated Post']]);
          }
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.update({
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
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            await kysely.updateTable('benchmark_users')
              .set({ name: 'Nested Updated' })
              .where('id', '=', 100)
              .execute();
            await kysely.updateTable('benchmark_posts')
              .set({ title: 'Updated Post' })
              .where('author_id', '=', 100)
              .execute();
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            await drizzleDb.update(drizzleUsers)
              .set({ name: 'Nested Updated' })
              .where(eq(drizzleUsers.id, 100));
            await drizzleDb.update(drizzlePosts)
              .set({ title: 'Updated Post' })
              .where(eq(drizzlePosts.author_id, 100));
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            await typeormUserRepo.update({ id: 100 }, { name: 'Nested Updated' });
            await typeormPostRepo.update({ author_id: 100 }, { title: 'Updated Post' });
          }
        },
      ],
    },
    
    // ============================================
    // Upsert
    // ============================================
    {
      name: 'Upsert',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: () => LiteUser.create(
            [
              [LiteUser.email, `upsert${upsertCounter++}@example.com`],
              [LiteUser.name, 'Upsert User'],
            ],
            { onConflict: LiteUser.email, onConflictUpdate: [LiteUser.name], returning: true }
          ) 
        },
        { 
          orm: 'Prisma', 
          fn: () => prisma.user.upsert({
            where: { email: `upsert${upsertCounter++}@example.com` },
            update: { name: 'Upsert User' },
            create: { email: `upsert${upsertCounter}@example.com`, name: 'Upsert User' },
          }) 
        },
        { 
          orm: 'Kysely', 
          fn: () => kysely.insertInto('benchmark_users')
            .values({
              email: `upsert${upsertCounter++}@example.com`,
              name: 'Upsert User',
            })
            .onConflict(oc => oc.column('email').doUpdateSet({ name: 'Upsert User' }))
            .execute() 
        },
        { 
          orm: 'Drizzle', 
          fn: () => drizzleDb.insert(drizzleUsers)
            .values({
              email: `upsert${upsertCounter++}@example.com`,
              name: 'Upsert User',
            })
            .onConflictDoUpdate({
              target: drizzleUsers.email,
              set: { name: 'Upsert User' },
            }) 
        },
        { 
          orm: 'TypeORM', 
          fn: () => typeormUserRepo.upsert(
            { email: `upsert${upsertCounter++}@example.com`, name: 'Upsert User' },
            ['email']
          ) 
        },
      ],
    },
    
    // ============================================
    // Nested upsert
    // ============================================
    {
      name: 'Nested upsert (user + post)',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            const user = await LiteUser.create(
              [
                [LiteUser.email, `nupsert${upsertCounter++}@example.com`],
                [LiteUser.name, 'Nested Upsert'],
              ],
              { onConflict: LiteUser.email, onConflictUpdate: [LiteUser.name], returning: true }
            );
            await LitePost.create([
              [LitePost.title, 'Upsert Post'],
              [LitePost.author_id, user.id!],
            ]);
            return user;
          }
        },
        { 
          orm: 'Prisma', 
          fn: async () => {
            const user = await prisma.user.upsert({
              where: { email: `nupsert${upsertCounter++}@example.com` },
              update: { name: 'Nested Upsert' },
              create: {
                email: `nupsert${upsertCounter}@example.com`,
                name: 'Nested Upsert',
                posts: { create: { title: 'Upsert Post' } },
              },
            });
            return user;
          }
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            const user = await kysely.insertInto('benchmark_users')
              .values({
                email: `nupsert${upsertCounter++}@example.com`,
                name: 'Nested Upsert',
              })
              .onConflict(oc => oc.column('email').doUpdateSet({ name: 'Nested Upsert' }))
              .returningAll()
              .executeTakeFirstOrThrow();
            await kysely.insertInto('benchmark_posts')
              .values({ title: 'Upsert Post', author_id: user.id })
              .execute();
            return user;
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            const [user] = await drizzleDb.insert(drizzleUsers)
              .values({
                email: `nupsert${upsertCounter++}@example.com`,
                name: 'Nested Upsert',
              })
              .onConflictDoUpdate({
                target: drizzleUsers.email,
                set: { name: 'Nested Upsert' },
              })
              .returning();
            await drizzleDb.insert(drizzlePosts)
              .values({ title: 'Upsert Post', author_id: user.id });
            return user;
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const result = await typeormUserRepo.upsert(
              { email: `nupsert${upsertCounter++}@example.com`, name: 'Nested Upsert' },
              ['email']
            );
            const user = await typeormUserRepo.findOneBy({ email: `nupsert${upsertCounter}@example.com` });
            if (user) {
              const post = typeormPostRepo.create({ title: 'Upsert Post', author_id: user.id });
              await typeormPostRepo.save(post);
            }
            return result;
          }
        },
      ],
    },
    
    // ============================================
    // Delete
    // ============================================
    {
      name: 'Delete',
      tests: [
        { 
          orm: 'litedbmodel', 
          fn: async () => {
            // First create then delete
            const user = await LiteUser.create([
              [LiteUser.email, `del${createCounter++}@example.com`],
              [LiteUser.name, 'Delete User'],
            ]);
            return LiteUser.delete([[LiteUser.id, user.id!]]);
          }
        },
        { 
          orm: 'Prisma', 
          fn: async () => {
            const user = await prisma.user.create({
              data: { email: `del${createCounter++}@example.com`, name: 'Delete User' },
            });
            return prisma.user.delete({ where: { id: user.id } });
          }
        },
        { 
          orm: 'Kysely', 
          fn: async () => {
            const user = await kysely.insertInto('benchmark_users')
              .values({ email: `del${createCounter++}@example.com`, name: 'Delete User' })
              .returningAll()
              .executeTakeFirstOrThrow();
            return kysely.deleteFrom('benchmark_users').where('id', '=', user.id).execute();
          }
        },
        { 
          orm: 'Drizzle', 
          fn: async () => {
            const [user] = await drizzleDb.insert(drizzleUsers)
              .values({ email: `del${createCounter++}@example.com`, name: 'Delete User' })
              .returning();
            return drizzleDb.delete(drizzleUsers).where(eq(drizzleUsers.id, user.id));
          }
        },
        { 
          orm: 'TypeORM', 
          fn: async () => {
            const user = typeormUserRepo.create({ email: `del${createCounter++}@example.com`, name: 'Delete User' });
            const saved = await typeormUserRepo.save(user);
            return typeormUserRepo.delete({ id: saved.id });
          }
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
            // Load users
            const users = await drizzleDb.select().from(drizzleUsers).orderBy(asc(drizzleUsers.id)).limit(100);
            const userIds = users.map(u => u.id);
            
            // Load posts for these users
            const posts = await drizzleDb.select().from(drizzlePosts)
              .where(drizzleSql`${drizzlePosts.author_id} IN ${userIds}`);
            const postIds = posts.map(p => p.id);
            
            // Load comments for these posts
            const comments = await drizzleDb.select().from(drizzleComments)
              .where(drizzleSql`${drizzleComments.post_id} IN ${postIds}`);
            
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
