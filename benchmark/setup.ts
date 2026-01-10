/**
 * Database setup for ORM benchmark
 * 
 * Based on Prisma's official orm-benchmarks:
 * https://github.com/prisma/orm-benchmarks
 */
import pg from 'pg';

const config = {
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5433'),
  database: process.env.DB_NAME || 'testdb',
  user: process.env.DB_USER || 'testuser',
  password: process.env.DB_PASSWORD || 'testpass',
};

async function setup() {
  const pool = new pg.Pool(config);
  
  console.log('Setting up benchmark database...');
  console.log(`Connecting to ${config.host}:${config.port}/${config.database}`);
  
  // Drop and recreate tables
  await pool.query(`
    DROP TABLE IF EXISTS benchmark_tenant_comments CASCADE;
    DROP TABLE IF EXISTS benchmark_tenant_posts CASCADE;
    DROP TABLE IF EXISTS benchmark_tenant_users CASCADE;
    DROP TABLE IF EXISTS benchmark_tenants CASCADE;
    DROP TABLE IF EXISTS benchmark_comments CASCADE;
    DROP TABLE IF EXISTS benchmark_posts CASCADE;
    DROP TABLE IF EXISTS benchmark_users CASCADE;
    
    CREATE TABLE benchmark_users (
      id SERIAL PRIMARY KEY,
      email VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255),
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE benchmark_posts (
      id SERIAL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      content TEXT,
      published BOOLEAN DEFAULT false,
      author_id INTEGER REFERENCES benchmark_users(id) ON DELETE CASCADE,
      created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE benchmark_comments (
      id SERIAL PRIMARY KEY,
      body TEXT NOT NULL,
      post_id INTEGER REFERENCES benchmark_posts(id) ON DELETE CASCADE,
      created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX idx_users_email ON benchmark_users(email);
    CREATE INDEX idx_posts_author_id ON benchmark_posts(author_id);
    CREATE INDEX idx_posts_published ON benchmark_posts(published);
    CREATE INDEX idx_comments_post_id ON benchmark_comments(post_id);
    
    -- Composite key tables for multi-key relation benchmark
    CREATE TABLE benchmark_tenants (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL
    );
    
    CREATE TABLE benchmark_tenant_users (
      tenant_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      name VARCHAR(255),
      PRIMARY KEY (tenant_id, user_id),
      FOREIGN KEY (tenant_id) REFERENCES benchmark_tenants(id) ON DELETE CASCADE
    );
    
    CREATE TABLE benchmark_tenant_posts (
      tenant_id INTEGER NOT NULL,
      post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      title VARCHAR(255) NOT NULL,
      PRIMARY KEY (tenant_id, post_id),
      FOREIGN KEY (tenant_id, user_id) REFERENCES benchmark_tenant_users(tenant_id, user_id) ON DELETE CASCADE
    );
    
    CREATE TABLE benchmark_tenant_comments (
      tenant_id INTEGER NOT NULL,
      comment_id INTEGER NOT NULL,
      post_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      PRIMARY KEY (tenant_id, comment_id),
      FOREIGN KEY (tenant_id, post_id) REFERENCES benchmark_tenant_posts(tenant_id, post_id) ON DELETE CASCADE
    );
    
    CREATE INDEX idx_tenant_posts_user ON benchmark_tenant_posts(tenant_id, user_id);
    CREATE INDEX idx_tenant_comments_post ON benchmark_tenant_comments(tenant_id, post_id);
  `);
  
  console.log('Tables created successfully');
  
  // Insert seed data for read benchmarks
  // Structure for nested relation benchmark:
  // 100 users → 1000 posts (10 per user) → 10000 comments (10 per post)
  console.log('Inserting seed data...');
  
  const NUM_USERS = 1000;  // Base users for general benchmarks
  const POSTS_PER_USER = 5;
  
  // For nested relation benchmark (first 100 users)
  const NESTED_USERS = 100;
  const NESTED_POSTS_PER_USER = 10;  // 100 * 10 = 1000 posts
  const COMMENTS_PER_POST = 10;      // 1000 * 10 = 10000 comments
  
  // Insert users
  console.log(`Inserting ${NUM_USERS} users...`);
  const userValues: string[] = [];
  for (let i = 1; i <= NUM_USERS; i++) {
    userValues.push(`('user${i}@example.com', 'User ${i}')`);
  }
  await pool.query(`
    INSERT INTO benchmark_users (email, name) VALUES ${userValues.join(', ')}
  `);
  
  // Insert posts for general benchmark (5 posts per user for users 101-1000)
  console.log(`Inserting posts for general benchmarks...`);
  const generalPostValues: string[] = [];
  let postId = 1;
  for (let userId = NESTED_USERS + 1; userId <= NUM_USERS; userId++) {
    for (let p = 1; p <= POSTS_PER_USER; p++) {
      const published = postId % 3 === 0 ? 'true' : 'false';
      generalPostValues.push(`('Post ${postId}', 'Content for post ${postId}', ${published}, ${userId})`);
      postId++;
    }
  }
  
  // Insert posts for nested relation benchmark (10 posts per user for first 100 users)
  console.log(`Inserting ${NESTED_USERS * NESTED_POSTS_PER_USER} posts for nested benchmark...`);
  const nestedPostValues: string[] = [];
  const nestedPostStartId = postId;
  for (let userId = 1; userId <= NESTED_USERS; userId++) {
    for (let p = 1; p <= NESTED_POSTS_PER_USER; p++) {
      const published = postId % 3 === 0 ? 'true' : 'false';
      nestedPostValues.push(`('Nested Post ${postId}', 'Content for nested post ${postId}', ${published}, ${userId})`);
      postId++;
    }
  }
  
  // Insert all posts
  if (generalPostValues.length > 0) {
    await pool.query(`
      INSERT INTO benchmark_posts (title, content, published, author_id) VALUES ${generalPostValues.join(', ')}
    `);
  }
  await pool.query(`
    INSERT INTO benchmark_posts (title, content, published, author_id) VALUES ${nestedPostValues.join(', ')}
  `);
  
  // Insert comments for nested relation benchmark (10 comments per post for first 1000 posts)
  console.log(`Inserting ${NESTED_USERS * NESTED_POSTS_PER_USER * COMMENTS_PER_POST} comments...`);
  const COMMENT_BATCH_SIZE = 1000;
  const totalNestedPosts = NESTED_USERS * NESTED_POSTS_PER_USER;
  
  for (let batch = 0; batch < (totalNestedPosts * COMMENTS_PER_POST) / COMMENT_BATCH_SIZE; batch++) {
    const commentValues: string[] = [];
    const startComment = batch * COMMENT_BATCH_SIZE;
    const endComment = Math.min(startComment + COMMENT_BATCH_SIZE, totalNestedPosts * COMMENTS_PER_POST);
    
    for (let i = startComment; i < endComment; i++) {
      const belongsToPost = nestedPostStartId + Math.floor(i / COMMENTS_PER_POST);
      commentValues.push(`('Comment ${i + 1} for post ${belongsToPost}', ${belongsToPost})`);
    }
    
    await pool.query(`
      INSERT INTO benchmark_comments (body, post_id) VALUES ${commentValues.join(', ')}
    `);
    
    if ((batch + 1) % 5 === 0) {
      console.log(`  Inserted ${endComment} comments...`);
    }
  }
  
  const totalPosts = generalPostValues.length + nestedPostValues.length;
  const totalComments = NESTED_USERS * NESTED_POSTS_PER_USER * COMMENTS_PER_POST;
  console.log(`Seed data inserted: ${NUM_USERS} users, ${totalPosts} posts, ${totalComments} comments`);
  
  // Insert composite key data for multi-key relation benchmark
  // 10 tenants × 100 users × 10 posts = 10000 posts
  console.log('Inserting composite key data...');
  const NUM_TENANTS = 10;
  const USERS_PER_TENANT = 100;
  const POSTS_PER_TENANT_USER = 10;
  
  // Insert tenants
  const tenantValues = Array.from({ length: NUM_TENANTS }, (_, i) => `('Tenant ${i + 1}')`);
  await pool.query(`INSERT INTO benchmark_tenants (name) VALUES ${tenantValues.join(', ')}`);
  
  // Insert tenant users
  const tenantUserValues: string[] = [];
  for (let t = 1; t <= NUM_TENANTS; t++) {
    for (let u = 1; u <= USERS_PER_TENANT; u++) {
      tenantUserValues.push(`(${t}, ${u}, 'Tenant${t} User${u}')`);
    }
  }
  await pool.query(`INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES ${tenantUserValues.join(', ')}`);
  
  // Insert tenant posts - post_id REPEATS per tenant (1-1000 for each tenant)
  // This ensures composite FK (tenant_id, post_id) is required for correct joins
  const tenantPostValues: string[] = [];
  for (let t = 1; t <= NUM_TENANTS; t++) {
    let localPostId = 1;  // Reset for each tenant!
    for (let u = 1; u <= USERS_PER_TENANT; u++) {
      for (let p = 1; p <= POSTS_PER_TENANT_USER; p++) {
        tenantPostValues.push(`(${t}, ${localPostId}, ${u}, 'T${t}Post ${localPostId}')`);
        localPostId++;
      }
    }
  }
  // Insert in batches
  const BATCH = 5000;
  for (let i = 0; i < tenantPostValues.length; i += BATCH) {
    const batch = tenantPostValues.slice(i, i + BATCH);
    await pool.query(`INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES ${batch.join(', ')}`);
  }
  
  // Insert tenant comments for MULTIPLE tenants (tenants 1-5)
  // post_id REPEATS per tenant (1-1000), so composite FK is required
  // - Each tenant: 100 users × 10 posts = 1000 posts (post_id 1-1000)
  // - Comments per post: 10
  // - Total: 5 tenants × 1000 posts × 10 comments = 50000 comments
  //
  // Example: tenant 1 has post_id 1-1000, tenant 2 ALSO has post_id 1-1000
  // A query using only post_id IN (1,2,3) would match posts from ALL tenants!
  console.log('Inserting tenant comments (multi-tenant, post_id repeats per tenant)...');
  const COMMENTS_PER_TENANT_POST = 10;
  const TENANTS_FOR_COMMENTS = 5;  // Use tenants 1-5
  const POSTS_PER_TENANT = USERS_PER_TENANT * POSTS_PER_TENANT_USER; // 1000 posts per tenant
  
  const tenantCommentValues: string[] = [];
  for (let t = 1; t <= TENANTS_FOR_COMMENTS; t++) {
    let localCommentId = 1;  // Reset for each tenant!
    for (let localPostId = 1; localPostId <= POSTS_PER_TENANT; localPostId++) {
      for (let c = 1; c <= COMMENTS_PER_TENANT_POST; c++) {
        // Both post_id and comment_id repeat per tenant
        tenantCommentValues.push(`(${t}, ${localCommentId}, ${localPostId}, 'T${t}Comment ${localCommentId}')`);
        localCommentId++;
      }
    }
  }
  
  // Insert in batches
  for (let i = 0; i < tenantCommentValues.length; i += BATCH) {
    const batch = tenantCommentValues.slice(i, i + BATCH);
    await pool.query(`INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES ${batch.join(', ')}`);
  }
  
  console.log(`Composite key data: ${NUM_TENANTS} tenants, ${NUM_TENANTS * USERS_PER_TENANT} tenant_users, ${tenantPostValues.length} tenant_posts, ${tenantCommentValues.length} tenant_comments (across ${TENANTS_FOR_COMMENTS} tenants)`);
  
  // Verify data
  const userCount = await pool.query('SELECT COUNT(*) FROM benchmark_users');
  const postCount = await pool.query('SELECT COUNT(*) FROM benchmark_posts');
  console.log(`Verified: ${userCount.rows[0].count} users, ${postCount.rows[0].count} posts`);
  
  await pool.end();
  console.log('Setup complete!');
}

setup().catch(console.error);
