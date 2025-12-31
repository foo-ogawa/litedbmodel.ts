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
    
    CREATE INDEX idx_users_email ON benchmark_users(email);
    CREATE INDEX idx_posts_author_id ON benchmark_posts(author_id);
    CREATE INDEX idx_posts_published ON benchmark_posts(published);
  `);
  
  console.log('Tables created successfully');
  
  // Insert seed data for read benchmarks
  // Using similar data volume as Prisma orm-benchmarks
  console.log('Inserting seed data...');
  
  const NUM_USERS = 1000;
  const POSTS_PER_USER = 5;
  
  // Insert users
  const userValues: string[] = [];
  for (let i = 1; i <= NUM_USERS; i++) {
    userValues.push(`('user${i}@example.com', 'User ${i}')`);
  }
  await pool.query(`
    INSERT INTO benchmark_users (email, name) VALUES ${userValues.join(', ')}
  `);
  
  // Insert posts (5 posts per user)
  const postValues: string[] = [];
  for (let userId = 1; userId <= NUM_USERS; userId++) {
    for (let p = 1; p <= POSTS_PER_USER; p++) {
      const postId = (userId - 1) * POSTS_PER_USER + p;
      const published = postId % 3 === 0 ? 'true' : 'false';
      postValues.push(`('Post ${postId}', 'Content for post ${postId}', ${published}, ${userId})`);
    }
  }
  await pool.query(`
    INSERT INTO benchmark_posts (title, content, published, author_id) VALUES ${postValues.join(', ')}
  `);
  
  console.log(`Seed data inserted: ${NUM_USERS} users, ${NUM_USERS * POSTS_PER_USER} posts`);
  
  // Verify data
  const userCount = await pool.query('SELECT COUNT(*) FROM benchmark_users');
  const postCount = await pool.query('SELECT COUNT(*) FROM benchmark_posts');
  console.log(`Verified: ${userCount.rows[0].count} users, ${postCount.rows[0].count} posts`);
  
  await pool.end();
  console.log('Setup complete!');
}

setup().catch(console.error);
