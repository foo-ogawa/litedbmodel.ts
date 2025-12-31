-- Test database initialization

-- Users table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  role VARCHAR(50) DEFAULT 'user',
  tags TEXT[] DEFAULT '{}',
  metadata JSONB,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  deleted_at TIMESTAMP WITH TIME ZONE
);

COMMENT ON TABLE users IS 'User accounts table';
COMMENT ON COLUMN users.id IS 'User ID';
COMMENT ON COLUMN users.name IS 'User display name';
COMMENT ON COLUMN users.email IS 'Email address';
COMMENT ON COLUMN users.is_active IS 'Whether user is active';
COMMENT ON COLUMN users.role IS 'User role';

-- Posts table
CREATE TABLE IF NOT EXISTS posts (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  title VARCHAR(255) NOT NULL,
  content TEXT,
  view_count INTEGER DEFAULT 0,
  published BOOLEAN DEFAULT FALSE,
  published_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE posts IS 'Blog posts table';

-- Tags table
CREATE TABLE IF NOT EXISTS tags (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL UNIQUE,
  color VARCHAR(7),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Post-Tag junction table (composite primary key)
CREATE TABLE IF NOT EXISTS post_tags (
  post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (post_id, tag_id)
);

-- Sample data
INSERT INTO users (name, email, is_active, role, tags, metadata) VALUES
  ('Alice', 'alice@example.com', TRUE, 'admin', ARRAY['vip', 'beta'], '{"level": 10}'),
  ('Bob', 'bob@example.com', TRUE, 'user', ARRAY['beta'], '{"level": 5}'),
  ('Charlie', 'charlie@example.com', FALSE, 'user', '{}', NULL);

INSERT INTO posts (user_id, title, content, published, view_count) VALUES
  (1, 'First Post', 'Hello World!', TRUE, 100),
  (1, 'Second Post', 'Another post', FALSE, 0),
  (2, 'Bob''s Post', 'Content here', TRUE, 50);

INSERT INTO tags (name, color) VALUES
  ('tech', '#0066cc'),
  ('news', '#cc0000'),
  ('tutorial', '#00cc00');

INSERT INTO post_tags (post_id, tag_id) VALUES
  (1, 1), (1, 2),
  (2, 1),
  (3, 3);

-- All types test table
CREATE TABLE IF NOT EXISTS all_types_test (
  id SERIAL PRIMARY KEY,
  -- Basic types
  int_val INTEGER,
  float_val DOUBLE PRECISION,
  bool_val BOOLEAN,
  text_val TEXT,
  varchar_val VARCHAR(255),
  -- Date/Time types
  timestamp_val TIMESTAMP WITH TIME ZONE,
  date_val DATE,
  -- Array types
  int_array INTEGER[],
  text_array TEXT[],
  bool_array BOOLEAN[],
  -- JSON types
  json_val JSONB,
  json_array_val JSONB
);

