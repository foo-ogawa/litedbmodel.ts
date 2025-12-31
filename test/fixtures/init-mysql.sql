-- Test database initialization for MySQL

-- Users table
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  role VARCHAR(50) DEFAULT 'user',
  tags JSON,
  metadata JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at TIMESTAMP NULL
);

-- Posts table
CREATE TABLE IF NOT EXISTS posts (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  content TEXT,
  view_count INT DEFAULT 0,
  published BOOLEAN DEFAULT FALSE,
  published_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Tags table
CREATE TABLE IF NOT EXISTS tags (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL UNIQUE,
  color VARCHAR(7),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Post-Tag junction table (composite primary key)
CREATE TABLE IF NOT EXISTS post_tags (
  post_id INT NOT NULL,
  tag_id INT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (post_id, tag_id),
  FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Sample data
INSERT INTO users (name, email, is_active, role, tags, metadata) VALUES
  ('Alice', 'alice@example.com', TRUE, 'admin', '["vip", "beta"]', '{"level": 10}'),
  ('Bob', 'bob@example.com', TRUE, 'user', '["beta"]', '{"level": 5}'),
  ('Charlie', 'charlie@example.com', FALSE, 'user', '[]', NULL);

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
  id INT AUTO_INCREMENT PRIMARY KEY,
  -- Basic types
  int_val INT,
  float_val DOUBLE,
  bool_val BOOLEAN,
  text_val TEXT,
  varchar_val VARCHAR(255),
  -- Date/Time types
  timestamp_val TIMESTAMP NULL,
  date_val DATE,
  -- JSON types (MySQL 5.7+)
  json_val JSON,
  json_array_val JSON
);

