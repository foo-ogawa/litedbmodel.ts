// GENERATED native setup; no serialized sidecar.
pub const STATEMENTS: &[&str] = &[
    "CREATE TABLE benchmark_users (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        email TEXT NOT NULL UNIQUE,\n        name TEXT,\n        created_at TEXT DEFAULT (datetime('now')),\n        updated_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_posts (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        title TEXT NOT NULL,\n        content TEXT,\n        published INTEGER DEFAULT 0,\n        author_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_comments (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        body TEXT NOT NULL,\n        post_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_tenant_users (\n        tenant_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        name TEXT,\n        PRIMARY KEY (tenant_id, user_id)\n      )",
    "CREATE TABLE benchmark_tenant_posts (\n        tenant_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        title TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, post_id)\n      )",
    "CREATE TABLE benchmark_tenant_comments (\n        tenant_id INTEGER NOT NULL,\n        comment_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        body TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, comment_id)\n      )",
];
