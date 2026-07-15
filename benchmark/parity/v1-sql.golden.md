# litedbmodel v1 ORM-path SQL parity golden (#64)

Captured live via `SqlLoggerMiddleware.execute` (driver-reach hook) by running each
bench op once against a real seeded DB, for all three dialects. No SQL is hand-written.

- `?` placeholders are the litedbmodel portable form. The postgres driver rewrites
  `?`->`$N` **inside** the driver (post-middleware), so it is not shown here.
- MySQL strips `RETURNING` and issues an internal follow-up `SELECT` below the
  middleware to simulate it; that internal SELECT is a driver detail, not captured.
- Transaction `BEGIN`/`COMMIT` are raw `connection.query()` below the middleware; not captured.

Regenerate: `npm run parity:capture` (see `benchmark/parity/README.md`).

## Statement counts (op x dialect)

| Op | sqlite | mysql | postgres |
| --- | ---: | ---: | ---: |
| Find all (limit 100) | 1 | 1 | 1 |
| Filter, paginate & sort | 1 | 1 | 1 |
| Nested find all (include posts) | 2 | 2 | 2 |
| Find first | 1 | 1 | 1 |
| Nested find first (include posts) | 2 | 2 | 2 |
| Find unique (by email) | 1 | 1 | 1 |
| Nested find unique (include posts) | 2 | 2 | 2 |
| Create | 1 | 1 | 1 |
| Nested create (with post) | 2 | 2 | 2 |
| Update | 1 | 1 | 1 |
| Nested update (update user + post) | 2 | 2 | 2 |
| Upsert | 1 | 1 | 1 |
| Nested upsert (user + post) | 2 | 2 | 2 |
| Delete | 2 | 2 | 2 |
| Create Many (10 records) | 1 | 1 | 1 |
| Upsert Many (10 records) | 1 | 1 | 1 |
| Update Many (10 different values) | 1 | 1 | 1 |
| Nested relations (100->1000->10000) | 3 | 3 | 3 |
| Nested relations (composite key, 5 tenants) | 3 | 3 | 3 |

## Full captured SQL

### Find all (limit 100)

**sqlite**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
```

**mysql**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
```

**postgres**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
```

### Filter, paginate & sort

**sqlite**

```sql
-- [1] params: [true]
SELECT * FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10
```

**mysql**

```sql
-- [1] params: [true]
SELECT * FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10
```

**postgres**

```sql
-- [1] params: [true]
SELECT * FROM benchmark_posts WHERE published = ?::boolean ORDER BY created_at DESC LIMIT 20 OFFSET 10
```

### Nested find all (include posts)

**sqlite**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
-- [2] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
SELECT * FROM benchmark_posts WHERE author_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**mysql**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
-- [2] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
SELECT * FROM benchmark_posts WHERE author_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**postgres**

```sql
-- [1] params: []
SELECT * FROM benchmark_users LIMIT 100
-- [2] params: [[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]]
SELECT * FROM benchmark_posts WHERE benchmark_posts.author_id = ANY(?::int[])
```

### Find first

**sqlite**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
```

**mysql**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
```

**postgres**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
```

### Nested find first (include posts)

**sqlite**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
-- [2] params: [1]
SELECT * FROM benchmark_posts WHERE author_id IN (?)
```

**mysql**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
-- [2] params: [1]
SELECT * FROM benchmark_posts WHERE author_id IN (?)
```

**postgres**

```sql
-- [1] params: ["User%"]
SELECT * FROM benchmark_users WHERE name LIKE ? LIMIT 1
-- [2] params: [[1]]
SELECT * FROM benchmark_posts WHERE benchmark_posts.author_id = ANY(?::int[])
```

### Find unique (by email)

**sqlite**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
```

**mysql**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
```

**postgres**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
```

### Nested find unique (include posts)

**sqlite**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
-- [2] params: [500]
SELECT * FROM benchmark_posts WHERE author_id IN (?)
```

**mysql**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
-- [2] params: [500]
SELECT * FROM benchmark_posts WHERE author_id IN (?)
```

**postgres**

```sql
-- [1] params: ["user500@example.com"]
SELECT * FROM benchmark_users WHERE email = ? LIMIT 1
-- [2] params: [[500]]
SELECT * FROM benchmark_posts WHERE benchmark_posts.author_id = ANY(?::int[])
```

### Create

**sqlite**

```sql
-- [1] params: ["bench10000@example.com","Benchmark User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?)
```

**mysql**

```sql
-- [1] params: ["bench10000@example.com","Benchmark User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?)
```

**postgres**

```sql
-- [1] params: ["bench10000@example.com","Benchmark User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?)
```

### Nested create (with post)

**sqlite**

```sql
-- [1] params: ["nested10001@example.com","Nested User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [502,"Content","Nested Post"]
INSERT INTO benchmark_posts (author_id, content, title) VALUES (?, ?, ?)
```

**mysql**

```sql
-- [1] params: ["nested10001@example.com","Nested User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [502,"Content","Nested Post"]
INSERT INTO benchmark_posts (author_id, content, title) VALUES (?, ?, ?)
```

**postgres**

```sql
-- [1] params: ["nested10001@example.com","Nested User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [502,"Content","Nested Post"]
INSERT INTO benchmark_posts (author_id, content, title) VALUES (?, ?, ?)
```

### Update

**sqlite**

```sql
-- [1] params: ["Updated User",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
```

**mysql**

```sql
-- [1] params: ["Updated User",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
```

**postgres**

```sql
-- [1] params: ["Updated User",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
```

### Nested update (update user + post)

**sqlite**

```sql
-- [1] params: ["Nested Updated",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
-- [2] params: ["Updated Post",100]
UPDATE benchmark_posts SET title = ? WHERE author_id = ?
```

**mysql**

```sql
-- [1] params: ["Nested Updated",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
-- [2] params: ["Updated Post",100]
UPDATE benchmark_posts SET title = ? WHERE author_id = ?
```

**postgres**

```sql
-- [1] params: ["Nested Updated",100]
UPDATE benchmark_users SET name = ? WHERE id = ?
-- [2] params: ["Updated Post",100]
UPDATE benchmark_posts SET title = ? WHERE author_id = ?
```

### Upsert

**sqlite**

```sql
-- [1] params: ["upsert20000@example.com","Upsert User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET name = excluded.name RETURNING id
```

**mysql**

```sql
-- [1] params: ["upsert20000@example.com","Upsert User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id
```

**postgres**

```sql
-- [1] params: ["upsert20000@example.com","Upsert User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id
```

### Nested upsert (user + post)

**sqlite**

```sql
-- [1] params: ["nupsert20001@example.com","Nested Upsert"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET name = excluded.name RETURNING id
-- [2] params: [504,"Upsert Post"]
INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)
```

**mysql**

```sql
-- [1] params: ["nupsert20001@example.com","Nested Upsert"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id
-- [2] params: [504,"Upsert Post"]
INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)
```

**postgres**

```sql
-- [1] params: ["nupsert20001@example.com","Nested Upsert"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id
-- [2] params: [504,"Upsert Post"]
INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)
```

### Delete

**sqlite**

```sql
-- [1] params: ["del10002@example.com","Delete User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [505]
DELETE FROM benchmark_users WHERE id = ?
```

**mysql**

```sql
-- [1] params: ["del10002@example.com","Delete User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [505]
DELETE FROM benchmark_users WHERE id = ?
```

**postgres**

```sql
-- [1] params: ["del10002@example.com","Delete User"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id
-- [2] params: [505]
DELETE FROM benchmark_users WHERE id = ?
```

### Create Many (10 records)

**sqlite**

```sql
-- [1] params: ["bulk10003@example.com","Bulk User 0","bulk10004@example.com","Bulk User 1","bulk10005@example.com","Bulk User 2","bulk10006@example.com","Bulk User 3","bulk10007@example.com","Bulk User 4","bulk10008@example.com","Bulk User 5","bulk10009@example.com","Bulk User 6","bulk10010@example.com","Bulk User 7","bulk10011@example.com","Bulk User 8","bulk10012@example.com","Bulk User 9"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
```

**mysql**

```sql
-- [1] params: ["bulk10003@example.com","Bulk User 0","bulk10004@example.com","Bulk User 1","bulk10005@example.com","Bulk User 2","bulk10006@example.com","Bulk User 3","bulk10007@example.com","Bulk User 4","bulk10008@example.com","Bulk User 5","bulk10009@example.com","Bulk User 6","bulk10010@example.com","Bulk User 7","bulk10011@example.com","Bulk User 8","bulk10012@example.com","Bulk User 9"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)
```

**postgres**

```sql
-- [1] params: [["bulk10003@example.com","bulk10004@example.com","bulk10005@example.com","bulk10006@example.com","bulk10007@example.com","bulk10008@example.com","bulk10009@example.com","bulk10010@example.com","bulk10011@example.com","bulk10012@example.com"],["Bulk User 0","Bulk User 1","Bulk User 2","Bulk User 3","Bulk User 4","Bulk User 5","Bulk User 6","Bulk User 7","Bulk User 8","Bulk User 9"]]
INSERT INTO benchmark_users (email, name) SELECT v.email, v.name FROM UNNEST(?::text[], ?::text[]) AS v(email, name)
```

### Upsert Many (10 records)

**sqlite**

```sql
-- [1] params: ["upsertbulk20002@example.com","Upsert Bulk 0","upsertbulk20003@example.com","Upsert Bulk 1","upsertbulk20004@example.com","Upsert Bulk 2","upsertbulk20005@example.com","Upsert Bulk 3","upsertbulk20006@example.com","Upsert Bulk 4","upsertbulk20007@example.com","Upsert Bulk 5","upsertbulk20008@example.com","Upsert Bulk 6","upsertbulk20009@example.com","Upsert Bulk 7","upsertbulk20010@example.com","Upsert Bulk 8","upsertbulk20011@example.com","Upsert Bulk 9"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?) ON CONFLICT (email) DO UPDATE SET name = excluded.name
```

**mysql**

```sql
-- [1] params: ["upsertbulk20002@example.com","Upsert Bulk 0","upsertbulk20003@example.com","Upsert Bulk 1","upsertbulk20004@example.com","Upsert Bulk 2","upsertbulk20005@example.com","Upsert Bulk 3","upsertbulk20006@example.com","Upsert Bulk 4","upsertbulk20007@example.com","Upsert Bulk 5","upsertbulk20008@example.com","Upsert Bulk 6","upsertbulk20009@example.com","Upsert Bulk 7","upsertbulk20010@example.com","Upsert Bulk 8","upsertbulk20011@example.com","Upsert Bulk 9"]
INSERT INTO benchmark_users (email, name) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)
```

**postgres**

```sql
-- [1] params: [["upsertbulk20002@example.com","upsertbulk20003@example.com","upsertbulk20004@example.com","upsertbulk20005@example.com","upsertbulk20006@example.com","upsertbulk20007@example.com","upsertbulk20008@example.com","upsertbulk20009@example.com","upsertbulk20010@example.com","upsertbulk20011@example.com"],["Upsert Bulk 0","Upsert Bulk 1","Upsert Bulk 2","Upsert Bulk 3","Upsert Bulk 4","Upsert Bulk 5","Upsert Bulk 6","Upsert Bulk 7","Upsert Bulk 8","Upsert Bulk 9"]]
INSERT INTO benchmark_users (email, name) SELECT v.email, v.name FROM UNNEST(?::text[], ?::text[]) AS v(email, name) ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
```

### Update Many (10 different values)

**sqlite**

```sql
-- [1] params: [100,"Updated Different 0",101,"Updated Different 1",102,"Updated Different 2",103,"Updated Different 3",104,"Updated Different 4",105,"Updated Different 5",106,"Updated Different 6",107,"Updated Different 7",108,"Updated Different 8",109,"Updated Different 9",100,101,102,103,104,105,106,107,108,109]
UPDATE benchmark_users SET name = CASE WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? WHEN id = ? THEN ? END WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**mysql**

```sql
-- [1] params: [100,"Updated Different 0",101,"Updated Different 1",102,"Updated Different 2",103,"Updated Different 3",104,"Updated Different 4",105,"Updated Different 5",106,"Updated Different 6",107,"Updated Different 7",108,"Updated Different 8",109,"Updated Different 9"]
UPDATE benchmark_users AS t JOIN (VALUES ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?), ROW(?, ?)) AS v(id, name) ON t.id = v.id SET t.name = v.name
```

**postgres**

```sql
-- [1] params: [[100,101,102,103,104,105,106,107,108,109],["Updated Different 0","Updated Different 1","Updated Different 2","Updated Different 3","Updated Different 4","Updated Different 5","Updated Different 6","Updated Different 7","Updated Different 8","Updated Different 9"]]
UPDATE benchmark_users AS t SET name = v.name FROM UNNEST(?::int[], ?::text[]) AS v(id, name) WHERE t.id = v.id
```

### Nested relations (100->1000->10000)

**sqlite**

```sql
-- [1] params: []
SELECT * FROM benchmark_users ORDER BY id ASC LIMIT 100
-- [2] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
SELECT * FROM benchmark_posts WHERE author_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
-- [3] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200]
SELECT * FROM benchmark_comments WHERE post_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**mysql**

```sql
-- [1] params: []
SELECT * FROM benchmark_users ORDER BY id ASC LIMIT 100
-- [2] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
SELECT * FROM benchmark_posts WHERE author_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
-- [3] params: [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200]
SELECT * FROM benchmark_comments WHERE post_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
```

**postgres**

```sql
-- [1] params: []
SELECT * FROM benchmark_users ORDER BY id ASC LIMIT 100
-- [2] params: [[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]]
SELECT * FROM benchmark_posts WHERE benchmark_posts.author_id = ANY(?::int[])
-- [3] params: [[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200]]
SELECT * FROM benchmark_comments WHERE benchmark_comments.post_id = ANY(?::int[])
```

### Nested relations (composite key, 5 tenants)

**sqlite**

```sql
-- [1] params: [1,2,3,4,5]
SELECT * FROM benchmark_tenant_users WHERE tenant_id IN (?, ?, ?, ?, ?) LIMIT 100
-- [2] params: [1,1,1,2,1,3,1,4,2,1,2,2,2,3,2,4,3,1,3,2,3,3,3,4,4,1,4,2,4,3,4,4,5,1,5,2,5,3,5,4]
SELECT * FROM benchmark_tenant_posts WHERE (tenant_id, user_id) IN ((?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?))
-- [3] params: [1,1,1,2,1,3,1,4,1,5,1,6,1,7,1,8,2,1,2,2,2,3,2,4,2,5,2,6,2,7,2,8,3,1,3,2,3,3,3,4,3,5,3,6,3,7,3,8,4,1,4,2,4,3,4,4,4,5,4,6,4,7,4,8,5,1,5,2,5,3,5,4,5,5,5,6,5,7,5,8]
SELECT * FROM benchmark_tenant_comments WHERE (tenant_id, post_id) IN ((?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?))
```

**mysql**

```sql
-- [1] params: [1,2,3,4,5]
SELECT * FROM benchmark_tenant_users WHERE tenant_id IN (?, ?, ?, ?, ?) LIMIT 100
-- [2] params: [1,1,1,2,1,3,1,4,2,1,2,2,2,3,2,4,3,1,3,2,3,3,3,4,4,1,4,2,4,3,4,4,5,1,5,2,5,3,5,4]
SELECT * FROM benchmark_tenant_posts WHERE (tenant_id, user_id) IN ((?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?))
-- [3] params: [1,1,1,2,1,3,1,4,1,5,1,6,1,7,1,8,2,1,2,2,2,3,2,4,2,5,2,6,2,7,2,8,3,1,3,2,3,3,3,4,3,5,3,6,3,7,3,8,4,1,4,2,4,3,4,4,4,5,4,6,4,7,4,8,5,1,5,2,5,3,5,4,5,5,5,6,5,7,5,8]
SELECT * FROM benchmark_tenant_comments WHERE (tenant_id, post_id) IN ((?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?))
```

**postgres**

```sql
-- [1] params: [1,2,3,4,5]
SELECT * FROM benchmark_tenant_users WHERE tenant_id IN (?, ?, ?, ?, ?) LIMIT 100
-- [2] params: [[1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5],[1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4,1,2,3,4]]
SELECT * FROM benchmark_tenant_posts JOIN unnest(?::int[], ?::int[]) AS _unnest_benchmark_tenant_posts(_unnest_benchmark_tenant_posts_tenant_id, _unnest_benchmark_tenant_posts_user_id) ON benchmark_tenant_posts.tenant_id = _unnest_benchmark_tenant_posts._unnest_benchmark_tenant_posts_tenant_id AND benchmark_tenant_posts.user_id = _unnest_benchmark_tenant_posts._unnest_benchmark_tenant_posts_user_id
-- [3] params: [[1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5],[1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8]]
SELECT * FROM benchmark_tenant_comments JOIN unnest(?::int[], ?::int[]) AS _unnest_benchmark_tenant_comments(_unnest_benchmark_tenant_comments_tenant_id, _unnest_benchmark_tenant_comments_post_id) ON benchmark_tenant_comments.tenant_id = _unnest_benchmark_tenant_comments._unnest_benchmark_tenant_comments_tenant_id AND benchmark_tenant_comments.post_id = _unnest_benchmark_tenant_comments._unnest_benchmark_tenant_comments_post_id
```

