# v1 ORM-path SQL parity golden (#64)

This directory holds the **parity SSoT**: the actual SQL that the litedbmodel **v1**
ORM path emits for every op in the ORM-comparison bench
(`benchmark/benchmark.ts` → `testCategories`), across all three dialects
(**sqlite** in-proc, **mysql** :3307, **postgres** :5433).

- [`v1-sql.golden.json`](./v1-sql.golden.json) — machine-readable: `op → dialect → [{ sql, params }, …]`.
- [`v1-sql.golden.md`](./v1-sql.golden.md) — human-readable (owner review): statement-count
  table + every captured statement with its params, grouped by op and dialect.

## How it is captured (no fabrication)

The harness is `test/parity/v1-sql-golden.test.ts`. It:

1. Copies the litedbmodel v1 models (`LiteUser`/`LitePost`/`LiteComment` and the
   composite-key `LiteTenant*` models) and **every** op's litedbmodel `fn` **verbatim**
   from `benchmark/benchmark.ts`.
2. For each dialect: applies the schema, seeds a small deterministic dataset, registers
   a `SqlLoggerMiddleware` (the same middleware pattern as
   `test/integration/LazyRelation.test.ts:38`), then runs each op's `fn` **once** and
   records the ordered list of `{ sql, params }` the middleware observed.

Every SQL string in the golden is a live capture from a real DB execution. Nothing is
hand-written or inferred from source.

### What the middleware sees

The `SqlLoggerMiddleware.execute` hook sits **above** the driver, so it captures the
portable `?`-placeholder SQL that litedbmodel emits — the parity SSoT. Consequently:

- **postgres** rewrites `?` → `$N` *inside* the driver (post-middleware); the golden
  keeps the `?` form.
- **mysql** strips `RETURNING` and issues an internal follow-up `SELECT` *below* the
  middleware to simulate it; that internal SELECT is a driver detail and is not captured.
- Transaction `BEGIN`/`COMMIT` run as raw `connection.query()` *below* the middleware
  and are not captured (they are dialect-standard and identical across ops).

## Seed (deterministic)

Seeded with parameterised INSERTs using **explicit ids** so the rows each op touches are
guaranteed present and stable (postgres SERIAL sequences are advanced with `setval` after
seeding so auto-increment INSERTs don't collide):

| Table | Rows |
| --- | --- |
| `benchmark_users` | ids `1..110` **plus** id `500` (so `user500@example.com` exists for *Find unique*; ids `100..109` exist for *Update* / *Update Many*) |
| `benchmark_posts` | 2 per user (authors `1..110`), fixed ids — gives *Nested find* rows and `author_id=100` for *Nested update* |
| `benchmark_comments` | 2 per post |
| `benchmark_tenant_users` | tenants `1..5` × 4 users |
| `benchmark_tenant_posts` | 2 per tenant-user; `post_id` **repeats per tenant** (so the composite FK is required) |
| `benchmark_tenant_comments` | 2 per tenant-post; `comment_id`/`post_id` **repeat per tenant** |

The exact seed parameters are also embedded in `v1-sql.golden.json` under `seed`.

The dataset is intentionally small — this golden captures **SQL shape + params**, not
performance; row counts only need to be large enough for each op to actually execute and
emit its full statement set.

## Regenerate / drift-check

```bash
# PG (:5433) and MySQL (:3307) must be up:
#   docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
npm run parity:capture
```

Regeneration is **byte-identical** (verified), so a drift check is just:

```bash
npm run parity:capture && git diff --exit-code benchmark/parity/
```

sqlite runs fully in-process (`:memory:`), so it needs no external service.
