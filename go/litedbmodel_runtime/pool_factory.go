// litedbmodel v2 SCP — Phase C (#89, go) driver POOL FACTORIES.
//
// The go analogue of the TS `makesql/pool-executor.ts` pgPoolFactory / mysqlPoolFactory (#87):
// [BuildRoutingConfig] OWNS pool construction so the C3 config's CONSTRUCTION knobs — pool sizing
// (MinPool/MaxPool) + KeepAlive/KeepAliveInitialDelayMillis — reach the real *sql.DB at construction.
//
// The critical go-specific point (the #87 blocker was passing a pre-built pool that IGNORED sizing):
// go's database/sql sizing is set on the *sql.DB AFTER sql.Open — so the factory calls sql.Open and
// THEN SetMaxOpenConns(maxPool) / SetMaxIdleConns(minPool) / SetConnMax{Lifetime,IdleTime}(keepAlive)
// on the FRESH *sql.DB. The configured MaxOpenConns is thus the SOLE bound on live connections; there
// is no second un-sized *sql.DB path. Session knobs (queryTimeout/searchPath/charset) are layered
// separately by [ConfiguredPool] on checkout.

package litedbmodel_runtime

import (
	"database/sql"
	"fmt"
	"time"
)

// applyPoolSizing applies the C3 CONSTRUCTION knobs to a freshly-opened *sql.DB (the go equivalent of
// pg's `new Pool({max, min, keepAlive})` — set on the *sql.DB, not passed to sql.Open). MaxOpenConns
// = MaxPool is the SOLE cap on live connections; MaxIdleConns = MinPool keeps that many warm. The
// KeepAlive intent (go has no raw TCP-keepalive knob on database/sql) is expressed as a max
// idle-time + lifetime bound (a documented per-driver deviation): a connection idle past
// KeepAliveInitialDelayMillis is recycled rather than left to a silently-dropped TCP socket.
func applyPoolSizing(db *sql.DB, config ResolvedConnectionConfig) {
	db.SetMaxOpenConns(config.MaxPool) // ← the SOLE cap source (config.MaxPool only)
	db.SetMaxIdleConns(config.MinPool)
	if config.KeepAlive {
		// Express keepAlive as a max idle/lifetime bound (go has no TCP-keepalive setter here).
		d := time.Duration(config.KeepAliveInitialDelayMillis) * time.Millisecond
		db.SetConnMaxIdleTime(d)
		db.SetConnMaxLifetime(d)
	}
}

// pgDSN builds a pgx-stdlib DSN from a resolved config (postgres://user:pass@host:port/db?sslmode=disable).
func pgDSN(c ResolvedConnectionConfig) string {
	sslmode := "disable"
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", c.User, c.Password, c.Host, c.Port, c.Database, sslmode)
}

// mysqlDSN builds a go-sql-driver DSN from a resolved config (user:pass@tcp(host:port)/db).
func mysqlDSN(c ResolvedConnectionConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}

// PgPoolFactory is a [PoolFactory] for Postgres (pgx stdlib): opens a *sql.DB from a
// [ResolvedConnectionConfig] and applies the pool SIZING (MaxOpenConns = MaxPool, MaxIdleConns =
// MinPool) + keepAlive AT CONSTRUCTION — the config is the sole source of the cap. Connection params
// (host/port/database/user/password) flow from the config into the DSN. Returns the owned-connection
// [Pool] adapter + a closer (db.Close). role is accepted for the reader/writer replica split (the
// caller may vary host per role via the config it passes); this factory ignores it (same DSN for both
// — a real replica split would carry a different host in each setup). Mirrors the TS pgPoolFactory.
func PgPoolFactory() PoolFactory {
	return func(config ResolvedConnectionConfig, _ string) (BuiltPool, error) {
		db, err := sql.Open("pgx", pgDSN(config))
		if err != nil {
			return BuiltPool{}, mapSqliteError(err)
		}
		applyPoolSizing(db, config)
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return BuiltPool{}, mapSqliteError(err)
		}
		return BuiltPool{Pool: NewSQLDBPool(db), Close: closeWithStmtCache(db)}, nil
	}
}

// closeWithStmtCache returns a closer that first drops db's prepared-statement cache, then closes the
// *sql.DB — so a pool teardown releases the Go-side stmt cache too (no leaked map entry / handle).
func closeWithStmtCache(db *sql.DB) func() error {
	return func() error {
		CloseDBStmtCache(db)
		return db.Close()
	}
}

// MysqlPoolFactory is a [PoolFactory] for MySQL (the RETURNING-emulating "mysql-scp" driver): opens a
// *sql.DB from a [ResolvedConnectionConfig] and applies the pool SIZING (MaxOpenConns = MaxPool) +
// keepAlive AT CONSTRUCTION (the config is the sole source of the cap). Connection params flow from
// the config into the DSN. Returns the owned-connection [Pool] adapter + a closer. Mirrors the TS
// mysqlPoolFactory (which notes mysql2 has no min idle floor — go's SetMaxIdleConns DOES honor
// MinPool, so go is stricter here; documented).
func MysqlPoolFactory() PoolFactory {
	return func(config ResolvedConnectionConfig, _ string) (BuiltPool, error) {
		registerMysqlScp()
		db, err := sql.Open("mysql-scp", mysqlDSN(config))
		if err != nil {
			return BuiltPool{}, mapSqliteError(err)
		}
		applyPoolSizing(db, config)
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return BuiltPool{}, mapSqliteError(err)
		}
		return BuiltPool{Pool: NewSQLDBPool(db), Close: closeWithStmtCache(db)}, nil
	}
}
