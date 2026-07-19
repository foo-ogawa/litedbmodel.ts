//go:build sqlite

package main

import (
	_ "github.com/mattn/go-sqlite3"
	gen "orm_bench_go/generated/sqlite"
	"orm_bench_go/wire"
)

func dialectName() string                        { return gen.Dialect }
func nativeCell(op string, db *wire.DB) string   { return gen.Native(op, db) }
