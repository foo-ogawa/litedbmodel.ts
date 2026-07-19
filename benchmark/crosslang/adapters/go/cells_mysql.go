//go:build mysql

package main

import (
	_ "github.com/go-sql-driver/mysql"
	gen "orm_bench_go/generated/mysql"
	"orm_bench_go/wire"
)

func dialectName() string                      { return gen.Dialect }
func nativeCell(op string, db *wire.DB) string { return gen.Native(op, db) }
