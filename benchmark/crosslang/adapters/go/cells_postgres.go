//go:build postgres

package main

import (
	_ "github.com/lib/pq"
	gen "orm_bench_go/generated/postgres"
	"orm_bench_go/wire"
)

func dialectName() string                      { return gen.Dialect }
func nativeCell(op string, db *wire.DB) string { return gen.Native(op, db) }
