// Package wire is the consumer-supplied de-box seam for the bc 0.8.10 go-typed-native contract
// (#152/#153): the generated modules reference wire.WireValue/WireRow/WireList (via
// --go-wire-import) and de-box each node's result wire INLINE against the statically declared type.
// This package supplies those interfaces + their concrete MATERIALIZED implementation (a driver-
// agnostic result the seam builds by column name) — the go twin of the rust cell's seam.rs WireValue.
package wire

import "strconv"

// Probe kinds — MUST match bc's per-module local consts (probeGot=0/probeWrong=1/probeAbsent=2/probeNull=3).
const (
	KGot    uint8 = 0
	KWrong  uint8 = 1
	KAbsent uint8 = 2
	KNull   uint8 = 3
)

// Probe result structs — field layout MUST match bc's per-module emission (Kind/Got/ActualWireType/Raw),
// since the generated runner reads `p.Kind`/`p.Got`/`p.ActualWireType`/`p.Raw` on the values these return.
type StringProbe struct {
	Kind           uint8
	Got            string
	ActualWireType string
	Raw            string
}
type NumberProbe struct {
	Kind           uint8
	Got            string // the raw numeric TEXT (the de-box parses + range-checks it)
	ActualWireType string
	Raw            string
}
type BoolProbe struct {
	Kind           uint8
	Got            bool
	ActualWireType string
	Raw            string
}
type RowProbe struct {
	Kind           uint8
	Got            WireRow
	ActualWireType string
	Raw            string
}
type ListProbe struct {
	Kind           uint8
	Got            WireList
	ActualWireType string
	Raw            string
}

// The three seam interfaces the generated modules reference as wire.WireValue/WireRow/WireList.
type WireValue interface {
	AsString() StringProbe
	AsNumber() NumberProbe
	AsBool() BoolProbe
	AsRow() RowProbe
	AsList() ListProbe
}
type WireRow interface {
	Keys() []string
	ProbeString(field string) StringProbe
	ProbeNumber(field string) NumberProbe
	ProbeBool(field string) BoolProbe
	ProbeRow(field string) RowProbe
	ProbeList(field string) ListProbe
}
type WireList interface {
	Len() int
	ElemString(i int) StringProbe
	ElemNumber(i int) NumberProbe
	ElemBool(i int) BoolProbe
	ElemRow(i int) RowProbe
	ElemList(i int) ListProbe
}

// ── concrete materialized wire (driver-agnostic, built by the seam by column name) ──
type cellKind uint8

const (
	cInt cellKind = iota
	cReal
	cText
	cBool
	cNull
)

// Cell — one materialized column value. The probe accessors coerce to the DECLARED type the de-box asks
// for (e.g. a pg BOOLEAN `published` answers ProbeNumber with 0/1 — the go twin of rust's WireCell).
type Cell struct {
	kind cellKind
	i    int64
	f    float64
	s    string
	b    bool
}

func CellInt(v int64) Cell   { return Cell{kind: cInt, i: v} }
func CellReal(v float64) Cell { return Cell{kind: cReal, f: v} }
func CellText(v string) Cell { return Cell{kind: cText, s: v} }
func CellBool(v bool) Cell   { return Cell{kind: cBool, b: v} }
func CellNull() Cell         { return Cell{kind: cNull} }

func (c Cell) tag() string {
	switch c.kind {
	case cInt, cReal:
		return "number"
	case cText:
		return "string"
	case cBool:
		return "bool"
	default:
		return "null"
	}
}
func (c Cell) raw() string {
	switch c.kind {
	case cInt:
		return strconv.FormatInt(c.i, 10)
	case cReal:
		return strconv.FormatFloat(c.f, 'g', -1, 64)
	case cText:
		return c.s
	case cBool:
		if c.b {
			return "true"
		}
		return "false"
	default:
		return "null"
	}
}
func (c Cell) numProbe() NumberProbe {
	switch c.kind {
	case cInt:
		return NumberProbe{Kind: KGot, Got: strconv.FormatInt(c.i, 10), ActualWireType: "number"}
	case cReal:
		return NumberProbe{Kind: KGot, Got: strconv.FormatFloat(c.f, 'g', -1, 64), ActualWireType: "number"}
	case cBool:
		v := "0"
		if c.b {
			v = "1"
		}
		return NumberProbe{Kind: KGot, Got: v, ActualWireType: "number"}
	case cNull:
		return NumberProbe{Kind: KNull, ActualWireType: "null", Raw: "null"}
	default:
		return NumberProbe{Kind: KWrong, ActualWireType: c.tag(), Raw: c.raw()}
	}
}
func (c Cell) strProbe() StringProbe {
	switch c.kind {
	case cText:
		return StringProbe{Kind: KGot, Got: c.s, ActualWireType: "string"}
	case cNull:
		return StringProbe{Kind: KNull, ActualWireType: "null", Raw: "null"}
	default:
		return StringProbe{Kind: KWrong, ActualWireType: c.tag(), Raw: c.raw()}
	}
}
func (c Cell) boolProbe() BoolProbe {
	switch c.kind {
	case cBool:
		return BoolProbe{Kind: KGot, Got: c.b, ActualWireType: "bool"}
	case cInt:
		return BoolProbe{Kind: KGot, Got: c.i != 0, ActualWireType: "bool"}
	case cNull:
		return BoolProbe{Kind: KNull, ActualWireType: "null", Raw: "null"}
	default:
		return BoolProbe{Kind: KWrong, ActualWireType: c.tag(), Raw: c.raw()}
	}
}

// Named accessors for the hand-written SDK baseline (which formats raw rows itself, not via the de-box).
func (c Cell) Int() int64 {
	switch c.kind {
	case cInt:
		return c.i
	case cBool:
		if c.b {
			return 1
		}
		return 0
	case cReal:
		return int64(c.f)
	case cText:
		n, _ := strconv.ParseInt(c.s, 10, 64)
		return n
	default:
		return 0
	}
}
func (c Cell) Str() string { return c.raw() }

// RowData — a materialized wire ROW (column name → cell), probed by field name.
type RowData struct {
	names []string
	cells []Cell
}

func (r RowData) get(field string) (Cell, bool) {
	for i, n := range r.names {
		if n == field {
			return r.cells[i], true
		}
	}
	return Cell{}, false
}

// SDK helpers (by column name).
func (r RowData) Int(field string) int64 {
	if c, ok := r.get(field); ok {
		return c.Int()
	}
	return 0
}
func (r RowData) Str(field string) string {
	if c, ok := r.get(field); ok {
		return c.Str()
	}
	return ""
}

// WireRow impl.
func (r RowData) Keys() []string { return r.names }
func (r RowData) ProbeString(field string) StringProbe {
	if c, ok := r.get(field); ok {
		return c.strProbe()
	}
	return StringProbe{Kind: KAbsent}
}
func (r RowData) ProbeNumber(field string) NumberProbe {
	if c, ok := r.get(field); ok {
		return c.numProbe()
	}
	return NumberProbe{Kind: KAbsent}
}
func (r RowData) ProbeBool(field string) BoolProbe {
	if c, ok := r.get(field); ok {
		return c.boolProbe()
	}
	return BoolProbe{Kind: KAbsent}
}
// The bench rows are flat (no nested obj/list within a row — relations are separate map nodes).
func (r RowData) ProbeRow(field string) RowProbe   { return RowProbe{Kind: KAbsent} }
func (r RowData) ProbeList(field string) ListProbe { return ListProbe{Kind: KAbsent} }

// ListData — a materialized wire LIST (the rows of a result), probed per element.
type ListData struct {
	rows []RowData
}

func (l ListData) Len() int { return len(l.rows) }
func (l ListData) ElemRow(i int) RowProbe {
	if i >= 0 && i < len(l.rows) {
		return RowProbe{Kind: KGot, Got: l.rows[i]}
	}
	return RowProbe{Kind: KAbsent}
}
// The bench lists are lists of rows (parents / children), never scalar-element lists.
func (l ListData) ElemString(i int) StringProbe { return StringProbe{Kind: KAbsent} }
func (l ListData) ElemNumber(i int) NumberProbe { return NumberProbe{Kind: KAbsent} }
func (l ListData) ElemBool(i int) BoolProbe     { return BoolProbe{Kind: KAbsent} }
func (l ListData) ElemList(i int) ListProbe     { return ListProbe{Kind: KAbsent} }

// Result — the handler's uniform return (a WireValue): a materialized result. AsList yields the rows (a
// read / single-write list); AsRow yields the first row (a tx-body single row / summary).
type Result struct {
	Rows []RowData
}

func (w Result) AsList() ListProbe { return ListProbe{Kind: KGot, Got: ListData{rows: w.Rows}} }
func (w Result) AsRow() RowProbe {
	if len(w.Rows) > 0 {
		return RowProbe{Kind: KGot, Got: w.Rows[0]}
	}
	return RowProbe{Kind: KNull, ActualWireType: "null"}
}
func (w Result) AsString() StringProbe { return StringProbe{Kind: KWrong, ActualWireType: "row"} }
func (w Result) AsNumber() NumberProbe { return NumberProbe{Kind: KWrong, ActualWireType: "row"} }
func (w Result) AsBool() BoolProbe     { return BoolProbe{Kind: KWrong, ActualWireType: "row"} }

// Summary — a no-RETURNING write's result: ONE summary row {changes,lastInsertRowid} (v1's mode-2 shape).
func Summary(changes, lastInsertRowid int64) Result {
	return Result{Rows: []RowData{{
		names: []string{"changes", "lastInsertRowid"},
		cells: []Cell{CellInt(changes), CellInt(lastInsertRowid)},
	}}}
}
