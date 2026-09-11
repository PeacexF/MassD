// Package record defines the unit of data flowing from a source adapter to the
// database writer. Sources own their schema; the writer only turns records into
// batched INSERT statements.
package record

import "strings"

type Conflict uint8

const (
	Fail Conflict = iota
	Ignore
	Replace
)

func (c Conflict) verb() string {
	switch c {
	case Ignore:
		return "INSERT OR IGNORE INTO "
	case Replace:
		return "INSERT OR REPLACE INTO "
	default:
		return "INSERT INTO "
	}
}

// Record is one row destined for one table. Cols and Vals are parallel.
type Record struct {
	Table    string
	Cols     []string
	Vals     []any
	Conflict Conflict
}

func New(table string) Record {
	return Record{Table: table, Cols: make([]string, 0, 12), Vals: make([]any, 0, 12)}
}

func (r Record) Set(col string, val any) Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, val)
	return r
}

// SetIf keeps optional fields NULL instead of empty when cond is false.
func (r Record) SetIf(cond bool, col string, val any) Record {
	if !cond {
		return r
	}
	return r.Set(col, val)
}

func (r Record) OnConflict(c Conflict) Record {
	r.Conflict = c
	return r
}

// Signature identifies records that can share one INSERT statement.
func (r Record) Signature() string {
	var b strings.Builder
	b.Grow(len(r.Table) + 8*len(r.Cols))
	b.WriteByte(byte('0' + r.Conflict))
	b.WriteString(r.Table)
	for _, c := range r.Cols {
		b.WriteByte(0)
		b.WriteString(c)
	}
	return b.String()
}

func (r Record) InsertSQL(rows int) string {
	var b strings.Builder
	b.WriteString(r.Conflict.verb())
	b.WriteString(r.Table)
	b.WriteString(" (")
	for i, c := range r.Cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(c)
	}
	b.WriteString(") VALUES ")
	group := "(" + strings.TrimSuffix(strings.Repeat("?,", len(r.Cols)), ",") + ")"
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(group)
	}
	return b.String()
}
