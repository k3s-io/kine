package sqllog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"
)

func BenchmarkRowsToEvents(b *testing.B) {
	tests := []struct {
		name string
		val  bool
		prev bool
	}{
		{"keys", false, false},
		{"list", true, false},
		{"watch", true, true},
	}

	for _, i := range []int{1, 100, 1000} {
		db := newDummyDB(b, i)
		for _, tt := range tests {
			b.Run(fmt.Sprintf("%s/%d", tt.name, i), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rows, err := db.Query(tt.name, tt.val, tt.prev)
					if err != nil {
						b.Fatal(err)
					}
					batch, _, err := RowsToEvents(rows, tt.val, tt.prev)
					if err != nil || len(batch.Events) != i {
						b.Fatal(err, len(batch.Events))
					}
				}
			})
		}
	}
}

func newDummyDB(b *testing.B, count int) *sql.DB {
	rows := make([][]any, count)
	for i := range rows {
		dest := make([]any, 11)
		dest[0] = 100000
		dest[1] = 0
		dest[2] = 10000 + i + 1
		dest[3] = fmt.Sprintf("/registry/pods/default/pod-%d", i+1)
		dest[4] = false
		dest[5] = false
		dest[6] = i + 1
		dest[7] = i + 1
		dest[8] = 0
		dest[9] = []byte("current")
		dest[10] = []byte("previous")
		rows[i] = dest
	}
	return sql.OpenDB(&dummyDB{b: b, rows: rows})
}

type dummyDB struct {
	b     *testing.B
	rows  [][]any
	scans int
	val   bool
	prev  bool
}

func (d *dummyDB) Connect(_ context.Context) (driver.Conn, error) {
	return d, nil
}

func (d *dummyDB) Driver() driver.Driver {
	return d
}

func (d *dummyDB) Open(_ string) (driver.Conn, error) {
	return d, nil
}

func (d *dummyDB) Prepare(_ string) (driver.Stmt, error) {
	return d, nil
}

func (d *dummyDB) Close() error {
	return nil
}

func (d *dummyDB) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (d *dummyDB) NumInput() int {
	return -1
}

func (d *dummyDB) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}

func (d *dummyDB) Query(args []driver.Value) (driver.Rows, error) {
	d.scans = 0
	var ok bool
	if d.val, ok = args[0].(bool); !ok {
		return nil, errors.New("arg not bool")
	}
	if d.prev, ok = args[1].(bool); !ok {
		return nil, errors.New("arg not bool")
	}
	return d, nil
}

func (d *dummyDB) Columns() []string {
	cols := 9
	if d.val {
		cols++
		if d.prev {
			cols++
		}
	}
	return make([]string, cols)
}

func (d *dummyDB) Next(dest []driver.Value) error {
	if d.scans >= len(d.rows) {
		return io.EOF
	}
	for i := range len(dest) {
		dest[i] = d.rows[d.scans][i]
	}
	d.scans++
	return nil
}
