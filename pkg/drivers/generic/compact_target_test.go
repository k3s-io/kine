package generic

import (
	"database/sql"
	"math"
	"testing"

	"github.com/k3s-io/kine/pkg/query"
	_ "modernc.org/sqlite"
)

func TestCompactTarget(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE kine (id INTEGER PRIMARY KEY); INSERT INTO kine VALUES (9), (17)"); err != nil {
		t.Fatal(err)
	}

	probe := &query.Named{Query: "SELECT id FROM kine WHERE id >= ? ORDER BY id LIMIT 1"}
	head := &query.Named{Query: "SELECT MAX(id) FROM kine"}
	invalid := &query.Named{Query: "SELECT id FROM missing_table"}
	for _, tc := range []struct {
		name           string
		target, retain int64
		probe, head    *query.Named
		want           int64
		wantErr        bool
	}{
		{"exact witness", 12, 5, probe, invalid, 12, false},
		{"witness beyond a gap", 10, 5, probe, invalid, 10, false},
		{"near head", 15, 5, probe, head, 12, false},
		{"entire history retained", 12, 20, probe, head, 0, false},
		{"threshold overflow", math.MaxInt64, 5, invalid, head, 12, false},
		{"no probe configured", 12, 3, nil, head, 12, false},
		{"probe error", 12, 5, invalid, head, 0, true},
		{"fallback error", 15, 5, probe, invalid, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Generic{DB: db, RevisionAtLeastSQL: tc.probe, CurrentRevSQL: tc.head, ErrCode: func(error) string { return "" }}
			tx, err := d.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.MustRollback()
			got, err := tx.CompactTarget(t.Context(), tc.target, tc.retain)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("CompactTarget(%d, %d) = %d, %v; want %d, wantErr=%t", tc.target, tc.retain, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
