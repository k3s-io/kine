//go:build !cgo

package sqlite

import (
	"path/filepath"
	"testing"
)

func TestTranslateDSN(t *testing.T) {
	cwd, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	for _, test := range []struct {
		name, dsn, want string
	}{
		{
			name: "relative path",
			dsn:  "db/state.db?_journal=WAL",
			want: "file://" + filepath.Join(cwd, "db", "state.db") + "?_pragma=journal_mode(WAL)",
		},
		{
			name: "absolute path",
			dsn:  "/var/lib/kine/db/state.db?_busy_timeout=30000",
			want: "file:///var/lib/kine/db/state.db?_pragma=busy_timeout(30000)",
		},
		{
			name: "path with characters that need escaping",
			dsn:  "/var/lib/my kine/100%.db?_journal=WAL",
			want: "file:///var/lib/my%20kine/100%25.db?_pragma=journal_mode(WAL)",
		},
		{
			name: "shared cache is dropped",
			dsn:  "/var/lib/kine/db/state.db?cache=shared",
			want: "file:///var/lib/kine/db/state.db",
		},
		{
			name: "file URI without authority",
			dsn:  "file:/var/lib/kine/db/state.db?mode=rwc&_journal=WAL",
			want: "file:/var/lib/kine/db/state.db?_pragma=journal_mode(WAL)&mode=rwc",
		},
		{
			name: "file URI with empty authority",
			dsn:  "file:///var/lib/kine/db/state.db?_journal=WAL",
			want: "file:///var/lib/kine/db/state.db?_pragma=journal_mode(WAL)",
		},
		{
			name: "file URI with relative path",
			dsn:  "file:db/state.db?_journal=WAL",
			want: "file:db/state.db?_pragma=journal_mode(WAL)",
		},
		{
			name: "in-memory database",
			dsn:  ":memory:",
			want: ":memory:",
		},
		{
			name: "in-memory database URI keeps shared cache",
			dsn:  "file::memory:?cache=shared",
			want: "file::memory:?cache=shared",
		},
		{
			name: "named in-memory database keeps shared cache",
			dsn:  "file:memdb?mode=memory&cache=shared",
			want: "file:memdb?cache=shared&mode=memory",
		},
		{
			name: "temporary database",
			dsn:  "",
			want: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := translateDSN(test.dsn)
			if err != nil {
				t.Fatalf("translateDSN(%q) failed: %v", test.dsn, err)
			}
			if got != test.want {
				t.Errorf("translateDSN(%q) = %q, want %q", test.dsn, got, test.want)
			}
		})
	}
}
