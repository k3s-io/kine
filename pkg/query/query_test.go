package query

import "testing"

func TestWithLimit(t *testing.T) {
	tests := []struct {
		name  string
		query string
		limit int64
		want  string
	}{
		{
			name:  "appended when the query has no token",
			query: "SELECT id FROM kine ORDER BY name",
			limit: 5,
			want:  "SELECT id FROM kine ORDER BY name LIMIT 5",
		},
		{
			name:  "not appended when there is no limit",
			query: "SELECT id FROM kine ORDER BY name",
			limit: 0,
			want:  "SELECT id FROM kine ORDER BY name",
		},
		{
			name:  "substituted in place of the token",
			query: "SELECT id FROM (SELECT id FROM kine ORDER BY name " + LimitToken + ") AS mkv",
			limit: 5,
			want:  "SELECT id FROM (SELECT id FROM kine ORDER BY name LIMIT 5) AS mkv",
		},
		{
			name:  "token removed when there is no limit",
			query: "SELECT id FROM (SELECT id FROM kine ORDER BY name " + LimitToken + ") AS mkv",
			limit: 0,
			want:  "SELECT id FROM (SELECT id FROM kine ORDER BY name) AS mkv",
		},
		{
			name:  "negative limit is treated as no limit",
			query: "SELECT id FROM kine " + LimitToken,
			limit: -1,
			want:  "SELECT id FROM kine",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New(tt.query, "?", false, "test")
			got := n.WithLimit(tt.limit)
			if got.Query != tt.want {
				t.Errorf("WithLimit(%d):\n got: %q\nwant: %q", tt.limit, got.Query, tt.want)
			}
			if got.Name != "test" {
				t.Errorf("name not preserved: %q", got.Name)
			}
			if n.Query != Strip(tt.query) {
				t.Errorf("receiver mutated: %q", n.Query)
			}
		})
	}
}
