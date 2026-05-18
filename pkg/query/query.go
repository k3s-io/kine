package query

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var whitespace = regexp.MustCompile(`[\n\t ]+`)
var params = regexp.MustCompile(`\?|\$[0-9]+`)

// Named is a named SQL query string that formats nicely when stringed.
// The name should be used for tracking in metrics, and printed in error logs.
type Named struct {
	Name  string
	Query string
}

// New returns a new named query, and handles replacing `?` parameters
// with whatever parameter characters the driver requires.
func New(query, param string, numbered bool, name string) *Named {
	if param != "?" || numbered {
		n := 0
		regex := regexp.MustCompile(`\?`)
		query = regex.ReplaceAllStringFunc(query, func(string) string {
			if numbered {
				n++
				return param + strconv.Itoa(n)
			}
			return param
		})
	}
	return &Named{Name: name, Query: Strip(query)}
}

// Append returns a copy of the named query, with additional text appended to the query string.
func (n *Named) Appendf(format string, a ...any) *Named {
	return &Named{Name: n.Name, Query: n.Query + " " + fmt.Sprintf(format, a...)}
}

// String nicely formats the query and name for printing in logs.
func (n *Named) String() string {
	if n.Name == "" {
		return n.Query
	}
	return n.Query + " /* " + n.Name + " */"
}

// Fill returns an instance of the query filled with the given args.
func (n *Named) Fill(args []any) *Filled {
	return &Filled{Named: n, Args: args}
}

// Filled prints the query with summarized parametrized arg values when stringed.
type Filled struct {
	*Named
	Args []any
}

func (f *Filled) String() string {
	return fmt.Sprintf(params.ReplaceAllString(f.Named.String(), "%v"), summarize(f.Args)...)
}

func (f *Filled) QueryString() string {
	return fmt.Sprintf(params.ReplaceAllString(f.Query, "%v"), summarize(f.Args)...)
}

// Stmt is a wrapper around sql.Stmt that remembers what query it was prepared from
type Stmt struct {
	*Named
	Stmt *sql.Stmt
}

func (s *Stmt) Close() error {
	if s.Stmt != nil {
		return s.Stmt.Close()
	}
	return nil
}

func Strip(s string) string {
	return strings.TrimSpace(whitespace.ReplaceAllString(s, " "))
}

func summarize(s []any) []any {
	ret := make([]any, len(s))
	for i := range s {
		switch v := s[i].(type) {
		case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, complex64, complex128, uintptr, bool:
			ret[i] = fmt.Sprint(v)
		case string:
			ret[i] = fmt.Sprintf("'%s'", v)
		case []byte:
			ret[i] = fmt.Sprintf("[%d]byte(...)", len(v))
		default:
			ret[i] = fmt.Sprintf("%T{...}", v)
		}
	}
	return ret
}
