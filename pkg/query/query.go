package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// LimitToken marks the position in a query where WithLimit should place the
// row limit clause. It is written as a comment so that a query carrying it
// remains valid SQL even if the limit is never applied.
const LimitToken = "/*limit*/"

var whitespace = regexp.MustCompile(`[\n\t ]+`)

// limitToken matches LimitToken along with any whitespace preceding it, so that
// removing the token does not leave a stray space behind.
var limitToken = regexp.MustCompile(`\s*` + regexp.QuoteMeta(LimitToken))
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

// WithLimit returns a copy of the named query with a row limit applied.
//
// Queries containing LimitToken have it replaced by the limit clause, which
// lets a driver position the limit inside a subquery instead of at the end of
// the statement. Queries without the token get the limit appended, as before.
// A limit of zero or less applies no limit, and only strips the token.
func (n *Named) WithLimit(limit int64) *Named {
	var clause string
	if limit > 0 {
		clause = " LIMIT " + strconv.FormatInt(limit, 10)
	}
	if limitToken.MatchString(n.Query) {
		return &Named{Name: n.Name, Query: limitToken.ReplaceAllLiteralString(n.Query, clause)}
	}
	if clause == "" {
		return n
	}
	return &Named{Name: n.Name, Query: n.Query + clause}
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
