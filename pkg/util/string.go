package util

import (
	"fmt"
	"regexp"
	"strings"
)

var whitespace = regexp.MustCompile(`[\n\t ]+`)
var params = regexp.MustCompile(`\?|\$[0-9]+`)

// Stripped wraps a string, stripping newlines and collapsing whitespace.
type Stripped string

func (s Stripped) String() string {
	return strings.TrimSpace(whitespace.ReplaceAllString(string(s), " "))
}

// Summarize wraps a slice of any, and when stringed only prints the type/length
// of complex arguments.
type Summarize []any

func (s Summarize) String() string {
	if len(s) == 0 {
		return ""
	}
	return fmt.Sprint(summarize(s))
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

// Fill wraps a parametrized query string and args, and when stringed prints the
// query string with parameters filled by summarized arg values.
//
//revive:disable-next-line:unexported-return
func Fill(query string, args []any) *filled {
	return &filled{query: query, args: args}
}

type filled struct {
	query string
	args  []any
}

func (f *filled) String() string {
	return fmt.Sprintf(params.ReplaceAllString(Stripped(f.query).String(), "%v"), summarize(f.args)...)
}
