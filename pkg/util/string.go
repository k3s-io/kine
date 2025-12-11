package util

import (
	"fmt"
	"regexp"
)

var whitespace = regexp.MustCompile("[\n\t ]+")

// Stripped wraps a string, stripping newlines and collapsing whitespace
type Stripped string

func (s Stripped) String() string {
	return whitespace.ReplaceAllString(string(s), " ")
}

// Summarize wraps a slice of any, and when stringed only prints the type/length of complex arguments
type Summarize []any

func (s Summarize) String() string {
	ret := make([]any, len(s))
	for i := range s {
		switch v := s[i].(type) {
		case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, complex64, complex128, uintptr, bool:
			ret[i] = v
		case string:
			ret[i] = fmt.Sprintf("'%s'", v)
		case []byte:
			ret[i] = fmt.Sprintf("[%d]byte(...)", len(v))
		default:
			ret[i] = fmt.Sprintf("%T{...}", v)
		}
	}
	return fmt.Sprint(ret)
}
