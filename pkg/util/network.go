package util

import "strings"

// SchemeAndAddress crudely splits a URL string into scheme and address,
// where the address includes everything after the scheme/authority separator.
func SchemeAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
