package util

import (
	"fmt"
	"net/url"
)

func ParseURL(urlStr string) (*url.URL, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse datastore DSN URL; ensure that parameters containing special characters use percent-encoding: %w", err)
	}
	return u, nil
}
