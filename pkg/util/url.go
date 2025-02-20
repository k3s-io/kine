package util

import (
	"net/url"

	"github.com/pkg/errors"
)

func ParseURL(urlStr string) (*url.URL, error) {
	u, err := url.Parse(urlStr)
	return u, errors.Wrap(err, "failed to parse datastore DSN URL; ensure that parameters containing special characters use percent-encoding")
}
