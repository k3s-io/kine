package util

import (
	"net/url"

	"github.com/pkg/errors"
)

func ParseURL(urlStr string) (*url.URL, error) {
	u, err := url.Parse(urlStr)
	return u, errors.Wrap(err, "error parsing URL. Ensure that any embedded values such as username & password have been URL encoded")
}
