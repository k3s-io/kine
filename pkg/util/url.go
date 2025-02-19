package util

import (
	"fmt"
	"net/url"
)

func ParseURL(urlStr string) (*url.URL, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing URL. Ensure that any embedded values such as username & password have been URL encoded: %w", err)
	}

	return u, nil
}
