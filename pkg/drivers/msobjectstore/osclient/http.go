package osclient

import (
	"io"
	"net/http"
	"time"
)

type httpClient struct {
	client *http.Client
}

func NewHTTPClient(timeOut time.Duration) HTTPClient {
	c := new(httpClient)
	c.client = &http.Client{Timeout: timeOut}

	return c
}

func (c *httpClient) Get(url string) (resp *http.Response, err error) {
	return c.client.Get(url)
}

func (c *httpClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	return c.client.Post(url, contentType, body)
}

func (c *httpClient) Put(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)

	return c.client.Do(req)
}

func (c *httpClient) Delete(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	return c.client.Do(req)
}
