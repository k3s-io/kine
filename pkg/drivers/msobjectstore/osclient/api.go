package osclient

import (
	"io"
	"net/http"
)

//type ObjectStoreClient interface {
//	Get(key string) (value *server.KeyValue, err error)
//	Add(key string, value *server.KeyValue) (err error)
//	Update(key string, value *server.KeyValue)
//	Delete(key string) (err error)
//	Keys() (keyList []string)
//	List(prefix string) (valueList []*server.KeyValue)
//}

type HTTPClient interface {
	Get(url string) (resp *http.Response, err error)
	Put(url, contentType string, body io.Reader) (resp *http.Response, err error)
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
	Delete(url string) (resp *http.Response, err error)
}
