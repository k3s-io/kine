package osclient

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/k3s-io/kine/pkg/server"
)

type KV struct {
	kv sync.Map
}

func (c *KV) Get(key string) (value *server.KeyValue, err error) {

	if v, ok := c.kv.Load(key); ok {
		if v, ok := v.(server.KeyValue); ok {
			return &v, nil
		}
		return nil, errors.New("ErrKeyCastingFailed")
	}

	return nil, errors.New("ErrKeyNotFound")
}

func (c *KV) Add(key string, value *server.KeyValue) (err error) {
	if _, ok := c.kv.Load(key); !ok {
		c.kv.Store(key, *value)
		return nil
	}

	return errors.New("ErrKeyAlreadyExist")
}

func (c *KV) Update(key string, value *server.KeyValue) (err error) {
	if _, ok := c.kv.Load(key); ok {
		c.kv.Store(key, *value)
		return nil
	}

	return errors.New("ErrKeyNotFound")
}

func (c *KV) Delete(key string) error {
	if _, ok := c.kv.Load(key); !ok {
		c.kv.Delete(key)
		return nil
	}

	return errors.New("ErrKeyNotFound")
}

func (c *KV) Keys() (keyList []string) {
	c.kv.Range(func(key, value interface{}) bool {
		keyList = append(keyList, fmt.Sprint(key))
		return true
	})
	return
}

func (c *KV) List(prefix string) (valueList []*server.KeyValue) {
	c.kv.Range(func(key, value interface{}) bool {
		var (
			ok bool
			v  server.KeyValue
		)

		if !strings.HasPrefix(fmt.Sprint(key), prefix) {
			return false
		}

		if v, ok = value.(server.KeyValue); !ok {
			return false
		}

		valueList = append(valueList, &v)

		return true
	})
	return
}
