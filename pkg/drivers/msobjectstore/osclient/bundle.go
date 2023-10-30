package osclient

import (
	"encoding/json"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

type Bundled interface {
	Contains(string) bool
	Upsert(string, *server.KeyValue) bool
	Remove(string) bool
	Get(string) (*server.KeyValue, bool)
	List(string, int64) []*server.KeyValue
	Encode() (string, error)
	Index() map[string]string
}

type ResourceBundle struct {
	Data map[string]*server.KeyValue `json:"data"`
}

func NewBundle() Bundled {
	return &ResourceBundle{Data: make(map[string]*server.KeyValue, 1)}
}

/*func (b *ResourceBundle) Decode(data string) (err error) {
	if err = json.Unmarshal([]byte(data), b); err != nil {
		logrus.Errorf("error while unmarshalling binary value from store: %s", err)
		err = ErrKeyCastingFailed
		return
	}

	return
}*/

func (b *ResourceBundle) Encode() (data string, err error) {
	var kvb []byte
	if kvb, err = json.Marshal(b); err != nil {
		logrus.Errorf("msobjectstore driver.buildServerKeyValueString: error marshalling value into server key value: %q", err)
		err = ErrMarshallFailed
		return
	}

	data = string(kvb)

	return
}

// Index returns a map with every resource name as keys
func (b *ResourceBundle) Index() (index map[string]string) {
	index = make(map[string]string, len(b.Data))

	for k, _ := range b.Data {
		index[k] = ""
	}

	return
}

func (b *ResourceBundle) Get(resourceName string) (data *server.KeyValue, ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		return
	}

	data, ok = b.Data[resourceName]

	return
}

func (b *ResourceBundle) List(filter string, limit int64) (kvs []*server.KeyValue) {
	logrus.Debugf("msobjectstore ResourceBundle.List: data: %+v, filter: %s, limit: %d", b.Data, filter, limit)

	if len(b.Data) == 0 {
		return
	}

	size := limit
	if int64(len(b.Data)) < limit {
		size = int64(len(b.Data))
	}

	kvs = make([]*server.KeyValue, 0, size)

	logrus.Debugf("msobjectstore driver.List: ranging over data: %+v", b.Data)
	for k, v := range b.Data {
		logrus.Debugf("msobjectstore driver.List: element key v: %v", v)

		if filter == "" || strings.HasPrefix(k, filter) {
			logrus.Debugf("msobjectstore driver.List: element %s: %v match filter: %s", k, v, filter)
			kvs = append(kvs, v)
		}

		if int64(len(kvs)) == size {
			break
		}
	}

	return
}

func (b *ResourceBundle) Contains(resourceName string) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		ok = false
		return
	}

	_, ok = b.Data[resourceName]

	return
}

func (b *ResourceBundle) Upsert(resourceName string, data *server.KeyValue) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		b.Data = map[string]*server.KeyValue{}
	}

	b.Data[resourceName] = data
	ok = true

	return
}

func (b *ResourceBundle) Remove(resourceName string) (ok bool) {
	if b.Data == nil || len(b.Data) == 0 {
		ok = true
		return
	}

	delete(b.Data, resourceName)
	ok = true

	return
}
