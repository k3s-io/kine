package msobjectstore

import (
	"encoding/json"
	"strings"
	"time"

	types "github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex"
	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/osclient"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

/*
parseKey parses ETCD key into MS Object Store keys
key example:

	/registry/sample-apiserver/gateway.mulesoft.com/apiinstances/default/object-store-api

returns:

	store: controlNodeStore
	partition: controlNodePartition
	resourceType: apiinstances
	resourceKey: default_object-store-api = <resource-namespace>_<resource-name>

	/registry/sample-apiserver/gateway.mulesoft.com/apiinstances/default/object-store-api-01
*/
func parseKey(key string) (store, partition, resourceType, resourceKey string, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "msobjectstore driver.parseKey %s => store=%s, partition=%s, resourceType=%s, resourceKey=%s, err=%v, duration=%s"
		logrus.Infof(fStr, key, store, partition, resourceType, resourceKey, err, dur)
	}()

	if !strings.HasPrefix(key, apiServerResourceKeyPrefix) {
		err = osclient.ErrInvalidKey
		logrus.Errorf("%s: string %s has not prefix %s", err.Error(), key, apiServerResourceKeyPrefix)
		return
	}

	tmpStr := strings.ReplaceAll(key, apiServerResourceKeyPrefix, "")
	tmpSlc := strings.Split(tmpStr, "/")
	if len(tmpSlc) != 3 {
		err = osclient.ErrInvalidKey
		logrus.Errorf("%s: slice %+v has size %v and not size %v", err.Error(), tmpSlc, len(tmpSlc), 3)
		return
	}

	resourceType = tmpSlc[0]
	resourceKey = tmpSlc[1] + "_" + tmpSlc[2]
	store = controlNodeStore
	partition = controlNodePartition

	return
}

func parseKeyValue(res types.GetResult, key string) (kv *server.KeyValue, err error) {
	if res.GetValue().ValueType != types.ObjectTypeBinary {
		err = osclient.ErrNotBinaryValue
		return
	}

	if res.GetValue().BinaryValue == "" {
		err = osclient.ErrNullValue
		return
	}

	if _, ok := res.GetValue().Metadata[key]; !ok {
		err = osclient.ErrKeyNotFound
		return
	}

	var resBundle osclient.ResourceBundle
	if err = json.Unmarshal([]byte(res.GetValue().BinaryValue), &resBundle); err != nil {
		logrus.Errorf("error while unmarshalling binary value from store: %s", err)
		err = osclient.ErrKeyCastingFailed
		kv = nil
		return
	}

	var ok bool
	if kv, ok = resBundle.Get(key); !ok {
		err = osclient.ErrKeyNotFound
		return
	}

	return
}

func parseBundleFromObjectValue(value string) (bundle osclient.Bundled, err error) {
	logrus.Infof("parseBundleFromObjectValue: %s", value)

	bundle = osclient.NewBundle()
	if err = json.Unmarshal([]byte(value), &bundle); err != nil {
		logrus.Errorf("error while unmarshalling binary value from store: %s", err)
		err = osclient.ErrKeyCastingFailed
	}

	logrus.Infof("parseBundleFromObjectValue: returning %+v", bundle)

	return
}
