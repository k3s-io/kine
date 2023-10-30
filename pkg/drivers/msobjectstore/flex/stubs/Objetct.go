package stubs

import (
	types "github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex"
)

type ObjectOption = func(*types.Object)

func NewObject(options ...ObjectOption) *types.Object {
	o := new(types.Object)

	for _, opt := range options {
		opt(o)
	}

	return o
}

func WithKey(key string) ObjectOption {
	return func(o *types.Object) {
		o.KeyID = key
	}
}

func WithMetadata(metadata map[string]string) ObjectOption {
	return func(o *types.Object) {
		o.Metadata = make(map[string]string)
		for k, v := range metadata {
			o.Metadata[k] = v
		}
	}

	//return func(o *types.Object) {
	//	o.Metadata = make(map[string]string)
	//	for i := 0; i < 1000; i++ {
	//		key, _ := randomtoken.Generate()
	//		o.Metadata[key] = key
	//	}
	//}
}

func WithStringValue(val string) ObjectOption {
	return func(o *types.Object) {
		o.ValueType = types.ObjectTypeString
		o.StringValue = val
	}
}

func WithBinaryValue(val string) ObjectOption {
	return func(o *types.Object) {
		o.ValueType = types.ObjectTypeBinary
		o.BinaryValue = val
	}
}
