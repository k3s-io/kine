package osclient

import (
	"errors"
)

const (
	osClientErrFmt = "ms-object-store-client: "
)

//func ErrKeyCastingFailed(msg string) error {
//	return errors.New(fmt.Sprintf("ms-object-store-client: casting failed with error: %q", msg))
//}

// Driver Errors
var (
	ErrKeyValueConfigRequired = errors.New("ms-object-store-driver: config required")
	ErrInvalidBucketName      = errors.New("ms-object-store-driver: invalid bucket name")
	ErrInvalidKey             = errors.New("ms-object-store-driver: invalid key")
	ErrNotBinaryValue         = errors.New("ms-object-store-driver: invalid value, value must be binary")
	ErrNullValue              = errors.New("ms-object-store-driver: invalid value, value must not be null")
	ErrKeyCastingFailed       = errors.New("ms-object-store-driver: casting failed")
	ErrMarshallFailed         = errors.New("ms-object-store-driver: marshall failed")
	ErrBucketNotFound         = errors.New("ms-object-store-driver: bucket not found")
	ErrBadBucket              = errors.New("ms-object-store-driver: bucket not valid key-value store")
	ErrKeyNotFound            = errors.New("ms-object-store-driver: key not found")
	ErrKeyAlreadyExists       = errors.New("ms-object-store-driver: key already exist")
	ErrKeyDeleted             = errors.New("ms-object-store-driver: key was deleted")
	ErrHistoryToLarge         = errors.New("ms-object-store-driver: history limited to a max of 64")
	ErrNoKeysFound            = errors.New("ms-object-store-driver: no keys found")
	ErrHealthCheckFailed      = errors.New("ms-object-store-driver: health check failed")
)

// Client Errors
var (
	ErrClientOperationFailed = errors.New("ms-object-store-client: failed while processing")
	ErrClientResponseFailed  = errors.New("ms-object-store-client: error response from client")
)
