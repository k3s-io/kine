package errors

import (
	"errors"
	"fmt"
)

const notFoundErrorMsg = "%s %s not found"

type storeError struct {
	err error
}

func (e storeError) Error() string {
	return e.err.Error()
}

type notFound struct {
	err error
}

func (e notFound) Error() string {
	return e.err.Error()
}

type casMismatch struct{}

func (e casMismatch) Error() string {
	return "Cas Mismatch"
}

func StoreNotFound(s string) error {
	return &notFound{err: fmt.Errorf(notFoundErrorMsg, "store", s)}
}

func PartitionNotFound(p string) error {
	return &notFound{err: fmt.Errorf(notFoundErrorMsg, "partition", p)}
}

func ObjectNotFound(k string) error {
	return &notFound{err: fmt.Errorf(notFoundErrorMsg, "object with key", k)}
}

func NotFound() error {
	return &notFound{err: errors.New("not found")}
}

func Internal(err error) error {
	return &storeError{err: fmt.Errorf("internal error: %s", err.Error())}
}

func CasMismatch() error {
	return &casMismatch{}
}

func IsCasMismatch(err error) bool {
	var e *casMismatch
	return errors.As(err, &e)
}

func IsNotFoundErr(err error) bool {
	var e *notFound
	return errors.As(err, &e)
}

func InvalidTTL(err error) error {
	return &storeError{err: fmt.Errorf("invalid TTL param: %s", err.Error())}
}
