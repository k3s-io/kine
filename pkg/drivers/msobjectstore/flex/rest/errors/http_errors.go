package errors

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/k3s-io/kine/pkg/drivers/msobjectstore/flex/sharedstorage/errors"
)

type HTTPError interface {
	GetName() string
	GetMessage() string
	GetStatusCode() int
	GetBytes() ([]byte, error)
}

type httpError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

func (e httpError) Error() string {
	return fmt.Sprintf("status: %d, name: %s, message: %s", e.Status, e.Name, e.Message)
}

func (e httpError) GetName() string {
	return e.Name
}

func (e httpError) GetMessage() string {
	return e.Message
}

func (e httpError) GetStatusCode() int {
	return e.Status
}

func (e httpError) GetBytes() ([]byte, error) {
	return json.Marshal(e)
}

func NotFound(m string) HTTPError {
	return &httpError{
		Name:    "NotFound",
		Message: m,
		Status:  http.StatusNotFound,
	}
}

func InvalidBody(m string) HTTPError {
	return &httpError{
		Name:    "InvalidBody",
		Message: "invalidBody: " + m,
		Status:  http.StatusBadRequest,
	}
}

func InternalError(m error) HTTPError {
	return &httpError{
		Name:    "InternalError",
		Message: m.Error(),
		Status:  http.StatusInternalServerError,
	}
}

func InvalidParam(m string) HTTPError {
	return &httpError{
		Name:    "InvalidParam",
		Message: "invalidParam: " + m,
		Status:  http.StatusBadRequest,
	}
}

func ServiceUnavailable(m string) HTTPError {
	return &httpError{
		Name:    "Service Unavailable",
		Message: m,
		Status:  http.StatusServiceUnavailable,
	}
}

func Error(err error) HTTPError {
	if errors.IsNotFoundErr(err) {
		return NotFound(err.Error())
	}

	return InternalError(err)
}
