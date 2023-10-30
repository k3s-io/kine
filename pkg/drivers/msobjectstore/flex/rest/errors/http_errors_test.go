package errors

import (
	"net/http"
	"testing"

	. "github.com/onsi/gomega"
)

func TestGetName(t *testing.T) {
	g := NewWithT(t)

	name := "appropiatedPlacesholder"

	err := httpError{
		Name:    name,
		Message: "bar",
		Status:  http.StatusNotFound,
	}

	g.Expect(err.GetName()).Should(Equal(name))
}

func TestGetMessage(t *testing.T) {
	g := NewWithT(t)

	message := "appropiatedPlacesholder"

	err := httpError{
		Name:    "appropiatedPlacesholder",
		Message: message,
		Status:  http.StatusNotFound,
	}

	g.Expect(err.GetMessage()).Should(Equal(message))
}

func TestGetStatusCode(t *testing.T) {
	g := NewWithT(t)

	status := http.StatusNotFound

	err := httpError{
		Name:    "appropiatedPlacesholder",
		Message: "bar",
		Status:  status,
	}

	g.Expect(err.GetStatusCode()).Should(Equal(status))
}

func TestServiceUnavailable(t *testing.T) {
	g := NewWithT(t)

	errMsg := "service unavailable"

	err := ServiceUnavailable(errMsg)

	g.Expect(err.GetMessage()).Should(Equal(errMsg))
	g.Expect(err.GetStatusCode()).Should(Equal(http.StatusServiceUnavailable))
	g.Expect(err.GetName()).Should(Equal("Service Unavailable"))
}
