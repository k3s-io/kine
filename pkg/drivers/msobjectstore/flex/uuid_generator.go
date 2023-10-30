package types

import (
	"github.com/google/uuid"
)

func NewUUIDCasGenerator() CasGenerator {
	return new(uuidCasGenerator)
}

type uuidCasGenerator struct{}

func (g *uuidCasGenerator) Generate(_ *Object) string {
	return uuid.New().String()
}
