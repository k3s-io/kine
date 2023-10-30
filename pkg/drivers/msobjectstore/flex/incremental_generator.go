package types

import "fmt"

func NewIncrementalCasGenerator(init int) CasGenerator {
	generator := new(incrementalCasGenerator)
	generator.cas = init
	return generator
}

type incrementalCasGenerator struct {
	cas int
}

func (c *incrementalCasGenerator) Generate(_ *Object) string {
	c.cas++
	return fmt.Sprintf("%d", c.cas)
}
