package types

func NewCoordinate(store string, partition string, key string) Coordinate {
	c := new(coordinate)
	c.store = store
	c.partition = partition
	c.key = key
	return c
}

type coordinate struct {
	store, partition, key string
}

func (c *coordinate) GetStore() string {
	return c.store
}

func (c *coordinate) GetPartition() string {
	return c.partition
}

func (c *coordinate) GetKey() string {
	return c.key
}
