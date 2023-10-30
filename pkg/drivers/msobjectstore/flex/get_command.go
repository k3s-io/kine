package types

func NewGetCommand(store string, partition string, key string) GetCmd {
	return NewCoordinate(store, partition, key)
}

func NewGetResult(object *Object, version string) GetResult {
	c := new(getResult)
	c.value = object
	c.version = version
	return c
}

type getResult struct {
	value   *Object
	version string
}

func (g *getResult) GetValue() *Object {
	return g.value
}

func (g *getResult) GetVersion() string {
	return g.version
}
