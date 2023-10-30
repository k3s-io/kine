package types

func NewStoreCmd(store string, partition string, key string, value *Object, options ...StoreCmdOpts) StoreCmd {
	cmd := new(storeCmd)
	cmd.Coordinate = NewCoordinate(store, partition, key)
	cmd.value = value
	cmd.matches = false
	cmd.noneMatches = false

	for _, op := range options {
		op(cmd)
	}

	return cmd
}

type storeCmd struct {
	Coordinate
	matchesVersion, noneMatchesVersion string
	matches, noneMatches               bool
	value                              *Object
}

type StoreCmdOpts = func(command *storeCmd)

func WithMatches(match string) StoreCmdOpts {
	return func(cmd *storeCmd) {
		if match != "" {
			cmd.matches = true
			cmd.matchesVersion = match
		}
	}
}

func WithNoneMatch(match string) StoreCmdOpts {
	return func(cmd *storeCmd) {
		if match != "" {
			cmd.noneMatches = true
			cmd.noneMatchesVersion = match
		}
	}
}

func (c *storeCmd) GetVersion() string {
	return c.matchesVersion
}

func (c *storeCmd) GetValue() *Object {
	return c.value
}

func (c *storeCmd) IsMatch() bool {
	return c.matches
}

func (c *storeCmd) GetMatchVersion() string {
	return c.matchesVersion
}

func (c *storeCmd) IsNoneMatch() bool {
	return c.noneMatches
}

func (c *storeCmd) GetNoneMatchVersion() string {
	return c.noneMatchesVersion
}
