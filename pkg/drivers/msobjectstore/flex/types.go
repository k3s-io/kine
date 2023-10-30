package types

// Storage is an interface to implement any object store strategy
type Storage interface {
	// UpsertStore creates or updates a store.
	UpsertStore(s *Store) error

	// GetStores retrieves a list of stores.
	// Additionally it may contain a page identifier
	GetStores() (*Stores, error)

	// GetPartitions retrieves a list of partitions in a given store.
	// Additionally it may contain a page identifier
	GetPartitions(store string) (*Partitions, error)

	// GetKeys retrieves a list of keys in a given partition
	// Additionally it may contain a page identifier
	GetKeys(store, partition string) (*Keys, error)

	// Store is used to save an object in the implemented backend
	Store(cmd StoreCmd) error

	// GetValue retrieves a stored object in a given partition by its key
	GetValue(cmd GetCmd) (GetResult, error)

	// DeleteValue deletes a stored object in a given partition by its key
	DeleteValue(store, partition, key string) error

	// DeletePartition deletes all stored object's in a given partition
	DeletePartition(store, partition string) error
}

type Partition interface {
	GetStore() string
	GetPartition() string
}

type Coordinate interface {
	Partition
	GetKey() string
}

type GetResult interface {
	GetValue() *Object
	GetVersion() string
}

type GetCmd interface {
	Coordinate
}

type StoreCmd interface {
	Coordinate
	GetValue() *Object
	IsMatch() bool
	GetMatchVersion() string
	IsNoneMatch() bool
	GetNoneMatchVersion() string
}

type CasGenerator interface {
	Generate(obj *Object) string
}
