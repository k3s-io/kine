package osclient

import "fmt"

// Client URLs formats
const (
	// TODO resolve flex url ar start up
	localHost          = "localhost"
	dockerComposeHost  = "flex"
	flexObjectStoreURL = "http://" + localHost + ":8081/api/v1"

	getStoresEndpoint = "/stores"

	upsertStoreEndpointFmt     = "/stores/%s"
	getPartitionsEndpointFmt   = "/stores/%s/partitions"
	deletePartitionEndpointFmt = "/stores/%s/partitions/%s"
	getKeysEndpointFmt         = "/stores/%s/partitions/%s/keys"
	crudObjectEndpointFmt      = "/stores/%s/partitions/%s/keys/%s"
)

func upsertStoreURL(store string) string {
	return flexObjectStoreURL + fmt.Sprintf(upsertStoreEndpointFmt, store)
}

func getPartitionsURL(partition string) string {
	return flexObjectStoreURL + fmt.Sprintf(getPartitionsEndpointFmt, partition)
}

func deletePartitionURL(store, partition string) string {
	return flexObjectStoreURL + fmt.Sprintf(deletePartitionEndpointFmt, store, partition)
}

func getKeysURL(store, partition string) string {
	return flexObjectStoreURL + fmt.Sprintf(getKeysEndpointFmt, store, partition)
}

func crudValueURL(store, partition, key string) string {
	return flexObjectStoreURL + fmt.Sprintf(crudObjectEndpointFmt, store, partition, key)
}
