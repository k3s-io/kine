package types

type Stores struct {
	Values        []*Store `json:"values"`
	NextPageToken string   `json:"next_page_token,omitempty"`
}

type Store struct {
	StoreID                       string `json:"storeId,omitempty"`
	Encrypted                     bool   `json:"encrypted,omitempty"`
	DefaultTTLSeconds             int    `json:"defaultTtlSeconds,omitempty"`
	MaximumEntries                int    `json:"maximumEntries,omitempty"`
	DefaultConfirmationTTLSeconds int    `json:"defaultConfirmationTtlSeconds,omitempty"`
}

type Partitions struct {
	Values        []string `json:"values"`
	NextPageToken string   `json:"next_page_token,omitempty"`
}

type Keys struct {
	Values        []*Key `json:"values"`
	NextPageToken string `json:"next_page_token,omitempty"`
}

type Key struct {
	KeyID string `json:"keyId,omitempty"`
}

type ObjectType string

const (
	ObjectTypeNumber = "NUMBER"
	ObjectTypeBinary = "BINARY"
	ObjectTypeString = "STRING"
)

type Object struct {
	KeyID       string            `json:"keyId,omitempty"`
	Index       int               `json:"index,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ValueType   ObjectType        `json:"valueType,omitempty"`
	StringValue string            `json:"stringValue,omitempty"`
	NumberValue int               `json:"numberValue,omitempty"`
	BinaryValue string            `json:"binaryValue,omitempty"`
}
