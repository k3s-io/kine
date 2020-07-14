package gorm

type KineEntry struct {
	ID             uint64 `gorm:"primaryKey;index:name_id;AUTO_INCREMENT"`
	Name           string `gorm:"index;index:name_id;uniqueIndex:name_prev_revision"`
	Created        bool
	Deleted        bool
	CreateRevision uint64
	PrevRevision   uint64 `gorm:"uniqueIndex:name_prev_revision"`
	Lease          uint64
	Value          []byte
	OldValue       []byte
}
