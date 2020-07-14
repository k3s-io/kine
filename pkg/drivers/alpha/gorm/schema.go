package gorm

type KineEntry struct {
	ID             int64  `gorm:"primaryKey;index:name_id;AUTO_INCREMENT"`
	Name           string `gorm:"index;index:name_id;uniqueIndex:name_prev_revision"`
	Created        bool
	Deleted        bool
	CreateRevision int64
	PrevRevision   int64 `gorm:"uniqueIndex:name_prev_revision"`
	Lease          int64
	Value          []byte
	OldValue       []byte
}
