package gorm

import (
	"time"

	uuid "github.com/satori/go.uuid"
	ogGorm "gorm.io/gorm"
)

// This one is schema-compatible with original design, and is preferred
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

// This one is a new schema design that is way more advanced
type KineGlobalState struct {
	CurrentRevisionID        uint64
	LastRanCompactRevisionID uint64

	// Gorm fields
	UpdatedAt time.Time
}

type KineKeyValueState struct {
	ID                uuid.UUID `gorm:"type:uuid;primary_key;"`
	Name              string    `gorm:"index;index:name_id;uniqueIndex:cur_rev_name"`
	CurrentVersion    uint64    `gorm:"index;uniqueIndex:cur_rev_name;"`
	CreatedAtRevision uint64
	UpdatedAtRevision uint64

	// Gorm fields
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt ogGorm.DeletedAt `gorm:"index"`

	// Foreign Keys
	CurrentLeaseID *string

	// Virtual Fields
	CurrentVersionData KineVersionData   `gorm:"foreignKey:KVEntityID;references:CurrentVersion;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
	VersionData        []KineVersionData `gorm:"foreignKey:KVEntityID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

type KineVersionData struct {
	Revision uint64 `gorm:"autoIncrement;primary_key;"`
	Value    []byte

	// Gorm fields
	CreatedAt time.Time
	DeletedAt ogGorm.DeletedAt `gorm:"index"`

	// Foreign Keys
	KVEntityID uuid.UUID
}

type KineLease struct {
	ID             string `gorm:"primary_key;"`
	AvailableUntil time.Time
	BoundKV        []KineKeyValueState `gorm:"foreignkey:CurrentLeaseID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// Gorm fields
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt ogGorm.DeletedAt `gorm:"index"`
}

func (kvs *KineKeyValueState) BeforeCreate(*ogGorm.DB) (err error) {
	kvs.ID = uuid.NewV4()
	return
}

func (l *KineLease) BeforeDelete(db *ogGorm.DB) (err error) {
	tx := db.Delete(&l.BoundKV)
	return tx.Error
}
