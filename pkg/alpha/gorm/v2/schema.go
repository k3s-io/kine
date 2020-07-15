package gorm

import (
	"time"

	"github.com/satori/go.uuid"
	"gorm.io/gorm"
)

// This one is a new schema design that is way more advanced
type KineGlobalState struct {
	CurrentRevisionID        uint64 `gorm:"default:1;"`
	LastRanCompactRevisionID uint64

	// Gorm fields
	UpdatedAt time.Time
}

type KineKeyValueState struct {
	ID                uuid.UUID `gorm:"type:uuid;primary_key;"`
	Name              string    `gorm:"index;index:name_id;uniqueIndex:cur_rev_name"`
	CurrentVersion    uint64    `gorm:"index;uniqueIndex:cur_rev_name;default:1;"`
	CreatedAtRevision uint64
	UpdatedAtRevision uint64

	// Gorm fields
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	// Foreign Keys
	CurrentLeaseID *string

	// Virtual Fields
	CurrentVersionData KineVersionData   `gorm:"foreignKey:KVEntityID;references:CurrentVersion;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
	VersionData        []KineVersionData `gorm:"foreignKey:KVEntityID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

type KineVersionData struct {
	Version uint64 `gorm:"autoIncrement;primary_key;"`
	Value   []byte

	// Gorm fields
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

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
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (kvs *KineKeyValueState) BeforeCreate(*gorm.DB) (err error) {
	kvs.ID = uuid.NewV4()
	return
}

func (l *KineLease) BeforeDelete(db *gorm.DB) (err error) {
	tx := db.Delete(&l.BoundKV)
	return tx.Error
}
