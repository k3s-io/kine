package gorm

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

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

type GormBacked struct {
	DB                   *gorm.DB
	HandleInsertionError func(error) error
}

var columns = "id as theid, name, created, deleted, create_revision, prev_revision, lease, value, old_value"

func (g *GormBacked) CurrentRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Limit(1).
		Order("id DESC").
		Select("id")
	return tx
}
func (g *GormBacked) FindMaxPossibleRevisionForPrefix(ctx context.Context, prefix string, subquery *gorm.DB) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Group("name").
		Where("name LIKE ?", prefix).
		Select("MAX(id) as mid")
	if subquery != nil {
		tx = tx.Where(subquery)
	}
	return tx
}

func (g *GormBacked) ListCurrentQueryBase(ctx context.Context, includeDeleted bool, columns string) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Order("id ASC").
		Where(&KineEntry{Deleted: includeDeleted}).
		Select(
			fmt.Sprintf("(?), (?), %s", columns),
			g.CurrentRevisionQuery(ctx), g.GetCompactRevisionQuery(ctx),
		)
	return tx
}

func (g *GormBacked) GetCompactRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Limit(1).
		Order("id DESC").
		Where(&KineEntry{Name: "compact_rev_key"}).
		Select("id")

	return tx
}

func (g *GormBacked) ListCurrentQuery(ctx context.Context, prefix string, limit int64, includeDeleted bool, subqueryForMaxKey *gorm.DB) *gorm.DB {
	return g.ListCurrentWithPrefixQuery(ctx, prefix, includeDeleted, subqueryForMaxKey, columns).
		Limit(int(limit))
}

func (g *GormBacked) ListCurrentWithPrefixQuery(ctx context.Context, prefix string, includeDeleted bool, subqueryForMaxKey *gorm.DB, columns string) *gorm.DB {
	subquery := g.FindMaxPossibleRevisionForPrefix(ctx, prefix, subqueryForMaxKey)
	tx := g.ListCurrentQueryBase(ctx, includeDeleted, columns).
		Joins("JOIN (?) mp ON id = mp.mid", subquery)
	return tx
}

func New(ctx context.Context, dialect gorm.Dialector) (*GormBacked, error) {
	db, err := gorm.Open(dialect, &gorm.Config{
		Logger: &Logger{},
	})
	if err != nil {
		return nil, err
	}

	rawDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// Actually, I don't think this is much needed
	// GORM offers automatic pinging so no worries
majorScope:
	for i := 0; i < 300; i++ {
		for i := 0; i < 3; i++ {
			if err = rawDB.Ping(); err == nil {
				break majorScope
			}
		}

		err = rawDB.Close()
		logrus.WithError(err).Error("failed to ping connection")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	if err := db.AutoMigrate(&KineEntry{}); err != nil {
		return nil, err
	}

	backend := &GormBacked{DB: db}
	return backend, nil
}
