package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

func (g *GormBacked) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	tx := g.ListCurrentQuery(ctx, prefix, limit, includeDeleted, nil)
	return tx.Rows()
}

var columns = "id as theid, name, created, deleted, create_revision, prev_revision, lease, value, old_value"

func (g *GormBacked) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	subquery := g.DB.WithContext(ctx).
		Where("id <= ?", revision)

	if startKey != "" {
		subsubquery := g.DB.WithContext(ctx).Model(&KineEntry{}).
			Where("name = ?", startKey).
			Where("id <= ?", revision).
			Select("id")
		subquery = subquery.Where("id > (?)", subsubquery)
	}

	tx := g.ListCurrentQuery(ctx, prefix, limit, includeDeleted, subquery)
	return tx.Rows()
}

func (g *GormBacked) Count(ctx context.Context, prefix string) (int64, int64, error) {
	kv := KineEntry{}
	tx := g.CurrentRevisionQuery(ctx).Find(&kv)
	if tx.Error == nil {
		var children int64
		tx := g.ListCurrentWithPrefixQuery(ctx, prefix, false, nil).
			Select("COUNT(theid)").
			Count(&children)
		if tx.Error == nil {
			return kv.ID, children, nil
		}
	}
	return 0, 0, tx.Error
}

func (g *GormBacked) CurrentRevision(ctx context.Context) (int64, error) {
	kv := KineEntry{}
	tx := g.CurrentRevisionQuery(ctx).Find(&kv)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	return kv.ID, tx.Error
}

func (g *GormBacked) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Limit(int(limit)).
		Order("id ASC").
		Where("name LIKE ?", prefix).
		Where("id > ?", rev).
		Select(
			fmt.Sprintf("(?), (?), %s", columns),
			g.CurrentRevisionQuery(ctx), g.GetCompactRevisionQuery(ctx),
		)

	return tx.Rows()
}

func (g *GormBacked) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (revision int64, err error) {
	defer func() {
		if g.HandleInsertionError != nil {
			if interceptErr := g.HandleInsertionError(err); interceptErr != nil {
				err = interceptErr
			}
		}
	}()

	entity := KineEntry{
		Name:           key,
		Created:        create,
		Deleted:        delete,
		CreateRevision: createRevision,
		PrevRevision:   previousRevision,
		Lease:          ttl,
		Value:          value,
		OldValue:       prevValue,
	}

	tx := g.DB.WithContext(ctx).
		Save(&entity)
	return entity.ID, tx.Error
}

func (g *GormBacked) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return g.DB.WithContext(ctx).
		Where(&KineEntry{ID: revision}).
		Select(
			fmt.Sprintf("0, 0, %s", columns),
		).
		Rows()
}

func (g *GormBacked) DeleteRevision(ctx context.Context, revision int64) error {
	tx := g.DB.WithContext(ctx).
		Delete(&KineEntry{ID: revision})
	return tx.Error
}

func (g *GormBacked) GetCompactRevision(ctx context.Context) (int64, error) {
	var kv KineEntry
	tx := g.GetCompactRevisionQuery(ctx).
		Last(&kv)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	return kv.PrevRevision, tx.Error
}

func (g *GormBacked) SetCompactRevision(ctx context.Context, revision int64) error {
	tx := g.GetCompactRevisionQuery(ctx).
		Updates(&KineEntry{PrevRevision: revision})
	return tx.Error
}

func (g *GormBacked) Fill(ctx context.Context, revision int64) error {
	tx := g.DB.WithContext(ctx).Create(
		&KineEntry{
			ID:             revision,
			Name:           fmt.Sprintf("gap-%d", revision),
			Created:        false,
			Deleted:        true,
			CreateRevision: 0,
			PrevRevision:   0,
			Lease:          0,
			Value:          nil,
			OldValue:       nil,
		},
	)
	return tx.Error
}

func (g *GormBacked) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

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

func (g *GormBacked) ListCurrentQueryBase(ctx context.Context, includeDeleted bool) *gorm.DB {
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
	return g.ListCurrentWithPrefixQuery(ctx, prefix, includeDeleted, subqueryForMaxKey).
		Limit(int(limit))
}

func (g *GormBacked) ListCurrentWithPrefixQuery(ctx context.Context, prefix string, includeDeleted bool, subqueryForMaxKey *gorm.DB) *gorm.DB {
	subquery := g.FindMaxPossibleRevisionForPrefix(ctx, prefix, subqueryForMaxKey)
	tx := g.ListCurrentQueryBase(ctx, includeDeleted).
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