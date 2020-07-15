package compat

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

func (g *Backend) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	tx := g.ListCurrentQuery(ctx, prefix, limit, includeDeleted, nil)
	return tx.Rows()
}

func (g *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	subquery := g.DB.WithContext(ctx).
		Where("id <= ?", revision)

	if startKey != "" {
		subsubquery := g.FindBestLatestKeyBoundByRevision(ctx, startKey, revision)
		subquery = subquery.Where("id > (?)", subsubquery)
	}

	tx := g.ListCurrentQuery(ctx, prefix, limit, includeDeleted, subquery)
	return tx.Rows()
}

func (g *Backend) Count(ctx context.Context, prefix string) (int64, int64, error) {
	kv := KineEntry{}
	tx := g.CurrentRevisionQuery(ctx).Find(&kv)
	if tx.Error == nil {
		var children int64
		tx := g.ListCurrentWithPrefixQuery(ctx, prefix, false, nil, "id as theid").
			Select("COUNT(theid)").
			Count(&children)
		if tx.Error == nil {
			return int64(kv.ID), children, nil
		}
	}
	return 0, 0, tx.Error
}

func (g *Backend) CurrentRevision(ctx context.Context) (revision int64, err error) {
	kv := KineEntry{}
	tx := g.CurrentRevisionQuery(ctx).Find(&kv)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return
	}
	return int64(kv.ID), tx.Error
}

func (g *Backend) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
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

func (g *Backend) Insert(
	ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value,
	prevValue []byte,
) (revision int64, err error) {
	defer func() {
		if interceptErr := g.HandleInsertionError(err); interceptErr != nil {
			err = interceptErr
		}
	}()

	entity := KineEntry{
		Name:           key,
		Created:        create,
		Deleted:        delete,
		CreateRevision: uint64(createRevision),
		PrevRevision:   uint64(previousRevision),
		Lease:          uint64(ttl),
		Value:          value,
		OldValue:       prevValue,
	}

	tx := g.DB.WithContext(ctx).
		Save(&entity)
	return int64(entity.ID), tx.Error
}

func (g *Backend) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return g.DB.WithContext(ctx).
		Where(&KineEntry{ID: uint64(revision)}).
		Select(
			fmt.Sprintf("0, 0, %s", columns),
		).
		Rows()
}

func (g *Backend) DeleteRevision(ctx context.Context, revision int64) error {
	tx := g.DB.WithContext(ctx).
		Delete(&KineEntry{ID: uint64(revision)})
	return tx.Error
}

func (g *Backend) GetCompactRevision(ctx context.Context) (revision int64, err error) {
	var kv KineEntry
	tx := g.GetCompactRevisionQuery(ctx).
		Last(&kv)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return
	}
	return int64(kv.PrevRevision), tx.Error
}

func (g *Backend) SetCompactRevision(ctx context.Context, revision int64) error {
	tx := g.GetCompactRevisionQuery(ctx).
		Updates(&KineEntry{PrevRevision: uint64(revision)})
	return tx.Error
}

func (g *Backend) Fill(ctx context.Context, revision int64) error {
	tx := g.DB.WithContext(ctx).Create(
		&KineEntry{
			ID:             uint64(revision),
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

func (g *Backend) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}
