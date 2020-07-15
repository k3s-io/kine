package compat

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

var columns = "id as theid, name, created, deleted, create_revision, prev_revision, lease, value, old_value"

func (g *Backend) CurrentRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Limit(1).
		Order("id DESC").
		Select("id")
	return tx
}

func (g *Backend) FindMaxPossibleRevisionForPrefix(ctx context.Context, prefix string, subquery *gorm.DB) *gorm.DB {
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

func (g *Backend) ListQueryBase(ctx context.Context, includeDeleted bool, columns string) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Order("id ASC").
		Where(&KineEntry{Deleted: includeDeleted}).
		Select(
			fmt.Sprintf("(?), (?), %s", columns),
			g.CurrentRevisionQuery(ctx), g.CurrentCompactRevisionQuery(ctx),
		)
	return tx
}

func (g *Backend) GetCompactRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Where(&KineEntry{Name: "compact_rev_key"}).
		Select("id")

	return tx
}

func (g *Backend) CurrentCompactRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.GetCompactRevisionQuery(ctx).
		Limit(1).
		Order("id DESC")

	return tx
}

func (g *Backend) ListQuery(ctx context.Context, prefix string, limit int64, includeDeleted bool, subqueryForMaxRevision *gorm.DB) *gorm.DB {
	return g.ListCurrentWithPrefixQuery(ctx, prefix, includeDeleted, subqueryForMaxRevision, columns).
		Limit(int(limit))
}

func (g *Backend) ListCurrentWithPrefixQuery(ctx context.Context, prefix string, includeDeleted bool, subqueryForMaxRevision *gorm.DB, columns string) *gorm.DB {
	subquery := g.FindMaxPossibleRevisionForPrefix(ctx, prefix, subqueryForMaxRevision)
	tx := g.ListQueryBase(ctx, includeDeleted, columns).
		Joins("JOIN (?) mp ON id = mp.mid", subquery)
	return tx
}

func (g *Backend) FindBestKeyBoundByRevision(ctx context.Context, startKey string, maxRevision int64) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Where("name = ?", startKey).
		Where("id <= ?", maxRevision).
		Select("id")
	return tx
}

func (g *Backend) FindBestLatestKeyBoundByRevision(ctx context.Context, startKey string, revision int64) *gorm.DB {
	tx := g.FindBestKeyBoundByRevision(ctx, startKey, revision).
		Limit(1).
		Order("id DESC")
	return tx
}
