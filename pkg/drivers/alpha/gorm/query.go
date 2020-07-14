package gorm

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

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
			g.CurrentRevisionQuery(ctx), g.CurrentCompactRevisionQuery(ctx),
		)
	return tx
}

func (g *GormBacked) GetCompactRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Where(&KineEntry{Name: "compact_rev_key"}).
		Select("id")

	return tx
}

func (g *GormBacked) CurrentCompactRevisionQuery(ctx context.Context) *gorm.DB {
	tx := g.GetCompactRevisionQuery(ctx).
		Limit(1).
		Order("id DESC")

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

func (g *GormBacked) FindBestKeyBoundByRevision(ctx context.Context, startKey string, maxRevision int64) *gorm.DB {
	tx := g.DB.WithContext(ctx).
		Model(&KineEntry{}).
		Where("name = ?", startKey).
		Where("id <= ?", maxRevision).
		Select("id")
	return tx
}

func (g *GormBacked) FindBestLatestKeyBoundByRevision(ctx context.Context, startKey string, revision int64) *gorm.DB {
	tx := g.FindBestKeyBoundByRevision(ctx, startKey, revision).
		Limit(1).
		Order("id DESC")
	return tx
}
