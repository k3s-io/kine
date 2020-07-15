package compat

import (
	"github.com/rancher/kine/pkg/alpha/gorm"
)

type Backend struct {
	gorm.DatabaseBackend
}

var columns = "id as theid, name, created, deleted, create_revision, prev_revision, lease, value, old_value"

func New(backend *gorm.DatabaseBackend) (*Backend, error) {
	if err := backend.DB.AutoMigrate(&KineEntry{}); err != nil {
		return nil, err
	}
	compatBackend := &Backend{*backend}
	return compatBackend, nil
}
