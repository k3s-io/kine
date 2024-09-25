package drivers

import (
	"context"
	"errors"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
)

var ErrUnknownDriver = errors.New("unknown driver")

func New(ctx context.Context, cfg *Config) (leaderElect bool, backend server.Backend, err error) {
	if cfg.Endpoint == "" {
		driver := GetDefault()
		if driver == nil {
			return false, nil, errors.New("no default driver found")
		}
		return driver(ctx, cfg)
	}

	if err := validateDSNuri(cfg.Endpoint); err != nil {
		return false, nil, err
	}

	cfg.Scheme, cfg.DataSourceName = util.SchemeAndAddress(cfg.Endpoint)

	driver, ok := Get(cfg.Scheme)
	if !ok {
		return false, nil, ErrUnknownDriver
	}
	return driver(ctx, cfg)
}

// validateDSNuri ensure that the given string is of that format <scheme://<authority>
func validateDSNuri(str string) error {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return nil
	}
	return errors.New("invalid datastore endpoint; endpoint should be a DSN URI in the format <scheme>://<authority>")
}
