package dqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/drivers/sqlite"
	"github.com/rancher/kine/pkg/server"
)

var (
	Dialer = client.DefaultDialFunc
	Logger = client.DefaultLogFunc
)

type opts struct {
	peers    []client.NodeInfo
	peerFile string
	dsn      string
}

func New(ctx context.Context, datasourceName string) (server.Backend, error) {
	opts, err := parseOpts(datasourceName)
	if err != nil {
		return nil, err
	}

	var nodeStore client.NodeStore
	if opts.peerFile != "" {
		nodeStore, err = client.DefaultNodeStore(opts.peerFile)
		if err != nil {
			return nil, err
		}
	} else {
		nodeStore = client.NewInmemNodeStore()
	}

	existing, err := nodeStore.Get(ctx)
	if err != nil {
		return nil, err
	}

	err = nodeStore.Set(ctx, append(existing, opts.peers...))
	if err != nil {
		return nil, err
	}

	d, err := driver.New(nodeStore,
		driver.WithLogFunc(Logger),
		driver.WithContext(ctx),
		driver.WithDialFunc(Dialer))
	if err != nil {
		return nil, err
	}

	sql.Register("dqlite", d)
	backend, dialect, err := sqlite.NewVariant("dqlite", opts.dsn)
	if err != nil {
		return nil, err
	}

	dialect.LockWrites = true
	return backend, nil
}

func parseOpts(dsn string) (opts, error) {
	result := opts{
		dsn: dsn,
	}

	parts := strings.SplitN(dsn, "?", 2)
	if len(parts) == 1 {
		return result, nil
	}

	values, err := url.ParseQuery(parts[1])
	if err != nil {
		return result, err
	}

	for k, vs := range values {
		if len(vs) == 0 {
			continue
		}

		switch k {
		case "peer":
			for _, v := range vs {
				parts := strings.SplitN(v, ":", 3)
				if len(parts) != 3 {
					return result, fmt.Errorf("must ID:IP:PORT format got: %s", v)
				}
				id, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					return result, errors.Wrapf(err, "failed to parse %s", parts[0])
				}
				result.peers = append(result.peers, client.NodeInfo{
					ID:      id,
					Address: parts[1] + ":" + parts[2],
				})
			}
			delete(values, k)
		case "peer-file":
			result.peerFile = vs[0]
			delete(values, k)
		}
	}

	if len(values) == 0 {
		result.dsn = parts[0]
	} else {
		result.dsn = fmt.Sprintf("%s?%s", parts[0], values.Encode())
	}

	return result, nil
}
