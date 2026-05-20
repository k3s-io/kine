package dialer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

var TTL = time.Second * 15

var CachingDialer = &dialer{
	Dialer: &net.Dialer{
		Timeout: time.Second * 5,
	},
}

// cacheEntry stores the addresses of the most recently resolved hostname.
// Since kine only connectes to a single datastore, a single entry is all
// that is generally necessary to serve lookups from cache.
type cacheEntry struct {
	host    string
	addrs   []string
	expires time.Time
}

// dialer wraps net.Dialer, caching successful hostname lookups with a short fixed TTL.
// If multiple addresses are returned, it attempts to dial them each sequentially.
// This differs from the default dialer, which does no caching and dials multiple addresses in parallel.
type dialer struct {
	*net.Dialer
	cache *cacheEntry
}

func (d *dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if !strings.HasPrefix(network, "tcp") {
		return d.Dialer.DialContext(ctx, network, address)
	}

	var (
		err        error
		host, port string
	)
	if host, port, err = net.SplitHostPort(address); err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return d.Dialer.DialContext(ctx, network, address)
	}

	entry := d.cache
	if entry == nil || entry.host != host || time.Now().After(entry.expires) {
		logrus.Tracef("Resolving hostname %s", host)
		addrs, err := d.Resolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addrs) == 0 {
			return nil, &net.AddrError{Err: "LookupHost returned empty address list", Addr: host}
		}
		entry = &cacheEntry{
			host:    host,
			addrs:   addrs,
			expires: time.Now().Add(TTL),
		}
		d.cache = entry
	}

	errs := []error{}
	for _, addr := range entry.addrs {
		conn, err := d.Dialer.DialContext(ctx, network, net.JoinHostPort(addr, port))
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}
