package nats

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	natsserver "github.com/k3s-io/kine/pkg/drivers/nats/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type Config struct {
	// Client URL which could be a list of comma separated URLs.
	clientURL string
	// Client connection options.
	clientOptions []nats.Option
	// Number of revisions to keep in history. Defaults to 10.
	revHistory uint8
	// Name of the bucket. Defaults to "kine".
	bucket string
	// Number of replicas for the bucket. Defaults to 1
	replicas int
	// Indicates the duration of a method before it is considered slow. Defaults to 500ms.
	slowThreshold time.Duration
	// If true, an embedded server will not be used.
	noEmbed bool
	// If true, use a socket for the embedded server.
	dontListen bool
	// Path to a server configuration file when embedded.
	serverConfig string
	// If true, the embedded server will log to stdout.
	stdoutLogging bool
	// The explicit host to listen on when embedded.
	host string
	// The explicit port to listen on when embedded.
	port int
	// Data directory.
	dataDir string
}

// parseConnection returns nats connection url, bucketName and []nats.Option, error
func parseConnection(dsn string, tlsInfo tls.Config) (*Config, error) {
	config := &Config{
		slowThreshold: defaultSlowMethod,
		revHistory:    defaultRevHistory,
		bucket:        defaultBucket,
		replicas:      defaultReplicas,
	}

	// Parse the first URL in the connection string which contains the
	// query parameters.
	connections := strings.Split(dsn, ",")
	u, err := url.Parse(connections[0])
	if err != nil {
		return nil, err
	}

	// Extract the host and port if embedded server is used.
	config.host = u.Hostname()
	if u.Port() != "" {
		config.port, _ = strconv.Atoi(u.Port())
	}

	// Extract the query parameters to build configuration.
	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	if v := queryMap.Get("bucket"); v != "" {
		config.bucket = v
	}

	if v := queryMap.Get("replicas"); v != "" {
		if r, err := strconv.ParseUint(v, 10, 8); err == nil {
			if r >= 1 && r <= 5 {
				config.replicas = int(r)
			} else {
				return nil, fmt.Errorf("invalid replicas, must be >= 1 and <= 5")
			}
		}
	}

	if d := queryMap.Get("slowMethod"); d != "" {
		if dur, err := time.ParseDuration(d); err == nil {
			config.slowThreshold = dur
		} else {
			return nil, fmt.Errorf("invalid slowMethod duration: %w", err)
		}
	}

	if r := queryMap.Get("revHistory"); r != "" {
		if revs, err := strconv.ParseUint(r, 10, 8); err == nil {
			if revs >= 2 && revs <= 64 {
				config.revHistory = uint8(revs)
			} else {
				return nil, fmt.Errorf("invalid revHistory, must be >= 2 and <= 64")
			}
		}
	}

	if tlsInfo.KeyFile != "" && tlsInfo.CertFile != "" {
		config.clientOptions = append(config.clientOptions, nats.ClientCert(tlsInfo.CertFile, tlsInfo.KeyFile))
	}

	if tlsInfo.CAFile != "" {
		config.clientOptions = append(config.clientOptions, nats.RootCAs(tlsInfo.CAFile))
	}

	// Simpler direct reference to creds file.
	if f := queryMap.Get("credsFile"); f != "" {
		config.clientOptions = append(config.clientOptions, nats.UserCredentials(f))
	}

	// Reference a full context file. Note this will override any other options.
	if f := queryMap.Get("contextFile"); f != "" {
		if u.Host != "" {
			return config, fmt.Errorf("when using context endpoint no host should be provided")
		}

		logrus.Debugf("loading nats context file: %s", f)

		natsContext, err := natscontext.NewFromFile(f)
		if err != nil {
			return nil, err
		}

		connections = strings.Split(natsContext.ServerURL(), ",")

		// command line options provided to kine will override the file
		// https://github.com/nats-io/jsm.go/blob/v0.0.29/natscontext/context.go#L257
		// allows for user, creds, nke, token, certificate, ca, inboxprefix from the context.json
		natsClientOpts, err := natsContext.NATSOptions(config.clientOptions...)
		if err != nil {
			return nil, err
		}
		config.clientOptions = natsClientOpts
	}

	connBuilder := strings.Builder{}
	for idx, c := range connections {
		if idx > 0 {
			connBuilder.WriteString(",")
		}

		u, err := url.Parse(c)
		if err != nil {
			return nil, err
		}

		if u.Scheme != "nats" {
			return nil, fmt.Errorf("invalid connection string=%s", c)
		}

		connBuilder.WriteString("nats://")

		if u.User != nil && idx == 0 {
			userInfo := strings.Split(u.User.String(), ":")
			if len(userInfo) > 1 {
				config.clientOptions = append(config.clientOptions, nats.UserInfo(userInfo[0], userInfo[1]))
			} else {
				config.clientOptions = append(config.clientOptions, nats.Token(userInfo[0]))
			}
		}
		connBuilder.WriteString(u.Host)
	}

	config.clientURL = connBuilder.String()

	// Config options only relevant if built with embedded NATS.
	if natsserver.Embedded {
		config.noEmbed = queryMap.Has("noEmbed")
		config.serverConfig = queryMap.Get("serverConfig")
		config.stdoutLogging = queryMap.Has("stdoutLogging")
		config.dontListen = queryMap.Has("dontListen")
		config.dataDir = queryMap.Get("dataDir")
	}

	logrus.Debugf("using config %#v", config)

	return config, nil
}
