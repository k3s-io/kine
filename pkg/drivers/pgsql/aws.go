package pgsql

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/k3s-io/kine/pkg/aws"
	"github.com/sirupsen/logrus"
)

// awsIAMConnOptions returns the pgx stdlib connection options required to
// authenticate to Postgres via RDS IAM when no password is supplied in the
// datastore DSN.
//
// Returns nil if no AWS region is configured, i.e. we are not in an AWS
// environment and RDS IAM was not intended.
func awsIAMConnOptions(_ context.Context, config *pgx.ConnConfig) ([]stdlib.OptionOpenDB, error) {

	// A configured region is the signal for whether we are in an AWS environment.
	// The AWS credential chain does not resolve a region, so it is read from the
	// standard AWS environment variables. When none is set, treat this as a
	// non-AWS environment where RDS IAM was not intended, and skip it silently so
	// normal passwordless authentication remains in place.
	region := aws.Region()
	if region == "" {
		logrus.Debugf("No password supplied in datastore DSN and no AWS region is configured (AWS_REGION/AWS_DEFAULT_REGION); assuming non-AWS passwordless authentication")
		return nil, nil
	}

	logrus.Debugf("AWS region %q configured; loading AWS credentials for RDS IAM authentication", region)

	// A region is configured, so this is an AWS environment and RDS IAM is the
	// intended authentication method. Credentials are now mandatory: a failure to
	// load them is a misconfiguration rather than a reason to fall back to
	// passwordless auth.
	creds := aws.Credentials()
	if val, err := aws.Retrieve(creds); err != nil || val.AccessKeyID == "" {
		return nil, fmt.Errorf("AWS region %q is configured but AWS credentials could not be loaded for RDS IAM authentication: %w", region, err)
	}

	logrus.Infof("No password supplied in datastore DSN; using AWS credential chain to generate RDS IAM authentication tokens for user %q", config.User)
	if config.TLSConfig == nil {
		logrus.Warnf("RDS IAM authentication requires TLS but sslmode appears to be disabled; the connection is likely to be rejected")
	}

	// The token is regenerated for every new connection, so the ~15 minute token
	// lifetime is honoured as the connection pool opens and recycles connections.
	// The token is only required for the initial handshake, so it is safe to generate it in a BeforeConnect hook.
	hook := func(_ context.Context, connConfig *pgx.ConnConfig) error {
		logrus.Debugf("Generating RDS IAM authentication token for user %q at %s:%d", connConfig.User, connConfig.Host, connConfig.Port)
		val, err := aws.Retrieve(creds)
		if err != nil {
			return fmt.Errorf("failed to load AWS credentials for RDS IAM authentication: %w", err)
		}
		endpoint := net.JoinHostPort(connConfig.Host, strconv.Itoa(int(connConfig.Port)))
		token, err := aws.BuildRDSIAMAuthToken(endpoint, region, connConfig.User, val, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to generate RDS IAM authentication token: %w", err)
		}
		connConfig.Password = token
		return nil
	}
	return []stdlib.OptionOpenDB{stdlib.OptionBeforeConnect(hook)}, nil
}
