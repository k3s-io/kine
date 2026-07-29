package pgsql

import (
	"context"
	"fmt"
	"net"
	"strconv"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"
)

// awsIAMConnOptions returns the pgx stdlib connection options required to
// authenticate to Postgres via RDS IAM when no password is supplied in the
// datastore DSN.
//
// Returns nil if AWS credentials could not be loaded or no AWS credentials were found.
func awsIAMConnOptions(ctx context.Context, config *pgx.ConnConfig) ([]stdlib.OptionOpenDB, error) {

	// Whether a region can be resolved is the signal for whether we are in an AWS
	// environment: LoadDefaultConfig errors when WithEC2IMDSRegion cannot reach
	// IMDS and no region is configured elsewhere. Treat that as a non-AWS
	// environment where RDS IAM was not intended, and skip it silently so normal
	// passwordless authentication remains in place.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithEC2IMDSRegion())
	if err != nil {
		logrus.Debugf("No password supplied in datastore DSN and no AWS region could be resolved (%v); assuming non-AWS passwordless authentication", err)
		return nil, nil
	}

	logrus.Debugf("AWS region %q resolved; AWS credentials loaded for RDS IAM authentication", awsCfg.Region)

	// A region resolved, so this is an AWS environment and RDS IAM is the intended
	// authentication method. Credentials are now mandatory: a failure to load them
	// is a misconfiguration rather than a reason to fall back to passwordless auth.
	if _, err := awsCfg.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("AWS region %q is configured but AWS credentials could not be loaded for RDS IAM authentication: %w", awsCfg.Region, err)
	}

	logrus.Infof("No password supplied in datastore DSN; using AWS default credential provider to generate RDS IAM authentication tokens for user %q", config.User)
	if config.TLSConfig == nil {
		logrus.Warnf("RDS IAM authentication requires TLS but sslmode appears to be disabled; the connection is likely to be rejected")
	}

	// The token is regenerated for every new connection, so the ~15 minute token lifetime
	// is honoured as the connection pool opens and recycles connections.
	hook := func(ctx context.Context, connConfig *pgx.ConnConfig) error {
		endpoint := net.JoinHostPort(connConfig.Host, strconv.Itoa(int(connConfig.Port)))
		token, err := awsauth.BuildAuthToken(ctx, endpoint, awsCfg.Region, connConfig.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("failed to generate RDS IAM authentication token: %w", err)
		}
		connConfig.Password = token
		return nil
	}
	return []stdlib.OptionOpenDB{stdlib.OptionBeforeConnect(hook)}, nil
}
