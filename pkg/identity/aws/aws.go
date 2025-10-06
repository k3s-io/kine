package aws

import (
	"context"
	"regexp"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/k3s-io/kine/pkg/identity"
)

func init() {
	identity.Register("aws", New)
}

var RegionRegexp = regexp.MustCompile(`([^.]+).rds.amazonaws.com:?(\d+)?`)

func New(ctx context.Context) (identity.TokenSource, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, config identity.Config) (string, error) {
		region := cfg.Region

		regions := RegionRegexp.FindStringSubmatch(config.Endpoint)
		if len(regions) > 1 {
			region = regions[1]
		}

		token, err := auth.BuildAuthToken(
			ctx,
			config.Endpoint,
			region,
			config.User,
			cfg.Credentials,
		)
		if err != nil {
			return "", err
		}

		return token, nil
	}, err
}
