package pgsql

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
)

// awsIAMConnOptions is only ever called by prepareConfig once it has already
// established that the DSN carries no password, so these tests exercise its
// remaining job: resolve the AWS region, probe the minio-go credential chain,
// and decide between engaging RDS IAM and skipping all AWS work for a non-AWS
// environment.

// cleanAWSEnv makes the region resolution and minio-go credential chain
// deterministic for tests: it disables IMDS (so the chain never reaches the
// network), points the shared credentials file at a nonexistent path, and clears
// any AWS_* variables that could otherwise leak the developer's real credentials
// or region into the test. Individual tests then set only the variables they need.
func cleanAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", missing)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN",
		"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
	} {
		t.Setenv(k, "")
	}
}

func mustParseConfig(t *testing.T, dsn string) *pgx.ConnConfig {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn %q: %v", dsn, err)
	}
	return config
}

// In a non-AWS environment no region resolves, so the function must skip AWS
// entirely rather than error — leaving normal passwordless auth in place.
func TestAWSIAMConnOptions_NoAWSConfig_SkipsAWS(t *testing.T) {
	cleanAWSEnv(t)

	config := mustParseConfig(t, "postgres://user@localhost:5432/db")
	opts, err := awsIAMConnOptions(context.Background(), config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts != nil {
		t.Fatalf("expected nil opts when AWS config cannot be loaded, got %d option(s)", len(opts))
	}
}

// A region resolves (so we are in an AWS environment) but no credentials do:
// RDS IAM is intended, so a failure to load credentials must be a hard error
// rather than a silent fall back to passwordless auth.
func TestAWSIAMConnOptions_RegionButNoCredentials_Errors(t *testing.T) {
	cleanAWSEnv(t)
	t.Setenv("AWS_REGION", "us-east-1")

	config := mustParseConfig(t, "postgres://user@localhost:5432/db")
	opts, err := awsIAMConnOptions(context.Background(), config)
	if err == nil {
		t.Fatal("expected an error when a region is configured but no credentials are available")
	}
	if opts != nil {
		t.Fatalf("expected nil opts on error, got %d option(s)", len(opts))
	}
}

// Credentials and a region both resolve: RDS IAM is engaged and exactly one
// BeforeConnect option is returned. The auth token is only created when
// the hook is called, so no network call happens here.
func TestAWSIAMConnOptions_CredentialsAndRegion_ReturnsOption(t *testing.T) {
	cleanAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")

	config := mustParseConfig(t, "postgres://user@localhost:5432/db")
	opts, err := awsIAMConnOptions(context.Background(), config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected exactly one conn option, got %d", len(opts))
	}
}
