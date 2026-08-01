package aws

import (
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// BuildRDSIAMAuthToken must produce a SigV4 presigned "connect" request for the
// rds-db service that is byte-for-byte identical to the token the AWS SDK's
// rds/auth.BuildAuthToken produces for the same inputs. These golden values were
// captured from that SDK function at the fixed signing time below, both with and
// without a session token — an exact match is what RDS validates against.
func TestBuildRDSIAMAuthToken_MatchesAWSSDK(t *testing.T) {
	signTime := time.Date(2026, 8, 1, 5, 12, 4, 0, time.UTC)
	const (
		endpoint = "mydb.abc123.us-east-1.rds.amazonaws.com:5432"
		region   = "us-east-1"
		dbUser   = "iamuser"
		ak       = "AKIAIOSFODNN7EXAMPLE"
		sk       = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)

	tests := []struct {
		name         string
		sessionToken string
		want         string
	}{
		{
			name: "without session token",
			want: "mydb.abc123.us-east-1.rds.amazonaws.com:5432?Action=connect&DBUser=iamuser&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260801%2Fus-east-1%2Frds-db%2Faws4_request&X-Amz-Date=20260801T051204Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=c48dbba92abac759166815b4dc2bc4ec48416dfd1ebcf95e3e3f6547e174ef99",
		},
		{
			name:         "with session token",
			sessionToken: "sess+tok/en=abc",
			want:         "mydb.abc123.us-east-1.rds.amazonaws.com:5432?Action=connect&DBUser=iamuser&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260801%2Fus-east-1%2Frds-db%2Faws4_request&X-Amz-Date=20260801T051204Z&X-Amz-Expires=900&X-Amz-Security-Token=sess%2Btok%2Fen%3Dabc&X-Amz-SignedHeaders=host&X-Amz-Signature=3f15459cce5b78e02696226293422728ac4ea6f36415fc764d6e3b94a10ca086",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildRDSIAMAuthToken(endpoint, region, dbUser, credentials.Value{
				AccessKeyID:     ak,
				SecretAccessKey: sk,
				SessionToken:    tt.sessionToken,
			}, signTime)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("token mismatch\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

// Region prefers AWS_REGION over AWS_DEFAULT_REGION and reports empty when
// neither is set. IMDS is disabled here so the test stays a pure env-precedence
// check with no network access.
func TestRegion(t *testing.T) {
	tests := []struct {
		name       string
		region     string
		defaultReg string
		want       string
	}{
		{name: "neither set", want: ""},
		{name: "default only", defaultReg: "us-west-2", want: "us-west-2"},
		{name: "region wins", region: "us-east-1", defaultReg: "us-west-2", want: "us-east-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			t.Setenv("AWS_REGION", tt.region)
			t.Setenv("AWS_DEFAULT_REGION", tt.defaultReg)
			if got := Region(); got != tt.want {
				t.Fatalf("Region() = %q, want %q", got, tt.want)
			}
		})
	}
}
