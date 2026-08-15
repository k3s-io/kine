package aws

import (
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	// imdsTimeout bounds the blocking calls made to the EC2 instance metadata
	// endpoint, so a non-AWS environment fails fast instead of hanging on the
	// link-local address.
	imdsTimeout = 5 * time.Second

	// imdsRegionPath is the IMDS path holding the instance's region. The
	// remaining IMDS endpoint, path and header values are taken from minio-go's
	// credentials package, which uses the same metadata service.
	imdsRegionPath = "/latest/meta-data/placement/region"
)

// regionFromIMDS fetches the region from the EC2 instance metadata service.
// It uses the IMDSv2 token flow, falling back to token-less IMDSv1 if a token
// cannot be obtained. Any failure (most commonly: not running on EC2) yields an
// empty string.
func regionFromIMDS() string {
	client := &http.Client{Timeout: imdsTimeout}
	endpoint := imdsEndpoint()

	// IMDSv2 requires a short-lived session token; IMDSv1 does not. Attempt to
	// obtain one but proceed without it so token-less instances still resolve.
	token := imdsToken(client, endpoint)

	req, err := http.NewRequest(http.MethodGet, endpoint+imdsRegionPath, nil)
	if err != nil {
		return ""
	}
	if token != "" {
		req.Header.Set(credentials.TokenRequestHeader, token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	region, err := io.ReadAll(io.LimitReader(resp.Body, 256))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(region))
}

// imdsToken fetches an IMDSv2 session token. It returns an empty string on any
// failure, so callers can fall back to token-less IMDSv1.
func imdsToken(client *http.Client, endpoint string) string {
	req, err := http.NewRequest(http.MethodPut, endpoint+credentials.TokenPath, nil)
	if err != nil {
		return ""
	}
	req.Header.Set(credentials.TokenRequestTTLHeader, credentials.TokenTTL)
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	token, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(token))
}

// imdsEndpoint returns the base URL of the EC2 instance metadata service,
// honouring the AWS_EC2_METADATA_SERVICE_ENDPOINT override.
func imdsEndpoint() string {
	if e := os.Getenv("AWS_EC2_METADATA_SERVICE_ENDPOINT"); e != "" {
		return strings.TrimRight(e, "/")
	}
	return credentials.DefaultIAMRoleEndpoint
}

// imdsDisabled reports whether EC2 instance metadata lookups have been disabled
// via the standard AWS_EC2_METADATA_DISABLED environment variable.
func imdsDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AWS_EC2_METADATA_DISABLED"))) {
	case "true", "1":
		return true
	default:
		return false
	}
}
