package aws

import (
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	// imdsTimeout bounds the blocking calls made to the EC2 instance metadata
	// endpoint, so a non-AWS environment fails fast instead of hanging on the
	// link-local address.
	imdsTimeout = 5 * time.Second

	// imdsDefaultEndpoint is the link-local base URL of the EC2 instance
	// metadata service (IMDS).
	imdsDefaultEndpoint = "http://169.254.169.254" //nolint:revive // HTTPS is not supported by IMDS.
	imdsTokenPath       = "/latest/api/token"
	imdsRegionPath      = "/latest/meta-data/placement/region"
	imdsTokenTTLHeader  = "X-aws-ec2-metadata-token-ttl-seconds"
	imdsTokenHeader     = "X-aws-ec2-metadata-token"
	imdsTokenTTL        = "21600"
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
		req.Header.Set(imdsTokenHeader, token)
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
	req, err := http.NewRequest(http.MethodPut, endpoint+imdsTokenPath, nil)
	if err != nil {
		return ""
	}
	req.Header.Set(imdsTokenTTLHeader, imdsTokenTTL)
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
	return imdsDefaultEndpoint
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
