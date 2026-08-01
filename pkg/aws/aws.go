// Package aws provides the AWS building blocks kine needs to authenticate to
// RDS databases via IAM: resolving the region, loading credentials from the
// standard AWS credential chain (using minio-go), and generating RDS IAM
// authentication tokens. It is deliberately free of any database-driver
// specifics so it can be shared across drivers.
package aws

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	// rdsSigningService is the SigV4 service name used when signing RDS IAM
	// authentication tokens.
	rdsSigningService = "rds-db"
	// rdsIAMTokenExpirySeconds is the validity window RDS grants an IAM auth
	// token. The token is regenerated for every new connection, so this bounds
	// how long an individual pooled connection's password stays valid.
	rdsIAMTokenExpirySeconds = 900 // 15 minutes
	// sigV4Algorithm is the SigV4 signing algorithm identifier.
	sigV4Algorithm = "AWS4-HMAC-SHA256"
	// iso8601BasicFormat and shortDateFormat are the timestamp layouts SigV4
	// expects for the X-Amz-Date query parameter and the credential scope.
	iso8601BasicFormat = "20060102T150405Z"
	shortDateFormat    = "20060102"
	// emptyPayloadHash is the SHA-256 of an empty body, which RDS expects for
	// the bodyless presigned "connect" request.
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// Region resolves the AWS region, preferring AWS_REGION, then
// AWS_DEFAULT_REGION, and finally the EC2 instance metadata service (IMDS) —
// mirroring the AWS SDK's default resolution chain (config.WithEC2IMDSRegion).
// It returns an empty string when no region can be resolved, which callers can
// treat as "not an AWS environment".
//
// The IMDS lookup is skipped when AWS_EC2_METADATA_DISABLED is set, and is
// bounded by a short timeout so a non-EC2 environment fails fast rather than
// hanging on the link-local metadata address.
func Region() string {
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}
	if imdsDisabled() {
		return ""
	}
	return regionFromIMDS()
}

// Credentials builds a minio-go credential chain equivalent to the AWS default
// credential provider: static environment variables, the shared credentials
// file, and finally the IAM provider (EC2 IMDS, ECS, EKS Pod Identity and IRSA
// web-identity).
//
// Only the bare EC2 IMDS path performs a blocking call to the link-local
// metadata endpoint, so AWS_EC2_METADATA_DISABLED is honoured (as the AWS SDKs
// do) to skip the IAM provider in non-AWS environments — unless container or
// web-identity credentials are configured, which never touch IMDS.
func Credentials() *credentials.Credentials {
	providers := []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.FileAWSCredentials{},
	}
	if !imdsDisabled() || hasContainerOrWebIdentityCreds() {
		providers = append(providers, &credentials.IAM{
			Client: &http.Client{Timeout: imdsTimeout},
		})
	}
	return credentials.NewChainCredentials(providers)
}

// Retrieve resolves a concrete credential value from the given chain using a
// bounded HTTP context, so credential retrieval never blocks indefinitely.
func Retrieve(creds *credentials.Credentials) (credentials.Value, error) {
	return creds.GetWithContext(&credentials.CredContext{
		Client: &http.Client{Timeout: imdsTimeout},
	})
}

// hasContainerOrWebIdentityCreds reports whether ECS/EKS container or IRSA
// web-identity credentials are configured. These credential sources are served
// by the IAM provider but never contact the EC2 metadata endpoint.
func hasContainerOrWebIdentityCreds() bool {
	for _, k := range []string{
		"AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
	} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

// BuildRDSIAMAuthToken returns an RDS IAM authentication token: a SigV4
// presigned "connect" request for the rds-db service, with the scheme stripped,
// used as the password for the database connection. The token is valid for 15
// minutes from now.
//
// endpoint must be of the form host:port. The output is byte-for-byte identical
// to the AWS SDK's rds/auth.BuildAuthToken for the same inputs.
func BuildRDSIAMAuthToken(endpoint, region, dbUser string, creds credentials.Value, now time.Time) (string, error) {
	// The scheme is arbitrary; it is only needed so the URL parses and to carry
	// the host:port authority. It is stripped from the returned token. No path is
	// set so the emitted token is "host:port?query", matching the AWS SDK exactly;
	// the canonical URI is still signed as "/" below.
	req, err := http.NewRequest(http.MethodGet, "https://"+endpoint, nil)
	if err != nil {
		return "", err
	}

	now = now.UTC()
	amzDate := now.Format(iso8601BasicFormat)
	dateStamp := now.Format(shortDateFormat)
	scope := strings.Join([]string{dateStamp, region, rdsSigningService, "aws4_request"}, "/")

	query := req.URL.Query()
	query.Set("Action", "connect")
	query.Set("DBUser", dbUser)
	query.Set("X-Amz-Algorithm", sigV4Algorithm)
	query.Set("X-Amz-Credential", creds.AccessKeyID+"/"+scope)
	query.Set("X-Amz-Date", amzDate)
	query.Set("X-Amz-Expires", strconv.Itoa(rdsIAMTokenExpirySeconds))
	query.Set("X-Amz-SignedHeaders", "host")
	if creds.SessionToken != "" {
		query.Set("X-Amz-Security-Token", creds.SessionToken)
	}
	// SigV4 requires the canonical query string to use %20 for spaces, whereas
	// url.Values.Encode uses '+'.
	req.URL.RawQuery = strings.ReplaceAll(query.Encode(), "+", "%20")

	// host is the only signed header; it must include the port.
	canonicalHeaders := "host:" + req.URL.Host + "\n"
	canonicalRequest := strings.Join([]string{
		http.MethodGet,
		"/",
		req.URL.RawQuery,
		canonicalHeaders,
		"host",
		emptyPayloadHash,
	}, "\n")

	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := deriveSigningKey(creds.SecretAccessKey, dateStamp, region, rdsSigningService)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	req.URL.RawQuery += "&X-Amz-Signature=" + signature

	return strings.TrimPrefix(req.URL.String(), "https://"), nil
}

// deriveSigningKey derives the SigV4 signing key for the given date, region and
// service.
func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
