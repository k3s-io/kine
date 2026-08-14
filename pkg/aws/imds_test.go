package aws

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// imdsServer is a fake EC2 instance metadata service. It serves the region from
// placement/region, requiring the IMDSv2 token exchange unless requireToken is
// false (which models a token-less IMDSv1 instance). Every incoming request is
// recorded in the returned slice pointer for assertions.
func imdsServer(t *testing.T, region string, requireToken bool) (*httptest.Server, *[]string) {
	t.Helper()
	const token = "AQAEXAMPLETOKEN"
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		switch {
		case r.Method == http.MethodPut && r.URL.Path == credentials.TokenPath:
			if !requireToken {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if r.Header.Get(credentials.TokenRequestTTLHeader) == "" {
				http.Error(w, "missing ttl header", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(token))
		case r.Method == http.MethodGet && r.URL.Path == imdsRegionPath:
			if requireToken && r.Header.Get(credentials.TokenRequestHeader) != token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(region))
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

// With no region in the environment, Region falls back to IMDS and returns the
// region it reports, using the IMDSv2 token flow.
func TestRegion_FromIMDSv2(t *testing.T) {
	srv, seen := imdsServer(t, "eu-west-1", true)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)

	if got := Region(); got != "eu-west-1" {
		t.Fatalf("Region() = %q, want %q", got, "eu-west-1")
	}
	if len(*seen) != 2 || (*seen)[0] != "PUT "+credentials.TokenPath {
		t.Fatalf("expected IMDSv2 token then region request, got %v", *seen)
	}
}

// A token-less IMDSv1 instance (token endpoint fails) must still resolve the
// region via a token-less GET.
func TestRegion_FromIMDSv1Fallback(t *testing.T) {
	srv, seen := imdsServer(t, "ap-southeast-2", false)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)

	if got := Region(); got != "ap-southeast-2" {
		t.Fatalf("Region() = %q, want %q", got, "ap-southeast-2")
	}
	if len(*seen) == 0 || (*seen)[len(*seen)-1] != "GET "+imdsRegionPath {
		t.Fatalf("expected a token-less region GET, got %v", *seen)
	}
}

// An explicit region in the environment must short-circuit before any IMDS call.
func TestRegion_EnvBeatsIMDS(t *testing.T) {
	srv, seen := imdsServer(t, "eu-west-1", true)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)

	if got := Region(); got != "us-east-1" {
		t.Fatalf("Region() = %q, want %q", got, "us-east-1")
	}
	if len(*seen) != 0 {
		t.Fatalf("expected no IMDS requests when AWS_REGION is set, got %v", *seen)
	}
}

// AWS_EC2_METADATA_DISABLED must skip the IMDS fallback entirely.
func TestRegion_MetadataDisabledSkipsIMDS(t *testing.T) {
	srv, seen := imdsServer(t, "eu-west-1", true)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)

	if got := Region(); got != "" {
		t.Fatalf("Region() = %q, want empty", got)
	}
	if len(*seen) != 0 {
		t.Fatalf("expected no IMDS requests when metadata is disabled, got %v", *seen)
	}
}
