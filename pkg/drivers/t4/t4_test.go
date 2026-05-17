package t4

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

func TestAWSConfigOptionsRequireT4CredentialsOrProfile(t *testing.T) {
	_, err := awsConfigOptions("", "")
	if err == nil {
		t.Fatal("awsConfigOptions returned nil error")
	}
	if !strings.Contains(err.Error(), "T4_S3_PROFILE") {
		t.Fatalf("awsConfigOptions error = %q, want mention of T4_S3_PROFILE", err)
	}
}

func TestAWSConfigOptionsUseT4Credentials(t *testing.T) {
	t.Setenv("T4_S3_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("T4_S3_SECRET_ACCESS_KEY", "test-secret-key")

	opts, err := awsConfigOptions("", "")
	if err != nil {
		t.Fatalf("awsConfigOptions returned error: %v", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		t.Fatalf("LoadDefaultConfig returned error: %v", err)
	}

	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve returned error: %v", err)
	}
	if creds.AccessKeyID != "test-access-key" {
		t.Fatalf("AccessKeyID = %q, want %q", creds.AccessKeyID, "test-access-key")
	}
	if creds.SecretAccessKey != "test-secret-key" {
		t.Fatalf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "test-secret-key")
	}
}

func TestAWSConfigOptionsAllowAWSChainWithT4Profile(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(configPath, []byte("[profile test-profile]\nregion = us-west-2\n"), 0600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("T4_S3_PROFILE", "test-profile")

	opts, err := awsConfigOptions("", "")
	if err != nil {
		t.Fatalf("awsConfigOptions returned error: %v", err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		t.Fatalf("LoadDefaultConfig returned error: %v", err)
	}
	if cfg.Credentials == nil {
		t.Fatal("Credentials provider is nil")
	}
}

func TestPeerTLSCredentialsDisabledWhenUnset(t *testing.T) {
	server, client, err := peerTLSCredentials("", "", "", false)
	if err != nil {
		t.Fatalf("peerTLSCredentials returned error: %v", err)
	}
	if server != nil || client != nil {
		t.Fatalf("peerTLSCredentials returned non-nil credentials when unset")
	}
}

func TestPeerTLSCredentialsRequireAllThreeFiles(t *testing.T) {
	tests := []struct {
		name string
		cert bool
		key  bool
		ca   bool
	}{
		{"only cert", true, false, false},
		{"only key", false, true, false},
		{"only ca", false, false, true},
		{"cert+key, no ca", true, true, false},
		{"cert+ca, no key", true, false, true},
		{"key+ca, no cert", false, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert, key := "", ""
			if tt.cert {
				cert = "/tmp/cert"
			}
			if tt.key {
				key = "/tmp/key"
			}
			ca := ""
			if tt.ca {
				ca = "/tmp/ca"
			}
			_, _, err := peerTLSCredentials(cert, key, ca, false)
			if err == nil {
				t.Fatal("peerTLSCredentials returned nil error")
			}
			if !strings.Contains(err.Error(), "together") {
				t.Fatalf("peerTLSCredentials error = %q, want mention of \"together\"", err)
			}
		})
	}
}

func TestPeerTLSCredentialsBuildsFromFiles(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := writeTestPeerKeypair(t, dir)

	server, client, err := peerTLSCredentials(certFile, keyFile, caFile, false)
	if err != nil {
		t.Fatalf("peerTLSCredentials returned error: %v", err)
	}
	if server == nil || client == nil {
		t.Fatal("peerTLSCredentials returned nil credentials")
	}
}

// writeTestPeerKeypair generates an ECDSA self-signed certificate and writes
// cert/key/ca PEM files into dir. The cert is its own CA — sufficient for
// exercising the file-loading path.
func writeTestPeerKeypair(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "t4-peer-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:         true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	for _, f := range [][2]any{{certFile, certPEM}, {keyFile, keyPEM}, {caFile, certPEM}} {
		if err := os.WriteFile(f[0].(string), f[1].([]byte), 0600); err != nil {
			t.Fatalf("WriteFile %s: %v", f[0], err)
		}
	}
	return certFile, keyFile, caFile
}

func TestAWSConfigOptionsRequireCompleteT4Credentials(t *testing.T) {
	tests := []struct {
		name            string
		accessKeyID     string
		secretAccessKey string
	}{
		{
			name:        "missing secret access key",
			accessKeyID: "test-access-key",
		},
		{
			name:            "missing access key ID",
			secretAccessKey: "test-secret-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("T4_S3_ACCESS_KEY_ID", tt.accessKeyID)
			t.Setenv("T4_S3_SECRET_ACCESS_KEY", tt.secretAccessKey)

			if _, err := awsConfigOptions("", ""); err == nil {
				t.Fatal("awsConfigOptions returned nil error")
			}
		})
	}
}
