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

	"github.com/k3s-io/kine/pkg/drivers"
)

func TestParseConfigFailsWithoutCredentialsWhenBucketSet(t *testing.T) {
	_, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
		S3Config:       drivers.S3Config{Bucket: "my-bucket"},
	})
	if err == nil {
		t.Fatal("parseConfig returned nil error")
	}
	if !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("parseConfig error = %q, want mention of credentials", err)
	}
}

func TestParseConfigRequiresCompleteStaticCredentials(t *testing.T) {
	tests := []struct {
		name string
		cfg  drivers.S3Config
	}{
		{"missing secret key", drivers.S3Config{Bucket: "my-bucket", AccessKey: "k"}},
		{"missing access key", drivers.S3Config{Bucket: "my-bucket", SecretKey: "s"}},
		{"session token without access key", drivers.S3Config{Bucket: "my-bucket", SessionToken: "t"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig(context.Background(), &drivers.Config{
				DataSourceName: t.TempDir(),
				S3Config:       tt.cfg,
			})
			if err == nil {
				t.Fatal("parseConfig returned nil error")
			}
		})
	}
}

func TestParseConfigAcceptsStaticCredentials(t *testing.T) {
	cfg, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
		S3Config: drivers.S3Config{
			Bucket:       "my-bucket",
			Endpoint:     "http://localhost:9000",
			AccessKey:    "k",
			SecretKey:    "s",
			SessionToken: "tok",
		},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ObjectStore == nil {
		t.Fatal("parseConfig: ObjectStore is nil")
	}
}

func TestParseConfigAcceptsProfile(t *testing.T) {
	cfg, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
		S3Config: drivers.S3Config{
			Bucket:   "my-bucket",
			Endpoint: "http://localhost:9000",
			Profile:  "default",
		},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ObjectStore == nil {
		t.Fatal("parseConfig: ObjectStore is nil")
	}
}

func TestParseConfigLocalOnlyNeedsNoCredentials(t *testing.T) {
	cfg, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.ObjectStore != nil {
		t.Fatal("parseConfig: ObjectStore should be nil without bucket")
	}
}

func TestParseConfigRequiresAdvertiseWhenBindSetWithoutServiceName(t *testing.T) {
	_, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
		PeerConfig:     drivers.PeerConfig{BindAddress: "0.0.0.0:3380"},
	})
	if err == nil {
		t.Fatal("parseConfig returned nil error")
	}
	if !strings.Contains(err.Error(), "peer-advertise-address") {
		t.Fatalf("parseConfig error = %q, want mention of --peer-advertise-address", err)
	}
}

func TestParseConfigBindWithServiceNameAutoDerivesAdvertise(t *testing.T) {
	cfg, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir() + "?service-name=kine.kube-system.svc.cluster.local",
		PeerConfig:     drivers.PeerConfig{BindAddress: "0.0.0.0:3380"},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.AdvertisePeerAddr == "" || cfg.AdvertisePeerAddr == cfg.PeerListenAddr {
		t.Fatalf("parseConfig: AdvertisePeerAddr should be derived from service-name, got %q", cfg.AdvertisePeerAddr)
	}
	if !strings.HasSuffix(cfg.AdvertisePeerAddr, ".kine.kube-system.svc.cluster.local:3380") {
		t.Fatalf("parseConfig: AdvertisePeerAddr = %q, want suffix .kine.kube-system.svc.cluster.local:3380", cfg.AdvertisePeerAddr)
	}
}

func TestParseConfigBindWithExplicitAdvertiseOK(t *testing.T) {
	cfg, err := parseConfig(context.Background(), &drivers.Config{
		DataSourceName: t.TempDir(),
		PeerConfig: drivers.PeerConfig{
			BindAddress:      "0.0.0.0:3380",
			AdvertiseAddress: "node-a.internal:3380",
		},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.AdvertisePeerAddr != "node-a.internal:3380" {
		t.Fatalf("parseConfig: AdvertisePeerAddr = %q, want node-a.internal:3380", cfg.AdvertisePeerAddr)
	}
}

func TestPeerTLSCredentialsDisabledWhenUnset(t *testing.T) {
	server, client, err := peerTLSCredentials("", "", "", false)
	if err != nil {
		t.Fatalf("peerTLSCredentials returned error: %v", err)
	}
	if server != nil || client != nil {
		t.Fatal("peerTLSCredentials returned non-nil credentials when unset")
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
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "t4-peer-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
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
