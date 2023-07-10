package nats

import (
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shengdoushi/base58"
)

var (
	keyAlphabet = base58.BitcoinAlphabet
)

// keyCodec turns keys like /this/is/a.test.key into Base58 encoded values
// split on `.` This is because NATS keys are split on . rather than /.
type keyCodec struct{}

func (e *keyCodec) EncodeRange(prefix string) (string, error) {
	if prefix == "/" {
		return ">", nil
	}

	ek, err := e.Encode(prefix)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.>", ek), nil
}

func (*keyCodec) Encode(key string) (retKey string, e error) {
	if key == "" {
		return "", jetstream.ErrInvalidKey
	}

	// Trim leading and trailing slashes.
	key = strings.Trim(key, "/")

	var parts []string
	for _, part := range strings.Split(key, "/") {
		parts = append(parts, base58.Encode([]byte(part), keyAlphabet))
	}

	if len(parts) == 0 {
		return "", jetstream.ErrInvalidKey
	}

	return strings.Join(parts, "."), nil
}

func (*keyCodec) Decode(key string) (retKey string, e error) {
	var parts []string

	for _, s := range strings.Split(key, ".") {
		decodedPart, err := base58.Decode(s, keyAlphabet)
		if err != nil {
			return "", err
		}
		parts = append(parts, string(decodedPart[:]))
	}

	if len(parts) == 0 {
		return "", jetstream.ErrInvalidKey
	}

	return fmt.Sprintf("/%s", strings.Join(parts, "/")), nil
}

// valueCodec is a codec that compresses values using s2.
type valueCodec struct{}

func (*valueCodec) Encode(src []byte, dst io.Writer) error {
	enc := s2.NewWriter(dst)
	err := enc.EncodeBuffer(src)
	if err != nil {
		enc.Close()
		return err
	}
	return enc.Close()
}

func (*valueCodec) Decode(src io.Reader, dst io.Writer) error {
	dec := s2.NewReader(src)
	_, err := io.Copy(dst, dec)
	return err
}
