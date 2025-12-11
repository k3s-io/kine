package nats

import (
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shengdoushi/base58"
)

const (
	noRootPrefix = "meta"
)

var keyAlphabet = base58.BitcoinAlphabet

// keyCodec turns keys like /this/is/a.test.key into Base58 encoded values
// split on `.` This is because NATS keys are split on . rather than /.
type keyCodec struct{}

func (e *keyCodec) EncodeRange(prefix string) (string, error) {
	if prefix == "/" {
		return ">", nil
	}

	if prefix == noRootPrefix {
		return fmt.Sprintf("%s.>", noRootPrefix), nil
	}

	ek, err := e.Encode(prefix)
	if err != nil {
		return "", err
	}

	enc := fmt.Sprintf("%s.>", ek)

	return enc, nil
}

func (*keyCodec) Encode(key string) (string, error) {
	if key == "" || key == "/" {
		return "", jetstream.ErrInvalidKey
	}

	hasRootPrefix := strings.HasPrefix(key, "/")

	// Trim leading and trailing slashes.
	key = strings.Trim(key, "/")

	var parts []string

	// Differentiate between key foo and /foo
	// foo -> meta.base58(foo)
	// /foo -> base58(foo)
	if !hasRootPrefix {
		parts = append(parts, noRootPrefix)
	}

	for _, part := range strings.Split(key, "/") {
		if part == "" {
			part = "/"
		}

		parts = append(parts, base58.Encode([]byte(part), keyAlphabet))
	}

	if len(parts) == 0 {
		return "", jetstream.ErrInvalidKey
	}

	enc := strings.Join(parts, ".")

	return enc, nil
}

func (*keyCodec) Decode(key string) (retKey string, e error) {
	var parts []string

	hasRootPrefix := !strings.HasPrefix(key, noRootPrefix)

	for _, s := range strings.Split(key, ".") {
		// Skip meta prefix. It represents the absence
		// of a leading '/'
		if s == noRootPrefix {
			continue
		}

		decodedPart, err := base58.Decode(s, keyAlphabet)
		if err != nil {
			return "", err
		}

		part := string(decodedPart)
		if part == "/" {
			part = ""
		}

		parts = append(parts, part)
	}

	if len(parts) == 0 {
		return "", jetstream.ErrInvalidKey
	}

	dk := strings.Join(parts, "/")
	if hasRootPrefix {
		dk = fmt.Sprintf("/%s", dk)
	}

	return dk, nil
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
