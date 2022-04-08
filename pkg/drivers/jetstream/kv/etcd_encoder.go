package kv

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
	"github.com/shengdoushi/base58"
)

// EtcdKeyCodec turns keys like /this/is/a.test.key into Base58 encoded values split on `/`
// This is because NATS Jetstream Keys are split on . rather than /
type EtcdKeyCodec struct{}

type S2ValueCodec struct{}

type PlainCodec struct{}

var (
	keyAlphabet = base58.BitcoinAlphabet
)

func (e *EtcdKeyCodec) EncodeRange(keys string) (string, error) {
	ek, err := e.Encode(keys)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(ek, ".") {
		return fmt.Sprintf("%s>", ek), nil
	}
	return ek, nil
}

func (*EtcdKeyCodec) Encode(key string) (retKey string, e error) {
	//defer func() {
	//	logrus.Debugf("encoded %s => %s", key, retKey)
	//}()
	parts := []string{}
	for _, part := range strings.Split(strings.TrimPrefix(key, "/"), "/") {
		if part == ">" || part == "*" {
			parts = append(parts, part)
			continue
		}
		parts = append(parts, base58.Encode([]byte(part), keyAlphabet))
	}

	if len(parts) == 0 {
		return "", nats.ErrInvalidKey
	}

	return strings.Join(parts, "."), nil
}

func (*EtcdKeyCodec) Decode(key string) (retKey string, e error) {
	//defer func() {
	//	logrus.Debugf("decoded %s => %s", key, retKey)
	//}()
	parts := []string{}
	for _, s := range strings.Split(key, ".") {
		decodedPart, err := base58.Decode(s, keyAlphabet)
		if err != nil {
			return "", err
		}
		parts = append(parts, string(decodedPart[:]))
	}
	if len(parts) == 0 {
		return "", nats.ErrInvalidKey
	}
	return fmt.Sprintf("/%s", strings.Join(parts, "/")), nil
}

func (*S2ValueCodec) Encode(src []byte, dst io.Writer) error {
	enc := s2.NewWriter(dst)
	err := enc.EncodeBuffer(src)
	if err != nil {
		enc.Close()
		return err
	}
	return enc.Close()
}

func (*S2ValueCodec) Decode(src io.Reader, dst io.Writer) error {
	dec := s2.NewReader(src)
	_, err := io.Copy(dst, dec)
	return err
}

func (*PlainCodec) Encode(src []byte, dst io.Writer) error {
	_, err := dst.Write(src)
	return err
}

func (*PlainCodec) Decode(src io.Reader, dst io.Writer) error {
	b, err := ioutil.ReadAll(src)
	if err != nil {
		return err
	}
	_, err = dst.Write(b)

	return err
}
