package nats

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestKeyEncode(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "", true},
		{"/", "", true},
		{"a", fmt.Sprintf("%s.2g", noRootPrefix), false},
		{"/a/a", "2g.2g", false},
		{"a/a", fmt.Sprintf("%s.2g.2g", noRootPrefix), false},
		{"/a/a/a", "2g.2g.2g", false},
		{"a/*/a", fmt.Sprintf("%s.2g.j.2g", noRootPrefix), false},
		{"/a/*/a/", "2g.j.2g.p", false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.Encode(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if out != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

func TestKeyDecode(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "/", false},
		{"2g", "/a", false},
		{"2g.2g", "/a/a", false},
		{"2g.2g.2g", "/a/a/a", false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.Decode(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if string(out) != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

func TestKeyEncodeRange(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "", true},
		{"/", ">", false},
		{"a", fmt.Sprintf("%s.2g.>", noRootPrefix), false},
		{"/a/a", "2g.2g.>", false},
		{"a/a/a", fmt.Sprintf("%s.2g.2g.2g.>", noRootPrefix), false},
		{"/a/*/a", "2g.j.2g.>", false},
		{"a/*/a", fmt.Sprintf("%s.2g.j.2g.>", noRootPrefix), false},
	}

	codec := &keyCodec{}

	for _, test := range tests {
		out, err := codec.EncodeRange(test.In)
		if err != nil {
			if !test.Err {
				t.Errorf("Expected no error for %q, got %v", test.In, err)
			}
			continue
		}
		if string(out) != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}

func TestValueCodecRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 4096, 65536, 1048577} {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			value := testValue(size)

			var encoded bytes.Buffer
			if err := (&valueCodec{}).Encode(value, &encoded); err != nil {
				t.Fatal(err)
			}

			var decoded bytes.Buffer
			if err := (&valueCodec{}).Decode(bytes.NewReader(encoded.Bytes()), &decoded); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(value, decoded.Bytes()) {
				t.Fatalf("decoded value does not match input: got %d bytes, want %d", decoded.Len(), len(value))
			}
		})
	}
}

func TestValueCodecConcurrentDecode(t *testing.T) {
	value := testValue(4096)

	var encoded bytes.Buffer
	if err := (&valueCodec{}).Encode(value, &encoded); err != nil {
		t.Fatal(err)
	}
	compressed := encoded.Bytes()

	const (
		workers    = 16
		iterations = 100
	)

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	codec := &valueCodec{}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				var decoded bytes.Buffer
				if err := codec.Decode(bytes.NewReader(compressed), &decoded); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(value, decoded.Bytes()) {
					errs <- fmt.Errorf("decoded value does not match input: got %d bytes, want %d", decoded.Len(), len(value))
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func BenchmarkValueCodecDecode(b *testing.B) {
	for _, size := range []int{4096, 65536} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			value := testValue(size)
			var encoded bytes.Buffer
			if err := (&valueCodec{}).Encode(value, &encoded); err != nil {
				b.Fatal(err)
			}
			compressed := encoded.Bytes()
			codec := &valueCodec{}

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()

			for range b.N {
				var decoded bytes.Buffer
				if err := codec.Decode(bytes.NewReader(compressed), &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func testValue(size int) []byte {
	value := make([]byte, size)
	_, _ = rand.New(rand.NewSource(int64(size))).Read(value)
	return value
}
