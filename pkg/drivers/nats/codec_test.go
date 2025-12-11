package nats

import (
	"fmt"
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
		{"/a/*/a/", "2g.j.2g", false},
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
		if out != test.Out {
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
		if out != test.Out {
			t.Errorf("Expected %q for %q, got %q", test.Out, test.In, out)
		}
	}
}
