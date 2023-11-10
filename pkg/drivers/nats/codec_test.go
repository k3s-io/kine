package nats

import "testing"

func TestKeyEncode(t *testing.T) {
	tests := []struct {
		In  string
		Out string
		Err bool
	}{
		{"", "", true},
		{"/", "", true},
		{"a", "2g", false},
		{"/a/a", "2g.2g", false},
		{"a/a", "2g.2g", false},
		{"a/a/a", "2g.2g.2g", false},
		{"a/*/a", "2g.j.2g", false},
		{"a/*/a/", "2g.j.2g", false},
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
		{"a", "2g.>", false},
		{"/a/a", "2g.2g.>", false},
		{"a/a/a", "2g.2g.2g.>", false},
		{"a/*/a", "2g.j.2g.>", false},
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
