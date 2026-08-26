package mediahandlers

import "testing"

func TestResolveGoalFileContentType(t *testing.T) {
	cases := []struct {
		name, filename, headerCT, want string
	}{
		{"pdf ext", "doc.pdf", "", "application/pdf"},
		{"pdf mime", "doc", "application/pdf", "application/pdf"},
		{"fb2 ext", "book.fb2", "", "application/x-fictionbook+xml"},
		{"fb2 mime", "book", "application/x-fictionbook+xml", "application/x-fictionbook+xml"},
		{"reject png", "x.png", "image/png", ""},
		{"reject empty", "x", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveGoalFileContentType(tc.filename, tc.headerCT)
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestMaxGoalFileBytes(t *testing.T) {
	if maxGoalFileBytes != 10<<20 {
		t.Fatalf("maxGoalFileBytes = %d, want 10MB", maxGoalFileBytes)
	}
}
