package minioserver

import "testing"

func TestParseStoryIDFromStoryImageFilename(t *testing.T) {
	userID := "f192b78e-399e-4fc5-9676-ce0bf65b220b"
	storyID := "01476d41-a092-4e75-8f3c-9bac5fdf096c"

	tests := []struct {
		name     string
		filename string
		pathUser string
		want     string
	}{
		{
			name:     "userId_storyId_stories.jpeg",
			filename: userID + "_" + storyID + "_stories.jpeg",
			pathUser: userID,
			want:     storyID,
		},
		{
			name:     "storyId_uuid.jpeg without matching user prefix",
			filename: storyID + "_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jpeg",
			pathUser: userID,
			want:     storyID,
		},
		{
			name:     "single uuid treated as story id",
			filename: storyID + ".jpeg",
			pathUser: userID,
			want:     storyID,
		},
		{
			name:     "unparseable",
			filename: "cover.jpeg",
			pathUser: userID,
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseStoryIDFromStoryImageFilename(tt.filename, tt.pathUser)
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}
