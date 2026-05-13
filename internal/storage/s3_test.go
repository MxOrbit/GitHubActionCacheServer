package storage

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestS3AdapterImplementsDirectDownload(t *testing.T) {
	var _ Adapter = (*S3Adapter)(nil)
	var _ DirectDownloadAdapter = (*S3Adapter)(nil)
}

func TestS3KeyUsesCachePrefix(t *testing.T) {
	adapter := &S3Adapter{keyPrefix: s3KeyPrefix("custom-cache")}

	if got := adapter.key("/folder/object"); got != "custom-cache/folder/object" {
		t.Fatalf("unexpected key: %s", got)
	}
}

func TestS3KeyPrefixDefault(t *testing.T) {
	if got := s3KeyPrefix(""); got != defaultS3KeyPrefix {
		t.Fatalf("unexpected key prefix: %s", got)
	}
}

func TestS3ClearUsesDelimitedPrefix(t *testing.T) {
	adapter := &S3Adapter{keyPrefix: s3KeyPrefix("gh-actions-cache")}

	if got := adapter.clearPrefix(); got != "gh-actions-cache/" {
		t.Fatalf("unexpected clear prefix: %s", got)
	}
}

func TestS3DeleteErrorsError(t *testing.T) {
	err := s3DeleteErrorsError([]types.Error{
		{
			Key:     aws.String("gh-actions-cache/folder/object"),
			Code:    aws.String("AccessDenied"),
			Message: aws.String("retention policy prevents deletion"),
		},
	})

	if err == nil {
		t.Fatal("expected per-object delete error")
	}
	for _, want := range []string{"gh-actions-cache/folder/object", "AccessDenied", "retention policy prevents deletion"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to contain %q, got %q", want, err.Error())
		}
	}
}

func TestIsDirectS3FolderObject(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   bool
	}{
		{
			name:   "direct object",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/a",
			want:   true,
		},
		{
			name:   "nested object",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/sub/b",
			want:   false,
		},
		{
			name:   "prefix marker",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/",
			want:   false,
		},
		{
			name:   "outside prefix",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/other/a",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDirectS3FolderObject(tt.prefix, tt.key); got != tt.want {
				t.Fatalf("unexpected direct object result: got %v want %v", got, tt.want)
			}
		})
	}
}
