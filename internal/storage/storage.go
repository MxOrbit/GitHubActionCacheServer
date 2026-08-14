package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
)

const (
	DriverFilesystem = "filesystem"
	DriverS3         = "s3"
)

var (
	ErrObjectNotFound     = errors.New("object not found in storage")
	ErrComposeUnsupported = errors.New("object sequence cannot be composed by this storage backend")
)

type ObjectNotFoundError struct {
	ObjectName string
}

func (e ObjectNotFoundError) Error() string {
	return fmt.Sprintf("%s: %s", ErrObjectNotFound, e.ObjectName)
}

func (e ObjectNotFoundError) Unwrap() error {
	return ErrObjectNotFound
}

type Adapter interface {
	UploadStream(ctx context.Context, objectName string, stream io.Reader) error
	CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string) error
	CreateDownloadStream(ctx context.Context, objectName string) (io.ReadCloser, error)
	InspectObject(ctx context.Context, objectName string) (ObjectMetadata, error)
	InspectFolderSummary(ctx context.Context, folderName string) (FolderSummary, error)
	InspectIndexedFolder(ctx context.Context, folderName string, expectedObjects int) (int64, error)
	WalkTopLevelFolders(ctx context.Context, visit func(folderName string) error) error
	ObjectExists(ctx context.Context, objectName string) (bool, error)
	DeleteFolder(ctx context.Context, folderName string) error
	CountFilesInFolder(ctx context.Context, folderName string) (int, error)
	Clear(ctx context.Context) error
}

type DirectDownloadAdapter interface {
	CreateDownloadURL(ctx context.Context, objectName string, ttl time.Duration) (string, error)
}

type ComposeAdapter interface {
	ComposeObjects(ctx context.Context, destinationObjectName string, sourceObjectNames []string) error
}

type FilesystemUsage struct {
	CapacityBytes int64
	UsedBytes     int64
}

type FilesystemUsageAdapter interface {
	FilesystemUsage(ctx context.Context) (FilesystemUsage, error)
}

type TemporaryUploadCleaner interface {
	WalkTemporaryUploads(ctx context.Context, visit func(ObjectMetadata) error) error
	CleanupTemporaryUploads(ctx context.Context, candidates []ObjectMetadata, cutoff time.Time) (int, error)
}

func NewAdapter(ctx context.Context, cfg config.StorageConfig) (Adapter, error) {
	switch cfg.Driver {
	case DriverFilesystem:
		return newFilesystemAdapter(cfg.FilesystemPath, cfg.FilesystemFsync)
	case DriverS3:
		return NewS3Adapter(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported STORAGE_DRIVER %q", cfg.Driver)
	}
}
