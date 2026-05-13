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

var ErrObjectNotFound = errors.New("object not found in storage")

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
	CreateDownloadStream(ctx context.Context, objectName string) (io.ReadCloser, error)
	DeleteFolder(ctx context.Context, folderName string) error
	CountFilesInFolder(ctx context.Context, folderName string) (int, error)
	Clear(ctx context.Context) error
}

type DirectDownloadAdapter interface {
	CreateDownloadURL(ctx context.Context, objectName string, ttl time.Duration) (string, error)
}

func NewAdapter(ctx context.Context, cfg config.StorageConfig) (Adapter, error) {
	switch cfg.Driver {
	case DriverFilesystem:
		return NewFilesystemAdapter(cfg.FilesystemPath)
	case DriverS3:
		return NewS3Adapter(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported STORAGE_DRIVER %q", cfg.Driver)
	}
}
