package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const filesystemUploadTempPrefix = ".upload-"

type FilesystemAdapter struct {
	root string
}

func NewFilesystemAdapter(root string) (*FilesystemAdapter, error) {
	if root == "" {
		return nil, fmt.Errorf("STORAGE_FILESYSTEM_PATH is required")
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve filesystem storage path: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("create filesystem storage path: %w", err)
	}

	return &FilesystemAdapter{root: abs}, nil
}

func (a *FilesystemAdapter) UploadStream(_ context.Context, objectName string, stream io.Reader) error {
	path, err := a.safePath(objectName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create object parent directory: %w", err)
	}

	file, err := os.CreateTemp(filepath.Dir(path), filesystemUploadTempPrefix+"*")
	if err != nil {
		return fmt.Errorf("create temporary object: %w", err)
	}
	tmpPath := file.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(file, stream); err != nil {
		_ = file.Close()
		return fmt.Errorf("write object: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary object: %w", err)
	}
	if err := commitFilesystemObject(tmpPath, path); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func (a *FilesystemAdapter) CreateDownloadStream(_ context.Context, objectName string) (io.ReadCloser, error) {
	path, err := a.safePath(objectName)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectNotFoundError{ObjectName: objectName}
		}
		return nil, fmt.Errorf("open object: %w", err)
	}
	return file, nil
}

func (a *FilesystemAdapter) DeleteFolder(_ context.Context, folderName string) error {
	path, err := a.safePath(folderName)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("delete folder: %w", err)
	}
	return nil
}

func (a *FilesystemAdapter) CountFilesInFolder(_ context.Context, folderName string) (int, error) {
	path, err := a.safePath(folderName)
	if err != nil {
		return 0, err
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read folder: %w", err)
	}

	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), filesystemUploadTempPrefix) {
			continue
		}
		if !entry.IsDir() {
			count++
		}
	}
	return count, nil
}

func (a *FilesystemAdapter) Clear(_ context.Context) error {
	if err := os.RemoveAll(a.root); err != nil {
		return fmt.Errorf("clear filesystem storage: %w", err)
	}
	if err := os.MkdirAll(a.root, 0o755); err != nil {
		return fmt.Errorf("recreate filesystem storage: %w", err)
	}
	return nil
}

func commitFilesystemObject(tmpPath, path string) error {
	if err := os.Rename(tmpPath, path); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("commit object: %w", err)
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("replace object: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("commit object: %w", err)
	}
	return nil
}

func (a *FilesystemAdapter) safePath(objectName string) (string, error) {
	if objectName == "" {
		return "", fmt.Errorf("object name is required")
	}
	if strings.Contains(objectName, `\`) {
		return "", fmt.Errorf("invalid object name")
	}

	localName := filepath.FromSlash(objectName)
	if filepath.IsAbs(localName) || strings.HasPrefix(localName, string(filepath.Separator)) {
		return "", fmt.Errorf("invalid object name")
	}

	cleaned := filepath.Clean(localName)
	if cleaned == "." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) || cleaned == ".." {
		return "", fmt.Errorf("invalid object name")
	}

	resolved := filepath.Join(a.root, cleaned)
	rel, err := filepath.Rel(a.root, resolved)
	if err != nil {
		return "", fmt.Errorf("resolve object path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("invalid object name")
	}

	return resolved, nil
}
