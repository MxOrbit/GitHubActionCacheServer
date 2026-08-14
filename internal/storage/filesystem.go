package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/bufferpool"
)

const (
	filesystemUploadTempPrefix = ".upload-"
	filesystemReadBatchSize    = 256
)

type FilesystemAdapter struct {
	root                  string
	syncUpload            func(*os.File) error
	removeTemporaryUpload func(string) error
}

func NewFilesystemAdapter(root string) (*FilesystemAdapter, error) {
	return newFilesystemAdapter(root, true)
}

func newFilesystemAdapter(root string, fsyncUploads bool) (*FilesystemAdapter, error) {
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

	adapter := &FilesystemAdapter{root: abs, removeTemporaryUpload: os.Remove}
	if fsyncUploads {
		adapter.syncUpload = (*os.File).Sync
	}
	return adapter, nil
}

func (a *FilesystemAdapter) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
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

	// Hide *os.File.ReadFrom so io.CopyBuffer uses the pooled buffer instead of
	// discarding it and allocating another buffer in the generic file fallback.
	destination := struct{ io.Writer }{Writer: file}
	if _, err := bufferpool.Copy(destination, contextReader{ctx: ctx, reader: stream}); err != nil {
		_ = file.Close()
		return fmt.Errorf("write object: %w", err)
	}
	if a.syncUpload != nil {
		if err := a.syncUpload(file); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync temporary object: %w", err)
		}
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

func (a *FilesystemAdapter) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	sourcePath, err := a.safePath(sourceObjectName)
	if err != nil {
		return err
	}
	destinationPath, err := a.safePath(destinationObjectName)
	if err != nil {
		return err
	}

	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectNotFoundError{ObjectName: sourceObjectName}
		}
		return fmt.Errorf("stat source object: %w", err)
	}
	if sourceInfo.IsDir() {
		return fmt.Errorf("source object is a directory")
	}
	if sourcePath == destinationPath {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		return fmt.Errorf("create object parent directory: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(destinationPath), filesystemUploadTempPrefix+"*")
	if err != nil {
		return fmt.Errorf("create temporary object path: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close temporary object path: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("prepare temporary object path: %w", err)
	}

	if err := os.Link(sourcePath, tempPath); err == nil {
		removeTemp := true
		defer func() {
			if removeTemp {
				_ = os.Remove(tempPath)
			}
		}()
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := commitFilesystemObject(tempPath, destinationPath); err != nil {
			return err
		}
		removeTemp = false
		return nil
	}

	stream, err := a.CreateDownloadStream(ctx, sourceObjectName)
	if err != nil {
		return err
	}
	copyErr := a.UploadStream(ctx, destinationObjectName, stream)
	closeErr := stream.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
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

func (a *FilesystemAdapter) InspectObject(ctx context.Context, objectName string) (ObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMetadata{}, err
	}
	path, err := a.safePath(objectName)
	if err != nil {
		return ObjectMetadata{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectMetadata{}, ObjectNotFoundError{ObjectName: objectName}
		}
		return ObjectMetadata{}, fmt.Errorf("stat object: %w", err)
	}
	if !info.Mode().IsRegular() {
		return ObjectMetadata{}, ObjectNotFoundError{ObjectName: objectName}
	}
	return ObjectMetadata{Name: objectName, SizeBytes: info.Size(), ModifiedAt: info.ModTime()}, nil
}

func (a *FilesystemAdapter) InspectFolderSummary(ctx context.Context, folderName string) (FolderSummary, error) {
	path, err := a.safePath(folderName)
	if err != nil {
		return FolderSummary{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FolderSummary{FolderName: folderName}, nil
		}
		return FolderSummary{}, fmt.Errorf("stat folder: %w", err)
	}
	if !info.IsDir() {
		return FolderSummary{}, fmt.Errorf("inspect folder %q: path is not a directory", folderName)
	}

	summary := FolderSummary{
		FolderName:       folderName,
		Exists:           true,
		NewestModifiedAt: info.ModTime(),
	}
	err = walkFilesystemDirectory(ctx, path, "", func(relativeName string, info os.FileInfo) error {
		if info.IsDir() {
			summary.NewestModifiedAt = newestTime(summary.NewestModifiedAt, info.ModTime())
			return nil
		}
		return addFolderSummaryObject(&summary, ObjectMetadata{
			Name:       filepath.ToSlash(relativeName),
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime(),
		})
	})
	if err != nil {
		return FolderSummary{}, fmt.Errorf("inspect folder summary %q: %w", folderName, err)
	}
	return summary, nil
}

func (a *FilesystemAdapter) InspectIndexedFolder(ctx context.Context, folderName string, expectedObjects int) (int64, error) {
	accumulator, err := newIndexedFolderAccumulator(expectedObjects)
	if err != nil {
		return 0, err
	}
	path, err := a.safePath(folderName)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return accumulator.result()
		}
		return 0, fmt.Errorf("stat indexed folder: %w", err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("inspect indexed folder %q: path is not a directory", folderName)
	}

	err = walkFilesystemDirectory(ctx, path, "", func(relativeName string, info os.FileInfo) error {
		if info.IsDir() {
			return nil
		}
		object := ObjectMetadata{
			Name:       filepath.ToSlash(relativeName),
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime(),
		}
		if err := validateObjectMetadata(object); err != nil {
			return err
		}
		return accumulator.add(object.Name, object.SizeBytes)
	})
	if err != nil {
		return 0, fmt.Errorf("inspect indexed folder %q: %w", folderName, err)
	}
	return accumulator.result()
}

func (a *FilesystemAdapter) WalkTopLevelFolders(ctx context.Context, visit func(folderName string) error) error {
	directory, err := os.Open(a.root)
	if err != nil {
		return fmt.Errorf("open storage root: %w", err)
	}
	defer directory.Close()

	for {
		entries, readErr := directory.ReadDir(filesystemReadBatchSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() {
				if err := visit(entry.Name()); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read storage root: %w", readErr)
		}
	}
}

func (a *FilesystemAdapter) WalkTemporaryUploads(ctx context.Context, visit func(ObjectMetadata) error) error {
	return walkFilesystemDirectory(ctx, a.root, "", func(relativeName string, info os.FileInfo) error {
		objectName := filepath.ToSlash(relativeName)
		if info.IsDir() || !isFilesystemTemporaryUpload(objectName) {
			return nil
		}
		object := ObjectMetadata{
			Name:       objectName,
			SizeBytes:  info.Size(),
			ModifiedAt: info.ModTime(),
		}
		if err := validateObjectMetadata(object); err != nil {
			return err
		}
		return visit(object)
	})
}

func walkFilesystemDirectory(
	ctx context.Context,
	directoryPath string,
	relativePath string,
	visit func(relativeName string, info os.FileInfo) error,
) error {
	directory, err := os.Open(directoryPath)
	if err != nil {
		return err
	}
	defer directory.Close()

	for {
		entries, readErr := directory.ReadDir(filesystemReadBatchSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			entryRelativePath := filepath.Join(relativePath, entry.Name())
			if err := visit(entryRelativePath, info); err != nil {
				return err
			}
			if entry.IsDir() {
				if err := walkFilesystemDirectory(ctx, filepath.Join(directoryPath, entry.Name()), entryRelativePath, visit); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (a *FilesystemAdapter) CleanupTemporaryUploads(ctx context.Context, candidates []ObjectMetadata, cutoff time.Time) (int, error) {
	deleted := 0
	var cleanupErrors []error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			cleanupErrors = append(cleanupErrors, err)
			return deleted, errors.Join(cleanupErrors...)
		}
		if !isFilesystemTemporaryUpload(candidate.Name) {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("refuse to clean non-temporary object %q", candidate.Name))
			continue
		}

		objectPath, err := a.safePath(candidate.Name)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("resolve temporary upload %q: %w", candidate.Name, err))
			continue
		}
		info, err := os.Lstat(objectPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect temporary upload %q: %w", candidate.Name, err))
			continue
		}
		if !info.Mode().IsRegular() {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("temporary upload %q is not a regular file", candidate.Name))
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := a.removeTemporaryUpload(objectPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			cleanupErrors = append(cleanupErrors, fmt.Errorf("delete temporary upload %q: %w", candidate.Name, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(cleanupErrors...)
}

func isFilesystemTemporaryUpload(objectName string) bool {
	baseName := objectName
	if separator := strings.LastIndexByte(baseName, '/'); separator >= 0 {
		baseName = baseName[separator+1:]
	}
	return strings.HasPrefix(baseName, filesystemUploadTempPrefix)
}

func (a *FilesystemAdapter) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	_, err := a.InspectObject(ctx, objectName)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (a *FilesystemAdapter) FilesystemUsage(ctx context.Context) (FilesystemUsage, error) {
	if err := ctx.Err(); err != nil {
		return FilesystemUsage{}, err
	}
	return filesystemUsage(a.root)
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

func (a *FilesystemAdapter) CountFilesInFolder(ctx context.Context, folderName string) (int, error) {
	path, err := a.safePath(folderName)
	if err != nil {
		return 0, err
	}

	directory, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("open folder: %w", err)
	}
	defer directory.Close()

	count := 0
	for {
		entries, readErr := directory.ReadDir(filesystemReadBatchSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if strings.HasPrefix(entry.Name(), filesystemUploadTempPrefix) {
				continue
			}
			if !entry.IsDir() {
				count++
			}
		}
		if errors.Is(readErr, io.EOF) {
			return count, nil
		}
		if readErr != nil {
			return 0, fmt.Errorf("read folder: %w", readErr)
		}
	}
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
