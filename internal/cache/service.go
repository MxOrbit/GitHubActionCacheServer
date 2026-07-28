package cache

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	entpredicate "github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagereaderlease"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
	"github.com/google/uuid"
)

const (
	directDownloadTTL              = storagelifecycle.DirectDownloadLeaseDuration
	lastDownloadedAtUpdateInterval = 10 * time.Minute
	abandonedUploadLifetime        = 24 * time.Hour
	mergeCleanupTimeout            = 10 * time.Second
	maxDanglingPurgeAttempts       = 10
)

var (
	ErrNoWriteScope        = errors.New("no scope with write permission found")
	ErrUploadAlreadyExists = errors.New("upload already exists")
	ErrUploadNotFound      = errors.New("upload not found")
	ErrNoPartsUploaded     = errors.New("no parts have been uploaded")
	ErrPartCountMismatch   = errors.New("uploaded part count does not match actual part count in storage")
	ErrCacheNotFound       = errors.New("cache not found")
)

type Service struct {
	db                    *ent.Client
	storage               storage.Adapter
	composer              storage.ComposeAdapter
	lifecycle             *storagelifecycle.Service
	enableDirectDownloads bool
	mergeCtx              context.Context
	mergeCancel           context.CancelFunc
	mergeSlots            chan struct{}
	mergeWG               sync.WaitGroup
	mergeMu               sync.Mutex
	acceptingMerges       bool
}

type Options struct {
	DB                    *ent.Client
	Storage               storage.Adapter
	EnableDirectDownloads bool
	MergeConcurrency      int
	Lifecycle             *storagelifecycle.Service
}

type CreateUploadResult struct {
	UploadID int64
}

type MatchResult struct {
	CacheEntry  *ent.CacheEntry
	DownloadURL string
}

func NewService(options Options) *Service {
	mergeConcurrency := options.MergeConcurrency
	if mergeConcurrency < 1 {
		mergeConcurrency = runtime.NumCPU()
	}
	if mergeConcurrency < 1 {
		mergeConcurrency = 1
	}
	mergeCtx, mergeCancel := context.WithCancel(context.Background())
	composer, _ := options.Storage.(storage.ComposeAdapter)
	lifecycle := options.Lifecycle
	if lifecycle == nil {
		lifecycle = storagelifecycle.New(options.DB)
	}

	return &Service{
		db:                    options.DB,
		storage:               options.Storage,
		composer:              composer,
		lifecycle:             lifecycle,
		enableDirectDownloads: options.EnableDirectDownloads,
		mergeCtx:              mergeCtx,
		mergeCancel:           mergeCancel,
		mergeSlots:            make(chan struct{}, mergeConcurrency),
		acceptingMerges:       true,
	}
}

func (s *Service) StopAcceptingMerges() {
	s.mergeMu.Lock()
	defer s.mergeMu.Unlock()

	s.acceptingMerges = false
}

func (s *Service) WaitForMerges(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.mergeWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mergeCancel()
		select {
		case <-done:
		case <-time.After(mergeCleanupTimeout):
		}
		return ctx.Err()
	}
}

func (s *Service) CreateUpload(ctx context.Context, key, version string, scope auth.CacheScope) (*CreateUploadResult, error) {
	writeScope, ok := WriteScope(scope)
	if !ok {
		return nil, ErrNoWriteScope
	}

	existingUploads, err := s.db.Upload.Query().
		Where(uploadTuple(key, version, writeScope, scope.RepoID)...).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("query existing uploads: %w", err)
	}

	for _, existingUpload := range existingUploads {
		if s.isUploadAbandoned(existingUpload) {
			if err := s.deleteUpload(ctx, existingUpload); err != nil {
				return nil, err
			}
			continue
		}
		return nil, ErrUploadAlreadyExists
	}

	uploadID, err := s.createUploadRecord(ctx, key, version, writeScope, scope.RepoID)
	if err != nil {
		return nil, err
	}

	return &CreateUploadResult{UploadID: uploadID}, nil
}

func (s *Service) UploadPart(ctx context.Context, uploadID int64, stream io.Reader) error {
	currentUpload, err := s.uploadByID(ctx, uploadID)
	if err != nil {
		return err
	}
	committedPartCount := 1
	return s.uploadObject(ctx, currentUpload, partObjectName(currentUpload.FolderName, 0), stream, &committedPartCount)
}

func (s *Service) UploadBlock(ctx context.Context, uploadID int64, blockID string, stream io.Reader) error {
	currentUpload, err := s.uploadByID(ctx, uploadID)
	if err != nil {
		return err
	}
	return s.uploadObject(ctx, currentUpload, blockObjectName(currentUpload.FolderName, blockID), stream, nil)
}

func (s *Service) CommitBlockList(ctx context.Context, uploadID int64, blockIDs []string) error {
	if len(blockIDs) == 0 {
		return nil
	}

	currentUpload, err := s.uploadByID(ctx, uploadID)
	if err != nil {
		return err
	}

	for index, blockID := range blockIDs {
		err := s.storage.CopyObject(
			ctx,
			blockObjectName(currentUpload.FolderName, blockID),
			partObjectName(currentUpload.FolderName, index),
		)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotFound) {
				return fmt.Errorf("%w: missing block %d", ErrPartCountMismatch, index)
			}
			return err
		}
	}
	if err := s.db.Upload.UpdateOneID(currentUpload.ID).
		SetCommittedPartCount(len(blockIDs)).
		Exec(ctx); err != nil {
		return fmt.Errorf("record committed block list: %w", err)
	}

	return nil
}

func (s *Service) CompleteUpload(ctx context.Context, key, version string, scope auth.CacheScope) (int64, error) {
	writeScope, ok := WriteScope(scope)
	if !ok {
		return 0, ErrNoWriteScope
	}

	currentUpload, err := s.uploadByTuple(ctx, key, version, writeScope, scope.RepoID)
	if err != nil {
		return 0, err
	}

	if currentUpload.FinishedPartUploadCount == 0 {
		_ = s.deleteUpload(ctx, currentUpload)
		return 0, ErrNoPartsUploaded
	}

	partCount, err := s.committedPartCount(ctx, currentUpload)
	if err != nil {
		if errors.Is(err, ErrPartCountMismatch) {
			_ = s.deleteUpload(ctx, currentUpload)
		}
		return 0, err
	}
	if partCount == 0 {
		_ = s.deleteUpload(ctx, currentUpload)
		return 0, ErrNoPartsUploaded
	}
	if partCount > currentUpload.FinishedPartUploadCount {
		_ = s.deleteUpload(ctx, currentUpload)
		return 0, fmt.Errorf(
			"%w: committed part count %d exceeds finished upload count %d",
			ErrPartCountMismatch,
			partCount,
			currentUpload.FinishedPartUploadCount,
		)
	}

	location, err := s.completeUploadRecord(ctx, currentUpload, writeScope, scope.RepoID, partCount)
	if err != nil {
		return 0, err
	}
	if s.enableDirectDownloads {
		s.tryStartMaterialization(location)
	}

	return currentUpload.ID, nil
}

func (s *Service) GetCacheEntryWithDownloadURL(ctx context.Context, keys []string, version string, scope auth.CacheScope, fallbackDownloadURL func(string) string) (*MatchResult, error) {
	if len(keys) == 0 || keys[0] == "" {
		return nil, ErrCacheNotFound
	}

	direct, directDownloads := s.storage.(storage.DirectDownloadAdapter)
	for attempt := 0; attempt < maxDanglingPurgeAttempts; attempt++ {
		cacheEntry, err := s.MatchCacheEntry(ctx, keys, version, scope)
		if err != nil {
			return nil, err
		}
		if cacheEntry == nil {
			return nil, nil
		}

		location, err := s.db.StorageLocation.Query().
			Where(storagelocation.ID(cacheEntry.LocationId)).
			Only(ctx)
		if err != nil {
			if !ent.IsNotFound(err) {
				return nil, fmt.Errorf("query matched storage location: %w", err)
			}
			if _, purgeErr := s.lifecycle.PurgeDanglingCacheEntry(ctx, cacheEntry.ID, cacheEntry.LocationId); purgeErr != nil {
				return nil, purgeErr
			}
			continue
		}

		validatedObjectName, representationAvailable := representationObjectName(location)
		if representationAvailable {
			representationAvailable, err = s.storage.ObjectExists(ctx, validatedObjectName)
			if err != nil {
				return nil, fmt.Errorf("validate cache storage representation: %w", err)
			}
		}
		if !representationAvailable {
			if _, err := s.lifecycle.PurgeDanglingCacheEntry(ctx, cacheEntry.ID, location.ID); err != nil {
				return nil, err
			}
			continue
		}

		downloadURL := fallbackDownloadURL(cacheEntry.ID)
		if s.enableDirectDownloads && directDownloads {
			lease, leaseErr := s.lifecycle.AcquireReader(ctx, cacheEntry.ID, storagelifecycle.AcquireReaderOptions{Direct: true})
			switch {
			case leaseErr == nil:
				directObjectName := readerLeaseObjectName(lease)
				if directObjectName != validatedObjectName {
					exists, probeErr := s.storage.ObjectExists(ctx, directObjectName)
					if probeErr != nil {
						s.releaseReaderLease(lease.ID)
						return nil, fmt.Errorf("validate direct-download storage representation: %w", probeErr)
					}
					if !exists {
						s.releaseReaderLease(lease.ID)
						if _, purgeErr := s.lifecycle.PurgeDanglingCacheEntry(ctx, cacheEntry.ID, lease.Location.ID); purgeErr != nil {
							return nil, purgeErr
						}
						continue
					}
				}
				url, signErr := direct.CreateDownloadURL(ctx, directObjectName, directDownloadTTL)
				if signErr != nil {
					s.releaseReaderLease(lease.ID)
					return nil, signErr
				}
				if extendErr := s.lifecycle.ExtendDirectReader(ctx, lease.ID); extendErr != nil {
					s.releaseReaderLease(lease.ID)
					return nil, extendErr
				}
				s.touchStorageLocationIfStale(ctx, lease.Location)
				downloadURL = url
			case errors.Is(leaseErr, storagelifecycle.ErrDirectRepresentation):
				s.tryStartMaterialization(location)
			case errors.Is(leaseErr, storagelifecycle.ErrLocationUnavailable):
				continue
			default:
				return nil, leaseErr
			}
		}

		return &MatchResult{CacheEntry: cacheEntry, DownloadURL: downloadURL}, nil
	}

	return nil, nil
}

func representationObjectName(location *ent.StorageLocation) (string, bool) {
	if location.MergedAt != nil {
		return mergedObjectName(location.FolderName), true
	}
	if location.PartsDeletedAt != nil || location.PartCount < 1 {
		return "", false
	}
	return partObjectName(location.FolderName, 0), true
}

func readerLeaseObjectName(lease *storagelifecycle.ReaderLease) string {
	if lease.Scope == storagereaderlease.ScopeStorage {
		return mergedObjectName(lease.Location.FolderName)
	}
	return partObjectName(lease.Location.FolderName, 0)
}

func (s *Service) MatchCacheEntry(ctx context.Context, keys []string, version string, scope auth.CacheScope) (*ent.CacheEntry, error) {
	scopes := ReadScopesByPermission(scope)
	if len(scopes) == 0 {
		return nil, nil
	}

	primaryKey := keys[0]
	restoreKeys := keys[1:]
	for _, cacheScope := range scopes {
		primaryMatch, err := s.findExactOrPrefixedCacheEntry(ctx, primaryKey, version, cacheScope, scope.RepoID)
		if err != nil || primaryMatch != nil {
			return primaryMatch, err
		}

		for _, restoreKey := range restoreKeys {
			restoreMatch, err := s.findExactOrPrefixedCacheEntry(ctx, restoreKey, version, cacheScope, scope.RepoID)
			if err != nil || restoreMatch != nil {
				return restoreMatch, err
			}
		}
	}

	return nil, nil
}

func (s *Service) Download(ctx context.Context, cacheEntryID string) (io.ReadCloser, error) {
	lease, err := s.lifecycle.AcquireReader(ctx, cacheEntryID, storagelifecycle.AcquireReaderOptions{})
	if err != nil {
		if errors.Is(err, storagelifecycle.ErrLocationUnavailable) {
			return nil, ErrCacheNotFound
		}
		return nil, err
	}
	location := lease.Location

	s.touchStorageLocationIfStale(ctx, location)

	var stream io.ReadCloser
	if lease.Scope == storagereaderlease.ScopeStorage {
		stream, err = s.openMerged(ctx, location)
	} else {
		s.tryStartMaterialization(location)
		stream, err = s.openParts(ctx, location)
	}
	if err != nil {
		s.releaseReaderLease(lease.ID)
		return nil, err
	}
	return newLeasedReadCloser(stream, s.lifecycle, lease), nil
}

func WriteScope(scope auth.CacheScope) (string, bool) {
	for _, candidate := range scope.Scopes {
		if candidate.Permission >= 2 {
			return candidate.Scope, true
		}
	}
	return "", false
}

func ReadScopesByPermission(scope auth.CacheScope) []string {
	scopes := append([]auth.Scope(nil), scope.Scopes...)
	sort.SliceStable(scopes, func(i, j int) bool {
		return scopes[i].Permission > scopes[j].Permission
	})

	values := make([]string, 0, len(scopes))
	for _, cacheScope := range scopes {
		if cacheScope.Permission < 1 {
			continue
		}
		values = append(values, cacheScope.Scope)
	}
	return values
}

func (s *Service) createUploadRecord(ctx context.Context, key, version, scope, repoID string) (int64, error) {
	for i := 0; i < 5; i++ {
		id, err := randomPositiveInt64()
		if err != nil {
			return 0, err
		}

		_, err = s.db.Upload.Create().
			SetID(id).
			SetFolderName(strconv.FormatInt(id, 10)).
			SetCommittedPartCount(0).
			SetCreatedAt(time.Now().UnixMilli()).
			SetKey(key).
			SetVersion(version).
			SetScope(scope).
			SetRepoId(repoID).
			Save(ctx)
		if err == nil {
			return id, nil
		}
		if ent.IsConstraintError(err) {
			existingUpload, queryErr := s.uploadByTuple(ctx, key, version, scope, repoID)
			if queryErr == nil && existingUpload != nil {
				return 0, ErrUploadAlreadyExists
			}
			if queryErr != nil && !errors.Is(queryErr, ErrUploadNotFound) {
				return 0, fmt.Errorf("query conflicting upload: %w", queryErr)
			}
			continue
		}
		return 0, fmt.Errorf("create upload: %w", err)
	}
	return 0, fmt.Errorf("create upload: exhausted id retries")
}

func (s *Service) uploadByID(ctx context.Context, uploadID int64) (*ent.Upload, error) {
	currentUpload, err := s.db.Upload.Query().Where(upload.ID(uploadID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, ErrUploadNotFound
		}
		return nil, fmt.Errorf("query upload: %w", err)
	}
	return currentUpload, nil
}

func (s *Service) uploadByTuple(ctx context.Context, key, version, scope, repoID string) (*ent.Upload, error) {
	currentUpload, err := s.db.Upload.Query().
		Where(uploadTuple(key, version, scope, repoID)...).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, ErrUploadNotFound
		}
		return nil, fmt.Errorf("query upload: %w", err)
	}
	return currentUpload, nil
}

func (s *Service) uploadObject(ctx context.Context, currentUpload *ent.Upload, objectName string, stream io.Reader, committedPartCount *int) error {
	if err := s.storage.UploadStream(ctx, objectName, stream); err != nil {
		return err
	}

	update := s.db.Upload.UpdateOneID(currentUpload.ID).
		SetLastPartUploadedAt(time.Now().UnixMilli()).
		AddFinishedPartUploadCount(1)
	if committedPartCount != nil {
		update.SetCommittedPartCount(*committedPartCount)
	}
	if err := update.Exec(ctx); err != nil {
		return fmt.Errorf("mark upload finished: %w", err)
	}

	return nil
}

func (s *Service) touchStorageLocationIfStale(ctx context.Context, location *ent.StorageLocation) {
	now := time.Now()
	staleBefore := now.Add(-lastDownloadedAtUpdateInterval).UnixMilli()
	if location.LastDownloadedAt != nil && *location.LastDownloadedAt >= staleBefore {
		return
	}

	_, _ = s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(location.ID),
			storagelocation.Or(
				storagelocation.LastDownloadedAtIsNil(),
				storagelocation.LastDownloadedAtLT(staleBefore),
			),
		).
		SetLastDownloadedAt(now.UnixMilli()).
		Save(ctx)
}

func uploadTuple(key, version, scope, repoID string) []entpredicate.Upload {
	return []entpredicate.Upload{
		upload.Key(key),
		upload.Version(version),
		upload.Scope(scope),
		upload.RepoId(repoID),
	}
}

func (s *Service) isUploadAbandoned(currentUpload *ent.Upload) bool {
	lastActivity := currentUpload.CreatedAt
	if currentUpload.LastPartUploadedAt != nil {
		lastActivity = *currentUpload.LastPartUploadedAt
	}
	return time.Since(time.UnixMilli(lastActivity)) > abandonedUploadLifetime
}

func (s *Service) deleteUpload(ctx context.Context, currentUpload *ent.Upload) error {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("start upload deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := storageoutbox.Enqueue(ctx, tx.Client(), currentUpload.FolderName); err != nil {
		return err
	}
	if err := tx.Upload.DeleteOneID(currentUpload.ID).Exec(ctx); err != nil {
		return fmt.Errorf("delete upload: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upload deletion: %w", err)
	}
	committed = true
	return nil
}

func (s *Service) completeUploadRecord(ctx context.Context, currentUpload *ent.Upload, scope, repoID string, partCount int) (*ent.StorageLocation, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	locationID := uuid.NewString()
	location, err := tx.StorageLocation.Create().
		SetID(locationID).
		SetFolderName(currentUpload.FolderName).
		SetPartCount(partCount).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("create storage location: %w", err)
	}

	existingCacheEntry, err := tx.CacheEntry.Query().
		Where(
			cacheentry.Key(currentUpload.Key),
			cacheentry.Version(currentUpload.Version),
			cacheentry.Scope(scope),
			cacheentry.RepoId(repoID),
		).
		WithLocation().
		Only(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, fmt.Errorf("query existing cache entry: %w", err)
	}

	if existingCacheEntry != nil {
		if _, err := tx.CacheEntry.UpdateOneID(existingCacheEntry.ID).
			SetUpdatedAt(time.Now().UnixMilli()).
			SetLocation(location).
			Save(ctx); err != nil {
			return nil, fmt.Errorf("update cache entry: %w", err)
		}
		if existingCacheEntry.LocationId != "" {
			if _, err := s.lifecycle.FenceDetachedLocation(ctx, tx.Client(), existingCacheEntry.LocationId); err != nil {
				return nil, err
			}
		}
	} else {
		if _, err := tx.CacheEntry.Create().
			SetID(uuid.NewString()).
			SetKey(currentUpload.Key).
			SetVersion(currentUpload.Version).
			SetScope(scope).
			SetRepoId(repoID).
			SetUpdatedAt(time.Now().UnixMilli()).
			SetLocation(location).
			Save(ctx); err != nil {
			return nil, fmt.Errorf("create cache entry: %w", err)
		}
	}

	if _, err := storageoutbox.Enqueue(ctx, tx.Client(), blocksFolderName(currentUpload.FolderName)); err != nil {
		return nil, err
	}

	if err := tx.Upload.DeleteOneID(currentUpload.ID).Exec(ctx); err != nil {
		return nil, fmt.Errorf("delete upload: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit upload: %w", err)
	}
	committed = true
	return location, nil
}

type cacheEntryMatch struct {
	key     string
	version string
	scope   string
	repoID  string
	prefix  bool
}

func (s *Service) findExactOrPrefixedCacheEntry(ctx context.Context, key string, version string, cacheScope string, repoID string) (*ent.CacheEntry, error) {
	exact, err := s.findCacheEntry(ctx, cacheEntryMatch{
		key:     key,
		version: version,
		scope:   cacheScope,
		repoID:  repoID,
		prefix:  false,
	})
	if err != nil || exact != nil {
		return exact, err
	}

	return s.findCacheEntry(ctx, cacheEntryMatch{
		key:     key,
		version: version,
		scope:   cacheScope,
		repoID:  repoID,
		prefix:  true,
	})
}

func (s *Service) findCacheEntry(ctx context.Context, match cacheEntryMatch) (*ent.CacheEntry, error) {
	query := s.db.CacheEntry.Query().
		Where(
			cacheentry.Version(match.version),
			cacheentry.Scope(match.scope),
			cacheentry.RepoId(match.repoID),
		)
	if match.prefix {
		query = query.Where(cacheentry.KeyHasPrefix(match.key)).Order(cacheentry.ByUpdatedAt(sql.OrderDesc()))
	} else {
		query = query.Where(cacheentry.Key(match.key))
	}

	cacheEntry, err := query.First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query cache entry: %w", err)
	}
	return cacheEntry, nil
}

func (s *Service) openMerged(ctx context.Context, location *ent.StorageLocation) (io.ReadCloser, error) {
	stream, err := s.storage.CreateDownloadStream(ctx, mergedObjectName(location.FolderName))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrCacheNotFound
		}
		return nil, err
	}
	return stream, nil
}

func (s *Service) openParts(ctx context.Context, location *ent.StorageLocation) (io.ReadCloser, error) {
	return newPartsReadCloser(ctx, s.storage, location.FolderName, location.PartCount), nil
}

func (s *Service) committedPartCount(ctx context.Context, currentUpload *ent.Upload) (int, error) {
	if currentUpload.CommittedPartCount != nil {
		return *currentUpload.CommittedPartCount, nil
	}

	count, err := s.storage.CountFilesInFolder(ctx, partsFolderName(currentUpload.FolderName))
	if err != nil {
		return 0, err
	}
	if err := s.ensureCommittedPartsAreContiguous(ctx, currentUpload.FolderName, count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Service) ensureCommittedPartsAreContiguous(ctx context.Context, folderName string, partCount int) error {
	for partIndex := 0; partIndex < partCount; partIndex++ {
		stream, err := s.storage.CreateDownloadStream(ctx, partObjectName(folderName, partIndex))
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotFound) {
				return fmt.Errorf("%w: missing part %d", ErrPartCountMismatch, partIndex)
			}
			return err
		}
		if err := stream.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) tryStartMaterialization(location *ent.StorageLocation) {
	if s.composer == nil || location.PartCount < 2 || location.MergedAt != nil || location.MaterializationUnsupportedAt != nil || location.PartsDeletedAt != nil {
		return
	}
	if !s.reserveMerge() {
		return
	}

	lease, err := s.lifecycle.AcquireMaterialization(s.mergeCtx, location.ID)
	if err != nil {
		s.finishReservedMerge()
		return
	}

	go s.materializeLocationInBackground(lease)
}

func (s *Service) materializeLocationInBackground(lease *storagelifecycle.MaterializationLease) {
	defer s.finishReservedMerge()
	location := lease.Location

	sources := make([]string, location.PartCount)
	for index := range sources {
		sources[index] = partObjectName(location.FolderName, index)
	}

	materializationCtx, cancel := context.WithCancel(s.mergeCtx)
	renewalDone := make(chan error, 1)
	go s.renewMaterializationLease(materializationCtx, cancel, lease, renewalDone)
	composeErr := s.composer.ComposeObjects(materializationCtx, mergedObjectName(location.FolderName), sources)
	cancel()
	renewalErr := <-renewalDone
	if renewalErr != nil {
		s.releaseMaterializationLease(location.ID, lease.Token)
		return
	}
	if composeErr != nil {
		if errors.Is(composeErr, storage.ErrComposeUnsupported) {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
			defer cleanupCancel()
			if markErr := s.lifecycle.MarkMaterializationUnsupported(cleanupCtx, location.ID, lease.Token); markErr != nil {
				s.releaseMaterializationLease(location.ID, lease.Token)
			}
		} else {
			s.releaseMaterializationLease(location.ID, lease.Token)
		}
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()
	if err := s.lifecycle.FinishMaterialization(cleanupCtx, location.ID, lease.Token); err != nil {
		s.releaseMaterializationLease(location.ID, lease.Token)
	}
}

func (s *Service) renewMaterializationLease(ctx context.Context, cancel context.CancelFunc, lease *storagelifecycle.MaterializationLease, done chan<- error) {
	ticker := time.NewTicker(lease.RenewAfter)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(ctx, mergeCleanupTimeout)
			err := s.lifecycle.RenewMaterialization(renewCtx, lease.Location.ID, lease.Token)
			renewCancel()
			if err != nil {
				if ctx.Err() != nil {
					done <- nil
					return
				}
				cancel()
				done <- err
				return
			}
		}
	}
}

func (s *Service) reserveMerge() bool {
	s.mergeMu.Lock()
	defer s.mergeMu.Unlock()

	if !s.acceptingMerges {
		return false
	}

	select {
	case s.mergeSlots <- struct{}{}:
	default:
		return false
	}
	s.mergeWG.Add(1)
	return true
}

func (s *Service) finishReservedMerge() {
	<-s.mergeSlots
	s.mergeWG.Done()
}

func (s *Service) releaseReaderLease(leaseID string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()
	_ = s.lifecycle.ReleaseReader(cleanupCtx, leaseID)
}

func (s *Service) releaseMaterializationLease(locationID, token string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()
	_ = s.lifecycle.ReleaseMaterialization(cleanupCtx, locationID, token)
}

func randomPositiveInt64() (int64, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("generate upload id: %w", err)
	}
	id := int64(binary.BigEndian.Uint64(buf[:]) & 0x7fffffffffffffff)
	if id == 0 {
		id = 1
	}
	return id, nil
}

func partsFolderName(folderName string) string {
	return folderName + "/parts"
}

func partObjectName(folderName string, partIndex int) string {
	return fmt.Sprintf("%s/%d", partsFolderName(folderName), partIndex)
}

func blocksFolderName(folderName string) string {
	return folderName + "/blocks"
}

func blockObjectName(folderName string, blockID string) string {
	return blocksFolderName(folderName) + "/" + base64.RawURLEncoding.EncodeToString([]byte(blockID))
}

func mergedObjectName(folderName string) string {
	return folderName + "/merged"
}
