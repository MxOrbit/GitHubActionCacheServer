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
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/google/uuid"
)

const (
	directDownloadTTL              = 10 * time.Minute
	abandonedUploadLifetime        = 24 * time.Hour
	mergeCleanupTimeout            = 10 * time.Second
	stalledMaterializationLifetime = 15 * time.Minute
)

var (
	ErrNoWriteScope        = errors.New("no scope with write permission found")
	ErrUploadAlreadyExists = errors.New("upload already exists")
	ErrUploadNotFound      = errors.New("upload not found")
	ErrNoPartsUploaded     = errors.New("no parts have been uploaded")
	ErrPartsStillUploading = errors.New("not all parts have been uploaded")
	ErrPartCountMismatch   = errors.New("uploaded part count does not match actual part count in storage")
	ErrCacheNotFound       = errors.New("cache not found")
	errMergeAlreadyStarted = errors.New("merge already started")
)

type Service struct {
	db                    *ent.Client
	storage               storage.Adapter
	composer              storage.ComposeAdapter
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

	return &Service{
		db:                    options.DB,
		storage:               options.Storage,
		composer:              composer,
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
	return s.uploadObjectWithCounters(ctx, currentUpload, partObjectName(currentUpload.FolderName, 0), stream, &committedPartCount)
}

func (s *Service) UploadBlock(ctx context.Context, uploadID int64, blockID string, stream io.Reader) error {
	currentUpload, err := s.uploadByID(ctx, uploadID)
	if err != nil {
		return err
	}
	return s.uploadObjectWithCounters(ctx, currentUpload, blockObjectName(currentUpload.FolderName, blockID), stream, nil)
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
		_ = s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx)
		return 0, ErrNoPartsUploaded
	}
	if currentUpload.StartedPartUploadCount != currentUpload.FinishedPartUploadCount {
		_ = s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx)
		return 0, fmt.Errorf("%w: only %d of %d parts uploaded", ErrPartsStillUploading, currentUpload.FinishedPartUploadCount, currentUpload.StartedPartUploadCount)
	}

	partCount, err := s.committedPartCount(ctx, currentUpload)
	if err != nil {
		if errors.Is(err, ErrPartCountMismatch) {
			_ = s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx)
		}
		return 0, err
	}
	if partCount == 0 {
		_ = s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx)
		return 0, ErrNoPartsUploaded
	}
	if partCount > currentUpload.FinishedPartUploadCount {
		_ = s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx)
		return 0, fmt.Errorf(
			"%w: committed part count %d exceeds finished upload count %d",
			ErrPartCountMismatch,
			partCount,
			currentUpload.FinishedPartUploadCount,
		)
	}

	location, oldFolderName, err := s.completeUploadRecord(ctx, currentUpload, writeScope, scope.RepoID, partCount)
	if err != nil {
		return 0, err
	}
	if oldFolderName != "" {
		s.deleteFolderBestEffort(ctx, oldFolderName)
	}
	s.deleteFolderBestEffort(ctx, blocksFolderName(currentUpload.FolderName))
	if s.enableDirectDownloads {
		s.tryStartMaterialization(location)
	}

	return currentUpload.ID, nil
}

func (s *Service) GetCacheEntryWithDownloadURL(ctx context.Context, keys []string, version string, scope auth.CacheScope, fallbackDownloadURL func(string) string) (*MatchResult, error) {
	if len(keys) == 0 || keys[0] == "" {
		return nil, ErrCacheNotFound
	}

	cacheEntry, err := s.MatchCacheEntry(ctx, keys, version, scope)
	if err != nil {
		return nil, err
	}
	if cacheEntry == nil {
		return nil, nil
	}

	downloadURL := fallbackDownloadURL(cacheEntry.ID)
	if s.enableDirectDownloads {
		if direct, ok := s.storage.(storage.DirectDownloadAdapter); ok {
			location, err := s.db.StorageLocation.Query().
				Where(storagelocation.ID(cacheEntry.LocationId)).
				Only(ctx)
			if err != nil {
				return nil, fmt.Errorf("query storage location: %w", err)
			}
			directObjectName := ""
			switch {
			case location.MergedAt != nil:
				directObjectName = mergedObjectName(location.FolderName)
			case location.PartCount == 1:
				directObjectName = partObjectName(location.FolderName, 0)
			default:
				s.tryStartMaterialization(location)
			}
			if directObjectName != "" {
				url, err := direct.CreateDownloadURL(ctx, directObjectName, directDownloadTTL)
				if err != nil {
					return nil, err
				}
				downloadURL = url
			}
		}
	}

	return &MatchResult{CacheEntry: cacheEntry, DownloadURL: downloadURL}, nil
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
	cacheEntry, err := s.db.CacheEntry.Query().
		Where(cacheentry.ID(cacheEntryID)).
		WithLocation().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, ErrCacheNotFound
		}
		return nil, fmt.Errorf("query cache entry: %w", err)
	}
	location := cacheEntry.Edges.Location
	if location == nil {
		return nil, ErrCacheNotFound
	}

	_ = s.db.StorageLocation.UpdateOneID(location.ID).
		SetLastDownloadedAt(time.Now().UnixMilli()).
		Exec(ctx)

	if location.MergedAt != nil {
		return s.openMerged(ctx, location)
	}
	s.tryStartMaterialization(location)
	return s.openParts(ctx, location)
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

func (s *Service) uploadObjectWithCounters(ctx context.Context, currentUpload *ent.Upload, objectName string, stream io.Reader, committedPartCount *int) error {
	if err := s.db.Upload.UpdateOneID(currentUpload.ID).AddStartedPartUploadCount(1).Exec(ctx); err != nil {
		return fmt.Errorf("mark upload started: %w", err)
	}

	if err := s.storage.UploadStream(ctx, objectName, stream); err != nil {
		_ = s.rollbackStartedPartUpload(ctx, currentUpload.ID)
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

func uploadTuple(key, version, scope, repoID string) []entpredicate.Upload {
	return []entpredicate.Upload{
		upload.Key(key),
		upload.Version(version),
		upload.Scope(scope),
		upload.RepoId(repoID),
	}
}

func (s *Service) rollbackStartedPartUpload(ctx context.Context, uploadID int64) error {
	if err := s.db.Upload.UpdateOneID(uploadID).AddStartedPartUploadCount(-1).Exec(ctx); err != nil {
		return fmt.Errorf("roll back started part upload: %w", err)
	}
	return nil
}

func (s *Service) isUploadAbandoned(currentUpload *ent.Upload) bool {
	lastActivity := currentUpload.CreatedAt
	if currentUpload.LastPartUploadedAt != nil {
		lastActivity = *currentUpload.LastPartUploadedAt
	}
	return time.Since(time.UnixMilli(lastActivity)) > abandonedUploadLifetime
}

func (s *Service) deleteUpload(ctx context.Context, currentUpload *ent.Upload) error {
	if err := s.db.Upload.DeleteOneID(currentUpload.ID).Exec(ctx); err != nil {
		return fmt.Errorf("delete abandoned upload: %w", err)
	}
	if err := s.storage.DeleteFolder(ctx, currentUpload.FolderName); err != nil {
		return err
	}
	return nil
}

func (s *Service) completeUploadRecord(ctx context.Context, currentUpload *ent.Upload, scope, repoID string, partCount int) (*ent.StorageLocation, string, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("start transaction: %w", err)
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
		return nil, "", fmt.Errorf("create storage location: %w", err)
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
		return nil, "", fmt.Errorf("query existing cache entry: %w", err)
	}

	oldFolderName := ""
	if existingCacheEntry != nil {
		if existingCacheEntry.Edges.Location != nil {
			oldFolderName = existingCacheEntry.Edges.Location.FolderName
		}
		if _, err := tx.CacheEntry.UpdateOneID(existingCacheEntry.ID).
			SetUpdatedAt(time.Now().UnixMilli()).
			SetLocation(location).
			Save(ctx); err != nil {
			return nil, "", fmt.Errorf("update cache entry: %w", err)
		}
		if existingCacheEntry.LocationId != "" {
			if err := tx.StorageLocation.DeleteOneID(existingCacheEntry.LocationId).Exec(ctx); err != nil {
				return nil, "", fmt.Errorf("delete old storage location: %w", err)
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
			return nil, "", fmt.Errorf("create cache entry: %w", err)
		}
	}

	if err := tx.Upload.DeleteOneID(currentUpload.ID).Exec(ctx); err != nil {
		return nil, "", fmt.Errorf("delete upload: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, "", fmt.Errorf("commit upload: %w", err)
	}
	committed = true
	return location, oldFolderName, nil
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

	mergeStartedAt, err := s.markMergeStarted(s.mergeCtx, location.ID)
	if err != nil {
		s.finishReservedMerge()
		return
	}

	go s.materializeLocationInBackground(location, mergeStartedAt)
}

func (s *Service) materializeLocationInBackground(location *ent.StorageLocation, mergeStartedAt int64) {
	defer s.finishReservedMerge()

	sources := make([]string, location.PartCount)
	for index := range sources {
		sources[index] = partObjectName(location.FolderName, index)
	}
	if err := s.composer.ComposeObjects(s.mergeCtx, mergedObjectName(location.FolderName), sources); err != nil {
		if errors.Is(err, storage.ErrComposeUnsupported) {
			if markErr := s.markMaterializationUnsupported(location.ID, mergeStartedAt); markErr != nil {
				s.rollbackMergeStart(location.ID, mergeStartedAt)
			}
		} else {
			s.rollbackMergeStart(location.ID, mergeStartedAt)
		}
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()
	if err := s.markMergeFinished(cleanupCtx, location.ID, mergeStartedAt); err != nil {
		s.rollbackMergeStart(location.ID, mergeStartedAt)
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

func (s *Service) rollbackMergeStart(locationID string, mergeStartedAt int64) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()
	_ = s.clearOwnMergeStart(cleanupCtx, locationID, mergeStartedAt)
}

func (s *Service) markMergeStarted(ctx context.Context, locationID string) (int64, error) {
	mergeStartedAt := time.Now().UnixMilli()
	stalledBefore := time.Now().Add(-stalledMaterializationLifetime).UnixMilli()
	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.Or(
				storagelocation.MergeStartedAtIsNil(),
				storagelocation.MergeStartedAtLT(stalledBefore),
			),
			storagelocation.MergedAtIsNil(),
			storagelocation.MaterializationUnsupportedAtIsNil(),
			storagelocation.PartsDeletedAtIsNil(),
		).
		SetMergeStartedAt(mergeStartedAt).
		Save(ctx)
	if err != nil {
		return 0, fmt.Errorf("mark merge started: %w", err)
	}
	if affected == 0 {
		return 0, errMergeAlreadyStarted
	}
	return mergeStartedAt, nil
}

func (s *Service) markMaterializationUnsupported(locationID string, mergeStartedAt int64) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
	defer cancel()

	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeStartedAt(mergeStartedAt),
			storagelocation.MergedAtIsNil(),
		).
		SetMaterializationUnsupportedAt(time.Now().UnixMilli()).
		ClearMergeStartedAt().
		Save(cleanupCtx)
	if err != nil {
		return fmt.Errorf("mark materialization unsupported: %w", err)
	}
	if affected == 0 {
		return errMergeAlreadyStarted
	}
	return nil
}

func (s *Service) markMergeFinished(ctx context.Context, locationID string, mergeStartedAt int64) error {
	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeStartedAt(mergeStartedAt),
			storagelocation.MergedAtIsNil(),
		).
		SetMergedAt(time.Now().UnixMilli()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("mark merge finished: %w", err)
	}
	if affected == 0 {
		return errMergeAlreadyStarted
	}
	return nil
}

func (s *Service) deleteFolderBestEffort(ctx context.Context, folderName string) {
	_ = s.storage.DeleteFolder(ctx, folderName)
}

func (s *Service) clearOwnMergeStart(ctx context.Context, locationID string, mergeStartedAt int64) error {
	_, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeStartedAt(mergeStartedAt),
			storagelocation.MergedAtIsNil(),
		).
		ClearMergeStartedAt().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("clear failed merge start: %w", err)
	}
	return nil
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
