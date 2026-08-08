package storagelifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagereaderlease"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storageoutbox"
	"github.com/google/uuid"
)

const (
	ReaderLeaseDuration           = 2 * time.Minute
	LeaseRenewalInterval          = 30 * time.Second
	DirectDownloadLeaseDuration   = 10 * time.Minute
	DeletionGracePeriod           = DirectDownloadLeaseDuration
	MaterializationLeaseDuration  = 2 * time.Minute
	readerAcquireRetryBackoff     = 2 * time.Millisecond
	legacyMaterializationLifetime = 15 * time.Minute
)

var (
	ErrLocationUnavailable      = errors.New("storage location is unavailable")
	ErrDirectRepresentation     = errors.New("storage location has no direct-download representation")
	ErrReaderLeaseLost          = errors.New("storage reader lease was lost")
	ErrMaterializationLeaseHeld = errors.New("materialization lease is already held")
	ErrMaterializationLeaseLost = errors.New("materialization lease was lost")
)

type Service struct {
	db                           *ent.Client
	now                          func() time.Time
	readerLeaseDuration          time.Duration
	leaseRenewalInterval         time.Duration
	directDownloadLeaseDuration  time.Duration
	deletionGracePeriod          time.Duration
	materializationLeaseDuration time.Duration
	materializationDisabled      bool
	readerAcquireRetryBackoff    time.Duration
}

type Options struct {
	Now                          func() time.Time
	ReaderLeaseDuration          time.Duration
	LeaseRenewalInterval         time.Duration
	DirectDownloadLeaseDuration  time.Duration
	DeletionGracePeriod          time.Duration
	MaterializationLeaseDuration time.Duration
	// MaterializationDisabled marks deployments without a storage composer, where
	// locations never merge and parts deletion never runs; reader acquisitions
	// then skip the parts row lock regardless of part count.
	MaterializationDisabled   bool
	ReaderAcquireRetryBackoff time.Duration
}

type ReaderLease struct {
	ID         string
	Location   *ent.StorageLocation
	Scope      storagereaderlease.Scope
	ExpiresAt  time.Time
	RenewAfter time.Duration
}

type AcquireReaderOptions struct {
	Direct bool
}

type DeletionResult struct {
	Task      *ent.StorageDeletion
	Fenced    bool
	Finalized bool
}

type PartsDeletionResult struct {
	Task      *ent.StorageDeletion
	PartCount int
}

type MaterializationLease struct {
	Location   *ent.StorageLocation
	Token      string
	RenewAfter time.Duration
}

type CapacityObservation struct {
	LocationID       string
	LeaseVersion     int64
	LastDownloadedAt *int64
	RecencyAt        int64
}

type CapacityEvictionResult struct {
	Claimed   bool
	Pinned    bool
	SizeBytes int64
}

func New(db *ent.Client) *Service {
	return NewWithOptions(db, Options{})
}

func NewWithOptions(db *ent.Client, options Options) *Service {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Service{
		db:                           db,
		now:                          now,
		readerLeaseDuration:          durationOrDefault(options.ReaderLeaseDuration, ReaderLeaseDuration),
		leaseRenewalInterval:         durationOrDefault(options.LeaseRenewalInterval, LeaseRenewalInterval),
		directDownloadLeaseDuration:  durationOrDefault(options.DirectDownloadLeaseDuration, DirectDownloadLeaseDuration),
		deletionGracePeriod:          durationOrDefault(options.DeletionGracePeriod, DeletionGracePeriod),
		materializationLeaseDuration: durationOrDefault(options.MaterializationLeaseDuration, MaterializationLeaseDuration),
		materializationDisabled:      options.MaterializationDisabled,
		readerAcquireRetryBackoff:    durationOrDefault(options.ReaderAcquireRetryBackoff, readerAcquireRetryBackoff),
	}
}

func (s *Service) AcquireReader(ctx context.Context, cacheEntryID string, options AcquireReaderOptions) (*ReaderLease, error) {
	for attempt := 0; attempt < 3; attempt++ {
		lease, retry, err := s.acquireReaderOnce(ctx, cacheEntryID, options)
		if err != nil {
			return nil, err
		}
		if !retry {
			return lease, nil
		}
		if attempt < 2 {
			timer := time.NewTimer(s.readerAcquireRetryBackoff * time.Duration(1<<attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, ErrLocationUnavailable
}

func (s *Service) acquireReaderOnce(ctx context.Context, cacheEntryID string, options AcquireReaderOptions) (*ReaderLease, bool, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("start reader lease transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	entry, err := tx.CacheEntry.Query().Where(cacheentry.ID(cacheEntryID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, false, ErrLocationUnavailable
		}
		return nil, false, fmt.Errorf("query cache entry for reader lease: %w", err)
	}

	location, err := tx.StorageLocation.Query().Where(storagelocation.ID(entry.LocationId)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			if err := tx.Rollback(); err != nil {
				return nil, false, fmt.Errorf("rollback missing location reader lease: %w", err)
			}
			committed = true
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("query storage location for reader lease: %w", err)
	}

	// A set deletionRequestedAt never clears, so this snapshot check is final.
	if location.DeletionRequestedAt != nil {
		if err := tx.Rollback(); err != nil {
			return nil, false, fmt.Errorf("rollback fenced reader lease: %w", err)
		}
		committed = true
		return nil, true, nil
	}

	scope := storagereaderlease.ScopeParts
	if location.MergedAt != nil {
		scope = storagereaderlease.ScopeStorage
	} else {
		if location.PartsDeletedAt != nil {
			return nil, false, ErrLocationUnavailable
		}
		if options.Direct && location.PartCount != 1 {
			if err := tx.Commit(); err != nil {
				return nil, false, fmt.Errorf("commit direct representation check: %w", err)
			}
			committed = true
			return nil, false, ErrDirectRepresentation
		}
		// The row lock is only paid while a future merge (and thus parts
		// deletion, which has no grace period) is still possible; merged,
		// single-part, unsupported and composer-less locations are covered by
		// the lease row alone (fence recheck, deletion grace, FK restrict).
		if !s.materializationDisabled && location.PartCount >= 2 && location.MaterializationUnsupportedAt == nil {
			affected, err := tx.StorageLocation.Update().
				Where(
					storagelocation.ID(location.ID),
					storagelocation.DeletionRequestedAtIsNil(),
					storagelocation.MergedAtIsNil(),
					storagelocation.PartsDeletedAtIsNil(),
				).
				AddLeaseVersion(1).
				Save(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("lock storage location for parts reader lease: %w", err)
			}
			if affected == 0 {
				if err := tx.Rollback(); err != nil {
					return nil, false, fmt.Errorf("rollback unavailable parts reader lease: %w", err)
				}
				committed = true
				return nil, true, nil
			}
		}
	}

	duration := s.readerLeaseDuration
	if options.Direct {
		duration = s.directDownloadLeaseDuration
	}
	expiresAt := s.now().Add(duration)
	id := uuid.NewString()
	if _, err := tx.StorageReaderLease.Create().
		SetID(id).
		SetStorageLocationId(location.ID).
		SetScope(scope).
		SetExpiresAt(expiresAt.UnixMilli()).
		Save(ctx); err != nil {
		if ent.IsConstraintError(err) {
			// Parent location deleted between the read above and this insert.
			if err := tx.Rollback(); err != nil {
				return nil, false, fmt.Errorf("rollback conflicting reader lease: %w", err)
			}
			committed = true
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("create storage reader lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit storage reader lease: %w", err)
	}
	committed = true

	return &ReaderLease{
		ID:         id,
		Location:   location,
		Scope:      scope,
		ExpiresAt:  expiresAt,
		RenewAfter: s.leaseRenewalInterval,
	}, false, nil
}

func (s *Service) RenewReader(ctx context.Context, leaseID string) error {
	now := s.now()
	affected, err := s.db.StorageReaderLease.Update().
		Where(
			storagereaderlease.ID(leaseID),
			storagereaderlease.ExpiresAtGT(now.UnixMilli()),
		).
		SetExpiresAt(now.Add(s.readerLeaseDuration).UnixMilli()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("renew storage reader lease: %w", err)
	}
	if affected == 0 {
		return ErrReaderLeaseLost
	}
	return nil
}

func (s *Service) ExtendDirectReader(ctx context.Context, leaseID string) error {
	now := s.now()
	affected, err := s.db.StorageReaderLease.Update().
		Where(
			storagereaderlease.ID(leaseID),
			storagereaderlease.ExpiresAtGT(now.UnixMilli()),
		).
		SetExpiresAt(now.Add(s.directDownloadLeaseDuration).UnixMilli()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("extend direct-download reader lease: %w", err)
	}
	if affected == 0 {
		return ErrReaderLeaseLost
	}
	return nil
}

func (s *Service) ReleaseReader(ctx context.Context, leaseID string) error {
	err := s.db.StorageReaderLease.DeleteOneID(leaseID).Exec(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return fmt.Errorf("release storage reader lease: %w", err)
	}
	return nil
}

func (s *Service) ReleaseReaders(ctx context.Context, leaseIDs []string) (int, error) {
	if len(leaseIDs) == 0 {
		return 0, nil
	}
	deleted, err := s.db.StorageReaderLease.Delete().
		Where(storagereaderlease.IDIn(leaseIDs...)).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("release storage reader leases: %w", err)
	}
	return deleted, nil
}

// PurgeDanglingCacheEntry removes an entry only if it still references the
// storage location that was confirmed missing. The location is fenced only
// after its final cache entry has been detached.
func (s *Service) PurgeDanglingCacheEntry(ctx context.Context, cacheEntryID, locationID string) (bool, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return false, fmt.Errorf("start dangling cache entry purge transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	deleted, err := tx.CacheEntry.Delete().
		Where(
			cacheentry.ID(cacheEntryID),
			cacheentry.LocationId(locationID),
		).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("delete dangling cache entry: %w", err)
	}
	if deleted > 0 {
		if _, err := s.FenceDetachedLocation(ctx, tx.Client(), locationID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit dangling cache entry purge: %w", err)
	}
	committed = true
	return deleted > 0, nil
}

// PurgeDanglingCacheEntryIfUnchanged removes an entry only while the storage
// representation and materialization ownership still match the state that was
// inspected before the missing object was confirmed. Reader leases no longer
// invalidate the observation on merged locations; active readers are handled
// by the fence's lease recheck and deletion grace instead.
func (s *Service) PurgeDanglingCacheEntryIfUnchanged(ctx context.Context, cacheEntryID string, observed *ent.StorageLocation) (bool, error) {
	if observed == nil || activeMaterialization(observed, s.now()) {
		return false, nil
	}

	tx, err := s.db.Tx(ctx)
	if err != nil {
		return false, fmt.Errorf("start conditional dangling cache entry purge transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	predicates := []predicate.StorageLocation{
		storagelocation.ID(observed.ID),
		storagelocation.FolderName(observed.FolderName),
		storagelocation.PartCount(observed.PartCount),
		storagelocation.LeaseVersion(observed.LeaseVersion),
		storagelocation.DeletionRequestedAtIsNil(),
	}
	predicates = appendObservedRepresentationPredicates(predicates, observed)
	affected, err := tx.StorageLocation.Update().
		Where(predicates...).
		AddLeaseVersion(1).
		Save(ctx)
	if err != nil {
		return false, fmt.Errorf("claim conditional dangling cache entry purge: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit stale dangling cache entry observation: %w", err)
		}
		committed = true
		return false, nil
	}

	deleted, err := tx.CacheEntry.Delete().
		Where(
			cacheentry.ID(cacheEntryID),
			cacheentry.LocationId(observed.ID),
		).
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("delete conditionally dangling cache entry: %w", err)
	}
	if deleted > 0 {
		if _, err := s.fenceDetachedLocation(ctx, tx.Client(), observed.ID, false); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit conditional dangling cache entry purge: %w", err)
	}
	committed = true
	return deleted > 0, nil
}

func (s *Service) MaterializationActive(location *ent.StorageLocation) bool {
	return location != nil && activeMaterialization(location, s.now())
}

func appendObservedRepresentationPredicates(predicates []predicate.StorageLocation, observed *ent.StorageLocation) []predicate.StorageLocation {
	predicates = appendOptionalInt64Predicate(predicates, observed.MergeStartedAt, storagelocation.MergeStartedAtIsNil, storagelocation.MergeStartedAt)
	predicates = appendOptionalStringPredicate(predicates, observed.MergeLeaseToken, storagelocation.MergeLeaseTokenIsNil, storagelocation.MergeLeaseToken)
	predicates = appendOptionalInt64Predicate(predicates, observed.MergeLeaseExpiresAt, storagelocation.MergeLeaseExpiresAtIsNil, storagelocation.MergeLeaseExpiresAt)
	predicates = appendOptionalInt64Predicate(predicates, observed.MergedAt, storagelocation.MergedAtIsNil, storagelocation.MergedAt)
	predicates = appendOptionalInt64Predicate(predicates, observed.MaterializationUnsupportedAt, storagelocation.MaterializationUnsupportedAtIsNil, storagelocation.MaterializationUnsupportedAt)
	predicates = appendOptionalInt64Predicate(predicates, observed.PartsDeletedAt, storagelocation.PartsDeletedAtIsNil, storagelocation.PartsDeletedAt)
	return predicates
}

func appendOptionalInt64Predicate(predicates []predicate.StorageLocation, value *int64, isNil func() predicate.StorageLocation, equal func(int64) predicate.StorageLocation) []predicate.StorageLocation {
	if value == nil {
		return append(predicates, isNil())
	}
	return append(predicates, equal(*value))
}

func appendOptionalStringPredicate(predicates []predicate.StorageLocation, value *string, isNil func() predicate.StorageLocation, equal func(string) predicate.StorageLocation) []predicate.StorageLocation {
	if value == nil {
		return append(predicates, isNil())
	}
	return append(predicates, equal(*value))
}

func (s *Service) RequestLocationDeletion(ctx context.Context, locationID string, deleteCacheEntries, requireOrphan bool) (DeletionResult, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("start storage location deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if requireOrphan {
		hasEntries, err := tx.CacheEntry.Query().Where(cacheentry.LocationId(locationID)).Exist(ctx)
		if err != nil {
			return DeletionResult{}, fmt.Errorf("query storage location references: %w", err)
		}
		if hasEntries {
			if err := tx.Commit(); err != nil {
				return DeletionResult{}, fmt.Errorf("commit referenced storage location check: %w", err)
			}
			committed = true
			return DeletionResult{}, nil
		}
	}
	var result DeletionResult
	if deleteCacheEntries {
		result, err = s.detachCacheEntriesAndFence(ctx, tx.Client(), locationID, true)
	} else {
		result, err = s.FenceDetachedLocation(ctx, tx.Client(), locationID)
	}
	if err != nil {
		return DeletionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeletionResult{}, fmt.Errorf("commit storage location deletion: %w", err)
	}
	committed = true
	return result, nil
}

// RequestCapacityEviction removes a location only while its access observation
// is still current. Committed active readers pin the location, preserving
// capacity eviction's LRU contract; an acquisition still in flight may be
// missed, in which case the fence defers physical deletion instead.
func (s *Service) RequestCapacityEviction(ctx context.Context, observed CapacityObservation) (CapacityEvictionResult, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return CapacityEvictionResult{}, fmt.Errorf("start capacity eviction transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	predicates := []predicate.StorageLocation{
		storagelocation.ID(observed.LocationID),
		storagelocation.LeaseVersion(observed.LeaseVersion),
		storagelocation.DeletionRequestedAtIsNil(),
		storagelocation.SizeBytesNotNil(),
	}
	predicates = appendOptionalInt64Predicate(
		predicates,
		observed.LastDownloadedAt,
		storagelocation.LastDownloadedAtIsNil,
		storagelocation.LastDownloadedAt,
	)
	affected, err := tx.StorageLocation.Update().
		Where(predicates...).
		AddLeaseVersion(1).
		Save(ctx)
	if err != nil {
		return CapacityEvictionResult{}, fmt.Errorf("claim capacity eviction candidate: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return CapacityEvictionResult{}, fmt.Errorf("commit stale capacity eviction observation: %w", err)
		}
		committed = true
		return CapacityEvictionResult{}, nil
	}

	location, err := tx.StorageLocation.Get(ctx, observed.LocationID)
	if err != nil {
		return CapacityEvictionResult{}, fmt.Errorf("query claimed capacity eviction location: %w", err)
	}
	newestEntry, err := tx.CacheEntry.Query().
		Where(cacheentry.LocationId(location.ID)).
		Order(ent.Desc(cacheentry.FieldUpdatedAt)).
		First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			if err := tx.Commit(); err != nil {
				return CapacityEvictionResult{}, fmt.Errorf("commit orphan capacity candidate check: %w", err)
			}
			committed = true
			return CapacityEvictionResult{}, nil
		}
		return CapacityEvictionResult{}, fmt.Errorf("query capacity candidate recency: %w", err)
	}
	currentRecency := newestEntry.UpdatedAt
	if location.LastDownloadedAt != nil {
		currentRecency = *location.LastDownloadedAt
	}
	if currentRecency != observed.RecencyAt {
		if err := tx.Commit(); err != nil {
			return CapacityEvictionResult{}, fmt.Errorf("commit changed capacity candidate recency: %w", err)
		}
		committed = true
		return CapacityEvictionResult{}, nil
	}

	now := s.now().UnixMilli()
	hasActiveReader, err := tx.StorageReaderLease.Query().
		Where(
			storagereaderlease.StorageLocationId(location.ID),
			storagereaderlease.ExpiresAtGT(now),
		).
		Exist(ctx)
	if err != nil {
		return CapacityEvictionResult{}, fmt.Errorf("query capacity candidate readers: %w", err)
	}
	if hasActiveReader {
		if err := tx.Commit(); err != nil {
			return CapacityEvictionResult{}, fmt.Errorf("commit pinned capacity candidate check: %w", err)
		}
		committed = true
		return CapacityEvictionResult{Pinned: true}, nil
	}

	result, err := s.detachCacheEntriesAndFence(ctx, tx.Client(), location.ID, false)
	if err != nil {
		return CapacityEvictionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CapacityEvictionResult{}, fmt.Errorf("commit capacity eviction: %w", err)
	}
	committed = true
	return CapacityEvictionResult{
		Claimed:   result.Fenced,
		SizeBytes: *location.SizeBytes,
	}, nil
}

func (s *Service) detachCacheEntriesAndFence(ctx context.Context, db *ent.Client, locationID string, lock bool) (DeletionResult, error) {
	if _, err := db.CacheEntry.Delete().Where(cacheentry.LocationId(locationID)).Exec(ctx); err != nil {
		return DeletionResult{}, fmt.Errorf("delete cache entries: %w", err)
	}
	return s.fenceDetachedLocation(ctx, db, locationID, lock)
}

// RequestExpiredLocationDeletion atomically rechecks retention eligibility
// against active reader leases and cache-entry replacement transactions.
func (s *Service) RequestExpiredLocationDeletion(ctx context.Context, locationID string, cutoff int64) (DeletionResult, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("start expired storage location deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	affected, err := tx.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		AddLeaseVersion(1).
		Save(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("lock expired storage location for deletion: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return DeletionResult{}, fmt.Errorf("commit skipped expired storage location deletion: %w", err)
		}
		committed = true
		return DeletionResult{}, nil
	}

	location, err := tx.StorageLocation.Query().Where(storagelocation.ID(locationID)).Only(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query expired storage location: %w", err)
	}
	now := s.now().UnixMilli()
	if location.LastDownloadedAt != nil && *location.LastDownloadedAt >= cutoff {
		if err := tx.Commit(); err != nil {
			return DeletionResult{}, fmt.Errorf("commit recently downloaded storage location check: %w", err)
		}
		committed = true
		return DeletionResult{}, nil
	}

	hasEntries, err := tx.CacheEntry.Query().Where(cacheentry.LocationId(locationID)).Exist(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query retained cache entries: %w", err)
	}
	hasRecentEntry, err := tx.CacheEntry.Query().
		Where(
			cacheentry.LocationId(locationID),
			cacheentry.UpdatedAtGTE(cutoff),
		).
		Exist(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query recently saved cache entries: %w", err)
	}
	hasActiveReader, err := tx.StorageReaderLease.Query().
		Where(
			storagereaderlease.StorageLocationId(locationID),
			storagereaderlease.ExpiresAtGT(now),
		).
		Exist(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query active readers for retained storage location: %w", err)
	}
	if !hasEntries || hasRecentEntry || hasActiveReader {
		if err := tx.Commit(); err != nil {
			return DeletionResult{}, fmt.Errorf("commit retained storage location check: %w", err)
		}
		committed = true
		return DeletionResult{}, nil
	}

	if _, err := tx.CacheEntry.Delete().Where(cacheentry.LocationId(locationID)).Exec(ctx); err != nil {
		return DeletionResult{}, fmt.Errorf("delete expired cache entries: %w", err)
	}
	result, err := s.fenceDetachedLocation(ctx, tx.Client(), locationID, false)
	if err != nil {
		return DeletionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeletionResult{}, fmt.Errorf("commit expired storage location deletion: %w", err)
	}
	committed = true
	return result, nil
}

func (s *Service) FenceDetachedLocation(ctx context.Context, db *ent.Client, locationID string) (DeletionResult, error) {
	return s.fenceDetachedLocation(ctx, db, locationID, true)
}

func (s *Service) fenceDetachedLocation(ctx context.Context, db *ent.Client, locationID string, lock bool) (DeletionResult, error) {
	if lock {
		affected, err := db.StorageLocation.Update().
			Where(storagelocation.ID(locationID)).
			AddLeaseVersion(1).
			Save(ctx)
		if err != nil {
			return DeletionResult{}, fmt.Errorf("lock storage location for deletion: %w", err)
		}
		if affected == 0 {
			return DeletionResult{}, nil
		}
	}

	hasEntries, err := db.CacheEntry.Query().Where(cacheentry.LocationId(locationID)).Exist(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query storage location references: %w", err)
	}
	if hasEntries {
		return DeletionResult{}, nil
	}

	location, err := db.StorageLocation.Query().Where(storagelocation.ID(locationID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return DeletionResult{}, nil
		}
		return DeletionResult{}, fmt.Errorf("query storage location for deletion: %w", err)
	}
	now := s.now()
	result := DeletionResult{Fenced: true}
	if location.DeletionRequestedAt == nil {
		requestedAt := now.UnixMilli()
		if err := db.StorageLocation.UpdateOneID(location.ID).
			SetDeletionRequestedAt(requestedAt).
			Exec(ctx); err != nil {
			return DeletionResult{}, fmt.Errorf("fence storage location deletion: %w", err)
		}
		location.DeletionRequestedAt = &requestedAt
	}

	if _, err := db.StorageReaderLease.Delete().
		Where(
			storagereaderlease.StorageLocationId(location.ID),
			storagereaderlease.ExpiresAtLTE(now.UnixMilli()),
		).
		Exec(ctx); err != nil {
		return DeletionResult{}, fmt.Errorf("purge expired reader leases for storage deletion: %w", err)
	}
	hasReaders, err := db.StorageReaderLease.Query().
		Where(
			storagereaderlease.StorageLocationId(location.ID),
			storagereaderlease.ExpiresAtGT(now.UnixMilli()),
		).
		Exist(ctx)
	if err != nil {
		return DeletionResult{}, fmt.Errorf("query active reader leases for storage deletion: %w", err)
	}
	if hasReaders || activeMaterialization(location, now) {
		return result, nil
	}
	if now.Before(time.UnixMilli(*location.DeletionRequestedAt).Add(s.deletionGracePeriod)) {
		return result, nil
	}

	task, err := storageoutbox.Enqueue(ctx, db, location.FolderName)
	if err != nil {
		return DeletionResult{}, err
	}
	if err := db.StorageLocation.DeleteOneID(location.ID).Exec(ctx); err != nil {
		return DeletionResult{}, fmt.Errorf("delete fenced storage location: %w", err)
	}
	result.Task = task
	result.Finalized = true
	return result, nil
}

func (s *Service) ClaimPartsDeletion(ctx context.Context, locationID string) (PartsDeletionResult, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return PartsDeletionResult{}, fmt.Errorf("start parts deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	affected, err := tx.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.DeletionRequestedAtIsNil(),
			storagelocation.MergedAtNotNil(),
			storagelocation.PartsDeletedAtIsNil(),
		).
		AddLeaseVersion(1).
		Save(ctx)
	if err != nil {
		return PartsDeletionResult{}, fmt.Errorf("lock storage location for parts deletion: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return PartsDeletionResult{}, fmt.Errorf("commit skipped parts deletion: %w", err)
		}
		committed = true
		return PartsDeletionResult{}, nil
	}
	location, err := tx.StorageLocation.Query().Where(storagelocation.ID(locationID)).Only(ctx)
	if err != nil {
		return PartsDeletionResult{}, fmt.Errorf("query storage location for parts deletion: %w", err)
	}
	now := s.now()
	if _, err := tx.StorageReaderLease.Delete().
		Where(
			storagereaderlease.StorageLocationId(location.ID),
			storagereaderlease.ScopeEQ(storagereaderlease.ScopeParts),
			storagereaderlease.ExpiresAtLTE(now.UnixMilli()),
		).
		Exec(ctx); err != nil {
		return PartsDeletionResult{}, fmt.Errorf("purge expired part reader leases: %w", err)
	}
	hasPartReaders, err := tx.StorageReaderLease.Query().
		Where(
			storagereaderlease.StorageLocationId(location.ID),
			storagereaderlease.ScopeEQ(storagereaderlease.ScopeParts),
			storagereaderlease.ExpiresAtGT(now.UnixMilli()),
		).
		Exist(ctx)
	if err != nil {
		return PartsDeletionResult{}, fmt.Errorf("query active part reader leases: %w", err)
	}
	if hasPartReaders {
		if err := tx.Commit(); err != nil {
			return PartsDeletionResult{}, fmt.Errorf("commit deferred parts deletion: %w", err)
		}
		committed = true
		return PartsDeletionResult{}, nil
	}

	task, err := storageoutbox.Enqueue(ctx, tx.Client(), location.FolderName+"/parts")
	if err != nil {
		return PartsDeletionResult{}, err
	}
	if err := tx.StorageLocation.UpdateOneID(location.ID).
		SetPartsDeletedAt(now.UnixMilli()).
		Exec(ctx); err != nil {
		return PartsDeletionResult{}, fmt.Errorf("mark parts deletion claimed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PartsDeletionResult{}, fmt.Errorf("commit parts deletion: %w", err)
	}
	committed = true
	return PartsDeletionResult{Task: task, PartCount: location.PartCount}, nil
}

func (s *Service) AcquireMaterialization(ctx context.Context, locationID string) (*MaterializationLease, error) {
	tx, err := s.db.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("start materialization lease transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	affected, err := tx.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		AddLeaseVersion(1).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock storage location for materialization: %w", err)
	}
	if affected == 0 {
		return nil, ErrMaterializationLeaseHeld
	}
	location, err := tx.StorageLocation.Query().Where(storagelocation.ID(locationID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, ErrMaterializationLeaseHeld
		}
		return nil, fmt.Errorf("query storage location for materialization: %w", err)
	}
	now := s.now()
	if location.PartCount < 2 || location.MergedAt != nil || location.MaterializationUnsupportedAt != nil || location.PartsDeletedAt != nil || activeMaterialization(location, now) {
		return nil, ErrMaterializationLeaseHeld
	}

	token := uuid.NewString()
	location, err = tx.StorageLocation.UpdateOneID(location.ID).
		SetMergeStartedAt(now.UnixMilli()).
		SetMergeLeaseToken(token).
		SetMergeLeaseExpiresAt(now.Add(s.materializationLeaseDuration).UnixMilli()).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire materialization lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit materialization lease: %w", err)
	}
	committed = true
	return &MaterializationLease{Location: location, Token: token, RenewAfter: s.leaseRenewalInterval}, nil
}

func (s *Service) RenewMaterialization(ctx context.Context, locationID, token string) error {
	now := s.now()
	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeLeaseToken(token),
			storagelocation.MergeLeaseExpiresAtGT(now.UnixMilli()),
			storagelocation.MergedAtIsNil(),
		).
		SetMergeLeaseExpiresAt(now.Add(s.materializationLeaseDuration).UnixMilli()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("renew materialization lease: %w", err)
	}
	if affected == 0 {
		return ErrMaterializationLeaseLost
	}
	return nil
}

func (s *Service) FinishMaterialization(ctx context.Context, locationID, token string) error {
	now := s.now()
	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeLeaseToken(token),
			storagelocation.MergeLeaseExpiresAtGT(now.UnixMilli()),
			storagelocation.MergedAtIsNil(),
		).
		SetMergedAt(now.UnixMilli()).
		ClearMergeLeaseToken().
		ClearMergeLeaseExpiresAt().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("finish materialization: %w", err)
	}
	if affected == 0 {
		return ErrMaterializationLeaseLost
	}
	return nil
}

func (s *Service) MarkMaterializationUnsupported(ctx context.Context, locationID, token string) error {
	now := s.now()
	affected, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeLeaseToken(token),
			storagelocation.MergeLeaseExpiresAtGT(now.UnixMilli()),
			storagelocation.MergedAtIsNil(),
		).
		SetMaterializationUnsupportedAt(now.UnixMilli()).
		ClearMergeStartedAt().
		ClearMergeLeaseToken().
		ClearMergeLeaseExpiresAt().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("mark materialization unsupported: %w", err)
	}
	if affected == 0 {
		return ErrMaterializationLeaseLost
	}
	return nil
}

func (s *Service) ReleaseMaterialization(ctx context.Context, locationID, token string) error {
	_, err := s.db.StorageLocation.Update().
		Where(
			storagelocation.ID(locationID),
			storagelocation.MergeLeaseToken(token),
			storagelocation.MergedAtIsNil(),
		).
		ClearMergeStartedAt().
		ClearMergeLeaseToken().
		ClearMergeLeaseExpiresAt().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("release materialization lease: %w", err)
	}
	return nil
}

func (s *Service) PurgeExpiredReaderLeases(ctx context.Context) (int, error) {
	deleted, err := s.db.StorageReaderLease.Delete().
		Where(storagereaderlease.ExpiresAtLTE(s.now().UnixMilli())).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("purge expired storage reader leases: %w", err)
	}
	return deleted, nil
}

func activeMaterialization(location *ent.StorageLocation, now time.Time) bool {
	if location.MergeLeaseToken != nil {
		return location.MergeLeaseExpiresAt != nil && *location.MergeLeaseExpiresAt > now.UnixMilli()
	}
	return location.MergedAt == nil && location.MaterializationUnsupportedAt == nil && location.MergeStartedAt != nil && *location.MergeStartedAt > now.Add(-legacyMaterializationLifetime).UnixMilli()
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}
