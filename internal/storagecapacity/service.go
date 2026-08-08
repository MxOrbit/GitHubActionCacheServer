package storagecapacity

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/rs/zerolog"
)

const (
	candidatePageSize       = 50
	defaultEnforceInterval  = 10 * time.Minute
	initialFailureRetry     = time.Minute
	maximumFailureRetry     = 10 * time.Minute
	targetBudgetNumerator   = int64(9)
	targetBudgetDenominator = int64(10)
)

var ErrUnresolvedSizes = errors.New("capacity eviction requires every active storage location size")

type Options struct {
	DB        *ent.Client
	Storage   storage.Adapter
	Config    config.CacheConfig
	Lifecycle *storagelifecycle.Service
	Logger    *zerolog.Logger
}

type Result struct {
	Mode                string
	BudgetBytes         int64
	TargetBytes         int64
	UsageBeforeBytes    int64
	UsageAfterBytes     int64
	PendingCreditBytes  int64
	Candidates          int
	ClaimedLocations    int
	ClaimedBytes        int64
	PinnedLocations     int
	StaleLocations      int
	UnresolvedLocations int
	Constrained         bool
}

type Service struct {
	db                   *ent.Client
	filesystem           storage.FilesystemUsageAdapter
	lifecycle            *storagelifecycle.Service
	logger               zerolog.Logger
	explicitBudgetBytes  int64
	explicitBudget       bool
	filesystemMaxPercent float64
	enabled              bool
	trigger              chan struct{}
	enforceInterval      time.Duration
	retryInitial         time.Duration
	retryMaximum         time.Duration
	enforce              func(context.Context) (Result, error)
}

type capacityCandidate struct {
	location *ent.StorageLocation
	recency  int64
}

type capacityCursor struct {
	recency    int64
	locationID string
}

func NewService(options Options) *Service {
	logger := zerolog.Nop()
	if options.Logger != nil {
		logger = *options.Logger
	}
	filesystem, filesystemEnabled := options.Storage.(storage.FilesystemUsageAdapter)
	lifecycle := options.Lifecycle
	if lifecycle == nil && options.DB != nil {
		lifecycle = storagelifecycle.New(options.DB)
	}
	service := &Service{
		db:                   options.DB,
		filesystem:           filesystem,
		lifecycle:            lifecycle,
		logger:               logger,
		explicitBudgetBytes:  options.Config.MaxSizeBytes,
		explicitBudget:       options.Config.MaxSizeBytesConfigured,
		filesystemMaxPercent: options.Config.FilesystemMaxUsagePercent,
		enabled:              options.Config.MaxSizeBytesConfigured || filesystemEnabled,
		trigger:              make(chan struct{}, 1),
		enforceInterval:      defaultEnforceInterval,
		retryInitial:         initialFailureRetry,
		retryMaximum:         maximumFailureRetry,
	}
	service.enforce = service.Enforce
	return service
}

func (s *Service) Trigger() {
	if !s.enabled {
		return
	}
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

func (s *Service) Run(ctx context.Context, ready <-chan struct{}) {
	if !s.enabled {
		return
	}
	s.logger.Info().Msg("capacity eviction waiting for storage size reconciliation")
	select {
	case <-ctx.Done():
		return
	case <-ready:
	}
	// The startup pass subsumes every finalize signal received before readiness.
	drainSignal(s.trigger)
	s.logger.Info().Msg("capacity eviction enabled after storage size reconciliation")

	ticker := time.NewTicker(s.enforceInterval)
	defer ticker.Stop()
	retryDelay := s.retryInitial
	run := true
	var retryTimer *time.Timer
	var retry <-chan time.Time
	defer func() { stopTimer(retryTimer) }()

	for {
		if run {
			result, err := s.enforce(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				s.logger.Error().
					Err(err).
					Dur("retry_in", retryDelay).
					Int("unresolved_locations", result.UnresolvedLocations).
					Msg("capacity eviction failed")
				retryTimer = resetTimer(retryTimer, retryDelay)
				retry = retryTimer.C
				retryDelay = min(retryDelay*2, s.retryMaximum)
			} else {
				stopTimer(retryTimer)
				retry = nil
				retryDelay = s.retryInitial
				s.logResult(result)
			}
			run = false
		}

		select {
		case <-ctx.Done():
			return
		case <-s.trigger:
			run = true
		case <-ticker.C:
			run = true
		case <-retry:
			retry = nil
			run = true
		}
	}
}

func (s *Service) Enforce(ctx context.Context) (Result, error) {
	if !s.enabled {
		return Result{}, nil
	}
	if s.db == nil || s.lifecycle == nil {
		return Result{}, fmt.Errorf("capacity eviction dependencies are required")
	}

	unresolved, err := s.db.StorageLocation.Query().
		Where(
			storagelocation.DeletionRequestedAtIsNil(),
			storagelocation.SizeBytesIsNil(),
		).
		Count(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("count unresolved capacity sizes: %w", err)
	}
	if unresolved > 0 {
		return Result{UnresolvedLocations: unresolved}, fmt.Errorf("%w: %d locations", ErrUnresolvedSizes, unresolved)
	}

	result, err := s.currentBudgetUsage(ctx)
	if err != nil {
		return Result{}, err
	}
	result.UsageAfterBytes = result.UsageBeforeBytes
	if result.UsageBeforeBytes <= result.BudgetBytes {
		return result, nil
	}

	var cursor *capacityCursor
	for result.UsageAfterBytes > result.TargetBytes {
		candidates, err := s.capacityCandidates(ctx, cursor)
		if err != nil {
			return result, err
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			result.Candidates++
			// Recency is mutable. A candidate that moves across this keyset
			// cursor may be revisited or deferred to the next periodic pass;
			// the transactional observation check preserves correctness.
			cursor = &capacityCursor{recency: candidate.recency, locationID: candidate.location.ID}
			eviction, err := s.lifecycle.RequestCapacityEviction(ctx, storagelifecycle.CapacityObservation{
				LocationID:       candidate.location.ID,
				LeaseVersion:     candidate.location.LeaseVersion,
				LastDownloadedAt: candidate.location.LastDownloadedAt,
				RecencyAt:        candidate.recency,
			})
			if err != nil {
				return result, fmt.Errorf("evict capacity candidate %s: %w", candidate.location.ID, err)
			}
			switch {
			case eviction.Claimed:
				result.ClaimedLocations++
				result.ClaimedBytes += eviction.SizeBytes
				result.UsageAfterBytes = max(0, result.UsageAfterBytes-eviction.SizeBytes)
			case eviction.Pinned:
				result.PinnedLocations++
			default:
				result.StaleLocations++
			}
			if result.UsageAfterBytes <= result.TargetBytes {
				break
			}
		}
	}
	result.Constrained = result.UsageAfterBytes > result.TargetBytes
	return result, nil
}

func (s *Service) currentBudgetUsage(ctx context.Context) (Result, error) {
	if s.explicitBudget {
		usage, err := sumLocationSizes(ctx, s.db.StorageLocation.Query().Where(storagelocation.DeletionRequestedAtIsNil()))
		if err != nil {
			return Result{}, fmt.Errorf("sum active cache payload sizes: %w", err)
		}
		return newUsageResult("explicit", s.explicitBudgetBytes, usage, 0), nil
	}
	if s.filesystem == nil {
		return Result{}, nil
	}
	filesystemUsage, err := s.filesystem.FilesystemUsage(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("inspect filesystem capacity: %w", err)
	}
	if filesystemUsage.CapacityBytes <= 0 || filesystemUsage.UsedBytes < 0 {
		return Result{}, fmt.Errorf("invalid filesystem usage: capacity=%d used=%d", filesystemUsage.CapacityBytes, filesystemUsage.UsedBytes)
	}
	budget, err := percentageBytes(filesystemUsage.CapacityBytes, s.filesystemMaxPercent)
	if err != nil {
		return Result{}, err
	}
	pendingCredit, err := sumLocationSizes(ctx, s.db.StorageLocation.Query().Where(storagelocation.DeletionRequestedAtNotNil()))
	if err != nil {
		return Result{}, fmt.Errorf("sum pending filesystem deletion credit: %w", err)
	}
	// Pending folders normally occupy at least their logical payload size. The
	// subtraction is deliberately conservative because extra representations,
	// blocks, and temporary files remain part of statfs usage until outbox deletion.
	effectiveUsage := max(0, filesystemUsage.UsedBytes-pendingCredit)
	return newUsageResult("filesystem", budget, effectiveUsage, pendingCredit), nil
}

func newUsageResult(mode string, budget, usage, pendingCredit int64) Result {
	return Result{
		Mode:               mode,
		BudgetBytes:        budget,
		TargetBytes:        ratioFloor(budget, targetBudgetNumerator, targetBudgetDenominator),
		UsageBeforeBytes:   usage,
		PendingCreditBytes: pendingCredit,
	}
}

func (s *Service) capacityCandidates(ctx context.Context, cursor *capacityCursor) ([]capacityCandidate, error) {
	predicates := []predicate.StorageLocation{
		storagelocation.DeletionRequestedAtIsNil(),
		storagelocation.SizeBytesNotNil(),
		storagelocation.HasCacheEntries(),
	}
	if cursor != nil {
		// The leading GTE lets the planner seek into (recencyAt, id); the
		// residual term filters the tie band.
		predicates = append(predicates, storagelocation.And(
			storagelocation.RecencyAtGTE(cursor.recency),
			storagelocation.Or(
				storagelocation.RecencyAtGT(cursor.recency),
				storagelocation.IDGT(cursor.locationID),
			),
		))
	}
	locations, err := s.db.StorageLocation.Query().
		Where(predicates...).
		Order(storagelocation.ByRecencyAt(), storagelocation.ByID()).
		Limit(candidatePageSize).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("query capacity eviction candidates: %w", err)
	}

	candidates := make([]capacityCandidate, 0, len(locations))
	for _, location := range locations {
		candidates = append(candidates, capacityCandidate{location: location, recency: location.RecencyAt})
	}
	return candidates, nil
}

func sumLocationSizes(ctx context.Context, query *ent.StorageLocationQuery) (int64, error) {
	var totals []struct {
		Bytes *int64 `json:"bytes"`
	}
	if err := query.
		Aggregate(ent.As(ent.Sum(storagelocation.FieldSizeBytes), "bytes")).
		Scan(ctx, &totals); err != nil {
		return 0, err
	}
	if len(totals) == 0 || totals[0].Bytes == nil {
		return 0, nil
	}
	return *totals[0].Bytes, nil
}

func percentageBytes(capacity int64, percent float64) (int64, error) {
	value := math.Floor(float64(capacity) * percent / 100)
	if value < 1 || value >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("filesystem budget exceeds supported range")
	}
	return int64(value), nil
}

func ratioFloor(value, numerator, denominator int64) int64 {
	return value/denominator*numerator + value%denominator*numerator/denominator
}

func drainSignal(signal <-chan struct{}) {
	select {
	case <-signal:
	default:
	}
}

func resetTimer(timer *time.Timer, delay time.Duration) *time.Timer {
	if timer == nil {
		return time.NewTimer(delay)
	}
	stopTimer(timer)
	timer.Reset(delay)
	return timer
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (s *Service) logResult(result Result) {
	if result.Constrained {
		s.logger.Warn().
			Str("mode", result.Mode).
			Int64("usage_bytes", result.UsageAfterBytes).
			Int64("budget_bytes", result.BudgetBytes).
			Int64("target_bytes", result.TargetBytes).
			Int64("remaining_bytes", max(0, result.UsageAfterBytes-result.TargetBytes)).
			Int("active_reader_locations", result.PinnedLocations).
			Int("stale_locations", result.StaleLocations).
			Int("claimed_locations", result.ClaimedLocations).
			Msg("capacity eviction could not reach its target")
		return
	}
	if result.ClaimedLocations > 0 {
		s.logger.Info().
			Str("mode", result.Mode).
			Int64("usage_before_bytes", result.UsageBeforeBytes).
			Int64("usage_after_bytes", result.UsageAfterBytes).
			Int64("budget_bytes", result.BudgetBytes).
			Int64("target_bytes", result.TargetBytes).
			Int("claimed_locations", result.ClaimedLocations).
			Int64("claimed_bytes", result.ClaimedBytes).
			Msg("capacity eviction completed")
	}
}
