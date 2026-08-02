package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/upload"
)

const uploadActivityRenewalTimeout = 10 * time.Second

func (s *Service) withUploadActivity(ctx context.Context, uploadID int64, operation func(context.Context) error) error {
	if err := s.touchUploadActivity(ctx, uploadID); err != nil {
		return err
	}

	activityCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	done := make(chan error, 1)
	go s.renewUploadActivity(activityCtx, cancel, uploadID, done)

	operationErr := operation(activityCtx)
	cancel(nil)
	activityErr := <-done
	if activityErr != nil {
		return activityErr
	}
	return operationErr
}

func (s *Service) renewUploadActivity(ctx context.Context, cancel context.CancelCauseFunc, uploadID int64, done chan<- error) {
	ticker := time.NewTicker(s.uploadHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			renewCtx, renewCancel := context.WithTimeout(ctx, uploadActivityRenewalTimeout)
			err := s.touchUploadActivity(renewCtx, uploadID)
			renewCancel()
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				done <- nil
				return
			}
			cancel(err)
			done <- err
			return
		}
	}
}

func (s *Service) touchUploadActivity(ctx context.Context, uploadID int64) error {
	affected, err := s.db.Upload.Update().
		Where(upload.ID(uploadID)).
		SetLastPartUploadedAt(s.now().UnixMilli()).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("renew upload activity: %w", err)
	}
	if affected == 0 {
		return ErrUploadNotFound
	}
	return nil
}
