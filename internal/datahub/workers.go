package datahub

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/middleware/asynqdl"
	"github.com/hibiken/asynq"
)

const (
	// taskTypeExpireUploads abandons uploads that were never finished.
	taskTypeExpireUploads = "datahub:expire-uploads"
	// queueDatahub is Datahub's own queue. Only Datahub's worker consumes it, so
	// the module adds no queue to any existing worker pool and cannot change how
	// the rest of the system schedules work.
	queueDatahub = "datahub"

	workerConcurrency   = 1
	expirySweepInterval = 10 * time.Minute
	expiryBatchSize     = 500
	expiryMaxRetry      = 5

	// expiryReason is what an abandoned Upload records, so an operator reading
	// the row knows nobody failed — the client simply never finished.
	expiryReason = "upload expired before it was completed"
)

// worker is Datahub's own asynq worker plus the schedule that feeds it.
type worker struct {
	server *asynq.Server
	client *asynq.Client
	stop   chan struct{}
}

// startBackgroundWorkers gives the module a worker on its own queue, wired to
// WeKnora's shared dead-letter middleware so a task that exhausts its retries
// lands in the same task_dead_letters table operators already query.
//
// A deployment without Redis (lite mode) has Datahub disabled anyway; when the
// module is enabled but Redis is somehow absent, it logs and carries on rather
// than refusing to serve uploads.
func (m *Module) startBackgroundWorkers() error {
	if m.redis == nil {
		logger.Warnf(context.Background(),
			"[Datahub] no Redis client: background tasks are disabled")
		return nil
	}

	mux := asynq.NewServeMux()
	mux.Use(asynqdl.MiddlewareWithCallback(m.deadLetters, nil))
	mux.HandleFunc(taskTypeExpireUploads, m.handleExpireUploads)

	// Datahub runs on the Redis client the rest of the server already built and
	// pinged, so pooling, TLS and timeouts match the deployment without this
	// module re-reading the environment.
	server := asynq.NewServerFromRedisClient(m.redis, asynq.Config{
		Concurrency: workerConcurrency,
		Queues:      map[string]int{queueDatahub: 1},
	})

	if err := server.Start(mux); err != nil {
		return err
	}

	m.worker = &worker{
		server: server,
		client: asynq.NewClientFromRedisClient(m.redis),
		stop:   make(chan struct{}),
	}
	go m.scheduleExpirySweeps(m.worker)

	logger.Infof(context.Background(),
		"[Datahub] background worker started: queue=%s interval=%s",
		queueDatahub, expirySweepInterval)
	return nil
}

// scheduleExpirySweeps enqueues the sweep periodically. The first sweep runs one
// interval after startup, which also keeps it clear of migrations.
func (m *Module) scheduleExpirySweeps(w *worker) {
	ticker := time.NewTicker(expirySweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.enqueueExpirySweep(w)
		case <-w.stop:
			return
		}
	}
}

// enqueueExpirySweep queues one sweep. Enqueue is a Redis write, so failing to
// schedule is logged and retried by the next tick rather than crashing.
func (m *Module) enqueueExpirySweep(w *worker) {
	task := asynq.NewTask(taskTypeExpireUploads, nil)
	if _, err := w.client.Enqueue(task,
		asynq.Queue(queueDatahub),
		asynq.MaxRetry(expiryMaxRetry),
	); err != nil {
		logger.Warnf(context.Background(),
			"[Datahub] failed to enqueue the expiry sweep: %v", err)
	}
}

// handleExpireUploads abandons uploads whose expiry has passed: it aborts the
// multipart session so the parts do not linger in object storage, then marks
// the Upload cancelled with a reason.
//
// Running it twice is harmless. Aborting an already-aborted session is a no-op
// in object storage, and marking only touches uploads still in flight, so the
// second run finds nothing left to do.
func (m *Module) handleExpireUploads(ctx context.Context, _ *asynq.Task) error {
	expired, err := m.store.ListExpiredUploads(ctx, time.Now().UTC(), expiryBatchSize)
	if err != nil {
		// Returning the error hands the task back to asynq for a retry.
		return err
	}
	if len(expired) == 0 {
		return nil
	}

	var failures []error
	for _, upload := range expired {
		if upload.ObjectUploadID != "" {
			if err := m.objects.AbortMultipartUpload(
				ctx, upload.Bucket, upload.ObjectKey, upload.ObjectUploadID,
			); err != nil {
				// The session may already be gone, or object storage may be
				// unreachable. Either way the record still has to be abandoned;
				// the reconciler sweeps any session left behind.
				logger.Warnf(ctx,
					"[Datahub] failed to abort expired upload upload_id=%s: %v",
					upload.UploadID, err)
			}
		}
		if err := m.store.MarkUploadExpired(
			ctx, upload.TenantID, upload.UploadID, expiryReason,
		); err != nil {
			failures = append(failures, err)
			continue
		}
		logger.Infof(ctx, "[Datahub] expired upload abandoned upload_id=%s status=%s",
			upload.UploadID, upload.Status)
	}

	logger.Infof(ctx, "[Datahub] expiry sweep finished: expired=%d failed=%d",
		len(expired), len(failures))
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return nil
}

// Close stops Datahub's worker and its schedule. Safe to call when no worker
// was started.
func (m *Module) Close() error {
	if m.worker == nil {
		return nil
	}
	close(m.worker.stop)
	m.worker.server.Shutdown()
	// The queue client was built from the server's Redis client, so it shares
	// that connection and must not close it: the container owns the Redis
	// client's lifetime.
	return nil
}
