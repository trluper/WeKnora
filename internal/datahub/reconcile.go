package datahub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/hibiken/asynq"
)

// handleReconcileUploads makes the database agree with object storage, in both
// directions:
//
//   - Every merged Upload must carry the facts of the object it points at. A
//     row that lost its ETag, size or timestamp (an interrupted complete, a
//     restored database, a bug) gets them back from object storage.
//   - An object under Datahub's prefix that no row points at is an orphan. It is
//     reported, never deleted: deleting a file nobody recorded is not this job's
//     call to make.
//
// Running it twice changes nothing the second time: filling facts is
// idempotent, and reporting orphans has no side effect.
func (m *Module) handleReconcileUploads(ctx context.Context, _ *asynq.Task) error {
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	filled, missing, factsFailed := m.reconcileUploadFacts(ctx)
	orphans, truncated, orphanErr := m.findOrphanObjects(ctx)

	logger.Infof(ctx,
		"[Datahub] reconcile finished: facts_filled=%d objects_missing=%d "+
			"orphans=%d listing_truncated=%v",
		filled, missing, len(orphans), truncated)
	for _, key := range orphans {
		// Reported, not deleted — an operator decides what an unrecorded object
		// means, and a report is what makes that decision possible.
		logger.Warnf(ctx, "[Datahub] orphan object with no Upload record: %s", key)
	}

	// A storage outage must be retried; a specific missing object is a finding,
	// not a failure.
	if factsFailed != nil {
		return factsFailed
	}
	return orphanErr
}

// reconcileUploadFacts fills in the physical facts of merged Uploads that are
// missing them. It returns how many it filled, how many pointed at an object
// that is gone, and the first error worth retrying.
func (m *Module) reconcileUploadFacts(ctx context.Context) (filled, missing int, retryErr error) {
	records, err := m.store.ListUploadsNeedingFacts(ctx, reconcileBatchSize)
	if err != nil {
		return 0, 0, err
	}

	for _, record := range records {
		facts, err := m.objects.StatObject(ctx, record.Bucket, record.ObjectKey)
		switch {
		case errors.Is(err, ErrObjectNotFound):
			missing++
			logger.Warnf(ctx,
				"[Datahub] upload record points at a missing object upload_id=%s object_key=%s",
				record.UploadID, record.ObjectKey)
			continue
		case err != nil:
			// Assume object storage is having a bad day: stop and let asynq
			// retry rather than burning through the batch.
			return filled, missing, fmt.Errorf("stat %s: %w", record.ObjectKey, err)
		}

		if err := m.store.UpdateUploadFacts(ctx, record.TenantID, record.UploadID, facts); err != nil {
			return filled, missing, err
		}
		filled++
	}
	return filled, missing, nil
}

// findOrphanObjects lists Datahub's own prefix and subtracts every key the
// database knows about. Only this module's prefix is listed, so WeKnora's own
// files in the same bucket are never mistaken for orphans.
func (m *Module) findOrphanObjects(ctx context.Context) (orphans []string, truncated bool, err error) {
	bucket := m.settings.ObjectStorage.Bucket

	knownKeys, err := m.store.KnownObjectKeys(ctx, reconcileListLimit)
	if err != nil {
		return nil, false, err
	}
	known := make(map[string]struct{}, len(knownKeys))
	for _, key := range knownKeys {
		known[key] = struct{}{}
	}

	objects, truncated, err := m.objects.ListObjects(ctx, bucket, objectPrefix, reconcileListLimit)
	if err != nil {
		return nil, false, err
	}
	if truncated {
		logger.Warnf(ctx,
			"[Datahub] object listing truncated at %d entries; orphans beyond that are not reported this run",
			reconcileListLimit)
	}

	for _, object := range objects {
		if _, recorded := known[object.Key]; recorded {
			continue
		}
		// A zero-byte placeholder is what an aborted multipart attempt can leave
		// behind; still worth reporting, so it stays in the list.
		orphans = append(orphans, object.Key)
	}
	return orphans, truncated, nil
}

// reconcileTimeout bounds one reconciliation, which walks object storage.
const reconcileTimeout = 10 * time.Minute

// handleRecomputeTestSummaries is the self-healing backstop for the incremental
// Test summary. The incremental update on every batch is exact; this task exists
// for the cases where it could not run to completion — a failed transaction, a
// restored database, an operator editing rows — and repairs them without anyone
// having to notice first.
func (m *Module) handleRecomputeTestSummaries(ctx context.Context, _ *asynq.Task) error {
	if m.testData == nil {
		return errors.New("datahub: test data store is not wired")
	}

	corrected, err := m.testData.RecomputeTestSummaries(ctx, summaryRecomputeBatch)
	if err != nil {
		// Returning the error hands the task back to asynq for a retry.
		return err
	}
	if corrected > 0 {
		logger.Warnf(ctx,
			"[Datahub] recomputed %d Event summaries that disagreed with their board results",
			corrected)
	}
	return nil
}
