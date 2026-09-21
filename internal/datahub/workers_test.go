package datahub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func expiredUpload(uploadID string, status UploadStatus) UploadRecord {
	return UploadRecord{
		TenantID:       42,
		UploadID:       uploadID,
		EventID:        "event-1",
		UserID:         "user-1",
		Bucket:         "weknora",
		ObjectKey:      "event-1/2026/09/21/" + uploadID + "/report.xlsx",
		ObjectUploadID: "multipart-" + uploadID,
		Status:         status,
		TotalParts:     3,
		ExpireAt:       time.Now().UTC().Add(-time.Hour),
	}
}

func sweepModule(store UploadStore, objects ObjectStore) *Module {
	return newModule(enabledSettings(), Parts{
		Objects: objects,
		Store:   store,
	})
}

func TestExpirySweepAbandonsExpiredUploads(t *testing.T) {
	store := &fakeUploadStore{expired: []UploadRecord{
		expiredUpload("upload-a", StatusUploading),
		expiredUpload("upload-b", StatusInit),
	}}
	objects := &fakeObjectStore{}
	module := sweepModule(store, objects)

	err := module.handleExpireUploads(context.Background(), asynq.NewTask(taskTypeExpireUploads, nil))

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"multipart-upload-a", "multipart-upload-b"}, objects.aborted,
		"the multipart sessions must be discarded, not left in object storage")
	require.ElementsMatch(t, []string{"upload-a", "upload-b"}, store.markedExpired)
	require.Equal(t, StatusCancelled, store.expired[0].Status)
	require.Equal(t, StatusCancelled, store.expired[1].Status)
}

func TestExpirySweepLeavesCompletedUploadsAlone(t *testing.T) {
	// The store only ever returns in-flight Uploads, so a merged one never
	// reaches the sweep. This pins that contract from the caller's side too.
	store := &fakeUploadStore{}
	objects := &fakeObjectStore{}
	module := sweepModule(store, objects)

	err := module.handleExpireUploads(context.Background(), asynq.NewTask(taskTypeExpireUploads, nil))

	require.NoError(t, err)
	require.Empty(t, objects.aborted)
	require.Empty(t, store.markedExpired)
}

func TestExpirySweepIsIdempotent(t *testing.T) {
	store := &fakeUploadStore{expired: []UploadRecord{
		expiredUpload("upload-a", StatusUploading),
	}}
	objects := &fakeObjectStore{}
	module := sweepModule(store, objects)
	task := asynq.NewTask(taskTypeExpireUploads, nil)

	require.NoError(t, module.handleExpireUploads(context.Background(), task))
	require.NoError(t, module.handleExpireUploads(context.Background(), task))

	// The first run cancelled it; the second must not cancel it again, so the
	// store records exactly one transition.
	require.Equal(t, []string{"upload-a"}, store.markedExpired)
}

func TestExpirySweepSurvivesObjectStorageFailure(t *testing.T) {
	store := &fakeUploadStore{expired: []UploadRecord{
		expiredUpload("upload-a", StatusUploading),
	}}
	// A session that cannot be aborted must not stop the record from being
	// abandoned — the reconciler sweeps whatever object storage left behind.
	objects := &fakeObjectStore{abortErr: errors.New("multipart session not found")}
	module := sweepModule(store, objects)

	err := module.handleExpireUploads(context.Background(), asynq.NewTask(taskTypeExpireUploads, nil))

	require.NoError(t, err)
	require.Equal(t, []string{"upload-a"}, store.markedExpired)
}

func TestExpirySweepReturnsStoreFailureForRetry(t *testing.T) {
	store := &fakeUploadStore{expiredErr: errors.New("postgres is down")}
	module := sweepModule(store, &fakeObjectStore{})

	err := module.handleExpireUploads(context.Background(), asynq.NewTask(taskTypeExpireUploads, nil))

	require.Error(t, err, "a failing sweep must be retried by asynq, not swallowed")
}

// deadLetterRecorder stands in for WeKnora's task_dead_letters table.
type deadLetterRecorder struct {
	records []*types.TaskDeadLetter
}

func (r *deadLetterRecorder) Insert(_ context.Context, dl *types.TaskDeadLetter) error {
	r.records = append(r.records, dl)
	return nil
}

func (r *deadLetterRecorder) ListByScope(
	_ context.Context, _, _, _ string, _ int,
) ([]*types.TaskDeadLetter, string, error) {
	return nil, "", nil
}

func (r *deadLetterRecorder) ListByTaskType(
	_ context.Context, _ string, _ string, _ int,
) ([]*types.TaskDeadLetter, string, error) {
	return nil, "", nil
}

func (r *deadLetterRecorder) DeleteByID(_ context.Context, _ int64) error { return nil }

// workerModule wires a Module onto a throwaway Redis so the real asynq worker
// runs, which is what proves the queue, the handler registration and the
// dead-letter middleware are actually connected.
func workerModule(t *testing.T, store UploadStore) *Module {
	t.Helper()
	client := newThrowawayRedis(t)

	module := newModule(enabledSettings(), Parts{
		Redis:   client,
		Objects: &fakeObjectStore{},
		Store:   store,
	})
	require.NoError(t, module.startBackgroundWorkers())
	t.Cleanup(func() { require.NoError(t, module.Close()) })
	return module
}

// newThrowawayRedis starts an in-process Redis. Environments that forbid
// binding a local socket (some sandboxes) skip rather than fail: the worker
// wiring is covered by the end-to-end run.
func newThrowawayRedis(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.NewMiniRedis()
	if err := server.Start(); err != nil {
		t.Skipf("cannot bind a local socket here, skipping worker wiring test: %v", err)
	}
	t.Cleanup(server.Close)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestBackgroundWorkerConsumesItsOwnQueue(t *testing.T) {
	store := &fakeUploadStore{expired: []UploadRecord{
		expiredUpload("upload-a", StatusUploading),
	}}
	module := workerModule(t, store)

	_, err := module.worker.client.Enqueue(
		asynq.NewTask(taskTypeExpireUploads, nil), asynq.Queue(queueDatahub),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(store.markedExpired) == 1
	}, 10*time.Second, 50*time.Millisecond,
		"the worker must consume the sweep from Datahub's own queue")
}

func TestTaskExhaustingItsRetriesLandsInTheDeadLetterTable(t *testing.T) {
	recorder := &deadLetterRecorder{}
	client := newThrowawayRedis(t)

	store := &fakeUploadStore{expiredErr: errors.New("postgres is down")}
	module := newModule(enabledSettings(), Parts{
		Redis:       client,
		Objects:     &fakeObjectStore{},
		Store:       store,
		DeadLetters: recorder,
	})
	require.NoError(t, module.startBackgroundWorkers())
	t.Cleanup(func() { require.NoError(t, module.Close()) })

	_, err := module.worker.client.Enqueue(
		asynq.NewTask(taskTypeExpireUploads, nil),
		asynq.Queue(queueDatahub),
		// No retries: the first failure is the final attempt, which is when the
		// shared middleware records the dead letter.
		asynq.MaxRetry(0),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(recorder.records) == 1
	}, 10*time.Second, 50*time.Millisecond,
		"a permanently failing task must land in the shared dead-letter table")
	require.Contains(t, recorder.records[0].TaskType, "datahub")
}
