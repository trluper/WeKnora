package datahub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

func needsFactsUpload(uploadID, objectKey string) UploadRecord {
	now := time.Now().UTC()
	return UploadRecord{
		TenantID:    42,
		UploadID:    uploadID,
		EventID:     "event-1",
		UserID:      "user-1",
		Filename:    "report.xlsx",
		Description: "第三季度报告",
		Category:    "sensor",
		Version:     "v3",
		Bucket:      "weknora",
		ObjectKey:   objectKey,
		StoragePath: objectKey,
		Status:      StatusMerged,
		TotalParts:  1,
		ExpireAt:    now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func reconcileTask() *asynq.Task {
	return asynq.NewTask(taskTypeReconcileUploads, nil)
}

func TestReconcileFillsMissingObjectFacts(t *testing.T) {
	record := needsFactsUpload("upload-a", objectPrefix+"event-1/2026/09/21/upload-a/report.xlsx")
	store := &fakeUploadStore{
		needingFacts: []UploadRecord{record},
		records:      map[string]*UploadRecord{"upload-a": &record},
	}
	facts := ObjectInfo{
		ETag:         "etag-from-storage",
		VersionID:    "version-7",
		ContentType:  "application/vnd.ms-excel",
		Size:         10485760,
		LastModified: time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC),
	}
	objects := &fakeObjectStore{
		objectFacts: map[string]ObjectInfo{record.ObjectKey: facts},
	}
	module := sweepModule(store, objects)

	err := module.handleReconcileUploads(context.Background(), reconcileTask())

	require.NoError(t, err)
	require.Equal(t, facts, store.factUpdates["upload-a"])
	require.Equal(t, "etag-from-storage", store.records["upload-a"].ETag)
	require.Equal(t, int64(10485760), store.records["upload-a"].FileSize)

	// The reconciler must not become a second writer for product metadata.
	require.Equal(t, "第三季度报告", store.records["upload-a"].Description)
	require.Equal(t, "sensor", store.records["upload-a"].Category)
	require.Equal(t, "v3", store.records["upload-a"].Version)
}

func TestReconcileIsIdempotent(t *testing.T) {
	record := needsFactsUpload("upload-a", objectPrefix+"event-1/2026/09/21/upload-a/report.xlsx")
	store := &fakeUploadStore{needingFacts: []UploadRecord{record}}
	objects := &fakeObjectStore{objectFacts: map[string]ObjectInfo{
		record.ObjectKey: {ETag: "etag-from-storage", Size: 10},
	}}
	module := sweepModule(store, objects)
	task := reconcileTask()

	require.NoError(t, module.handleReconcileUploads(context.Background(), task))
	firstCalls := objects.statCalls
	require.NoError(t, module.handleReconcileUploads(context.Background(), task))

	// The second run re-reads the same candidate list (the fake does not
	// re-filter), but the write it produces is identical, so nothing changes.
	require.Equal(t, 2*firstCalls, objects.statCalls)
	require.Equal(t, 1, len(store.factUpdates))
}

func TestReconcileReportsMissingObjectsWithoutFailing(t *testing.T) {
	record := needsFactsUpload("upload-gone", objectPrefix+"event-1/2026/09/21/upload-gone/report.xlsx")
	store := &fakeUploadStore{needingFacts: []UploadRecord{record}}
	// No objectFacts entry: StatObject reports ErrObjectNotFound.
	objects := &fakeObjectStore{}
	module := sweepModule(store, objects)

	err := module.handleReconcileUploads(context.Background(), reconcileTask())

	require.NoError(t, err, "a missing object is a finding, not a task failure")
	require.Empty(t, store.factUpdates)
}

func TestReconcileRetriesWhenObjectStorageIsUnreachable(t *testing.T) {
	record := needsFactsUpload("upload-a", objectPrefix+"event-1/2026/09/21/upload-a/report.xlsx")
	store := &fakeUploadStore{needingFacts: []UploadRecord{record}}
	objects := &fakeObjectStore{statErr: errors.New("connection refused")}
	module := sweepModule(store, objects)

	err := module.handleReconcileUploads(context.Background(), reconcileTask())

	require.Error(t, err, "object storage being down must be retried by asynq")
	require.Empty(t, store.factUpdates)
}

func TestReconcileReportsOrphanObjects(t *testing.T) {
	known := objectPrefix + "event-1/2026/09/21/upload-a/report.xlsx"
	orphan := objectPrefix + "event-1/2026/09/21/upload-b/left-behind.xlsx"
	outsidePrefix := "tenant-7/knowledge/2026/doc.pdf"

	store := &fakeUploadStore{knownKeys: []string{known}}
	objects := &fakeObjectStore{listedObjects: []ObjectRef{
		{Key: known, Size: 10},
		{Key: orphan, Size: 20},
		// The listing is scoped to Datahub's prefix in production; the fake
		// returns exactly what the real listing would.
	}}
	module := sweepModule(store, objects)

	orphans, truncated, err := module.findOrphanObjects(context.Background())

	require.NoError(t, err)
	require.False(t, truncated)
	require.Equal(t, []string{orphan}, orphans)
	require.NotContains(t, orphans, outsidePrefix,
		"objects outside Datahub's prefix are not this module's business")
}

func TestReconcilePropagatesStoreFailures(t *testing.T) {
	store := &fakeUploadStore{needingFactsErr: errors.New("postgres is down")}
	module := sweepModule(store, &fakeObjectStore{})

	err := module.handleReconcileUploads(context.Background(), reconcileTask())

	require.Error(t, err)
}
