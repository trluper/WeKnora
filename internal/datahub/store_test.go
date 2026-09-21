package datahub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Datahub only ships Postgres DDL, so the parts of it that SQLite cannot
// exercise — the migration itself and the real store's SQL — run against a
// disposable Postgres. Without one, they skip rather than silently pass.
//
//	go run ./cmd/server   # or any throwaway instance, for example:
//	docker run -d --rm -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=weknora \
//	  -p 55433:5432 paradedb/paradedb:v0.22.6-pg17
//	WEKNORA_DATAHUB_TEST_POSTGRES_DSN=postgres://postgres:pg@localhost:55433/weknora?sslmode=disable \
//	  go test ./internal/datahub/...
func testPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("WEKNORA_DATAHUB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_DATAHUB_TEST_POSTGRES_DSN to a disposable Postgres to run this test")
	}
	return dsn
}

// migrateToRepoRoot points golang-migrate's file source at the repo's
// migrations, which it resolves relative to the working directory.
func migrateToRepoRoot(t *testing.T) {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Clean(filepath.Join(dir, "..", ".."))
	require.FileExists(t, filepath.Join(root, "go.mod"), "expected the repo root two levels up")

	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() { _ = os.Chdir(previous) })
}

func openTestStore(t *testing.T, dsn string) (*gorm.DB, UploadStore) {
	t.Helper()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db, NewPostgresUploadStore(db)
}

// migrateUp applies every pending migration, tolerating a database that a
// previous test in the same package already migrated.
func migrateUp(t *testing.T, m *migrate.Migrate) {
	t.Helper()
	err := m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		require.NoError(t, err)
	}
}

func sampleUpload(tenantID uint64, uploadID string) (*UploadRecord, *UploadParts) {
	return &UploadRecord{
			TenantID:       tenantID,
			UploadID:       uploadID,
			EventID:        "event-1",
			Scenario:       ScenarioExperiment,
			UserID:         "user-1",
			Filename:       "board-report.xlsx",
			FileSize:       1024,
			ContentType:    "application/vnd.ms-excel",
			Bucket:         "weknora",
			ObjectKey:      "event-1/2026/09/20/" + uploadID + "/board-report.xlsx",
			ObjectUploadID: "multipart-1",
			Status:         StatusUploading,
			TotalParts:     3,
			PartSize:       defaultPartSize,
			Metadata:       map[string]string{"line": "A"},
			ExpireAt:       time.Now().UTC().Add(time.Hour),
		}, &UploadParts{
			TenantID:   tenantID,
			UploadID:   uploadID,
			PartBitmap: NewPartBitmap(3).Bytes(),
		}
}

func TestDatahubMigrationAndStoreRoundTrip(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, store := openTestStore(t, dsn)
	ctx := context.Background()

	upload, parts := sampleUpload(7, "upload-round-trip")
	require.NoError(t, store.Create(ctx, upload, parts))

	// The reserved columns exist but carry nothing: the upload flow only ever
	// leaves the empty value behind, never content.
	var reserved struct {
		SummaryMarkdown string
		Headline        string
		AnalysisState   string
		KeywordCount    int
	}
	require.NoError(t, db.Raw(
		`SELECT summary_markdown, headline, analysis_state,
		        COALESCE(jsonb_array_length(keywords), 0) AS keyword_count
		 FROM datahub_uploads WHERE tenant_id = ? AND upload_id = ?`,
		7, "upload-round-trip",
	).Scan(&reserved).Error)
	require.Empty(t, reserved.SummaryMarkdown, "reserved columns must start empty")
	require.Empty(t, reserved.Headline)
	require.Empty(t, reserved.AnalysisState)
	require.Zero(t, reserved.KeywordCount)

	got, err := store.Get(ctx, 7, "upload-round-trip")
	require.NoError(t, err)
	require.Equal(t, "event-1", got.EventID)
	require.Equal(t, StatusUploading, got.Status)
	require.Equal(t, int64(1024), got.FileSize)
	require.Equal(t, "A", got.Metadata["line"], "jsonb metadata survives the round trip")
	require.Equal(t, ScenarioExperiment, got.Scenario)

	// Another tenant must not see it, and must not be able to tell the
	// difference between "not yours" and "does not exist".
	_, err = store.Get(ctx, 8, "upload-round-trip")
	require.ErrorIs(t, err, ErrUploadNotFound)

	// One Upload is one row: the unique key is (tenant_id, upload_id).
	duplicate, duplicateParts := sampleUpload(7, "upload-round-trip")
	err = store.Create(ctx, duplicate, duplicateParts)
	require.Error(t, err)

	// Datahub's migrations must roll back and forward cleanly. Roll back to the
	// version below the first of them rather than "one step", so this keeps
	// working as the module gains migrations.
	latest, dirty, err := m.Version()
	require.NoError(t, err)
	require.False(t, dirty)
	require.NoError(t, m.Migrate(datahubFirstMigration-1), "down migrations")
	var tables int
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('datahub_uploads', 'datahub_upload_parts',
		                      'datahub_test_details', 'datahub_test_summaries')`,
	).Scan(&tables).Error)
	require.Zero(t, tables, "the down migrations remove every Datahub table")

	require.NoError(t, m.Migrate(latest), "up migrations again")
	_, err = store.Get(ctx, 7, "upload-round-trip")
	require.ErrorIs(t, err, ErrUploadNotFound, "the table came back empty")
}

// datahubFirstMigration is the version of Datahub's first migration; rolling
// back to one below it leaves the module's tables dropped.
const datahubFirstMigration = 108

func TestDatahubStoreCreateIsAtomic(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, store := openTestStore(t, dsn)
	ctx := context.Background()

	first, firstParts := sampleUpload(9, "upload-atomic-1")
	require.NoError(t, store.Create(ctx, first, firstParts))

	// A second upload whose parts row collides with the first on the composite
	// primary key: the parts insert fails, so the upload row written moments
	// earlier in the same transaction must not survive.
	second, secondParts := sampleUpload(9, "upload-atomic-2")
	secondParts.UploadID = firstParts.UploadID

	err = store.Create(ctx, second, secondParts)
	require.Error(t, err)

	_, err = store.Get(ctx, 9, "upload-atomic-2")
	require.True(t, errors.Is(err, ErrUploadNotFound),
		"the upload row must be rolled back with its parts row")

	var count int64
	require.NoError(t, db.Model(&UploadParts{}).
		Where("tenant_id = ? AND upload_id = ?", 9, "upload-atomic-1").
		Count(&count).Error)
	require.Equal(t, int64(1), count, "the first, committed parts row is untouched")
}

func TestDatahubRegisterPartsAgainstPostgres(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, store := openTestStore(t, dsn)
	ctx := context.Background()

	upload, parts := sampleUpload(11, "upload-parts")
	require.NoError(t, store.Create(ctx, upload, parts))

	batch := []CompletedPart{
		{PartNumber: 1, ETag: "etag-1", Size: 5242880},
		{PartNumber: 2, ETag: "etag-2", Size: 5242880},
	}
	result, err := store.RegisterParts(ctx, 11, "upload-parts", batch)
	require.NoError(t, err)
	require.Equal(t, 2, result.CompletedParts)
	require.Equal(t, 3, result.TotalParts)
	require.False(t, result.CompletedParts >= result.TotalParts)

	// Replaying the batch must not double count.
	replayed, err := store.RegisterParts(ctx, 11, "upload-parts", batch)
	require.NoError(t, err)
	require.Equal(t, 2, replayed.CompletedParts)

	// The Upload row mirrors the progress so listings do not need a join.
	stored, err := store.Get(ctx, 11, "upload-parts")
	require.NoError(t, err)
	require.Equal(t, 2, stored.CompletedParts)

	// Part metadata survives the round trip, which is what resume relies on.
	row, err := store.GetParts(ctx, 11, "upload-parts")
	require.NoError(t, err)
	entries, err := decodePartMeta(row.PartMeta)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "etag-1", entries[0].ETag)
	require.True(t, PartBitmapFromBytes(row.PartBitmap).IsSet(1))
	require.False(t, PartBitmapFromBytes(row.PartBitmap).IsSet(3))

	final, err := store.RegisterParts(ctx, 11, "upload-parts",
		[]CompletedPart{{PartNumber: 3, ETag: "etag-3", Size: 1024}})
	require.NoError(t, err)
	require.True(t, final.CompletedParts >= final.TotalParts, "the last part completes the upload")

	// Once the upload leaves the uploading state, parts are refused.
	require.NoError(t, db.Exec(
		`UPDATE datahub_uploads SET status = ? WHERE tenant_id = ? AND upload_id = ?`,
		StatusMerged, 11, "upload-parts",
	).Error)
	_, err = store.RegisterParts(ctx, 11, "upload-parts",
		[]CompletedPart{{PartNumber: 1, ETag: "etag-1b", Size: 1}})
	require.ErrorIs(t, err, ErrUploadNotUploading)
}

func TestDatahubRegisterPartsSerialisesConcurrentBatches(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	_, store := openTestStore(t, dsn)
	ctx := context.Background()

	upload, parts := sampleUpload(12, "upload-concurrent")
	upload.TotalParts = 4
	parts.PartBitmap = NewPartBitmap(4).Bytes()
	require.NoError(t, store.Create(ctx, upload, parts))

	// Two clients delivering different halves at the same time: the row lock
	// must serialise them so neither batch is lost.
	batches := [][]CompletedPart{
		{{PartNumber: 1, ETag: "e1", Size: 1}, {PartNumber: 2, ETag: "e2", Size: 1}},
		{{PartNumber: 3, ETag: "e3", Size: 1}, {PartNumber: 4, ETag: "e4", Size: 1}},
	}
	errs := make([]error, len(batches))
	var wg sync.WaitGroup
	for i, batch := range batches {
		wg.Add(1)
		go func(index int, batch []CompletedPart) {
			defer wg.Done()
			_, errs[index] = store.RegisterParts(ctx, 12, "upload-concurrent", batch)
		}(i, batch)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "batch %d", i)
	}
	stored, err := store.Get(ctx, 12, "upload-concurrent")
	require.NoError(t, err)
	require.Equal(t, 4, stored.CompletedParts, "no batch was lost to a concurrent write")

	row, err := store.GetParts(ctx, 12, "upload-concurrent")
	require.NoError(t, err)
	entries, err := decodePartMeta(row.PartMeta)
	require.NoError(t, err)
	require.Len(t, entries, 4)
}

func TestDatahubCompleteUploadAgainstPostgres(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, store := openTestStore(t, dsn)
	ctx := context.Background()

	upload, parts := sampleUpload(13, "upload-complete")
	require.NoError(t, store.Create(ctx, upload, parts))

	require.NoError(t, store.ClaimUploadMerge(ctx, 13, "upload-complete"))
	// Only one caller may hold the merge.
	require.ErrorIs(t, store.ClaimUploadMerge(ctx, 13, "upload-complete"), ErrUploadNotUploading)

	facts := ObjectInfo{
		ETag:         "etag-final",
		VersionID:    "version-9",
		ContentType:  "application/vnd.ms-excel",
		Size:         10485760,
		LastModified: time.Now().UTC(),
	}
	product := ProductMetadata{
		Description:  "第三季度板件测试报告",
		Category:     "sensor",
		ImportantKey: "板件,筛分",
		Version:      "v3",
	}
	merged, err := store.FinalizeUploadMerge(ctx, 13, "upload-complete", facts, product)
	require.NoError(t, err)
	require.Equal(t, StatusMerged, merged.Status)
	require.Equal(t, "etag-final", merged.ETag)
	require.Equal(t, "version-9", merged.ObjectVersionID)
	require.Equal(t, int64(10485760), merged.FileSize, "the real object size replaces the declared one")
	require.Equal(t, merged.TotalParts, merged.CompletedParts)
	require.NotNil(t, merged.CompletedAt)
	require.Equal(t, product.Description, merged.Description)
	require.Equal(t, product.Version, merged.Version)
	require.Empty(t, merged.ErrorMessage)

	// Merging twice is a state error, not a silent rewrite.
	_, err = store.FinalizeUploadMerge(ctx, 13, "upload-complete", facts, product)
	require.ErrorIs(t, err, ErrUploadNotUploading)

	// The reserved columns must still be untouched by the upload flow.
	var reserved struct {
		SummaryMarkdown string
		Headline        string
		AnalysisState   string
		KeywordCount    int
	}
	require.NoError(t, db.Raw(
		`SELECT summary_markdown, headline, analysis_state,
		        COALESCE(jsonb_array_length(keywords), 0) AS keyword_count
		 FROM datahub_uploads WHERE tenant_id = ? AND upload_id = ?`,
		13, "upload-complete",
	).Scan(&reserved).Error)
	require.Empty(t, reserved.SummaryMarkdown)
	require.Empty(t, reserved.Headline)
	require.Empty(t, reserved.AnalysisState)
	require.Zero(t, reserved.KeywordCount)

	// A failed merge is recorded with its reason and is terminal.
	failedUpload, failedParts := sampleUpload(13, "upload-fail")
	require.NoError(t, store.Create(ctx, failedUpload, failedParts))
	require.NoError(t, store.ClaimUploadMerge(ctx, 13, "upload-fail"))

	failed, err := store.FailUploadMerge(ctx, 13, "upload-fail", "part etag mismatch")
	require.NoError(t, err)
	require.Equal(t, StatusFailed, failed.Status)
	require.Equal(t, "part etag mismatch", failed.ErrorMessage)
	_, err = store.FailUploadMerge(ctx, 13, "upload-fail", "again")
	require.ErrorIs(t, err, ErrUploadNotUploading)
}

func TestDatahubListUploadsAgainstPostgres(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	_, store := openTestStore(t, dsn)
	ctx := context.Background()

	// Three finished uploads across two owners, plus one still in flight.
	seed := []struct {
		uploadID string
		userID   string
		filename string
		size     int64
		status   UploadStatus
	}{
		{"list-1", "user-a", "alpha.xlsx", 300, StatusMerged},
		{"list-2", "user-a", "beta.xlsx", 100, StatusMerged},
		{"list-3", "user-b", "gamma.xlsx", 200, StatusMerged},
		{"list-4", "user-a", "in-flight.xlsx", 400, StatusUploading},
	}
	for _, item := range seed {
		record, parts := sampleUpload(20, item.uploadID)
		record.UserID = item.userID
		record.Filename = item.filename
		record.FileSize = item.size
		record.Status = item.status
		require.NoError(t, store.Create(ctx, record, parts))
	}

	// A plain member sees only their own finished uploads.
	own, err := store.ListUploads(ctx, UploadQuery{
		TenantID:    20,
		OwnerUserID: "user-a",
		Statuses:    []UploadStatus{StatusMerged},
		Count:       50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), own.Total)
	require.Len(t, own.Records, 2)

	// An admin sees the whole tenant.
	all, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, Statuses: []UploadStatus{StatusMerged}, Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), all.Total)

	// Without the status filter the in-flight upload shows up too.
	everything, err := store.ListUploads(ctx, UploadQuery{TenantID: 20, Count: 50})
	require.NoError(t, err)
	require.Equal(t, int64(4), everything.Total)

	// Filename search is a case-insensitive substring match.
	search, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, FileName: "ALPHA", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), search.Total)
	require.Equal(t, "alpha.xlsx", search.Records[0].Filename)

	// A wildcard typed by the client is a literal, not a match-everything.
	wildcard, err := store.ListUploads(ctx, UploadQuery{TenantID: 20, FileName: "%", Count: 50})
	require.NoError(t, err)
	require.Zero(t, wildcard.Total)

	// Sorting is whitelisted; unknown fields fall back rather than erroring.
	sorted, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, Statuses: []UploadStatus{StatusMerged}, SortBy: "-file_size", Count: 50,
	})
	require.NoError(t, err)
	require.Len(t, sorted.Records, 3)
	require.Equal(t, int64(300), sorted.Records[0].FileSize)
	require.Equal(t, int64(100), sorted.Records[2].FileSize)

	fallback, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, SortBy: "-; DROP TABLE datahub_uploads", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), fallback.Total)

	// Paging keeps the total independent of the page size.
	firstPage, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, Statuses: []UploadStatus{StatusMerged}, SortBy: "-file_size",
		Offset: 0, Count: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), firstPage.Total)
	require.Len(t, firstPage.Records, 1)
	secondPage, err := store.ListUploads(ctx, UploadQuery{
		TenantID: 20, Statuses: []UploadStatus{StatusMerged}, SortBy: "-file_size",
		Offset: 1, Count: 1,
	})
	require.NoError(t, err)
	require.Len(t, secondPage.Records, 1)
	require.NotEqual(t, firstPage.Records[0].UploadID, secondPage.Records[0].UploadID)

	// Another tenant sees nothing of the above.
	otherTenant, err := store.ListUploads(ctx, UploadQuery{TenantID: 21, Count: 50})
	require.NoError(t, err)
	require.Zero(t, otherTenant.Total)
}

func TestDatahubUpsertTestDetailsAgainstPostgres(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, _ := openTestStore(t, dsn)
	store := NewPostgresTestDataStore(db)
	ctx := context.Background()

	readSummary := func(tenantID uint64, eventID string) TestSummary {
		t.Helper()
		var summary TestSummary
		require.NoError(t, db.Raw(
			`SELECT * FROM datahub_test_summaries WHERE tenant_id = ? AND event_id = ?`,
			tenantID, eventID,
		).Scan(&summary).Error)
		return summary
	}
	countDetails := func(tenantID uint64, eventID string) int64 {
		t.Helper()
		var count int64
		require.NoError(t, db.Raw(
			`SELECT count(*) FROM datahub_test_details WHERE tenant_id = ? AND event_id = ?`,
			tenantID, eventID,
		).Scan(&count).Error)
		return count
	}

	batch := []TestDetailInput{
		{EventID: "event-td", BoardID: "board-1", TestResult: TestResultPassed, UserID: "user-a"},
		{EventID: "event-td", BoardID: "board-2", TestResult: TestResultPassed, UserID: "user-a"},
		{EventID: "event-td", BoardID: "board-3", TestResult: TestResultFailed,
			FailedReason: "焊点虚接", UserID: "user-b"},
	}
	first, err := store.UpsertTestDetails(ctx, 30, "event-td", batch)
	require.NoError(t, err)
	require.Equal(t, int64(3), first.Inserted)
	require.Zero(t, first.Updated)

	summary := readSummary(30, "event-td")
	require.Equal(t, int64(3), summary.TotalCount)
	require.Equal(t, int64(2), summary.PassedCount)
	require.Equal(t, int64(1), summary.FailedCount)

	// Replaying the batch writes no second row and moves nothing.
	replay, err := store.UpsertTestDetails(ctx, 30, "event-td", batch)
	require.NoError(t, err)
	require.Zero(t, replay.Inserted)
	require.Equal(t, int64(3), replay.Updated)
	require.Equal(t, int64(3), countDetails(30, "event-td"))
	summary = readSummary(30, "event-td")
	require.Equal(t, int64(3), summary.TotalCount)
	require.Equal(t, int64(2), summary.PassedCount)
	require.Equal(t, int64(1), summary.FailedCount)

	// A corrected verdict flips one count each way; the total does not move.
	flip, err := store.UpsertTestDetails(ctx, 30, "event-td", []TestDetailInput{
		{EventID: "event-td", BoardID: "board-2", TestResult: TestResultFailed,
			FailedReason: "复测不合格", UserID: "user-a"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), flip.Updated)
	summary = readSummary(30, "event-td")
	require.Equal(t, int64(3), summary.TotalCount)
	require.Equal(t, int64(1), summary.PassedCount)
	require.Equal(t, int64(2), summary.FailedCount)

	// Re-stating the same verdict is a no-op, not drift.
	_, err = store.UpsertTestDetails(ctx, 30, "event-td", []TestDetailInput{
		{EventID: "event-td", BoardID: "board-2", TestResult: TestResultFailed,
			FailedReason: "复测不合格", UserID: "user-a"},
	})
	require.NoError(t, err)
	summary = readSummary(30, "event-td")
	require.Equal(t, int64(3), summary.TotalCount)
	require.Equal(t, int64(1), summary.PassedCount)
	require.Equal(t, int64(2), summary.FailedCount)

	// Two clients writing different boards of the same Event at once: the
	// Event-level lock must keep both batches and neither may lose an update.
	batches := [][]TestDetailInput{
		makeTestDetails("event-race", "board-1", 10, TestResultPassed, "user-a"),
		makeTestDetails("event-race", "board-2", 10, TestResultFailed, "user-b"),
	}
	errs := make([]error, len(batches))
	var wg sync.WaitGroup
	for i, rows := range batches {
		wg.Add(1)
		go func(index int, rows []TestDetailInput) {
			defer wg.Done()
			_, errs[index] = store.UpsertTestDetails(ctx, 30, "event-race", rows)
		}(i, rows)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "batch %d", i)
	}
	raceSummary := readSummary(30, "event-race")
	require.Equal(t, int64(20), raceSummary.TotalCount)
	require.Equal(t, int64(10), raceSummary.PassedCount)
	require.Equal(t, int64(10), raceSummary.FailedCount)

	// The same Event id in another tenant is a different Event entirely.
	_, err = store.UpsertTestDetails(ctx, 31, "event-td", []TestDetailInput{
		{EventID: "event-td", BoardID: "board-9", TestResult: TestResultPassed, UserID: "user-z"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), readSummary(31, "event-td").TotalCount)
	require.Equal(t, int64(3), readSummary(30, "event-td").TotalCount,
		"another tenant's write must not touch this tenant's summary")
}

func makeTestDetails(
	eventID, boardPrefix string, count int, result int, userID string,
) []TestDetailInput {
	rows := make([]TestDetailInput, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, TestDetailInput{
			EventID:    eventID,
			BoardID:    fmt.Sprintf("%s-%d", boardPrefix, i),
			TestResult: result,
			UserID:     userID,
			FailedReason: func() string {
				if result == TestResultFailed {
					return "不合格"
				}
				return ""
			}(),
		})
	}
	return rows
}

func TestDatahubTestDataQueriesAgainstPostgres(t *testing.T) {
	dsn := testPostgresDSN(t)
	migrateToRepoRoot(t)

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	migrateUp(t, m)

	db, _ := openTestStore(t, dsn)
	store := NewPostgresTestDataStore(db)
	ctx := context.Background()

	// Two Events in tenant 40: one shared by two testers, one belonging to a
	// single tester, plus an Event in another tenant with the same id.
	_, err = store.UpsertTestDetails(ctx, 40, "event-shared", append(
		makeTestDetails("event-shared", "a-board", 10, TestResultPassed, "user-a"),
		makeTestDetails("event-shared", "b-board", 10, TestResultFailed, "user-a")...,
	))
	require.NoError(t, err)
	_, err = store.UpsertTestDetails(ctx, 40, "event-shared",
		makeTestDetails("event-shared", "c-board", 10, TestResultPassed, "user-b"))
	require.NoError(t, err)
	_, err = store.UpsertTestDetails(ctx, 40, "event-solo",
		makeTestDetails("event-solo", "d-board", 5, TestResultPassed, "user-c"))
	require.NoError(t, err)
	_, err = store.UpsertTestDetails(ctx, 41, "event-shared",
		makeTestDetails("event-shared", "x-board", 3, TestResultPassed, "user-z"))
	require.NoError(t, err)

	// Summary: participants see the Events they contributed to.
	participant, err := store.ListTestSummaries(ctx, TestSummaryQuery{
		TenantID: 40, ParticipantUserID: "user-b", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), participant.Total)
	require.Equal(t, "event-shared", participant.Records[0].EventID)
	require.Equal(t, int64(30), participant.Records[0].TotalCount)
	require.Equal(t, int64(20), participant.Records[0].PassedCount)
	require.Equal(t, int64(10), participant.Records[0].FailedCount)

	// A member who recorded nothing for either Event sees nothing.
	outsider, err := store.ListTestSummaries(ctx, TestSummaryQuery{
		TenantID: 40, ParticipantUserID: "user-outsider", Count: 50,
	})
	require.NoError(t, err)
	require.Zero(t, outsider.Total)

	// An admin sees every Event in the tenant, and only in that tenant.
	admin, err := store.ListTestSummaries(ctx, TestSummaryQuery{TenantID: 40, Count: 50})
	require.NoError(t, err)
	require.Equal(t, int64(2), admin.Total)

	tenant41, err := store.ListTestSummaries(ctx, TestSummaryQuery{TenantID: 41, Count: 50})
	require.NoError(t, err)
	require.Equal(t, int64(1), tenant41.Total)
	require.Equal(t, int64(3), tenant41.Records[0].TotalCount)

	// Sorting and paging are whitelisted and stable.
	largest, err := store.ListTestSummaries(ctx, TestSummaryQuery{
		TenantID: 40, SortBy: "-total_count", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, "event-shared", largest.Records[0].EventID)
	secondPage, err := store.ListTestSummaries(ctx, TestSummaryQuery{
		TenantID: 40, SortBy: "-total_count", Offset: 1, Count: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), secondPage.Total)
	require.Len(t, secondPage.Records, 1)
	require.Equal(t, "event-solo", secondPage.Records[0].EventID)

	// Detail listings: everybody sees their own boards, an admin sees them all.
	own, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-shared", OwnerUserID: "user-a", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(20), own.Total)

	everyBoard, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-shared", Count: 50,
	})
	require.NoError(t, err)
	require.Equal(t, int64(30), everyBoard.Total)

	stranger, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-shared", OwnerUserID: "user-outsider", Count: 50,
	})
	require.NoError(t, err)
	require.Zero(t, stranger.Total)

	// Paging keeps the total independent of the page, and ordering is applied.
	firstPage, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-shared", SortBy: "board_id", Count: 10, Offset: 0,
	})
	require.NoError(t, err)
	require.Equal(t, int64(30), firstPage.Total)
	require.Len(t, firstPage.Records, 10)
	secondDetailPage, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-shared", SortBy: "board_id", Count: 10, Offset: 10,
	})
	require.NoError(t, err)
	require.Len(t, secondDetailPage.Records, 10)
	require.NotEqual(t, firstPage.Records[0].BoardID, secondDetailPage.Records[0].BoardID)
	for i := 1; i < len(firstPage.Records); i++ {
		require.LessOrEqual(t, firstPage.Records[i-1].BoardID, firstPage.Records[i].BoardID)
	}

	// Volume: an Event of the size this system actually produces still pages
	// correctly, and the summary is maintained alongside it.
	_, err = store.UpsertTestDetails(ctx, 40, "event-big",
		makeTestDetails("event-big", "big-board", 2000, TestResultPassed, "user-a"))
	require.NoError(t, err)
	bigPage, err := store.ListTestDetails(ctx, TestDetailQuery{
		TenantID: 40, EventID: "event-big", Count: 100, Offset: 1990,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2000), bigPage.Total)
	require.Len(t, bigPage.Records, 10, "the last page holds only what is left")
	bigSummary, err := store.ListTestSummaries(ctx, TestSummaryQuery{
		TenantID: 40, EventID: "event-big", Count: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2000), bigSummary.Records[0].TotalCount)
	require.Equal(t, int64(2000), bigSummary.Records[0].PassedCount)
}
