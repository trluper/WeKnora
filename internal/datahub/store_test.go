package datahub

import (
	"context"
	"errors"
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

	// Our migration must roll back and forward cleanly.
	require.NoError(t, m.Steps(-1), "down migration")
	var tables int
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('datahub_uploads', 'datahub_upload_parts')`,
	).Scan(&tables).Error)
	require.Zero(t, tables, "the down migration removes both tables")

	require.NoError(t, m.Steps(1), "up migration again")
	_, err = store.Get(ctx, 7, "upload-round-trip")
	require.ErrorIs(t, err, ErrUploadNotFound, "the table came back empty")
}

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
