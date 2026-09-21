package datahub

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
