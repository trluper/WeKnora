package datahub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// End-to-end run against real infrastructure. It is the only test in this
// package that needs all three dependencies at once:
//
//	WEKNORA_DATAHUB_E2E_POSTGRES_DSN   postgres://…      (migrations run here)
//	WEKNORA_DATAHUB_E2E_REDIS_ADDR     host:port         (queue + worker)
//	WEKNORA_DATAHUB_E2E_MINIO_ENDPOINT host:port         (multipart uploads)
//	WEKNORA_DATAHUB_E2E_MINIO_ACCESS_KEY / _SECRET_KEY / _BUCKET
//
// Without them it skips, like the rest of the suite. `docs/adr/*` and the
// ticket trail record why each moving part exists; this file is the proof that
// they add up to a working whole.
func TestDatahubEndToEnd(t *testing.T) {
	dsn := os.Getenv("WEKNORA_DATAHUB_E2E_POSTGRES_DSN")
	redisAddr := os.Getenv("WEKNORA_DATAHUB_E2E_REDIS_ADDR")
	endpoint := os.Getenv("WEKNORA_DATAHUB_E2E_MINIO_ENDPOINT")
	accessKey := os.Getenv("WEKNORA_DATAHUB_E2E_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("WEKNORA_DATAHUB_E2E_MINIO_SECRET_KEY")
	bucket := os.Getenv("WEKNORA_DATAHUB_E2E_MINIO_BUCKET")
	if dsn == "" || redisAddr == "" || endpoint == "" || bucket == "" {
		t.Skip("set the WEKNORA_DATAHUB_E2E_* variables to run the end-to-end test")
	}

	// Datahub enables itself from the deployment's own storage configuration,
	// so the test configures the deployment rather than reaching inside.
	t.Setenv("STORAGE_TYPE", "minio")
	t.Setenv("DB_DRIVER", "postgres")
	t.Setenv("MINIO_ENDPOINT", endpoint)
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKey)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("MINIO_BUCKET_NAME", bucket)

	e2eMigrate(t, dsn)
	ensureBucket(t, endpoint, accessKey, secretKey, bucket)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = redisClient.Close() })
	require.NoError(t, redisClient.Ping(context.Background()).Err())

	// This is the production constructor: real Postgres stores, real S3 client,
	// real queue and worker.
	module, err := New(db, redisClient, repository.NewTaskDeadLetterRepository(db))
	require.NoError(t, err)
	require.NotNil(t, module)
	t.Cleanup(func() { require.NoError(t, module.Close()) })

	const (
		tenantA = 9001
		tenantB = 9002
		userA   = "e2e-user-a"
		userB   = "e2e-user-b"
	)
	engineA := e2eEngine(module, tenantA, userA, types.TenantRoleContributor)
	engineB := e2eEngine(module, tenantA, userB, types.TenantRoleContributor)
	engineOtherTenant := e2eEngine(module, tenantB, "e2e-user-c", types.TenantRoleOwner)

	// --- upload chain -----------------------------------------------------
	payload := bytes.Repeat([]byte("board-report "), 1024)
	uploadID, objectKey := e2eUpload(t, engineA, "event-e2e-1", payload)

	versions := e2eGet(t, engineA, "/api/v1/datahub/upload/versions")
	require.Contains(t, versions, uploadID, "a completed upload shows up as a version")
	require.Contains(t, versions, "e2e-report.xlsx")

	metadata := e2ePost(t, engineA, "/api/v1/datahub/upload/metadata",
		map[string]any{"count": 10, "file_name": "e2e-report"})
	require.Contains(t, metadata, uploadID)
	require.Contains(t, metadata, objectKey, "the metadata listing carries the object key")
	require.True(t, strings.HasPrefix(objectKey, objectPrefix),
		"objects live under Datahub's own prefix, got %q", objectKey)
	require.Contains(t, metadata, "第三季度板件测试报告", "product metadata round-trips")

	// --- visibility -------------------------------------------------------
	require.NotContains(t, e2eGet(t, engineB, "/api/v1/datahub/upload/versions"), uploadID,
		"another member of the same tenant must not see this upload")
	require.NotContains(t, e2eGet(t, engineOtherTenant, "/api/v1/datahub/upload/versions"), uploadID,
		"another tenant must not see this upload")

	parts := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/datahub/upload/"+uploadID+"/parts", nil)
	engineB.ServeHTTP(parts, req)
	require.Equal(t, http.StatusForbidden, parts.Code, "body=%s", parts.Body.String())

	crossTenant := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/datahub/upload/"+uploadID+"/parts", nil)
	engineOtherTenant.ServeHTTP(crossTenant, req)
	require.Equal(t, http.StatusNotFound, crossTenant.Code,
		"a cross-tenant lookup must not even reveal that the upload exists")

	// --- test data chain --------------------------------------------------
	rows := make([]map[string]any, 0, 3)
	for i := 1; i <= 2; i++ {
		rows = append(rows, map[string]any{
			"event_id": "event-e2e-2", "board_id": fmt.Sprintf("board-%d", i),
			"test_result": TestResultPassed,
		})
	}
	rows = append(rows, map[string]any{
		"event_id": "event-e2e-2", "board_id": "board-3",
		"test_result": TestResultFailed, "failed_reason": "焊点虚接",
	})
	e2ePost(t, engineA, "/api/v1/datahub/test-data/batch-upload",
		map[string]any{"detail_datas": rows, "count": len(rows)})

	overview := e2eGet(t, engineA, "/api/v1/datahub/test-data/overview-data?event_id=event-e2e-2")
	require.Contains(t, overview, `"total_count":3`)
	require.Contains(t, overview, `"passed_count":2`)
	require.Contains(t, overview, `"failed_count":1`)

	ownDetails := e2eGet(t, engineA, "/api/v1/datahub/test-data/detail-data?event_id=event-e2e-2&count=50")
	require.Contains(t, ownDetails, `"total":3`)
	require.Contains(t, ownDetails, "焊点虚接")

	require.Contains(t,
		e2eGet(t, engineB, "/api/v1/datahub/test-data/detail-data?event_id=event-e2e-2&count=50"),
		`"total":0`, "a non-participant sees no board results")
	require.Contains(t,
		e2eGet(t, engineB, "/api/v1/datahub/test-data/overview-data?event_id=event-e2e-2"),
		`"total":0`, "and no Event summary either")

	// --- background tasks, all three, for real ---------------------------
	e2eExpiredUploadIsSwept(t, module, db)
	e2eMissingFactsAreRestored(t, module, db, tenantA, uploadID, objectKey)
	e2eDriftedSummaryIsRecomputed(t, module, db, tenantA)
}

// e2eUpload drives init -> part PUT -> register -> complete and returns the
// upload id and its object key.
func e2eUpload(
	t *testing.T, engine *gin.Engine, eventID string, payload []byte,
) (uploadID, objectKey string) {
	t.Helper()

	initBody := map[string]any{
		"filename":     "e2e-report.xlsx",
		"file_size":    len(payload),
		"content_type": "application/vnd.ms-excel",
		"event_id":     eventID,
		"total_parts":  1,
	}
	rec := e2ePostRecorder(t, engine, "/api/v1/datahub/upload/init", initBody)
	require.Equal(t, http.StatusOK, rec.Code, "init failed: %s", rec.Body.String())

	var initResp struct {
		Data struct {
			UploadID      string         `json:"upload_id"`
			ObjectKey     string         `json:"object_key"`
			PresignedURLs map[int]string `json:"presigned_urls"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
	require.Len(t, initResp.Data.PresignedURLs, 1)
	uploadID, objectKey = initResp.Data.UploadID, initResp.Data.ObjectKey

	// The client PUTs the part straight to object storage, exactly as the real
	// client does — no bytes pass through the application.
	putReq, err := http.NewRequest(
		http.MethodPut, initResp.Data.PresignedURLs[1], bytes.NewReader(payload),
	)
	require.NoError(t, err)
	putResp, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	defer func() { _ = putResp.Body.Close() }()
	_, _ = io.Copy(io.Discard, putResp.Body)
	require.Equal(t, http.StatusOK, putResp.StatusCode, "presigned part upload failed")

	etag := putResp.Header.Get("ETag")
	require.NotEmpty(t, etag)

	register := e2ePostRecorder(t, engine, "/api/v1/datahub/upload/part", map[string]any{
		"upload_id": uploadID,
		"parts": []map[string]any{{
			"part_number": 1, "etag": etag, "size": len(payload),
		}},
	})
	require.Equal(t, http.StatusOK, register.Code, "register failed: %s", register.Body.String())
	require.Contains(t, register.Body.String(), `"is_complete":true`)

	complete := e2ePostRecorder(t, engine, "/api/v1/datahub/upload/complete", map[string]any{
		"upload_id":     uploadID,
		"description":   "第三季度板件测试报告",
		"category":      "sensor",
		"important_key": "板件,筛分",
		"version":       "v3",
	})
	require.Equal(t, http.StatusOK, complete.Code, "complete failed: %s", complete.Body.String())
	require.Contains(t, complete.Body.String(), `"status":"merged"`)
	return uploadID, objectKey
}

// e2eExpiredUploadIsSwept proves the expiry task abandons an upload whose time
// ran out, against the real queue and worker.
func e2eExpiredUploadIsSwept(t *testing.T, module *Module, db *gorm.DB) {
	t.Helper()

	stale := &UploadRecord{
		TenantID:       9001,
		UploadID:       "e2e-expired-upload",
		EventID:        "event-e2e-1",
		Scenario:       ScenarioExperiment,
		UserID:         "e2e-user-a",
		Filename:       "abandoned.xlsx",
		FileSize:       10,
		Bucket:         module.settings.ObjectStorage.Bucket,
		ObjectKey:      objectPrefix + "event-e2e-1/2026/09/21/e2e-expired-upload/abandoned.xlsx",
		ObjectUploadID: "never-opened",
		Status:         StatusUploading,
		TotalParts:     1,
		PartSize:       defaultPartSize,
		ExpireAt:       time.Now().UTC().Add(-time.Hour),
	}
	parts := &UploadParts{
		TenantID:   stale.TenantID,
		UploadID:   stale.UploadID,
		PartBitmap: NewPartBitmap(1).Bytes(),
	}
	require.NoError(t, module.store.Create(context.Background(), stale, parts))

	module.enqueue(module.worker, taskTypeExpireUploads, expiryMaxRetry)

	require.Eventually(t, func() bool {
		record, err := module.store.Get(context.Background(), stale.TenantID, stale.UploadID)
		return err == nil && record.Status == StatusCancelled
	}, 30*time.Second, 200*time.Millisecond,
		"the expiry sweep must abandon an upload past its expiry")

	var reason string
	require.NoError(t, db.Raw(
		`SELECT error_msg FROM datahub_uploads WHERE tenant_id = ? AND upload_id = ?`,
		stale.TenantID, stale.UploadID,
	).Scan(&reason).Error)
	require.Contains(t, reason, "expired")
}

// e2eMissingFactsAreRestored corrupts a completed Upload's facts and proves the
// reconciler restores them from object storage.
func e2eMissingFactsAreRestored(
	t *testing.T, module *Module, db *gorm.DB,
	tenantID uint64, uploadID, objectKey string,
) {
	t.Helper()

	require.NoError(t, db.Exec(`
		UPDATE datahub_uploads
		   SET etag = '', file_size = 0, last_modified = NULL
		 WHERE tenant_id = ? AND upload_id = ?`, tenantID, uploadID,
	).Error)

	module.enqueue(module.worker, taskTypeReconcileUploads, expiryMaxRetry)

	require.Eventually(t, func() bool {
		record, err := module.store.Get(context.Background(), tenantID, uploadID)
		return err == nil && record.ETag != "" && record.FileSize > 0 && record.LastModified != nil
	}, 30*time.Second, 200*time.Millisecond,
		"the reconciler must restore facts from object storage")

	var stored struct {
		ETag string `gorm:"column:etag"`
		Size int64  `gorm:"column:file_size"`
	}
	require.NoError(t, db.Raw(
		`SELECT etag, file_size FROM datahub_uploads WHERE tenant_id = ? AND upload_id = ?`,
		tenantID, uploadID,
	).Scan(&stored).Error)
	require.NotEmpty(t, stored.ETag)
	require.Greater(t, stored.Size, int64(0))
	require.NotEmpty(t, objectKey)
}

// e2eDriftedSummaryIsRecomputed corrupts an Event's counters and proves the
// backstop puts them back.
func e2eDriftedSummaryIsRecomputed(t *testing.T, module *Module, db *gorm.DB, tenantID uint64) {
	t.Helper()

	require.NoError(t, db.Exec(`
		UPDATE datahub_test_summaries
		   SET total_count = 99, passed_count = 42, failed_count = 7
		 WHERE tenant_id = ? AND event_id = ?`, tenantID, "event-e2e-2",
	).Error)

	module.enqueue(module.worker, taskTypeRecomputeSummaries, expiryMaxRetry)

	require.Eventually(t, func() bool {
		var summary struct {
			Total  int64 `gorm:"column:total_count"`
			Passed int64 `gorm:"column:passed_count"`
			Failed int64 `gorm:"column:failed_count"`
		}
		if err := db.Raw(`
			SELECT total_count, passed_count, failed_count
			  FROM datahub_test_summaries
			 WHERE tenant_id = ? AND event_id = ?`, tenantID, "event-e2e-2",
		).Scan(&summary).Error; err != nil {
			return false
		}
		return summary.Total == 3 && summary.Passed == 2 && summary.Failed == 1
	}, 30*time.Second, 200*time.Millisecond,
		"the recompute backstop must restore a drifted summary")
}

// --- helpers --------------------------------------------------------------

func e2eMigrate(t *testing.T, dsn string) {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Clean(filepath.Join(dir, "..", ".."))

	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() { _ = os.Chdir(previous) })

	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	if err := m.Up(); err != nil && !errorsIsNoChange(err) {
		require.NoError(t, err)
	}
}

func errorsIsNoChange(err error) bool {
	return err != nil && err.Error() == "no change"
}

func ensureBucket(t *testing.T, endpoint, accessKey, secretKey, bucket string) {
	t.Helper()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	exists, err := client.BucketExists(context.Background(), bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}))
	}
}

func e2eEngine(module *Module, tenantID uint64, userID string, role types.TenantRole) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	group := engine.Group("/api/v1")
	group.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		ctx = context.WithValue(ctx, types.TenantIDContextKey, tenantID)
		ctx = context.WithValue(ctx, types.UserIDContextKey, userID)
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	RegisterRoutes(group, module)
	return engine
}

func e2ePostRecorder(
	t *testing.T, engine *gin.Engine, path string, body any,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)
	return rec
}

func e2ePost(t *testing.T, engine *gin.Engine, path string, body any) string {
	t.Helper()
	rec := e2ePostRecorder(t, engine, path, body)
	require.Equal(t, http.StatusOK, rec.Code, "%s: %s", path, rec.Body.String())
	return rec.Body.String()
}

func e2eGet(t *testing.T, engine *gin.Engine, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	engine.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "%s: %s", path, rec.Body.String())
	return rec.Body.String()
}
