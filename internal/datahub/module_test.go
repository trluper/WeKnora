package datahub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// fakeObjectStore stands in for MinIO/S3 so the upload flow can be exercised
// without object storage. Its failure hooks drive the abort paths.
type fakeObjectStore struct {
	created      bool
	createErr    error
	presignErr   error
	presignCalls int
	aborted      []string
}

var _ ObjectStore = (*fakeObjectStore)(nil)

func (f *fakeObjectStore) CreateMultipartUpload(
	_ context.Context, _, _ string,
) (string, error) {
	if f.createErr != nil {
		return "", f.createErr
	}
	f.created = true
	return "fake-multipart-id", nil
}

func (f *fakeObjectStore) PresignUploadPart(
	_ context.Context, _, _, _ string, partNumber int, _ time.Duration,
) (string, error) {
	if f.presignErr != nil {
		return "", f.presignErr
	}
	f.presignCalls++
	return fmt.Sprintf("https://object-storage.invalid/part/%d", partNumber), nil
}

func (f *fakeObjectStore) CompletedParts(
	_ context.Context, _, _, _ string,
) ([]CompletedPart, error) {
	return nil, nil
}

func (f *fakeObjectStore) CompleteMultipartUpload(
	_ context.Context, _, _, _ string, _ []CompletedPart,
) (ObjectInfo, error) {
	return ObjectInfo{ETag: "fake-etag"}, nil
}

func (f *fakeObjectStore) AbortMultipartUpload(_ context.Context, _, _, uploadID string) error {
	f.aborted = append(f.aborted, uploadID)
	return nil
}

// fakeUploadStore records what the upload flow tried to persist, and can be
// told to fail so the "database write failed" abort path is testable without a
// database.
type fakeUploadStore struct {
	uploads   []*UploadRecord
	parts     []*UploadParts
	createErr error
}

var _ UploadStore = (*fakeUploadStore)(nil)

func (f *fakeUploadStore) Create(
	_ context.Context, upload *UploadRecord, parts *UploadParts,
) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.uploads = append(f.uploads, upload)
	f.parts = append(f.parts, parts)
	return nil
}

func (f *fakeUploadStore) Get(
	_ context.Context, _ uint64, _ string,
) (*UploadRecord, error) {
	return nil, ErrUploadNotFound
}

func envFrom(pairs map[string]string) func(string) string {
	return func(key string) string { return pairs[key] }
}

func TestLoadSettingsStaysDisabledWithoutS3CompatibleStorage(t *testing.T) {
	// The shipped defaults — including lite mode — must not be affected by
	// this module existing.
	for _, storageType := range []string{"", "local", "cos", "tos", "obs", "oss"} {
		name := storageType
		if name == "" {
			name = "<unset>"
		}
		t.Run(name, func(t *testing.T) {
			settings, err := LoadSettings(envFrom(map[string]string{
				"DB_DRIVER":    "sqlite",
				"STORAGE_TYPE": storageType,
			}))

			require.NoError(t, err, "an unrelated storage backend must not fail startup")
			require.False(t, settings.Enabled)
		})
	}
}

func TestLoadSettingsEnablesMinIO(t *testing.T) {
	settings, err := LoadSettings(envFrom(map[string]string{
		"DB_DRIVER":               "postgres",
		"STORAGE_TYPE":            "minio",
		"MINIO_ENDPOINT":          "minio:9000",
		"MINIO_ACCESS_KEY_ID":     "access-key",
		"MINIO_SECRET_ACCESS_KEY": "secret-key",
		"MINIO_BUCKET_NAME":       "weknora",
		"MINIO_USE_SSL":           "true",
	}))

	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.Equal(t, providerMinIO, settings.ObjectStorage.Provider)
	require.Equal(t, "minio:9000", settings.ObjectStorage.Endpoint)
	require.Equal(t, "weknora", settings.ObjectStorage.Bucket)
	require.True(t, settings.ObjectStorage.UseSSL)
}

func TestLoadSettingsEnablesS3(t *testing.T) {
	settings, err := LoadSettings(envFrom(map[string]string{
		"DB_DRIVER":         "postgres",
		"STORAGE_TYPE":      "s3",
		"S3_ENDPOINT":       "s3.example.com",
		"S3_REGION":         "ap-east-1",
		"S3_ACCESS_KEY":     "access-key",
		"S3_SECRET_KEY":     "secret-key",
		"S3_BUCKET_NAME":    "weknora",
		"S3_PATH_PREFIX":    "ignored-by-datahub/",
		"UNRELATED_SETTING": "x",
	}))

	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.Equal(t, providerS3, settings.ObjectStorage.Provider)
	require.Equal(t, "ap-east-1", settings.ObjectStorage.Region)
}

func TestLoadSettingsRejectsEnabledModuleOnNonPostgres(t *testing.T) {
	// Choosing S3-compatible storage means Datahub is meant to be on, and it
	// only ships Postgres migrations.
	settings, err := LoadSettings(envFrom(map[string]string{
		"DB_DRIVER":               "sqlite",
		"STORAGE_TYPE":            "minio",
		"MINIO_ENDPOINT":          "minio:9000",
		"MINIO_ACCESS_KEY_ID":     "access-key",
		"MINIO_SECRET_ACCESS_KEY": "secret-key",
		"MINIO_BUCKET_NAME":       "weknora",
	}))

	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres")
	require.Contains(t, err.Error(), "sqlite")
	require.False(t, settings.Enabled)
}

func TestLoadSettingsRejectsIncompleteObjectStorage(t *testing.T) {
	_, err := LoadSettings(envFrom(map[string]string{
		"DB_DRIVER":               "postgres",
		"STORAGE_TYPE":            "minio",
		"MINIO_ENDPOINT":          "minio:9000",
		"MINIO_ACCESS_KEY_ID":     "access-key",
		"MINIO_SECRET_ACCESS_KEY": "secret-key",
	}))

	require.Error(t, err)
	require.Contains(t, err.Error(), "MINIO_BUCKET_NAME")
}

func enabledSettings() Settings {
	return Settings{
		Enabled:     true,
		DBDriver:    driverPostgres,
		StorageType: providerMinIO,
		ObjectStorage: ObjectStorage{
			Provider: providerMinIO,
			Endpoint: "minio:9000",
			Bucket:   "weknora",
		},
	}
}

// newTestEngine mounts the Datahub routes the way a deployment does: under the
// authenticated /api/v1 group, with the caller injected the way the auth
// middleware injects it.
func newTestEngine(module *Module, caller *types.Caller) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	group := engine.Group("/api/v1")
	if caller != nil {
		group.Use(func(c *gin.Context) {
			ctx := c.Request.Context()
			ctx = context.WithValue(ctx, types.TenantIDContextKey, caller.TenantID)
			ctx = context.WithValue(ctx, types.UserIDContextKey, caller.UserID)
			ctx = context.WithValue(ctx, types.TenantRoleContextKey, caller.Role)
			c.Request = c.Request.WithContext(ctx)
			c.Next()
		})
	}
	RegisterRoutes(group, module)
	return engine
}

func get(t *testing.T, engine *gin.Engine, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	engine.ServeHTTP(rec, req)
	return rec
}

func TestHealthRejectsUnauthenticatedCaller(t *testing.T) {
	engine := newTestEngine(newModule(enabledSettings(), Parts{Objects: &fakeObjectStore{}}), nil)

	rec := get(t, engine, "/api/v1/datahub/health")

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestHealthReportsDependencyState(t *testing.T) {
	module := newModule(enabledSettings(), Parts{Objects: &fakeObjectStore{}})
	engine := newTestEngine(module, &types.Caller{TenantID: 7, UserID: "user-1"})

	rec := get(t, engine, "/api/v1/datahub/health")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	var body struct {
		Module       string `json:"module"`
		Status       string `json:"status"`
		Enabled      bool   `json:"enabled"`
		Dependencies struct {
			Database struct {
				Status string `json:"status"`
			} `json:"database"`
			Redis struct {
				Status string `json:"status"`
			} `json:"redis"`
			ObjectStorage struct {
				Status   string `json:"status"`
				Wired    bool   `json:"wired"`
				Provider string `json:"provider"`
				Bucket   string `json:"bucket"`
			} `json:"object_storage"`
		} `json:"dependencies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	require.Equal(t, "datahub", body.Module)
	require.True(t, body.Enabled)
	require.Equal(t, providerMinIO, body.Dependencies.ObjectStorage.Provider)
	require.Equal(t, "weknora", body.Dependencies.ObjectStorage.Bucket)
	require.True(t, body.Dependencies.ObjectStorage.Wired)
	require.Equal(t, "ok", body.Dependencies.ObjectStorage.Status)
	// No database or Redis was injected, so the module reports itself degraded
	// rather than pretending it can serve traffic.
	require.Equal(t, "not_wired", body.Dependencies.Database.Status)
	require.Equal(t, "disabled", body.Dependencies.Redis.Status)
	require.Equal(t, "degraded", body.Status)
}

func TestHealthReportsUnwiredObjectStore(t *testing.T) {
	module := newModule(enabledSettings(), Parts{})
	engine := newTestEngine(module, &types.Caller{TenantID: 7, UserID: "user-1"})

	rec := get(t, engine, "/api/v1/datahub/health")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), `"status":"not_wired"`)
	require.Contains(t, rec.Body.String(), `"wired":false`)
}

func TestRegisterRoutesSkipsDisabledModule(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	disabled := newModule(Settings{StorageType: defaultStorageType}, Parts{})

	RegisterRoutes(engine.Group("/api/v1"), disabled)
	RegisterRoutes(engine.Group("/api/v1"), nil)

	require.Empty(t, engine.Routes(), "a disabled module mounts nothing")
}
