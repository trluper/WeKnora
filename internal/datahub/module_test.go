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
	abortErr     error

	storedParts   []CompletedPart
	listErr       error
	completeErr   error
	completeCalls int
	completedWith []CompletedPart

	objectFacts    map[string]ObjectInfo
	statErr        error
	statCalls      int
	listedObjects  []ObjectRef
	listObjectsErr error
	listTruncated  bool
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
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.storedParts, nil
}

func (f *fakeObjectStore) CompleteMultipartUpload(
	_ context.Context, _, _, _ string, parts []CompletedPart,
) (ObjectInfo, error) {
	if f.completeErr != nil {
		return ObjectInfo{}, f.completeErr
	}
	f.completeCalls++
	f.completedWith = parts
	return ObjectInfo{
		ETag:         "merged-etag",
		VersionID:    "version-1",
		ContentType:  "application/vnd.ms-excel",
		Size:         10485760,
		LastModified: time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC),
	}, nil
}

func (f *fakeObjectStore) AbortMultipartUpload(_ context.Context, _, _, uploadID string) error {
	f.aborted = append(f.aborted, uploadID)
	return f.abortErr
}

func (f *fakeObjectStore) StatObject(
	_ context.Context, _, objectKey string,
) (ObjectInfo, error) {
	f.statCalls++
	if f.statErr != nil {
		return ObjectInfo{}, f.statErr
	}
	if facts, ok := f.objectFacts[objectKey]; ok {
		return facts, nil
	}
	return ObjectInfo{}, fmt.Errorf("%w: %s", ErrObjectNotFound, objectKey)
}

func (f *fakeObjectStore) ListObjects(
	_ context.Context, _, _ string, limit int,
) ([]ObjectRef, bool, error) {
	if f.listObjectsErr != nil {
		return nil, false, f.listObjectsErr
	}
	objects := f.listedObjects
	truncated := f.listTruncated
	if limit > 0 && len(objects) > limit {
		objects = objects[:limit]
		truncated = true
	}
	return objects, truncated, nil
}

// fakeUploadStore records what the upload flow tried to persist, and can be
// told to fail so the "database write failed" abort path is testable without a
// database.
type fakeUploadStore struct {
	uploads   []*UploadRecord
	parts     []*UploadParts
	createErr error

	records       map[string]*UploadRecord
	partsRows     map[string]*UploadParts
	registerErr   error
	registerCalls int
	lastBatch     []CompletedPart

	claimErr     error
	finalizeErr  error
	claims       int
	mergeFacts   ObjectInfo
	mergeProduct ProductMetadata
	failReasons  []string

	lastQuery UploadQuery
	listErr   error
	listPage  *UploadPage

	expired        []UploadRecord
	expiredErr     error
	expireCalls    int
	markedExpired  []string
	markExpiredErr error

	needingFacts    []UploadRecord
	needingFactsErr error
	factUpdates     map[string]ObjectInfo
	updateFactErr   error
	knownKeys       []string
	knownKeysErr    error
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
	f.put(upload, parts)
	return nil
}

func (f *fakeUploadStore) Get(
	_ context.Context, tenantID uint64, uploadID string,
) (*UploadRecord, error) {
	if record, ok := f.records[uploadID]; ok && record.TenantID == tenantID {
		return record, nil
	}
	return nil, ErrUploadNotFound
}

func (f *fakeUploadStore) GetParts(
	_ context.Context, tenantID uint64, uploadID string,
) (*UploadParts, error) {
	if row, ok := f.partsRows[uploadID]; ok && row.TenantID == tenantID {
		return row, nil
	}
	return nil, ErrUploadNotFound
}

// RegisterParts mirrors the real store's contract: already-registered parts are
// skipped, never duplicated.
func (f *fakeUploadStore) RegisterParts(
	_ context.Context, tenantID uint64, uploadID string, parts []CompletedPart,
) (*RegisterPartsResult, error) {
	f.registerCalls++
	f.lastBatch = parts
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	record, ok := f.records[uploadID]
	if !ok || record.TenantID != tenantID {
		return nil, ErrUploadNotFound
	}
	row, ok := f.partsRows[uploadID]
	if !ok {
		return nil, ErrUploadNotFound
	}
	if record.Status != StatusUploading {
		return nil, ErrUploadNotUploading
	}

	bitmap := PartBitmapFromBytes(row.PartBitmap)
	entries, err := decodePartMeta(row.PartMeta)
	if err != nil {
		return nil, err
	}
	added := 0
	for _, part := range parts {
		if bitmap.IsSet(part.PartNumber) {
			continue
		}
		bitmap.Set(part.PartNumber)
		entries = append(entries, partMetaEntry{
			PartNumber: part.PartNumber, ETag: part.ETag, Size: part.Size,
		})
		added++
	}
	if added > 0 {
		row.PartBitmap = bitmap.Bytes()
		if row.PartMeta, err = encodePartMeta(entries); err != nil {
			return nil, err
		}
		row.CompletedParts += added
		if row.CompletedParts > record.TotalParts {
			row.CompletedParts = record.TotalParts
		}
		row.Version++
	}
	record.CompletedParts = row.CompletedParts
	return &RegisterPartsResult{
		CompletedParts: row.CompletedParts,
		TotalParts:     record.TotalParts,
	}, nil
}

func (f *fakeUploadStore) ClaimUploadMerge(
	_ context.Context, tenantID uint64, uploadID string,
) error {
	f.claims++
	if f.claimErr != nil {
		return f.claimErr
	}
	record, ok := f.records[uploadID]
	if !ok || record.TenantID != tenantID {
		return ErrUploadNotFound
	}
	if record.Status != StatusUploading {
		return ErrUploadNotUploading
	}
	record.Status = StatusMerging
	return nil
}

func (f *fakeUploadStore) FinalizeUploadMerge(
	_ context.Context, tenantID uint64, uploadID string,
	facts ObjectInfo, product ProductMetadata,
) (*UploadRecord, error) {
	if f.finalizeErr != nil {
		return nil, f.finalizeErr
	}
	record, ok := f.records[uploadID]
	if !ok || record.TenantID != tenantID {
		return nil, ErrUploadNotFound
	}
	if record.Status != StatusMerging {
		return nil, ErrUploadNotUploading
	}
	f.mergeFacts, f.mergeProduct = facts, product
	record.Status = StatusMerged
	record.ETag = facts.ETag
	record.ObjectVersionID = facts.VersionID
	record.Description = product.Description
	record.Category = product.Category
	record.ImportantKey = product.ImportantKey
	record.Version = product.Version
	record.CompletedParts = record.TotalParts
	record.ErrorMessage = ""
	return record, nil
}

func (f *fakeUploadStore) FailUploadMerge(
	_ context.Context, tenantID uint64, uploadID string, reason string,
) (*UploadRecord, error) {
	record, ok := f.records[uploadID]
	if !ok || record.TenantID != tenantID {
		return nil, ErrUploadNotFound
	}
	if record.Status != StatusMerging {
		return nil, ErrUploadNotUploading
	}
	f.failReasons = append(f.failReasons, reason)
	record.Status = StatusFailed
	record.ErrorMessage = reason
	return record, nil
}

// ListUploads records the query so tests can assert how the caller's visibility
// was translated into a filter. The real filtering and paging is the Postgres
// implementation's business, and is covered there.
func (f *fakeUploadStore) ListUploads(
	_ context.Context, query UploadQuery,
) (*UploadPage, error) {
	f.lastQuery = query
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.listPage != nil {
		return f.listPage, nil
	}
	return &UploadPage{}, nil
}

// ListExpiredUploads and MarkUploadExpired mirror the real store: listing is
// cross-tenant and bounded, and marking only touches Uploads still in flight.
func (f *fakeUploadStore) ListExpiredUploads(
	_ context.Context, now time.Time, limit int,
) ([]UploadRecord, error) {
	if f.expiredErr != nil {
		return nil, f.expiredErr
	}
	// Mirror the real store: only in-flight Uploads past their expiry.
	expired := make([]UploadRecord, 0, len(f.expired))
	for _, record := range f.expired {
		if record.ExpireAt.After(now) {
			continue
		}
		switch record.Status {
		case StatusInit, StatusUploading, StatusMerging:
			expired = append(expired, record)
		}
	}
	if len(expired) > limit {
		expired = expired[:limit]
	}
	return expired, nil
}

func (f *fakeUploadStore) MarkUploadExpired(
	_ context.Context, _ uint64, uploadID, _ string,
) error {
	f.expireCalls++
	if f.markExpiredErr != nil {
		return f.markExpiredErr
	}
	for i := range f.expired {
		if f.expired[i].UploadID != uploadID {
			continue
		}
		switch f.expired[i].Status {
		case StatusInit, StatusUploading, StatusMerging:
			f.expired[i].Status = StatusCancelled
			f.markedExpired = append(f.markedExpired, uploadID)
		}
	}
	return nil
}

// ListUploadsNeedingFacts, UpdateUploadFacts and KnownObjectKeys mirror the real
// store's contract. UpdateUploadFacts records only what the real one writes, so
// a test can assert the other columns were left alone.
func (f *fakeUploadStore) ListUploadsNeedingFacts(
	_ context.Context, limit int,
) ([]UploadRecord, error) {
	if f.needingFactsErr != nil {
		return nil, f.needingFactsErr
	}
	records := f.needingFacts
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (f *fakeUploadStore) UpdateUploadFacts(
	_ context.Context, tenantID uint64, uploadID string, facts ObjectInfo,
) error {
	if f.updateFactErr != nil {
		return f.updateFactErr
	}
	if f.factUpdates == nil {
		f.factUpdates = map[string]ObjectInfo{}
	}
	f.factUpdates[uploadID] = facts
	if record, ok := f.records[uploadID]; ok && record.TenantID == tenantID {
		record.ETag = facts.ETag
		record.ObjectVersionID = facts.VersionID
		if facts.Size > 0 {
			record.FileSize = facts.Size
		}
		if !facts.LastModified.IsZero() {
			lastModified := facts.LastModified
			record.LastModified = &lastModified
		}
	}
	return nil
}

func (f *fakeUploadStore) KnownObjectKeys(_ context.Context, limit int) ([]string, error) {
	if f.knownKeysErr != nil {
		return nil, f.knownKeysErr
	}
	keys := f.knownKeys
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}

// put seeds both maps; Create and the test fixtures both use it.
func (f *fakeUploadStore) put(record *UploadRecord, parts *UploadParts) {
	if f.records == nil {
		f.records = map[string]*UploadRecord{}
		f.partsRows = map[string]*UploadParts{}
	}
	f.records[record.UploadID] = record
	f.partsRows[parts.UploadID] = parts
}

// seedUpload installs an Upload the part flow can act on.
func seedUpload(
	store *fakeUploadStore, tenantID uint64, userID string, totalParts int, status UploadStatus,
) *UploadRecord {
	record := &UploadRecord{
		TenantID:       tenantID,
		UploadID:       "upload-1",
		EventID:        "event-1",
		Scenario:       ScenarioExperiment,
		UserID:         userID,
		Filename:       "board-report.xlsx",
		FileSize:       1024,
		Bucket:         "weknora",
		ObjectKey:      "event-1/2026/09/21/upload-1/board-report.xlsx",
		StoragePath:    "event-1/2026/09/21/upload-1/board-report.xlsx",
		ObjectUploadID: "multipart-1",
		Status:         status,
		TotalParts:     totalParts,
		PartSize:       defaultPartSize,
		ExpireAt:       time.Now().UTC().Add(time.Hour),
	}
	parts := &UploadParts{
		TenantID:   tenantID,
		UploadID:   record.UploadID,
		PartBitmap: NewPartBitmap(totalParts).Bytes(),
	}
	store.put(record, parts)
	return record
}

// fakeTestDataStore mirrors the real store's contract: a board re-reported in
// place is an update, and the Event's summary moves by exactly the difference.
type fakeTestDataStore struct {
	details map[string]map[string]int
	summary map[string]TestSummary

	err      error
	calls    int
	lastRows map[string][]TestDetailInput

	summaryPage      *TestSummaryPage
	detailPage       *TestDetailPage
	listErr          error
	lastSummaryQuery TestSummaryQuery
	lastDetailQuery  TestDetailQuery

	recomputeResult int
	recomputeErr    error
	recomputeCalls  int
}

var _ TestDataStore = (*fakeTestDataStore)(nil)

func newFakeTestDataStore() *fakeTestDataStore {
	return &fakeTestDataStore{
		details:  map[string]map[string]int{},
		summary:  map[string]TestSummary{},
		lastRows: map[string][]TestDetailInput{},
	}
}

func (f *fakeTestDataStore) UpsertTestDetails(
	_ context.Context, _ uint64, eventID string, rows []TestDetailInput,
) (*UpsertTestDetailsResult, error) {
	f.calls++
	f.lastRows[eventID] = rows
	if f.err != nil {
		return nil, f.err
	}
	if f.details[eventID] == nil {
		f.details[eventID] = map[string]int{}
	}

	result := &UpsertTestDetailsResult{}
	summary := f.summary[eventID]
	summary.EventID = eventID
	for _, row := range rows {
		previous, existed := f.details[eventID][row.BoardID]
		switch {
		case !existed:
			result.Inserted++
			summary.TotalCount++
			if row.TestResult == TestResultPassed {
				summary.PassedCount++
			} else {
				summary.FailedCount++
			}
		case previous != row.TestResult:
			result.Updated++
			if row.TestResult == TestResultPassed {
				summary.PassedCount++
				summary.FailedCount--
			} else {
				summary.PassedCount--
				summary.FailedCount++
			}
		default:
			result.Updated++
		}
		f.details[eventID][row.BoardID] = row.TestResult
	}
	f.summary[eventID] = summary
	return result, nil
}

// ListTestSummaries and ListTestDetails record the query so tests can assert
// how the caller's visibility became a filter. The real filtering, paging and
// sorting is the Postgres implementation's job and is covered there.
func (f *fakeTestDataStore) ListTestSummaries(
	_ context.Context, query TestSummaryQuery,
) (*TestSummaryPage, error) {
	f.lastSummaryQuery = query
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.summaryPage != nil {
		return f.summaryPage, nil
	}
	return &TestSummaryPage{}, nil
}

func (f *fakeTestDataStore) ListTestDetails(
	_ context.Context, query TestDetailQuery,
) (*TestDetailPage, error) {
	f.lastDetailQuery = query
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.detailPage != nil {
		return f.detailPage, nil
	}
	return &TestDetailPage{}, nil
}

func (f *fakeTestDataStore) RecomputeTestSummaries(
	_ context.Context, _ int,
) (int, error) {
	f.recomputeCalls++
	if f.recomputeErr != nil {
		return 0, f.recomputeErr
	}
	return f.recomputeResult, nil
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
