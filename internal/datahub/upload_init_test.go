package datahub

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const initPath = "/api/v1/datahub/upload/init"

type initBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		UploadID      string         `json:"upload_id"`
		MinIOUploadID string         `json:"minio_upload_id"`
		Bucket        string         `json:"bucket"`
		ObjectKey     string         `json:"object_key"`
		PartSize      int64          `json:"part_size"`
		TotalParts    int            `json:"total_parts"`
		PresignedURLs map[int]string `json:"presigned_urls"`
	} `json:"data"`
}

func validInitBody() map[string]any {
	return map[string]any{
		"filename":     "board-report.xlsx",
		"file_size":    10485760,
		"content_type": "application/vnd.ms-excel",
		"event_id":     "event-2026-001",
		"total_parts":  3,
	}
}

func postInit(t *testing.T, engine *gin.Engine, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, initPath, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)
	return rec
}

func newInitTestModule(objects ObjectStore, store UploadStore) *Module {
	return newModule(enabledSettings(), Parts{Objects: objects, Store: store})
}

func caller() *types.Caller {
	return &types.Caller{TenantID: 42, UserID: "user-1", Role: types.TenantRoleContributor}
}

func TestInitUploadRejectsUnauthenticatedCaller(t *testing.T) {
	module := newInitTestModule(&fakeObjectStore{}, &fakeUploadStore{})
	engine := newTestEngine(module, nil)

	rec := postInit(t, engine, validInitBody())

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestInitUploadRejectsInvalidRequests(t *testing.T) {
	cases := map[string]struct {
		mutate map[string]any
		want   string
	}{
		"missing filename": {map[string]any{"filename": ""}, "filename is required"},
		"blank filename":   {map[string]any{"filename": "   "}, "filename is required"},
		"zero size":        {map[string]any{"file_size": 0}, "file_size must be at least 1 byte"},
		"negative size":    {map[string]any{"file_size": -1}, "file_size must be at least 1 byte"},
		"oversized file":   {map[string]any{"file_size": maxFileSize + 1}, "file_size exceeds"},
		"missing event":    {map[string]any{"event_id": ""}, "event_id is required"},
		"missing type":     {map[string]any{"content_type": ""}, "content_type is required"},
		"zero parts":       {map[string]any{"total_parts": 0}, "total_parts must be at least 1"},
		"too many parts":   {map[string]any{"total_parts": maxParts + 1}, "total_parts exceeds"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			objects := &fakeObjectStore{}
			store := &fakeUploadStore{}
			engine := newTestEngine(newInitTestModule(objects, store), caller())

			body := validInitBody()
			for key, value := range tc.mutate {
				body[key] = value
			}

			rec := postInit(t, engine, body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.want)
			// A rejected request must not touch storage or the database.
			require.False(t, objects.created, "no multipart session should be opened")
			require.Empty(t, store.uploads, "nothing should be persisted")
		})
	}
}

func TestInitUploadOpensSessionAndRecordsUpload(t *testing.T) {
	objects := &fakeObjectStore{}
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postInit(t, engine, validInitBody())

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	var body initBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Zero(t, body.Code)
	require.NotEmpty(t, body.Data.UploadID)
	require.Equal(t, "fake-multipart-id", body.Data.MinIOUploadID)
	require.Equal(t, "weknora", body.Data.Bucket)
	require.Equal(t, 3, body.Data.TotalParts)
	require.Equal(t, defaultPartSize, body.Data.PartSize)
	require.Len(t, body.Data.PresignedURLs, 3, "every part gets a URL in one response")
	require.Equal(t, 3, objects.presignCalls)

	require.Len(t, store.uploads, 1)
	record := store.uploads[0]
	require.Equal(t, StatusUploading, record.Status)
	require.Equal(t, uint64(42), record.TenantID, "tenant comes from the caller")
	require.Equal(t, "user-1", record.UserID, "user comes from the caller")
	require.Equal(t, ScenarioExperiment, record.Scenario, "scenario defaults to experiment")
	require.Equal(t, "event-2026-001", record.EventID)
	require.NotContains(t, record.ObjectKey, "..", "object keys stay inside their prefix")

	require.Len(t, store.parts, 1)
	require.GreaterOrEqual(t, len(store.parts[0].PartBitmap)*8, 3,
		"bitmap is sized to cover every part")
}

func TestInitUploadIgnoresClientSuppliedUser(t *testing.T) {
	store := &fakeUploadStore{}
	engine := newTestEngine(
		newInitTestModule(&fakeObjectStore{}, store),
		caller(),
	)
	body := validInitBody()
	body["user_id"] = "somebody-else"

	rec := postInit(t, engine, body)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.uploads[0].UserID)
}

func TestInitUploadAbortsSessionWhenStoreFails(t *testing.T) {
	objects := &fakeObjectStore{}
	store := &fakeUploadStore{createErr: errors.New("postgres is down")}
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postInit(t, engine, validInitBody())

	require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, []string{"fake-multipart-id"}, objects.aborted,
		"the multipart session must not be left behind")
}

func TestInitUploadFailsWhenObjectStorageRefuses(t *testing.T) {
	objects := &fakeObjectStore{createErr: errors.New("connection refused")}
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postInit(t, engine, validInitBody())

	require.Equal(t, http.StatusBadGateway, rec.Code, "body=%s", rec.Body.String())
	require.Empty(t, store.uploads, "nothing should be persisted")
}

func TestInitUploadAbortsSessionWhenSigningFails(t *testing.T) {
	objects := &fakeObjectStore{presignErr: errors.New("signing failed")}
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postInit(t, engine, validInitBody())

	require.Equal(t, http.StatusBadGateway, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, []string{"fake-multipart-id"}, objects.aborted)
	require.Empty(t, store.uploads)
}

func TestInitUploadRejectsMalformedBody(t *testing.T) {
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, &fakeUploadStore{}), caller())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, initPath, bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "invalid request body")
}

func TestObjectKeySanitizesTraversal(t *testing.T) {
	key := objectKeyFor("event-1", "upload-1", "../../etc/passwd")

	require.Contains(t, key, "event-1/")
	require.Contains(t, key, "upload-1/")
	require.NotContains(t, key, "..")
	require.True(t, len(key) > 0)
}
