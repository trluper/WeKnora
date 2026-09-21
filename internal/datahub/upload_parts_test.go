package datahub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const (
	registerPartPath = "/api/v1/datahub/upload/part"
	listPartsPath    = "/api/v1/datahub/upload/upload-1/parts"
)

func postJSON(t *testing.T, engine *gin.Engine, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)
	return rec
}

func getPath(engine *gin.Engine, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	engine.ServeHTTP(rec, req)
	return rec
}

func batchBody(parts ...int) map[string]any {
	list := make([]map[string]any, 0, len(parts))
	for _, part := range parts {
		list = append(list, map[string]any{
			"part_number": part,
			"etag":        fmt.Sprintf("etag-%d", part),
			"size":        5242880,
		})
	}
	return map[string]any{"upload_id": "upload-1", "parts": list}
}

func partResponse(t *testing.T, rec *httptest.ResponseRecorder) registerPartResponse {
	t.Helper()
	var body struct {
		Code int                  `json:"code"`
		Data registerPartResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body.Data
}

func TestRegisterPartsRejectsUnauthenticatedCaller(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), nil)

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestRegisterPartsRejectsInvalidRequests(t *testing.T) {
	cases := map[string]map[string]any{
		"missing upload_id": {"parts": []map[string]any{{"part_number": 1, "etag": "e", "size": 1}}},
		"no parts":          {"upload_id": "upload-1", "parts": []map[string]any{}},
		"part number zero":  {"upload_id": "upload-1", "parts": []map[string]any{{"part_number": 0, "etag": "e", "size": 1}}},
		"missing etag":      {"upload_id": "upload-1", "parts": []map[string]any{{"part_number": 1, "etag": "", "size": 1}}},
		"zero size":         {"upload_id": "upload-1", "parts": []map[string]any{{"part_number": 1, "etag": "e", "size": 0}}},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			store := &fakeUploadStore{}
			seedUpload(store, 42, "user-1", 3, StatusUploading)
			engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

			rec := postJSON(t, engine, registerPartPath, body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
			require.Zero(t, store.registerCalls, "a rejected request must not reach the store")
		})
	}
}

func TestRegisterPartsIsIdempotentAcrossRetries(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	first := postJSON(t, engine, registerPartPath, batchBody(1, 2))
	require.Equal(t, http.StatusOK, first.Code, "body=%s", first.Body.String())
	require.Equal(t, 2, partResponse(t, first).CompletedParts)
	require.False(t, partResponse(t, first).IsComplete)

	// The client retries the same batch after a flaky response.
	retry := postJSON(t, engine, registerPartPath, batchBody(1, 2))
	require.Equal(t, http.StatusOK, retry.Code, "body=%s", retry.Body.String())
	require.Equal(t, 2, partResponse(t, retry).CompletedParts, "no double counting")

	final := postJSON(t, engine, registerPartPath, batchBody(3))
	require.True(t, partResponse(t, final).IsComplete, "the last part completes the upload")
	require.Equal(t, 3, partResponse(t, final).CompletedParts)
}

func TestRegisterPartsAcceptsLegacySinglePartForm(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, registerPartPath, map[string]any{
		"upload_id":   "upload-1",
		"part_number": 1,
		"etag":        "etag-1",
		"size":        5242880,
	})

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, 1, partResponse(t, rec).CompletedParts)
}

func TestRegisterPartsRejectsOutOfRangePart(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, registerPartPath, batchBody(4))

	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "out of range")
	require.Zero(t, store.registerCalls)
}

func TestRegisterPartsRejectsOtherUsersUpload(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 42, UserID: "user-2", Role: types.TenantRoleContributor,
	})

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
	require.Zero(t, store.registerCalls)
}

func TestRegisterPartsAllowsTenantAdminOnAnotherUsersUpload(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 42, UserID: "admin-1", Role: types.TenantRoleAdmin,
	})

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
}

func TestRegisterPartsHidesUploadsOutsideTheTenant(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 99, UserID: "user-9", Role: types.TenantRoleAdmin,
	})

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusNotFound, rec.Code, "body=%s", rec.Body.String())
	require.NotContains(t, rec.Body.String(), "user-1", "existence must not leak across tenants")
}

func TestRegisterPartsRejectsFinishedUpload(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusMerged)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusConflict, rec.Code, "body=%s", rec.Body.String())
}

func TestRegisterPartsConflictIsRetryable(t *testing.T) {
	store := &fakeUploadStore{registerErr: ErrPartsConflict}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusConflict, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "retry")
}

func TestRegisterPartsReportsStoreFailure(t *testing.T) {
	store := &fakeUploadStore{registerErr: errors.New("postgres is down")}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, registerPartPath, batchBody(1))

	require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
}

func TestListPartsReturnsRegisteredParts(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())
	require.Equal(t, http.StatusOK, postJSON(t, engine, registerPartPath, batchBody(1, 3)).Code)

	rec := getPath(engine, listPartsPath)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	var body struct {
		Data listPartsResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "upload-1", body.Data.UploadID)
	require.Equal(t, 2, body.Data.CompletedParts)
	require.Equal(t, 3, body.Data.TotalParts)
	require.Len(t, body.Data.Parts, 2)
	require.Equal(t, 1, body.Data.Parts[0].PartNumber)
	require.Equal(t, "etag-1", body.Data.Parts[0].ETag)
	require.Equal(t, "uploaded", body.Data.Parts[0].Status)
}

func TestListPartsRejectsOtherUsersUpload(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 42, UserID: "user-2", Role: types.TenantRoleViewer,
	})

	rec := getPath(engine, listPartsPath)

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestListPartsHidesUploadsOutsideTheTenant(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 99, UserID: "user-9", Role: types.TenantRoleOwner,
	})

	rec := getPath(engine, listPartsPath)

	require.Equal(t, http.StatusNotFound, rec.Code, "body=%s", rec.Body.String())
}
