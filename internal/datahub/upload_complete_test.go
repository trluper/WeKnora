package datahub

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

const completePath = "/api/v1/datahub/upload/complete"

func completeBody() map[string]any {
	return map[string]any{
		"upload_id":     "upload-1",
		"description":   "第三季度板件测试报告",
		"category":      "sensor",
		"important_key": "板件,筛分",
		"version":       "v3",
		"parts": []map[string]any{
			{"part_number": 1, "etag": "etag-1"},
			{"part_number": 2, "etag": "etag-2"},
			{"part_number": 3, "etag": "etag-3"},
		},
	}
}

func uploadedParts() []CompletedPart {
	return []CompletedPart{
		{PartNumber: 1, ETag: "etag-1", Size: 5242880},
		{PartNumber: 2, ETag: "etag-2", Size: 5242880},
		{PartNumber: 3, ETag: "etag-3", Size: 1024},
	}
}

func completeResponse(t *testing.T, rec *httptest.ResponseRecorder) completeUploadResponse {
	t.Helper()
	var body struct {
		Data completeUploadResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body.Data
}

func TestCompleteUploadRejectsUnauthenticatedCaller(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{storedParts: uploadedParts()}, store), nil)

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestCompleteUploadRejectsInvalidRequests(t *testing.T) {
	cases := map[string]struct {
		mutate func(body map[string]any)
		want   string
	}{
		"missing upload_id": {
			func(body map[string]any) { body["upload_id"] = "" },
			"upload_id is required",
		},
		"missing description": {
			func(body map[string]any) { body["description"] = "" },
			"description is required",
		},
		"description too long": {
			func(body map[string]any) { body["description"] = strings.Repeat("x", maxDescriptionLength+1) },
			"description must be at most",
		},
		"category too long": {
			func(body map[string]any) { body["category"] = strings.Repeat("c", maxCategoryLength+1) },
			"category must be at most",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			objects := &fakeObjectStore{storedParts: uploadedParts()}
			store := &fakeUploadStore{}
			seedUpload(store, 42, "user-1", 3, StatusUploading)
			engine := newTestEngine(newInitTestModule(objects, store), caller())

			body := completeBody()
			tc.mutate(body)

			rec := postJSON(t, engine, completePath, body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.want)
			require.Zero(t, objects.completeCalls, "a rejected request must not merge anything")
			require.Zero(t, store.claims)
		})
	}
}

func TestCompleteUploadMergesAndRecordsFacts(t *testing.T) {
	objects := &fakeObjectStore{storedParts: uploadedParts()}
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	body := completeResponse(t, rec)
	require.Equal(t, "upload-1", body.UploadID)
	require.Equal(t, string(StatusMerged), body.Status)
	require.Equal(t, "merged-etag", body.FileHash)
	require.NotEmpty(t, body.StoragePath)

	require.Equal(t, 1, objects.completeCalls)
	require.Len(t, objects.completedWith, 3, "object storage's own part list is what gets merged")
	require.Equal(t, "version-1", store.mergeFacts.VersionID)
	require.Equal(t, "第三季度板件测试报告", store.mergeProduct.Description)
	require.Equal(t, "v3", store.mergeProduct.Version)

	record := store.records["upload-1"]
	require.Equal(t, StatusMerged, record.Status)
	require.Equal(t, "merged-etag", record.ETag)
	require.Equal(t, 3, record.CompletedParts)
	require.Empty(t, record.ErrorMessage)
}

func TestCompleteUploadIsIdempotent(t *testing.T) {
	objects := &fakeObjectStore{storedParts: uploadedParts()}
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	first := postJSON(t, engine, completePath, completeBody())
	require.Equal(t, http.StatusOK, first.Code, "body=%s", first.Body.String())

	// The client retries: same answer, and object storage is not asked to merge
	// a session that no longer exists.
	second := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusOK, second.Code, "body=%s", second.Body.String())
	require.Equal(t, 1, objects.completeCalls, "the merge happens once")
	require.Equal(t, string(StatusMerged), completeResponse(t, second).Status)
}

func TestCompleteUploadRefusesWhenNothingWasUploaded(t *testing.T) {
	objects := &fakeObjectStore{}
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "no parts")
	require.Zero(t, store.claims, "an early refusal must not park the upload in merging")
	require.Equal(t, StatusUploading, store.records["upload-1"].Status)
}

func TestCompleteUploadRecordsMergeFailure(t *testing.T) {
	objects := &fakeObjectStore{
		storedParts: uploadedParts(),
		completeErr: errors.New("part etag mismatch"),
	}
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusBadGateway, rec.Code, "body=%s", rec.Body.String())
	require.Len(t, store.failReasons, 1)
	require.Contains(t, store.failReasons[0], "part etag mismatch")
	record := store.records["upload-1"]
	require.Equal(t, StatusFailed, record.Status)
	require.Contains(t, record.ErrorMessage, "part etag mismatch")
}

func TestCompleteUploadRejectsFinishedUpload(t *testing.T) {
	objects := &fakeObjectStore{storedParts: uploadedParts()}
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusCancelled)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusConflict, rec.Code, "body=%s", rec.Body.String())
	require.Zero(t, objects.completeCalls)
}

func TestCompleteUploadReportsConcurrentCompletion(t *testing.T) {
	objects := &fakeObjectStore{storedParts: uploadedParts()}
	store := &fakeUploadStore{claimErr: ErrUploadNotUploading}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(newInitTestModule(objects, store), caller())

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusConflict, rec.Code, "body=%s", rec.Body.String())
	require.Zero(t, objects.completeCalls, "only the claim winner merges")
}

func TestCompleteUploadRejectsOtherUsersUpload(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(
		newInitTestModule(&fakeObjectStore{storedParts: uploadedParts()}, store),
		&types.Caller{TenantID: 42, UserID: "user-2", Role: types.TenantRoleContributor},
	)

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestCompleteUploadHidesUploadsOutsideTheTenant(t *testing.T) {
	store := &fakeUploadStore{}
	seedUpload(store, 42, "user-1", 3, StatusUploading)
	engine := newTestEngine(
		newInitTestModule(&fakeObjectStore{storedParts: uploadedParts()}, store),
		&types.Caller{TenantID: 99, UserID: "user-9", Role: types.TenantRoleOwner},
	)

	rec := postJSON(t, engine, completePath, completeBody())

	require.Equal(t, http.StatusNotFound, rec.Code, "body=%s", rec.Body.String())
}
