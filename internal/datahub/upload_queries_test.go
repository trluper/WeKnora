package datahub

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

const (
	versionsPath = "/api/v1/datahub/upload/versions"
	metadataPath = "/api/v1/datahub/upload/metadata"
)

func mergedUpload(uploadID, userID, fileName, version string, createdAt time.Time) UploadRecord {
	return UploadRecord{
		TenantID:    42,
		UploadID:    uploadID,
		EventID:     "event-1",
		Scenario:    ScenarioExperiment,
		UserID:      userID,
		Filename:    fileName,
		FileSize:    2048,
		Bucket:      "weknora",
		ObjectKey:   "event-1/2026/09/21/" + uploadID + "/" + fileName,
		StoragePath: "event-1/2026/09/21/" + uploadID + "/" + fileName,
		Status:      StatusMerged,
		Version:     version,
		Description: "报告",
		ETag:        "etag-" + uploadID,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

func TestListUploadVersionsRequiresAuthentication(t *testing.T) {
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), nil)

	rec := getPath(engine, versionsPath)

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestListUploadVersionsScopesToCaller(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeUploadStore{listPage: &UploadPage{
		Total: 2,
		Records: []UploadRecord{
			mergedUpload("upload-2", "user-1", "b.xlsx", "v2", now),
			mergedUpload("upload-1", "user-1", "a.xlsx", "v1", now.Add(-time.Hour)),
		},
	}}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := getPath(engine, versionsPath)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.lastQuery.OwnerUserID,
		"a contributor only lists their own uploads")
	require.Equal(t, uint64(42), store.lastQuery.TenantID)
	require.Equal(t, []UploadStatus{StatusMerged}, store.lastQuery.Statuses,
		"an in-flight upload is not a version yet")

	var body struct {
		Data uploadVersionListResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Data.Versions, 2)
	require.Equal(t, "upload-2", body.Data.Versions[0].UploadID)
	require.Equal(t, "b.xlsx", body.Data.Versions[0].FileName)
	require.Equal(t, "v2", body.Data.Versions[0].Version)
}

func TestListUploadVersionsGivesAdminsTheWholeTenant(t *testing.T) {
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 42, UserID: "admin-1", Role: types.TenantRoleAdmin,
	})

	rec := getPath(engine, versionsPath)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Empty(t, store.lastQuery.OwnerUserID, "an admin sees every owner in the tenant")
}

func TestListUploadVersionsReportsStoreFailure(t *testing.T) {
	store := &fakeUploadStore{listErr: errors.New("postgres is down")}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := getPath(engine, versionsPath)

	require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
}

func TestSearchUploadMetadataScopesToCallerAndPages(t *testing.T) {
	store := &fakeUploadStore{listPage: &UploadPage{Total: 7, Records: []UploadRecord{
		mergedUpload("upload-3", "user-1", "board.xlsx", "v3", time.Now().UTC()),
	}}}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

	rec := postJSON(t, engine, metadataPath, map[string]any{
		"count":     10,
		"offset":    5,
		"sort_by":   "-file_size",
		"event_id":  "event-1",
		"file_name": "board",
	})

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.lastQuery.OwnerUserID)
	require.Equal(t, "event-1", store.lastQuery.EventID)
	require.Equal(t, "board", store.lastQuery.FileName)
	require.Equal(t, "-file_size", store.lastQuery.SortBy)
	require.Equal(t, 10, store.lastQuery.Count)
	require.Equal(t, 5, store.lastQuery.Offset)

	var body struct {
		Data searchMetadataListResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(7), body.Data.Total, "total is the match count, not the page size")
	require.Len(t, body.Data.List, 1)
	item := body.Data.List[0]
	require.Equal(t, "upload-3", item.UploadID)
	require.Equal(t, "etag-upload-3", item.ETag)
	require.Equal(t, "报告", item.Description)
	require.Equal(t, "board.xlsx", item.FileName)
	require.Equal(t, string(StatusMerged), item.Status)
	require.Empty(t, item.Summary, "summary is a reserved column with no writer yet")
	require.Empty(t, item.AnalysisState)
}

func TestSearchUploadMetadataAppliesSafeDefaults(t *testing.T) {
	cases := map[string]map[string]any{
		"no count":        {},
		"negative count":  {"count": -3},
		"oversized":       {"count": 10000},
		"negative offset": {"count": 5, "offset": -10},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			store := &fakeUploadStore{}
			engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), caller())

			rec := postJSON(t, engine, metadataPath, body)

			require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
			require.GreaterOrEqual(t, store.lastQuery.Count, 1)
			require.LessOrEqual(t, store.lastQuery.Count, metadataMaxSize)
			require.GreaterOrEqual(t, store.lastQuery.Offset, 0)
		})
	}
}

func TestSearchUploadMetadataGivesAdminsTheWholeTenant(t *testing.T) {
	store := &fakeUploadStore{}
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, store), &types.Caller{
		TenantID: 42, UserID: "admin-1", Role: types.TenantRoleOwner,
	})

	rec := postJSON(t, engine, metadataPath, map[string]any{"count": 5})

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Empty(t, store.lastQuery.OwnerUserID)
}

func TestSearchUploadMetadataRejectsMalformedBody(t *testing.T) {
	engine := newTestEngine(newInitTestModule(&fakeObjectStore{}, &fakeUploadStore{}), caller())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, metadataPath, bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
}
