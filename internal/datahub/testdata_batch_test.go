package datahub

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const batchUploadPath = "/api/v1/datahub/test-data/batch-upload"

func detailRow(eventID, boardID string, result int, reason string) map[string]any {
	row := map[string]any{
		"event_id":    eventID,
		"board_id":    boardID,
		"test_result": result,
	}
	if reason != "" {
		row["failed_reason"] = reason
	}
	return row
}

func testDataBatchBody(rows ...map[string]any) map[string]any {
	return map[string]any{"detail_datas": rows, "count": len(rows)}
}

func batchResponse(t *testing.T, rec *httptest.ResponseRecorder) testDataBatchResponse {
	t.Helper()
	var body struct {
		Data testDataBatchResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body.Data
}

func newTestDataEngine(store TestDataStore, caller *types.Caller) *gin.Engine {
	return newTestEngine(newModule(enabledSettings(), Parts{
		Objects:  &fakeObjectStore{},
		Store:    &fakeUploadStore{},
		TestData: store,
	}), caller)
}

func TestUploadTestDataRejectsUnauthenticatedCaller(t *testing.T) {
	engine := newTestDataEngine(newFakeTestDataStore(), nil)

	rec := postJSON(t, engine, batchUploadPath, testDataBatchBody(detailRow("event-1", "board-1", 1, "")))

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestUploadTestDataRejectsMalformedBatch(t *testing.T) {
	cases := map[string]map[string]any{
		"missing count": {
			"detail_datas": []map[string]any{detailRow("event-1", "board-1", 1, "")},
		},
		"zero count": {
			"detail_datas": []map[string]any{detailRow("event-1", "board-1", 1, "")},
			"count":        0,
		},
		"count mismatch": {
			"detail_datas": []map[string]any{detailRow("event-1", "board-1", 1, "")},
			"count":        2,
		},
		"no rows": {
			"detail_datas": []map[string]any{},
			"count":        0,
		},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			store := newFakeTestDataStore()
			engine := newTestDataEngine(store, caller())

			rec := postJSON(t, engine, batchUploadPath, body)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
			require.Zero(t, store.calls, "a rejected batch must not reach the store")
		})
	}
}

func TestUploadTestDataReportsBadRowsAndKeepsGoodOnes(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())
	body := testDataBatchBody(
		detailRow("event-1", "board-1", TestResultPassed, ""),
		detailRow("", "board-2", TestResultPassed, ""),
		detailRow("event-1", "", TestResultPassed, ""),
		detailRow("event-1", "board-3", 7, ""),
		detailRow("event-1", "board-4", TestResultFailed, ""),
	)

	rec := postJSON(t, engine, batchUploadPath, body)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	response := batchResponse(t, rec)
	require.Equal(t, int64(1), response.SuccessCount, "the one good row is written")
	require.Zero(t, response.UpdatedCount)
	require.Equal(t, int64(4), response.FailedCount)
	require.Len(t, response.FailedItems, 4)

	reasons := map[string]string{}
	for _, item := range response.FailedItems {
		reasons[item.BoardID] = item.Reason
	}
	require.Contains(t, reasons["board-2"], "event_id is required")
	require.Contains(t, reasons[""], "board_id is required")
	require.Contains(t, reasons["board-3"], "test_result must be 0 (failed) or 1 (passed)")
	require.Contains(t, reasons["board-4"], "failed_reason is required")

	require.Equal(t, map[string]int{"board-1": TestResultPassed}, store.details["event-1"])
}

func TestUploadTestDataIsIdempotentAndCountsCorrections(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())
	batch := testDataBatchBody(
		detailRow("event-1", "board-1", TestResultPassed, ""),
		detailRow("event-1", "board-2", TestResultFailed, "焊点虚接"),
	)

	first := postJSON(t, engine, batchUploadPath, batch)
	require.Equal(t, http.StatusOK, first.Code, "body=%s", first.Body.String())
	require.Equal(t, int64(2), batchResponse(t, first).SuccessCount)
	require.Equal(t, int64(1), store.summary["event-1"].PassedCount)
	require.Equal(t, int64(1), store.summary["event-1"].FailedCount)

	// The same batch again: nothing new, both rows counted as corrections.
	second := postJSON(t, engine, batchUploadPath, batch)
	require.Equal(t, int64(0), batchResponse(t, second).SuccessCount)
	require.Equal(t, int64(2), batchResponse(t, second).UpdatedCount)
	require.Equal(t, int64(2), store.summary["event-1"].TotalCount, "no double counting")

	// A board that flips moves one count in each direction, total unchanged.
	flip := postJSON(t, engine, batchUploadPath,
		testDataBatchBody(detailRow("event-1", "board-2", TestResultPassed, "")))
	require.Equal(t, http.StatusOK, flip.Code, "body=%s", flip.Body.String())
	require.Equal(t, int64(2), store.summary["event-1"].PassedCount)
	require.Equal(t, int64(0), store.summary["event-1"].FailedCount)
	require.Equal(t, int64(2), store.summary["event-1"].TotalCount)
}

func TestUploadTestDataGroupsRowsByEvent(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())
	body := testDataBatchBody(
		detailRow("event-1", "board-1", TestResultPassed, ""),
		detailRow("event-2", "board-1", TestResultFailed, "缺件"),
		detailRow("event-1", "board-2", TestResultPassed, ""),
	)

	rec := postJSON(t, engine, batchUploadPath, body)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Len(t, store.lastRows["event-1"], 2, "each Event is written in one batch")
	require.Len(t, store.lastRows["event-2"], 1)
	require.Equal(t, int64(2), store.summary["event-1"].PassedCount)
	require.Equal(t, int64(1), store.summary["event-2"].FailedCount)
}

func TestUploadTestDataTakesTheOwnerFromTheSession(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())
	row := detailRow("event-1", "board-1", TestResultPassed, "")
	row["user_id"] = "somebody-else"

	rec := postJSON(t, engine, batchUploadPath, testDataBatchBody(row))

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.lastRows["event-1"][0].UserID,
		"the client's user_id is ignored")
}

func TestUploadTestDataLastVerdictWinsWithinABatch(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())
	body := testDataBatchBody(
		detailRow("event-1", "board-1", TestResultPassed, ""),
		detailRow("event-1", "board-1", TestResultFailed, "复测不合格"),
	)

	rec := postJSON(t, engine, batchUploadPath, body)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, TestResultFailed, store.details["event-1"]["board-1"])
	require.Equal(t, int64(1), store.summary["event-1"].TotalCount)
	require.Equal(t, int64(1), store.summary["event-1"].FailedCount)
}

func TestUploadTestDataReportsStoreFailure(t *testing.T) {
	store := newFakeTestDataStore()
	store.err = errors.New("postgres is down")
	engine := newTestDataEngine(store, caller())

	rec := postJSON(t, engine, batchUploadPath, testDataBatchBody(detailRow("event-1", "board-1", 1, "")))

	require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
}

func TestUploadTestDataWithoutStoreIsRefused(t *testing.T) {
	engine := newTestEngine(newModule(enabledSettings(), Parts{
		Objects: &fakeObjectStore{},
		Store:   &fakeUploadStore{},
	}), caller())

	rec := postJSON(t, engine, batchUploadPath, testDataBatchBody(detailRow("event-1", "board-1", 1, "")))

	require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
}
