package datahub

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

const (
	overviewPath = "/api/v1/datahub/test-data/overview-data"
	detailsPath  = "/api/v1/datahub/test-data/detail-data"
)

func sampleSummary(eventID string, total, passed, failed int64) TestSummary {
	now := time.Now().UTC()
	return TestSummary{
		EventID:     eventID,
		TotalCount:  total,
		PassedCount: passed,
		FailedCount: failed,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func sampleDetail(id int64, boardID string, result int16, userID string) TestDetail {
	now := time.Now().UTC()
	return TestDetail{
		ID:           id,
		EventID:      "event-1",
		BoardID:      boardID,
		TestResult:   result,
		FailedReason: "焊点虚接",
		UserID:       userID,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

func TestTestDataOverviewRequiresAuthentication(t *testing.T) {
	engine := newTestDataEngine(newFakeTestDataStore(), nil)

	rec := getPath(engine, overviewPath)

	require.Equal(t, http.StatusUnauthorized, rec.Code, "body=%s", rec.Body.String())
}

func TestTestDataOverviewScopesMembersToTheirOwnEvents(t *testing.T) {
	store := newFakeTestDataStore()
	store.summaryPage = &TestSummaryPage{
		Total:   1,
		Records: []TestSummary{sampleSummary("event-1", 3, 2, 1)},
	}
	engine := newTestDataEngine(store, caller())

	rec := getPath(engine, overviewPath+"?count=10&offset=0")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.lastSummaryQuery.ParticipantUserID,
		"a member only sees Events they contributed to")
	require.Equal(t, uint64(42), store.lastSummaryQuery.TenantID)
	require.Equal(t, 10, store.lastSummaryQuery.Count)

	var body struct {
		Data overviewResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Data.Total)
	require.Len(t, body.Data.List, 1)
	require.Equal(t, "event-1", body.Data.List[0].EventID)
	require.Equal(t, int64(3), body.Data.List[0].TotalCount)
	require.Equal(t, int64(2), body.Data.List[0].PassedCount)
	require.Equal(t, int64(1), body.Data.List[0].FailedCount)
}

func TestTestDataOverviewGivesAdminsEveryEvent(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, &types.Caller{
		TenantID: 42, UserID: "admin-1", Role: types.TenantRoleAdmin,
	})

	rec := getPath(engine, overviewPath+"?event_id=event-9&sort_by=total_count")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Empty(t, store.lastSummaryQuery.ParticipantUserID)
	require.Equal(t, "event-9", store.lastSummaryQuery.EventID)
	require.Equal(t, "total_count", store.lastSummaryQuery.SortBy)
}

func TestTestDataDetailsRequireAnEvent(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, caller())

	rec := getPath(engine, detailsPath)

	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "event_id is required")
}

func TestTestDataDetailsScopeMembersToTheirOwnBoards(t *testing.T) {
	store := newFakeTestDataStore()
	store.detailPage = &TestDetailPage{
		Total:   200,
		Records: []TestDetail{sampleDetail(11, "board-1", TestResultPassed, "user-1")},
	}
	engine := newTestDataEngine(store, caller())

	rec := getPath(engine, detailsPath+"?event_id=event-1&count=50&offset=100&sort_by=-test_result")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, "user-1", store.lastDetailQuery.OwnerUserID,
		"a member only sees their own board verdicts")
	require.Equal(t, "event-1", store.lastDetailQuery.EventID)
	require.Equal(t, 100, store.lastDetailQuery.Offset)
	require.Equal(t, 50, store.lastDetailQuery.Count)
	require.Equal(t, "-test_result", store.lastDetailQuery.SortBy)

	var body struct {
		Data detailResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, int64(200), body.Data.Total, "total is the match count, not the page size")
	require.Len(t, body.Data.List, 1)
	require.Equal(t, "board-1", body.Data.List[0].BoardID)
	require.Equal(t, int16(TestResultPassed), body.Data.List[0].TestResult)
	require.Equal(t, "焊点虚接", body.Data.List[0].FailedReason)
}

func TestTestDataDetailsGiveAdminsEveryBoard(t *testing.T) {
	store := newFakeTestDataStore()
	engine := newTestDataEngine(store, &types.Caller{
		TenantID: 42, UserID: "admin-1", Role: types.TenantRoleOwner,
	})

	rec := getPath(engine, detailsPath+"?event_id=event-1")

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Empty(t, store.lastDetailQuery.OwnerUserID)
}

func TestTestDataQueriesApplySafePageBounds(t *testing.T) {
	cases := []string{
		"",
		"?count=0",
		"?count=-5",
		"?count=100000",
		"?offset=-20",
	}
	for _, suffix := range cases {
		t.Run(suffix, func(t *testing.T) {
			store := newFakeTestDataStore()
			engine := newTestDataEngine(store, caller())

			rec := getPath(engine, overviewPath+suffix)

			require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
			require.GreaterOrEqual(t, store.lastSummaryQuery.Count, 1)
			require.LessOrEqual(t, store.lastSummaryQuery.Count, testDataMaxSize)
			require.GreaterOrEqual(t, store.lastSummaryQuery.Offset, 0)
		})
	}
}

func TestTestDataQueriesReportStoreFailure(t *testing.T) {
	store := newFakeTestDataStore()
	store.listErr = errors.New("postgres is down")
	engine := newTestDataEngine(store, caller())

	overview := getPath(engine, overviewPath)
	details := getPath(engine, detailsPath+"?event_id=event-1")

	require.Equal(t, http.StatusInternalServerError, overview.Code)
	require.Equal(t, http.StatusInternalServerError, details.Code)
}
