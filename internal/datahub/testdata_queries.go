package datahub

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

const (
	// testDataPageSize is the page used when a client asks for a silly number,
	// matching the sibling implementation's bounds of 1..100.
	testDataPageSize = 100
	testDataMaxSize  = 100
)

type overviewData struct {
	EventID     string    `json:"event_id"`
	TotalCount  int64     `json:"total_count"`
	PassedCount int64     `json:"passed_count"`
	FailedCount int64     `json:"failed_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type overviewResponse struct {
	Total int64          `json:"total"`
	List  []overviewData `json:"list"`
}

type detailData struct {
	ID           int64     `json:"id"`
	EventID      string    `json:"event_id"`
	BoardID      string    `json:"board_id"`
	TestResult   int16     `json:"test_result"`
	FailedReason string    `json:"failed_reason"`
	UserID       string    `json:"user_id"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type detailResponse struct {
	Total int64        `json:"total"`
	List  []detailData `json:"list"`
}

// pageParams reads the shared count/offset form fields, applying the same
// bounds the sibling implementation used.
func pageParams(c *gin.Context) (count, offset int) {
	count, _ = strconv.Atoi(c.Query("count"))
	if count <= 0 || count > testDataMaxSize {
		count = testDataPageSize
	}
	offset, _ = strconv.Atoi(c.Query("offset"))
	if offset < 0 {
		offset = 0
	}
	return count, offset
}

// testDataOverview reports each Event's overall test progress.
//
// Visibility is the rule the plan settled on: a member sees an Event's summary
// once they have recorded something for it, and tenant admins see every Event.
func (m *Module) testDataOverview(c *gin.Context) {
	if m.testData == nil {
		respondError(c, http.StatusInternalServerError, "test data store is not wired")
		return
	}
	count, offset := pageParams(c)

	caller := types.CallerFromContext(c.Request.Context())
	query := TestSummaryQuery{
		TenantID: caller.TenantID,
		EventID:  c.Query("event_id"),
		SortBy:   c.Query("sort_by"),
		Offset:   offset,
		Count:    count,
	}
	if !callerOwnsWholeTenant(c, caller) {
		query.ParticipantUserID = caller.UserID
	}

	page, err := m.testData.ListTestSummaries(c.Request.Context(), query)
	if err != nil {
		logger.Errorf(c.Request.Context(), "[Datahub] list test overview failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to list test overview")
		return
	}

	list := make([]overviewData, 0, len(page.Records))
	for _, summary := range page.Records {
		list = append(list, overviewData{
			EventID:     summary.EventID,
			TotalCount:  summary.TotalCount,
			PassedCount: summary.PassedCount,
			FailedCount: summary.FailedCount,
			CreatedAt:   summary.CreatedAt,
			UpdatedAt:   summary.UpdatedAt,
		})
	}
	respondOK(c, overviewResponse{Total: page.Total, List: list})
}

// testDataDetails pages one Event's board results.
//
// A member sees their own board verdicts; tenant admins see everybody's. The
// Event id is required, which is also what keeps the query on the
// (tenant_id, event_id, created_at) index.
func (m *Module) testDataDetails(c *gin.Context) {
	if m.testData == nil {
		respondError(c, http.StatusInternalServerError, "test data store is not wired")
		return
	}
	eventID := c.Query("event_id")
	if eventID == "" {
		respondError(c, http.StatusBadRequest, "event_id is required")
		return
	}
	count, offset := pageParams(c)

	caller := types.CallerFromContext(c.Request.Context())
	query := TestDetailQuery{
		TenantID: caller.TenantID,
		EventID:  eventID,
		SortBy:   c.Query("sort_by"),
		Offset:   offset,
		Count:    count,
	}
	if !callerOwnsWholeTenant(c, caller) {
		query.OwnerUserID = caller.UserID
	}

	page, err := m.testData.ListTestDetails(c.Request.Context(), query)
	if err != nil {
		logger.Errorf(c.Request.Context(), "[Datahub] list test details failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to list test details")
		return
	}

	list := make([]detailData, 0, len(page.Records))
	for _, detail := range page.Records {
		list = append(list, detailData{
			ID:           detail.ID,
			EventID:      detail.EventID,
			BoardID:      detail.BoardID,
			TestResult:   detail.TestResult,
			FailedReason: detail.FailedReason,
			UserID:       detail.UserID,
			CreatedAt:    detail.CreatedAt,
			UpdatedAt:    detail.UpdatedAt,
		})
	}
	respondOK(c, detailResponse{Total: page.Total, List: list})
}
