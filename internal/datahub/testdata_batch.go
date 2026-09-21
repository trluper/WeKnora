package datahub

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

const (
	maxEventIDLength      = 64
	maxBoardIDLength      = 64
	maxFailedReasonLength = 255
	// maxTestDetailsPerRequest bounds one batch. It is far above the 30–60 rows
	// a client sends in practice, and exists so a single request cannot
	// monopolise the Event's write lock.
	maxTestDetailsPerRequest = 5000
)

// batchDetailData keeps the sibling implementation's contract, including the
// per-row user_id it used to trust. That value is accepted and ignored: the
// Record owner always comes from the session.
type batchDetailData struct {
	UserID       string `json:"user_id"`
	EventID      string `json:"event_id"`
	BoardID      string `json:"board_id"`
	TestResult   int    `json:"test_result"`
	FailedReason string `json:"failed_reason"`
}

type testDataBatchRequest struct {
	DetailDatas []batchDetailData `json:"detail_datas"`
	Count       int64             `json:"count"`
}

type failedItem struct {
	BoardID string `json:"board_id"`
	EventID string `json:"event_id"`
	Reason  string `json:"reason"`
}

type testDataBatchResponse struct {
	SuccessCount int64        `json:"success_count"`
	UpdatedCount int64        `json:"updated_count"`
	FailedCount  int64        `json:"failed_count"`
	FailedItems  []failedItem `json:"failed_items"`
}

func (r *batchDetailData) normalize() {
	r.EventID = strings.TrimSpace(r.EventID)
	r.BoardID = strings.TrimSpace(r.BoardID)
	r.FailedReason = strings.TrimSpace(r.FailedReason)
}

// validate reports the first problem with one row. A row that fails here is
// reported back in failed_items and does not stop the rest of the batch.
func (r *batchDetailData) validate() error {
	switch {
	case r.EventID == "":
		return errors.New("event_id is required")
	case len(r.EventID) > maxEventIDLength:
		return fmt.Errorf("event_id must be at most %d characters", maxEventIDLength)
	case r.BoardID == "":
		return errors.New("board_id is required")
	case len(r.BoardID) > maxBoardIDLength:
		return fmt.Errorf("board_id must be at most %d characters", maxBoardIDLength)
	case r.TestResult != TestResultPassed && r.TestResult != TestResultFailed:
		return errors.New("test_result must be 0 (failed) or 1 (passed)")
	case r.TestResult == TestResultFailed && r.FailedReason == "":
		return errors.New("failed_reason is required when test_result is 0")
	case len(r.FailedReason) > maxFailedReasonLength:
		return fmt.Errorf("failed_reason must be at most %d characters", maxFailedReasonLength)
	}
	return nil
}

// uploadTestData writes a batch of board verdicts. Bad rows are reported
// individually; good rows are written, grouped by Event because the summary is
// maintained per Event and each Event is written under its own lock.
func (m *Module) uploadTestData(c *gin.Context) {
	var req testDataBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := req.validateBatch(); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if m.testData == nil {
		respondError(c, http.StatusInternalServerError, "test data store is not wired")
		return
	}

	caller := types.CallerFromContext(c.Request.Context())
	response := testDataBatchResponse{FailedItems: []failedItem{}}

	byEvent := make(map[string][]TestDetailInput)
	eventOrder := make([]string, 0, len(req.DetailDatas))
	for i := range req.DetailDatas {
		row := &req.DetailDatas[i]
		row.normalize()
		if err := row.validate(); err != nil {
			response.FailedItems = append(response.FailedItems, failedItem{
				BoardID: row.BoardID,
				EventID: row.EventID,
				Reason:  err.Error(),
			})
			continue
		}
		if _, seen := byEvent[row.EventID]; !seen {
			eventOrder = append(eventOrder, row.EventID)
		}
		byEvent[row.EventID] = append(byEvent[row.EventID], TestDetailInput{
			EventID:      row.EventID,
			BoardID:      row.BoardID,
			TestResult:   row.TestResult,
			FailedReason: row.FailedReason,
			// The Record owner is the caller, never the value the client sent.
			UserID: caller.UserID,
		})
	}

	for _, eventID := range eventOrder {
		result, err := m.testData.UpsertTestDetails(
			c.Request.Context(), caller.TenantID, eventID, byEvent[eventID],
		)
		if err != nil {
			logger.Errorf(c.Request.Context(),
				"[Datahub] upsert test details failed event_id=%s: %v", eventID, err)
			respondError(c, http.StatusInternalServerError, "failed to store test data")
			return
		}
		response.SuccessCount += result.Inserted
		response.UpdatedCount += result.Updated
	}

	response.FailedCount = int64(len(response.FailedItems))
	respondOK(c, response)
}

// validateBatch mirrors the sibling implementation's request-level checks: the
// declared count must match the rows, and the batch has an upper bound.
func (r *testDataBatchRequest) validateBatch() error {
	switch {
	case r.Count <= 0:
		return errors.New("count is required")
	case int64(len(r.DetailDatas)) != r.Count:
		return fmt.Errorf("count mismatch: declared %d, got %d rows", r.Count, len(r.DetailDatas))
	case len(r.DetailDatas) > maxTestDetailsPerRequest:
		return fmt.Errorf("detail_datas exceeds the %d row limit per request", maxTestDetailsPerRequest)
	}
	return nil
}
