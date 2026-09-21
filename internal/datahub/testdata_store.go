package datahub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// TestDetailInput is one board verdict as it arrives. UserID is the Record owner
// the server derived from the session, never a client-supplied value.
type TestDetailInput struct {
	EventID      string
	BoardID      string
	TestResult   int
	FailedReason string
	UserID       string
}

// UpsertTestDetailsResult reports what one batch did.
type UpsertTestDetailsResult struct {
	Inserted int64
	Updated  int64
}

// TestDataStore is the persistence boundary for Test detail and Test summary.
type TestDataStore interface {
	// UpsertTestDetails writes one Event's board results and keeps that Event's
	// summary in step with them. Every row must belong to eventID.
	UpsertTestDetails(
		ctx context.Context, tenantID uint64, eventID string, rows []TestDetailInput,
	) (*UpsertTestDetailsResult, error)
	// ListTestSummaries pages Event summaries, optionally restricted to the
	// Events one user contributed to.
	ListTestSummaries(ctx context.Context, query TestSummaryQuery) (*TestSummaryPage, error)
	// ListTestDetails pages one Event's board results, optionally restricted to
	// one Record owner.
	ListTestDetails(ctx context.Context, query TestDetailQuery) (*TestDetailPage, error)
}

// TestSummaryQuery filters and pages Event summaries.
type TestSummaryQuery struct {
	TenantID uint64
	// EventID, when set, restricts the listing to that one Event.
	EventID string
	// ParticipantUserID, when set, restricts the listing to Events this user
	// recorded at least one board for. Callers that are not tenant admins
	// always set it; admins leave it empty to see every Event in the tenant.
	ParticipantUserID string
	SortBy            string
	Offset            int
	Count             int
}

// TestSummaryPage is one page of Event summaries plus the total match count.
type TestSummaryPage struct {
	Total   int64
	Records []TestSummary
}

// TestDetailQuery filters and pages one Event's board results.
type TestDetailQuery struct {
	TenantID uint64
	EventID  string
	// OwnerUserID, when set, restricts the listing to one Record owner. Callers
	// that are not tenant admins always set it.
	OwnerUserID string
	SortBy      string
	Offset      int
	Count       int
}

// TestDetailPage is one page of board results plus the total match count.
type TestDetailPage struct {
	Total   int64
	Records []TestDetail
}

type postgresTestDataStore struct {
	db *gorm.DB
}

// NewPostgresTestDataStore returns the real, Postgres-backed TestDataStore.
func NewPostgresTestDataStore(db *gorm.DB) TestDataStore {
	return &postgresTestDataStore{db: db}
}

// UpsertTestDetails is the whole reason the summary lives in its own row.
//
// The transaction starts by locking the Event's summary row. That single lock
// serialises every batch for the same Event, which is what lets the summary be
// maintained by exact deltas: the previous verdicts read a moment later cannot
// change under us, and two clients correcting different boards at the same time
// cannot both count the same board. Batches for different Events never contend.
//
// Deliberately not used: recomputing the summary by counting the Event's rows on
// every batch. At 30–60 rows per request against an Event that grows to 100 000
// boards, that is quadratic.
func (s *postgresTestDataStore) UpsertTestDetails(
	ctx context.Context, tenantID uint64, eventID string, rows []TestDetailInput,
) (*UpsertTestDetailsResult, error) {
	result := &UpsertTestDetailsResult{}
	if len(rows) == 0 {
		return result, nil
	}

	// A board named twice in one batch is a correction: the last verdict wins.
	deduped := make([]TestDetailInput, 0, len(rows))
	position := make(map[string]int, len(rows))
	for _, row := range rows {
		if index, seen := position[row.BoardID]; seen {
			deduped[index] = row
			continue
		}
		position[row.BoardID] = len(deduped)
		deduped = append(deduped, row)
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()

		// 1. Make sure the summary row exists, then hold its lock.
		if err := tx.Exec(`
			INSERT INTO datahub_test_summaries
				(tenant_id, event_id, total_count, passed_count, failed_count, created_at, updated_at)
			VALUES (?, ?, 0, 0, 0, ?, ?)
			ON CONFLICT (tenant_id, event_id) DO NOTHING`,
			tenantID, eventID, now, now,
		).Error; err != nil {
			return fmt.Errorf("ensure test summary: %w", err)
		}
		var summaryID int64
		if err := tx.Raw(`
			SELECT id FROM datahub_test_summaries
			 WHERE tenant_id = ? AND event_id = ? FOR UPDATE`,
			tenantID, eventID,
		).Scan(&summaryID).Error; err != nil {
			return fmt.Errorf("lock test summary: %w", err)
		}

		// 2. Read the verdicts these boards already had.
		previous, err := previousVerdicts(tx, tenantID, eventID, deduped)
		if err != nil {
			return err
		}

		totalDelta, passedDelta, failedDelta := summaryDeltas(deduped, previous)

		// 3. Write the board results, learning which were new.
		values := make([]string, 0, len(deduped))
		args := make([]any, 0, len(deduped)*6)
		for _, row := range deduped {
			values = append(values, "(?, ?, ?, ?, ?, ?, ?, ?)")
			args = append(args, tenantID, eventID, row.BoardID, row.TestResult,
				row.FailedReason, row.UserID, now, now)
		}
		var outcomes []struct {
			Inserted bool
		}
		sql := fmt.Sprintf(`
			INSERT INTO datahub_test_details
				(tenant_id, event_id, board_id, test_result, failed_reason, user_id, created_at, updated_at)
			VALUES %s
			ON CONFLICT (tenant_id, event_id, board_id) DO UPDATE
			   SET test_result = EXCLUDED.test_result,
			       failed_reason = EXCLUDED.failed_reason,
			       user_id = EXCLUDED.user_id,
			       updated_at = EXCLUDED.updated_at
			RETURNING (xmax = 0) AS inserted`,
			strings.Join(values, ", "),
		)
		if err := tx.Raw(sql, args...).Scan(&outcomes).Error; err != nil {
			return fmt.Errorf("upsert test details: %w", err)
		}
		for _, outcome := range outcomes {
			if outcome.Inserted {
				result.Inserted++
			} else {
				result.Updated++
			}
		}

		// 4. Move the Event's summary by exactly the difference this batch made.
		if err := tx.Exec(`
			UPDATE datahub_test_summaries
			   SET total_count = total_count + ?,
			       passed_count = passed_count + ?,
			       failed_count = failed_count + ?,
			       updated_at = ?
			 WHERE tenant_id = ? AND event_id = ?`,
			totalDelta, passedDelta, failedDelta, now, tenantID, eventID,
		).Error; err != nil {
			return fmt.Errorf("update test summary: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func previousVerdicts(
	tx *gorm.DB, tenantID uint64, eventID string, rows []TestDetailInput,
) (map[string]int, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(rows)), ",")
	args := make([]any, 0, len(rows)+2)
	args = append(args, tenantID, eventID)
	for _, row := range rows {
		args = append(args, row.BoardID)
	}

	var existing []TestDetail
	sql := fmt.Sprintf(`
		SELECT board_id, test_result FROM datahub_test_details
		 WHERE tenant_id = ? AND event_id = ? AND board_id IN (%s)`,
		placeholders,
	)
	if err := tx.Raw(sql, args...).Scan(&existing).Error; err != nil {
		return nil, fmt.Errorf("read previous verdicts: %w", err)
	}

	previous := make(map[string]int, len(existing))
	for _, detail := range existing {
		previous[detail.BoardID] = int(detail.TestResult)
	}
	return previous, nil
}

// summaryDeltas is the difference a batch makes to an Event's totals. A board
// that flips between passed and failed moves one count up and the other down
// without changing the total.
func summaryDeltas(
	rows []TestDetailInput, previous map[string]int,
) (total, passed, failed int64) {
	for _, row := range rows {
		old, existed := previous[row.BoardID]
		switch {
		case !existed:
			total++
			if row.TestResult == TestResultPassed {
				passed++
			} else {
				failed++
			}
		case old == row.TestResult:
			// Same verdict re-reported: nothing moves.
		case row.TestResult == TestResultPassed:
			passed++
			failed--
		default:
			passed--
			failed++
		}
	}
	return total, passed, failed
}

// testSummarySortColumns accepts both the form vocabulary the existing clients
// send (create_time / update_time) and the column names, so neither has to
// change. Anything else falls back to newest-first.
var testSummarySortColumns = map[string]string{
	"create_time":  "created_at",
	"created_at":   "created_at",
	"update_time":  "updated_at",
	"updated_at":   "updated_at",
	"total_count":  "total_count",
	"passed_count": "passed_count",
	"failed_count": "failed_count",
	"event_id":     "event_id",
}

// testDetailSortColumns is the detail listing's equivalent whitelist.
var testDetailSortColumns = map[string]string{
	"create_time": "created_at",
	"created_at":  "created_at",
	"update_time": "updated_at",
	"updated_at":  "updated_at",
	"test_result": "test_result",
	"board_id":    "board_id",
}

// sortClause turns a client's sort_by into SQL, restricted to the whitelist.
func sortClause(columns map[string]string, sortBy, fallback string) string {
	column, direction := columns[fallback], "DESC"
	if sortBy == "" {
		return column + " " + direction
	}
	field := sortBy
	if trimmed, ok := strings.CutPrefix(sortBy, "-"); ok {
		field = trimmed
	} else {
		direction = "ASC"
	}
	mapped, ok := columns[field]
	if !ok {
		return columns[fallback] + " DESC"
	}
	return mapped + " " + direction
}

func (s *postgresTestDataStore) ListTestSummaries(
	ctx context.Context, query TestSummaryQuery,
) (*TestSummaryPage, error) {
	page := &TestSummaryPage{}

	filter := func(db *gorm.DB) *gorm.DB {
		db = db.Where("tenant_id = ?", query.TenantID)
		if query.EventID != "" {
			db = db.Where("event_id = ?", query.EventID)
		}
		if query.ParticipantUserID != "" {
			// A member sees an Event's overall progress only once they have
			// recorded something for it.
			db = db.Where(`
				EXISTS (
					SELECT 1 FROM datahub_test_details d
					 WHERE d.tenant_id = datahub_test_summaries.tenant_id
					   AND d.event_id = datahub_test_summaries.event_id
					   AND d.user_id = ?
				)`, query.ParticipantUserID)
		}
		return db
	}

	if err := filter(s.db.WithContext(ctx).Model(&TestSummary{})).
		Count(&page.Total).Error; err != nil {
		return nil, fmt.Errorf("count test summaries: %w", err)
	}
	if err := filter(s.db.WithContext(ctx).Model(&TestSummary{})).
		Order(sortClause(testSummarySortColumns, query.SortBy, "create_time")).
		Offset(query.Offset).
		Limit(query.Count).
		Find(&page.Records).Error; err != nil {
		return nil, fmt.Errorf("list test summaries: %w", err)
	}
	return page, nil
}

func (s *postgresTestDataStore) ListTestDetails(
	ctx context.Context, query TestDetailQuery,
) (*TestDetailPage, error) {
	page := &TestDetailPage{}

	filter := func(db *gorm.DB) *gorm.DB {
		// Every index on this table leads with tenant_id, and a detail listing
		// is always scoped to one Event, so this pair keeps the scan on the
		// (tenant_id, event_id, created_at) index even at 100 000 boards.
		db = db.Where("tenant_id = ? AND event_id = ?", query.TenantID, query.EventID)
		if query.OwnerUserID != "" {
			db = db.Where("user_id = ?", query.OwnerUserID)
		}
		return db
	}

	if err := filter(s.db.WithContext(ctx).Model(&TestDetail{})).
		Count(&page.Total).Error; err != nil {
		return nil, fmt.Errorf("count test details: %w", err)
	}
	if err := filter(s.db.WithContext(ctx).Model(&TestDetail{})).
		Order(sortClause(testDetailSortColumns, query.SortBy, "create_time")).
		Offset(query.Offset).
		Limit(query.Count).
		Find(&page.Records).Error; err != nil {
		return nil, fmt.Errorf("list test details: %w", err)
	}
	return page, nil
}
