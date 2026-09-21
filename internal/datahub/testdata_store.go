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
