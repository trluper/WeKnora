package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrUploadNotFound is returned when an Upload does not exist in the caller's
// tenant — which is also what a caller from another tenant must see, so the two
// cases are deliberately indistinguishable.
var ErrUploadNotFound = errors.New("datahub: upload not found")

// ErrUploadNotUploading is returned when parts arrive for an Upload that is
// already merging, finished, or cancelled.
var ErrUploadNotUploading = errors.New("datahub: upload is not accepting parts")

// ErrPartsConflict is returned when the part row changed under us. It is
// retryable: the caller should send the same batch again.
var ErrPartsConflict = errors.New("datahub: part registration conflict")

// RegisterPartsResult reports the part row after a successful merge.
type RegisterPartsResult struct {
	CompletedParts int
	TotalParts     int
}

// ProductMetadata is what the client submits when finishing an upload: the
// human-authored facts about the file, as opposed to what object storage knows.
type ProductMetadata struct {
	Description  string
	Category     string
	ImportantKey string
	Version      string
}

// UploadQuery filters and pages an Upload listing.
type UploadQuery struct {
	TenantID uint64
	// OwnerUserID restricts the listing to one Record owner. Callers that are
	// not tenant admins always set it; admins leave it empty to see the whole
	// tenant.
	OwnerUserID string
	EventID     string
	FileName    string
	// Statuses, when non-empty, restricts the listing to those statuses.
	Statuses []UploadStatus
	SortBy   string
	Offset   int
	Count    int
}

// UploadPage is one page of an Upload listing plus the total match count, which
// is what the client needs to page without guessing.
type UploadPage struct {
	Total   int64
	Records []UploadRecord
}

// UploadStore is the persistence boundary for Upload records. It exists as an
// interface so the upload flow's behaviour — including its failure paths — can
// be exercised without a database; the Postgres implementation below is the
// real one, and its SQL is covered by an environment-gated test.
type UploadStore interface {
	// Create writes the Upload record and its part-registration row together,
	// or neither.
	Create(ctx context.Context, upload *UploadRecord, parts *UploadParts) error
	// Get returns one Upload, scoped to its tenant.
	Get(ctx context.Context, tenantID uint64, uploadID string) (*UploadRecord, error)
	// GetParts returns one Upload's part-registration row.
	GetParts(ctx context.Context, tenantID uint64, uploadID string) (*UploadParts, error)
	// RegisterParts merges a batch of arrived parts into the bitmap and part
	// metadata. Registering the same part twice is a no-op, never a duplicate.
	RegisterParts(
		ctx context.Context, tenantID uint64, uploadID string, parts []CompletedPart,
	) (*RegisterPartsResult, error)
	// ClaimUploadMerge moves an Upload from uploading to merging so exactly one
	// caller performs the merge. Returns ErrUploadNotUploading when it is not
	// claimable (already merging, finished, cancelled, or failed).
	ClaimUploadMerge(ctx context.Context, tenantID uint64, uploadID string) error
	// FinalizeUploadMerge writes the object-storage facts and product metadata
	// and moves the Upload to merged. It never touches the columns reserved for
	// a future summarizer, so re-running it cannot clobber their output.
	FinalizeUploadMerge(
		ctx context.Context, tenantID uint64, uploadID string,
		facts ObjectInfo, product ProductMetadata,
	) (*UploadRecord, error)
	// FailUploadMerge records why a merge failed and moves the Upload to failed.
	FailUploadMerge(
		ctx context.Context, tenantID uint64, uploadID string, reason string,
	) (*UploadRecord, error)
	// ListUploads returns matching Uploads newest-first by default.
	ListUploads(ctx context.Context, query UploadQuery) (*UploadPage, error)
	// ListExpiredUploads returns unfinished Uploads whose expiry has passed.
	// It is deliberately cross-tenant: it serves the background sweep, not a
	// user request.
	ListExpiredUploads(ctx context.Context, now time.Time, limit int) ([]UploadRecord, error)
	// MarkUploadExpired abandons one Upload, recording why. It is idempotent:
	// an Upload that already left the in-flight states is left alone.
	MarkUploadExpired(ctx context.Context, tenantID uint64, uploadID, reason string) error
	// ListUploadsNeedingFacts returns merged Uploads whose object-storage facts
	// are missing or incomplete, so the reconciler can fill them in.
	ListUploadsNeedingFacts(ctx context.Context, limit int) ([]UploadRecord, error)
	// UpdateUploadFacts writes only the object-storage fact columns of one
	// Upload. Product metadata and the reserved columns are never touched.
	UpdateUploadFacts(ctx context.Context, tenantID uint64, uploadID string, facts ObjectInfo) error
	// KnownObjectKeys returns the object keys every recorded Upload points at.
	KnownObjectKeys(ctx context.Context, limit int) ([]string, error)
}

// inFlightUploadStatuses are the states an Upload can still be abandoned from.
func inFlightUploadStatuses() []UploadStatus {
	return []UploadStatus{StatusInit, StatusUploading, StatusMerging}
}

// partMetaEntry is one registered part as stored. The sibling implementation
// used protobuf for this blob; JSON is used here because it needs no generated
// types and nothing outside this module reads the column.
type partMetaEntry struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

func decodePartMeta(raw []byte) ([]partMetaEntry, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var entries []partMetaEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("decode part metadata: %w", err)
	}
	return entries, nil
}

func encodePartMeta(entries []partMetaEntry) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(entries)
	if err != nil {
		return nil, fmt.Errorf("encode part metadata: %w", err)
	}
	return encoded, nil
}

type postgresUploadStore struct {
	db *gorm.DB
}

// NewPostgresUploadStore returns the real, Postgres-backed UploadStore.
func NewPostgresUploadStore(db *gorm.DB) UploadStore {
	return &postgresUploadStore{db: db}
}

func (s *postgresUploadStore) Create(
	ctx context.Context, upload *UploadRecord, parts *UploadParts,
) error {
	if upload == nil || parts == nil {
		return errors.New("datahub: upload and parts are both required")
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(upload).Error; err != nil {
			return fmt.Errorf("create upload record: %w", err)
		}
		if err := tx.Create(parts).Error; err != nil {
			return fmt.Errorf("create upload parts: %w", err)
		}
		return nil
	})
}

func (s *postgresUploadStore) Get(
	ctx context.Context, tenantID uint64, uploadID string,
) (*UploadRecord, error) {
	var record UploadRecord
	err := s.db.WithContext(ctx).
		Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get upload record: %w", err)
	}
	return &record, nil
}

func (s *postgresUploadStore) GetParts(
	ctx context.Context, tenantID uint64, uploadID string,
) (*UploadParts, error) {
	var parts UploadParts
	err := s.db.WithContext(ctx).
		Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
		Take(&parts).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get upload parts: %w", err)
	}
	return &parts, nil
}

// RegisterParts merges a batch under a row lock. Holding the lock is what makes
// concurrent registrations of the same Upload serialise instead of losing
// updates; the version column guards the write itself, so a row that changed
// underneath us produces a retryable conflict rather than a silent overwrite.
func (s *postgresUploadStore) RegisterParts(
	ctx context.Context, tenantID uint64, uploadID string, parts []CompletedPart,
) (*RegisterPartsResult, error) {
	result := &RegisterPartsResult{}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var upload UploadRecord
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("status", "total_parts", "completed_parts").
			Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
			Take(&upload).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrUploadNotFound
		}
		if err != nil {
			return fmt.Errorf("lock upload record: %w", err)
		}
		if upload.Status != StatusUploading {
			return ErrUploadNotUploading
		}

		var row UploadParts
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
			Take(&row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrUploadNotFound
		}
		if err != nil {
			return fmt.Errorf("lock upload parts: %w", err)
		}

		entries, err := decodePartMeta(row.PartMeta)
		if err != nil {
			return err
		}
		bitmap := PartBitmapFromBytes(row.PartBitmap)
		if bitmap == nil {
			bitmap = NewPartBitmap(upload.TotalParts)
		}

		added := 0
		for _, part := range parts {
			if bitmap.IsSet(part.PartNumber) {
				continue // Already registered: a retry of the same batch.
			}
			bitmap.Set(part.PartNumber)
			entries = append(entries, partMetaEntry{
				PartNumber: part.PartNumber,
				ETag:       part.ETag,
				Size:       part.Size,
			})
			added++
		}

		if added == 0 {
			result.CompletedParts = row.CompletedParts
			result.TotalParts = upload.TotalParts
			return nil
		}

		completed := row.CompletedParts + added
		if completed > upload.TotalParts {
			completed = upload.TotalParts
		}
		encoded, err := encodePartMeta(entries)
		if err != nil {
			return err
		}

		updated := tx.Exec(`
			UPDATE datahub_upload_parts
			   SET part_bitmap = ?, part_meta = ?, completed_parts = ?,
			       version = version + 1, updated_at = ?
			 WHERE tenant_id = ? AND upload_id = ? AND version = ?`,
			bitmap.Bytes(), encoded, completed, time.Now().UTC(),
			tenantID, uploadID, row.Version,
		)
		if updated.Error != nil {
			return fmt.Errorf("update upload parts: %w", updated.Error)
		}
		if updated.RowsAffected == 0 {
			return ErrPartsConflict
		}

		if err := tx.Exec(`
			UPDATE datahub_uploads
			   SET completed_parts = ?, updated_at = ?
			 WHERE tenant_id = ? AND upload_id = ?`,
			completed, time.Now().UTC(), tenantID, uploadID,
		).Error; err != nil {
			return fmt.Errorf("update upload progress: %w", err)
		}

		result.CompletedParts = completed
		result.TotalParts = upload.TotalParts
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *postgresUploadStore) ClaimUploadMerge(
	ctx context.Context, tenantID uint64, uploadID string,
) error {
	claimed := s.db.WithContext(ctx).Exec(`
		UPDATE datahub_uploads
		   SET status = ?, updated_at = ?
		 WHERE tenant_id = ? AND upload_id = ? AND status = ?`,
		StatusMerging, time.Now().UTC(), tenantID, uploadID, StatusUploading,
	)
	if claimed.Error != nil {
		return fmt.Errorf("claim upload merge: %w", claimed.Error)
	}
	if claimed.RowsAffected == 0 {
		// Tell "no such upload" apart from "not in a claimable state" so the
		// caller can answer 404 rather than 409.
		if _, err := s.Get(ctx, tenantID, uploadID); err != nil {
			return err
		}
		return ErrUploadNotUploading
	}
	return nil
}

func (s *postgresUploadStore) FinalizeUploadMerge(
	ctx context.Context, tenantID uint64, uploadID string,
	facts ObjectInfo, product ProductMetadata,
) (*UploadRecord, error) {
	var record UploadRecord
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := lockUpload(tx, tenantID, uploadID)
		if err != nil {
			return err
		}
		if current.Status != StatusMerging {
			return ErrUploadNotUploading
		}

		now := time.Now().UTC()
		err = tx.Exec(`
			UPDATE datahub_uploads
			   SET status = ?,
			       etag = ?, object_version_id = ?, last_modified = ?,
			       file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
			       content_type = CASE WHEN ? <> '' THEN ? ELSE content_type END,
			       description = ?, category = ?, important_key = ?, version = ?,
			       completed_parts = total_parts,
			       completed_at = ?, updated_at = ?, error_msg = ''
			 WHERE tenant_id = ? AND upload_id = ?`,
			StatusMerged,
			facts.ETag, facts.VersionID, nullTime(facts.LastModified),
			facts.Size, facts.Size,
			facts.ContentType, facts.ContentType,
			product.Description, product.Category, product.ImportantKey, product.Version,
			now, now,
			tenantID, uploadID,
		).Error
		if err != nil {
			return fmt.Errorf("finalize upload merge: %w", err)
		}

		record, err = loadUpload(tx, tenantID, uploadID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *postgresUploadStore) FailUploadMerge(
	ctx context.Context, tenantID uint64, uploadID string, reason string,
) (*UploadRecord, error) {
	var record UploadRecord
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := lockUpload(tx, tenantID, uploadID)
		if err != nil {
			return err
		}
		if current.Status != StatusMerging {
			return ErrUploadNotUploading
		}

		now := time.Now().UTC()
		if err := tx.Exec(`
			UPDATE datahub_uploads
			   SET status = ?, error_msg = ?, updated_at = ?
			 WHERE tenant_id = ? AND upload_id = ?`,
			StatusFailed, reason, now, tenantID, uploadID,
		).Error; err != nil {
			return fmt.Errorf("fail upload merge: %w", err)
		}

		record, err = loadUpload(tx, tenantID, uploadID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// uploadSortColumns is the whitelist of sortable fields. Anything else falls
// back to created_at, so a client cannot sort by an arbitrary expression.
var uploadSortColumns = map[string]string{
	"upload_id":     "upload_id",
	"event_id":      "event_id",
	"user_id":       "user_id",
	"file_name":     "filename",
	"file_size":     "file_size",
	"content_type":  "content_type",
	"status":        "status",
	"created_at":    "created_at",
	"updated_at":    "updated_at",
	"last_modified": "last_modified",
}

func (s *postgresUploadStore) ListUploads(
	ctx context.Context, query UploadQuery,
) (*UploadPage, error) {
	page := &UploadPage{}

	filter := func(db *gorm.DB) *gorm.DB {
		db = db.Where("tenant_id = ?", query.TenantID)
		if query.OwnerUserID != "" {
			db = db.Where("user_id = ?", query.OwnerUserID)
		}
		if query.EventID != "" {
			db = db.Where("event_id = ?", query.EventID)
		}
		if query.FileName != "" {
			db = db.Where(`filename ILIKE ? ESCAPE '\'`, "%"+escapeLikePattern(query.FileName)+"%")
		}
		if len(query.Statuses) > 0 {
			db = db.Where("status IN ?", query.Statuses)
		}
		return db
	}

	base := s.db.WithContext(ctx).Model(&UploadRecord{})
	if err := filter(base.Session(&gorm.Session{})).Count(&page.Total).Error; err != nil {
		return nil, fmt.Errorf("count uploads: %w", err)
	}

	records := s.db.WithContext(ctx).Model(&UploadRecord{})
	records = filter(records.Session(&gorm.Session{})).
		Order(uploadSortClause(query.SortBy)).
		Offset(query.Offset).
		Limit(query.Count)
	if err := records.Find(&page.Records).Error; err != nil {
		return nil, fmt.Errorf("list uploads: %w", err)
	}
	return page, nil
}

func (s *postgresUploadStore) ListExpiredUploads(
	ctx context.Context, now time.Time, limit int,
) ([]UploadRecord, error) {
	var records []UploadRecord
	err := s.db.WithContext(ctx).
		Where("status IN ?", inFlightUploadStatuses()).
		Where("expire_at < ?", now).
		// Oldest first, so a long-stalled backlog drains in order.
		Order("expire_at ASC").
		Limit(limit).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("list expired uploads: %w", err)
	}
	return records, nil
}

func (s *postgresUploadStore) MarkUploadExpired(
	ctx context.Context, tenantID uint64, uploadID, reason string,
) error {
	err := s.db.WithContext(ctx).Model(&UploadRecord{}).
		Where("tenant_id = ? AND upload_id = ? AND status IN ?",
			tenantID, uploadID, inFlightUploadStatuses()).
		Updates(map[string]any{
			"status":     StatusCancelled,
			"error_msg":  reason,
			"updated_at": time.Now().UTC(),
		}).Error
	if err != nil {
		return fmt.Errorf("mark upload expired: %w", err)
	}
	return nil
}

func (s *postgresUploadStore) ListUploadsNeedingFacts(
	ctx context.Context, limit int,
) ([]UploadRecord, error) {
	var records []UploadRecord
	err := s.db.WithContext(ctx).
		Where("status = ?", StatusMerged).
		Where("etag = '' OR file_size <= 0 OR last_modified IS NULL OR object_key = ''").
		Order("completed_at ASC NULLS FIRST").
		Limit(limit).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("list uploads needing facts: %w", err)
	}
	return records, nil
}

// UpdateUploadFacts is deliberately column-scoped. A reconciler that wrote the
// whole row would be a second writer for product metadata and for the columns a
// future summarizer owns, which is exactly what the module's design forbids.
func (s *postgresUploadStore) UpdateUploadFacts(
	ctx context.Context, tenantID uint64, uploadID string, facts ObjectInfo,
) error {
	err := s.db.WithContext(ctx).Exec(`
		UPDATE datahub_uploads
		   SET etag = CASE WHEN ? <> '' THEN ? ELSE etag END,
		       object_version_id = CASE WHEN ? <> '' THEN ? ELSE object_version_id END,
		       content_type = CASE WHEN ? <> '' THEN ? ELSE content_type END,
		       file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END,
		       last_modified = COALESCE(?, last_modified),
		       updated_at = ?
		 WHERE tenant_id = ? AND upload_id = ?`,
		facts.ETag, facts.ETag,
		facts.VersionID, facts.VersionID,
		facts.ContentType, facts.ContentType,
		facts.Size, facts.Size,
		nullTime(facts.LastModified),
		time.Now().UTC(),
		tenantID, uploadID,
	).Error
	if err != nil {
		return fmt.Errorf("update upload facts: %w", err)
	}
	return nil
}

func (s *postgresUploadStore) KnownObjectKeys(
	ctx context.Context, limit int,
) ([]string, error) {
	var keys []string
	err := s.db.WithContext(ctx).Model(&UploadRecord{}).
		Where("object_key <> ''").
		Order("id").
		Limit(limit).
		Pluck("object_key", &keys).Error
	if err != nil {
		return nil, fmt.Errorf("list known object keys: %w", err)
	}
	return keys, nil
}

// uploadSortClause turns "‑file_size" / "created_at" into SQL, defaulting to
// newest-first.
func uploadSortClause(sortBy string) string {
	column := uploadSortColumns["created_at"]
	direction := "DESC"
	if sortBy != "" {
		field := sortBy
		if trimmed, ok := strings.CutPrefix(sortBy, "-"); ok {
			field = trimmed
			direction = "DESC"
		} else {
			direction = "ASC"
		}
		if mapped, ok := uploadSortColumns[field]; ok {
			column = mapped
		} else {
			column, direction = uploadSortColumns["created_at"], "DESC"
		}
	}
	return column + " " + direction
}

// escapeLikePattern neutralises the wildcards a client could otherwise smuggle
// into a search term.
func escapeLikePattern(value string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value)
}

// lockUpload takes the row lock the merge transitions rely on.
func lockUpload(tx *gorm.DB, tenantID uint64, uploadID string) (*UploadRecord, error) {
	var record UploadRecord
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock upload record: %w", err)
	}
	return &record, nil
}

func loadUpload(tx *gorm.DB, tenantID uint64, uploadID string) (UploadRecord, error) {
	var record UploadRecord
	err := tx.Where("tenant_id = ? AND upload_id = ?", tenantID, uploadID).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return record, ErrUploadNotFound
	}
	if err != nil {
		return record, fmt.Errorf("load upload record: %w", err)
	}
	return record, nil
}

// nullTime keeps a zero timestamp out of the database rather than storing year
// one, which is what a missing Last-Modified from object storage looks like.
func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
