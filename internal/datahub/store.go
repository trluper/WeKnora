package datahub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
