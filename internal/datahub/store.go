package datahub

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// ErrUploadNotFound is returned when an Upload does not exist in the caller's
// tenant — which is also what a caller from another tenant must see, so the two
// cases are deliberately indistinguishable.
var ErrUploadNotFound = errors.New("datahub: upload not found")

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
