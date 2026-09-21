package datahub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// maxPartsPerRequest bounds one registration batch. The sibling implementation
// capped it at 20; the ceiling here is loose on purpose, so a client that
// batches differently is not rejected for it.
const maxPartsPerRequest = 1000

type partRegisterInfo struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

// registerPartRequest keeps the sibling implementation's contract, including
// the single-part form (part_number / etag / size at the top level) that older
// clients still send.
type registerPartRequest struct {
	UploadID   string             `json:"upload_id"`
	UserID     string             `json:"user_id"`
	Parts      []partRegisterInfo `json:"parts"`
	PartNumber *int               `json:"part_number"`
	ETag       *string            `json:"etag"`
	Size       *int64             `json:"size"`
}

type registerPartResponse struct {
	UploadID       string `json:"upload_id"`
	CompletedParts int    `json:"completed_parts"`
	TotalParts     int    `json:"total_parts"`
	IsComplete     bool   `json:"is_complete"`
}

type partInfo struct {
	PartNumber int       `json:"part_number"`
	ETag       string    `json:"etag"`
	Size       int64     `json:"size"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

type listPartsResponse struct {
	UploadID       string     `json:"upload_id"`
	Parts          []partInfo `json:"parts"`
	CompletedParts int        `json:"completed_parts"`
	TotalParts     int        `json:"total_parts"`
}

// normalize folds the legacy single-part fields into the batch form so the rest
// of the flow only deals with one shape.
func (r *registerPartRequest) normalize() {
	if len(r.Parts) > 0 || r.PartNumber == nil {
		return
	}
	part := partRegisterInfo{PartNumber: *r.PartNumber}
	if r.ETag != nil {
		part.ETag = *r.ETag
	}
	if r.Size != nil {
		part.Size = *r.Size
	}
	r.Parts = []partRegisterInfo{part}
}

// validate covers everything checkable without loading the Upload. The
// out-of-range check needs the Upload's total_parts and happens after loading.
func (r *registerPartRequest) validate() error {
	if r.UploadID == "" {
		return errors.New("upload_id is required")
	}
	if len(r.Parts) == 0 {
		return errors.New("parts is required")
	}
	if len(r.Parts) > maxPartsPerRequest {
		return fmt.Errorf("parts exceeds the %d part limit per request", maxPartsPerRequest)
	}
	for _, part := range r.Parts {
		switch {
		case part.PartNumber < 1:
			return errors.New("part_number must be at least 1")
		case part.ETag == "":
			return fmt.Errorf("etag is required for part %d", part.PartNumber)
		case part.Size < 1:
			return fmt.Errorf("size must be at least 1 for part %d", part.PartNumber)
		}
	}
	return nil
}

// registerUploadParts merges a batch of arrived parts. Registering the same part
// again is a no-op, so a client that retries a batch — or a whole upload — gets
// the same answer instead of duplicates.
func (m *Module) registerUploadParts(c *gin.Context) {
	var req registerPartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid request body")
		return
	}
	req.normalize()
	if err := req.validate(); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	record, allowed := m.authorizeUpload(c, req.UploadID)
	if !allowed {
		return
	}

	for _, part := range req.Parts {
		if part.PartNumber > record.TotalParts {
			respondError(c, http.StatusBadRequest,
				fmt.Sprintf("part_number %d out of range [1, %d]",
					part.PartNumber, record.TotalParts))
			return
		}
	}
	if record.Status != StatusUploading {
		respondError(c, http.StatusConflict,
			fmt.Sprintf("upload is not accepting parts (status %s)", record.Status))
		return
	}

	arrived := make([]CompletedPart, 0, len(req.Parts))
	for _, part := range req.Parts {
		arrived = append(arrived, CompletedPart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
			Size:       part.Size,
		})
	}

	caller := types.CallerFromContext(c.Request.Context())
	result, err := m.store.RegisterParts(
		c.Request.Context(), caller.TenantID, req.UploadID, arrived,
	)
	if err != nil {
		switch {
		case errors.Is(err, ErrUploadNotFound):
			respondError(c, http.StatusNotFound, "upload not found")
		case errors.Is(err, ErrUploadNotUploading), errors.Is(err, ErrPartsConflict):
			// Both are retryable from the client's point of view: the
			// second means the row moved under us.
			respondError(c, http.StatusConflict, "upload is not accepting parts, please retry")
		default:
			logger.Errorf(c.Request.Context(), "[Datahub] register parts failed: %v", err)
			respondError(c, http.StatusInternalServerError, "failed to register parts")
		}
		return
	}

	respondOK(c, registerPartResponse{
		UploadID:       req.UploadID,
		CompletedParts: result.CompletedParts,
		TotalParts:     result.TotalParts,
		IsComplete:     result.CompletedParts >= result.TotalParts,
	})
}

// listUploadParts reports which parts are registered, which is what a client
// needs after an interruption to resume only the missing ones.
func (m *Module) listUploadParts(c *gin.Context) {
	uploadID := c.Param("upload_id")
	if uploadID == "" {
		respondError(c, http.StatusBadRequest, "upload_id is required")
		return
	}

	record, allowed := m.authorizeUpload(c, uploadID)
	if !allowed {
		return
	}

	caller := types.CallerFromContext(c.Request.Context())
	row, err := m.store.GetParts(c.Request.Context(), caller.TenantID, uploadID)
	if err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			respondError(c, http.StatusNotFound, "upload not found")
			return
		}
		logger.Errorf(c.Request.Context(), "[Datahub] list parts failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to list parts")
		return
	}

	entries, err := decodePartMeta(row.PartMeta)
	if err != nil {
		logger.Errorf(c.Request.Context(), "[Datahub] decode part metadata failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to list parts")
		return
	}

	parts := make([]partInfo, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, partInfo{
			PartNumber: entry.PartNumber,
			ETag:       entry.ETag,
			Size:       entry.Size,
			Status:     "uploaded",
			// Timestamps are kept per row rather than per part, so every part
			// reports when the row last changed.
			CreatedAt: row.UpdatedAt,
		})
	}

	respondOK(c, listPartsResponse{
		UploadID:       record.UploadID,
		Parts:          parts,
		CompletedParts: row.CompletedParts,
		TotalParts:     record.TotalParts,
	})
}

// authorizeUpload loads an Upload and decides whether the caller may act on it.
// It writes the refusal itself and reports whether the caller may continue. A
// missing Upload and another tenant's Upload are both 404, so existence never
// leaks across tenants; a same-tenant Upload belonging to somebody else is 403.
func (m *Module) authorizeUpload(c *gin.Context, uploadID string) (*UploadRecord, bool) {
	ctx := c.Request.Context()
	caller := types.CallerFromContext(ctx)

	record, err := m.store.Get(ctx, caller.TenantID, uploadID)
	if err != nil {
		if errors.Is(err, ErrUploadNotFound) {
			respondError(c, http.StatusNotFound, "upload not found")
			return nil, false
		}
		logger.Errorf(ctx, "[Datahub] load upload failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to load upload")
		return nil, false
	}
	if !callerMayActOnUpload(ctx, record, caller) {
		respondError(c, http.StatusForbidden, "not allowed to access this upload")
		return nil, false
	}
	return record, true
}

// callerMayActOnUpload implements the module's visibility rule: the Record
// owner, or Tenant admin and above.
func callerMayActOnUpload(
	ctx context.Context, record *UploadRecord, caller types.Caller,
) bool {
	if record.TenantID != caller.TenantID {
		return false
	}
	if record.UserID == caller.UserID {
		return true
	}
	return caller.Role.HasPermission(types.TenantRoleAdmin) ||
		types.IsSystemAdminFromContext(ctx)
}
