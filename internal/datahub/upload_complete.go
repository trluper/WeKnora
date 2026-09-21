package datahub

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

const (
	maxDescriptionLength  = 2000
	maxCategoryLength     = 64
	maxImportantKeyLength = 2000
	maxVersionLength      = 64
)

type completePartInfo struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}

// completeUploadRequest keeps the sibling implementation's contract. The client
// still sends the parts it believes it uploaded; object storage's own listing is
// what this module trusts, so those entries are accepted and not used.
type completeUploadRequest struct {
	UploadID     string             `json:"upload_id"`
	UserID       string             `json:"user_id"`
	Parts        []completePartInfo `json:"parts"`
	Description  string             `json:"description"`
	Category     string             `json:"category"`
	ImportantKey string             `json:"important_key"`
	Version      string             `json:"version"`
}

type completeUploadResponse struct {
	UploadID    string `json:"upload_id"`
	Status      string `json:"status"`
	StoragePath string `json:"storage_path"`
	FileHash    string `json:"file_hash"`
}

func (r *completeUploadRequest) validate() error {
	switch {
	case r.UploadID == "":
		return errors.New("upload_id is required")
	case r.Description == "":
		return errors.New("description is required")
	case len(r.Description) > maxDescriptionLength:
		return fmt.Errorf("description must be at most %d characters", maxDescriptionLength)
	case len(r.Category) > maxCategoryLength:
		return fmt.Errorf("category must be at most %d characters", maxCategoryLength)
	case len(r.ImportantKey) > maxImportantKeyLength:
		return fmt.Errorf("important_key must be at most %d characters", maxImportantKeyLength)
	case len(r.Version) > maxVersionLength:
		return fmt.Errorf("version must be at most %d characters", maxVersionLength)
	}
	return nil
}

// completeUpload merges the parts into one object and writes the Upload record's
// object-storage facts and product metadata.
//
// The order matters. Object storage is asked which parts it actually holds
// before anything is claimed, so a client that calls complete too early gets a
// 400 without the Upload being parked in a state it cannot leave. Only then is
// the merge claimed, which is what keeps two concurrent completes from both
// calling CompleteMultipartUpload.
func (m *Module) completeUpload(c *gin.Context) {
	var req completeUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := req.validate(); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	record, allowed := m.authorizeUpload(c, req.UploadID)
	if !allowed {
		return
	}

	caller := types.CallerFromContext(c.Request.Context())
	ctx := c.Request.Context()

	// Completing twice is the client retrying, not a second upload: answer with
	// what is already stored.
	if record.Status == StatusMerged {
		respondOK(c, completeUploadResponse{
			UploadID:    record.UploadID,
			Status:      string(record.Status),
			StoragePath: record.StoragePath,
			FileHash:    record.ETag,
		})
		return
	}
	if record.Status != StatusUploading {
		respondError(c, http.StatusConflict,
			fmt.Sprintf("upload cannot be completed (status %s)", record.Status))
		return
	}

	storedParts, err := m.objects.CompletedParts(
		ctx, record.Bucket, record.ObjectKey, record.ObjectUploadID,
	)
	if err != nil {
		logger.Errorf(ctx, "[Datahub] list parts before complete failed: %v", err)
		respondError(c, http.StatusBadGateway, "object storage is unavailable")
		return
	}
	if len(storedParts) == 0 {
		respondError(c, http.StatusBadRequest, "no parts have been uploaded yet")
		return
	}

	if err := m.store.ClaimUploadMerge(ctx, caller.TenantID, req.UploadID); err != nil {
		switch {
		case errors.Is(err, ErrUploadNotFound):
			respondError(c, http.StatusNotFound, "upload not found")
		case errors.Is(err, ErrUploadNotUploading):
			respondError(c, http.StatusConflict, "upload is already being completed")
		default:
			logger.Errorf(ctx, "[Datahub] claim merge failed: %v", err)
			respondError(c, http.StatusInternalServerError, "failed to complete upload")
		}
		return
	}

	facts, err := m.objects.CompleteMultipartUpload(
		ctx, record.Bucket, record.ObjectKey, record.ObjectUploadID, storedParts,
	)
	if err != nil {
		// The merge is claimed, so it has to be resolved either way.
		if _, failErr := m.store.FailUploadMerge(ctx, caller.TenantID, req.UploadID, err.Error()); failErr != nil {
			logger.Errorf(ctx, "[Datahub] recording merge failure failed: %v", failErr)
		}
		logger.Errorf(ctx, "[Datahub] complete multipart upload failed: %v", err)
		respondError(c, http.StatusBadGateway, "object storage failed to merge the upload")
		return
	}

	// Some S3 implementations (MinIO among them) do not report Last-Modified or
	// the content type from CompleteMultipartUpload, so ask for the object's own
	// facts once. Best-effort: a failed probe leaves the reconciler to fill them
	// in later rather than failing an upload that has already been merged.
	if probed, probeErr := m.objects.StatObject(ctx, record.Bucket, record.ObjectKey); probeErr == nil {
		if probed.ETag == "" {
			probed.ETag = facts.ETag
		}
		facts = probed
	} else {
		logger.Warnf(ctx,
			"[Datahub] could not read back facts after merge upload_id=%s: %v",
			req.UploadID, probeErr)
	}

	merged, err := m.store.FinalizeUploadMerge(ctx, caller.TenantID, req.UploadID, facts, ProductMetadata{
		Description:  req.Description,
		Category:     req.Category,
		ImportantKey: req.ImportantKey,
		Version:      req.Version,
	})
	if err != nil {
		logger.Errorf(ctx, "[Datahub] finalize merge failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to complete upload")
		return
	}

	respondOK(c, completeUploadResponse{
		UploadID:    merged.UploadID,
		Status:      string(merged.Status),
		StoragePath: merged.StoragePath,
		FileHash:    merged.ETag,
	})
}
