package datahub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Upload limits. These mirror the sibling implementation's configured
// defaults: a 10 GiB ceiling, 5 MiB parts, at most 10 000 parts.
const (
	maxFileSize       = int64(10) << 30
	maxParts          = 10000
	defaultPartSize   = int64(5) << 20
	defaultPresignTTL = 15 * time.Minute
	defaultTaskExpiry = 24 * time.Hour
)

// Failure classes the handler maps onto HTTP statuses. Storage trouble is a
// condition the caller may retry; store trouble is ours.
var (
	errObjectStorage = errors.New("object storage")
	errUploadStore   = errors.New("upload store")
)

// initUploadRequest keeps the sibling implementation's field names, so a client
// migrating over only changes its base URL. UserID is accepted and ignored.
type initUploadRequest struct {
	Filename    string            `json:"filename"`
	FileSize    int64             `json:"file_size"`
	ContentType string            `json:"content_type"`
	EventID     string            `json:"event_id"`
	Scenario    string            `json:"scenario"`
	TotalParts  int               `json:"total_parts"`
	UserID      string            `json:"user_id"`
	Metadata    map[string]string `json:"metadata"`
}

// initUploadResponse is likewise field-for-field the existing contract.
type initUploadResponse struct {
	UploadID      string         `json:"upload_id"`
	MinIOUploadID string         `json:"minio_upload_id"`
	Bucket        string         `json:"bucket"`
	ObjectKey     string         `json:"object_key"`
	PartSize      int64          `json:"part_size"`
	TotalParts    int            `json:"total_parts"`
	PresignedURLs map[int]string `json:"presigned_urls,omitempty"`
	ExpireAt      time.Time      `json:"expire_at"`
}

// apiResponse is the envelope the existing clients already parse.
type apiResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func respondOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, apiResponse{Code: 0, Message: "success", Data: data})
}

func respondError(c *gin.Context, status int, message string) {
	c.JSON(status, apiResponse{Code: status, Message: message})
}

func (r *initUploadRequest) normalize() {
	r.Filename = strings.TrimSpace(r.Filename)
	r.ContentType = strings.TrimSpace(r.ContentType)
	r.EventID = strings.TrimSpace(r.EventID)
	r.Scenario = strings.TrimSpace(r.Scenario)
	if r.Scenario == "" {
		r.Scenario = ScenarioExperiment
	}
}

// validate reports the first problem with the request in client-readable
// terms. Every limit is checked before any state is created, so a rejected
// request leaves nothing behind to clean up.
func (r *initUploadRequest) validate() error {
	switch {
	case r.Filename == "":
		return errors.New("filename is required")
	case len(r.Filename) > 255:
		return errors.New("filename must be at most 255 characters")
	case r.FileSize < 1:
		return errors.New("file_size must be at least 1 byte")
	case r.FileSize > maxFileSize:
		return fmt.Errorf("file_size exceeds the %d byte limit", maxFileSize)
	case r.ContentType == "":
		return errors.New("content_type is required")
	case len(r.ContentType) > 128:
		return errors.New("content_type must be at most 128 characters")
	case r.EventID == "":
		return errors.New("event_id is required")
	case len(r.EventID) > 64:
		return errors.New("event_id must be at most 64 characters")
	case len(r.Scenario) > 32:
		return errors.New("scenario must be at most 32 characters")
	case r.TotalParts < 1:
		return errors.New("total_parts must be at least 1")
	case r.TotalParts > maxParts:
		return fmt.Errorf("total_parts exceeds the %d part limit", maxParts)
	}
	return nil
}

// initUpload opens one multipart session, signs every part, and records the
// Upload as in progress. Nothing is persisted until the session exists, and any
// later failure aborts the session, so a failed init leaves no half-state.
func (m *Module) initUpload(c *gin.Context) {
	var req initUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid request body")
		return
	}
	req.normalize()
	if err := req.validate(); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	caller := types.CallerFromContext(c.Request.Context())
	response, err := m.startUpload(c.Request.Context(), caller, &req)
	if err != nil {
		if errors.Is(err, errObjectStorage) {
			logger.Errorf(c.Request.Context(), "[Datahub] init upload rejected by object storage: %v", err)
			respondError(c, http.StatusBadGateway, "object storage is unavailable")
			return
		}
		logger.Errorf(c.Request.Context(), "[Datahub] init upload failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to initialize upload")
		return
	}
	respondOK(c, response)
}

func (m *Module) startUpload(
	ctx context.Context, caller types.Caller, req *initUploadRequest,
) (*initUploadResponse, error) {
	bucket := m.settings.ObjectStorage.Bucket
	uploadID := uuid.NewString()
	objectKey := objectKeyFor(req.EventID, uploadID, req.Filename)

	objectUploadID, err := m.objects.CreateMultipartUpload(ctx, bucket, objectKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errObjectStorage, err)
	}

	presigned := make(map[int]string, req.TotalParts)
	for part := 1; part <= req.TotalParts; part++ {
		signed, err := m.objects.PresignUploadPart(
			ctx, bucket, objectKey, objectUploadID, part, defaultPresignTTL,
		)
		if err != nil {
			m.abortMultipartUpload(ctx, bucket, objectKey, objectUploadID)
			return nil, fmt.Errorf("%w: %v", errObjectStorage, err)
		}
		presigned[part] = signed
	}

	expireAt := time.Now().UTC().Add(defaultTaskExpiry)
	record := &UploadRecord{
		TenantID:       caller.TenantID,
		UploadID:       uploadID,
		EventID:        req.EventID,
		Scenario:       req.Scenario,
		UserID:         caller.UserID,
		Filename:       req.Filename,
		FileSize:       req.FileSize,
		ContentType:    req.ContentType,
		Bucket:         bucket,
		ObjectKey:      objectKey,
		StoragePath:    objectKey,
		ObjectUploadID: objectUploadID,
		Status:         StatusUploading,
		TotalParts:     req.TotalParts,
		PartSize:       defaultPartSize,
		Metadata:       req.Metadata,
		ExpireAt:       expireAt,
	}
	parts := &UploadParts{
		TenantID:   caller.TenantID,
		UploadID:   uploadID,
		PartBitmap: NewPartBitmap(req.TotalParts).Bytes(),
	}
	if err := m.store.Create(ctx, record, parts); err != nil {
		m.abortMultipartUpload(ctx, bucket, objectKey, objectUploadID)
		return nil, fmt.Errorf("%w: %v", errUploadStore, err)
	}

	return &initUploadResponse{
		UploadID:      uploadID,
		MinIOUploadID: objectUploadID,
		Bucket:        bucket,
		ObjectKey:     objectKey,
		PartSize:      defaultPartSize,
		TotalParts:    req.TotalParts,
		PresignedURLs: presigned,
		ExpireAt:      expireAt,
	}, nil
}

// abortMultipartUpload discards a session we can no longer use. Failing to
// abort is logged but never masks the error that caused it — leftovers are the
// reconciler's job.
func (m *Module) abortMultipartUpload(ctx context.Context, bucket, objectKey, uploadID string) {
	if err := m.objects.AbortMultipartUpload(ctx, bucket, objectKey, uploadID); err != nil {
		logger.Warnf(ctx,
			"[Datahub] failed to abort multipart upload object_key=%s upload_id=%s: %v",
			objectKey, uploadID, err)
	}
}

// objectKeyFor spreads objects across days so no single prefix grows without
// bound, and keeps only the basename of a client-supplied filename so a crafted
// name cannot walk out of its prefix.
func objectKeyFor(eventID, uploadID, filename string) string {
	now := time.Now().UTC()
	return path.Join(
		eventID,
		fmt.Sprintf("%d", now.Year()),
		fmt.Sprintf("%02d", now.Month()),
		fmt.Sprintf("%02d", now.Day()),
		uploadID,
		sanitizeFilename(filename),
	)
}

func sanitizeFilename(filename string) string {
	base := path.Base(strings.ReplaceAll(filename, "\\", "/"))
	if base == "" || base == "." || base == "/" {
		return "file"
	}
	return base
}
