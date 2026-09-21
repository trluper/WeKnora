package datahub

import (
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

const (
	// maxVersionList caps one version listing; the sibling implementation
	// returned everything, which is unbounded for a long-lived account.
	maxVersionList = 1000
	// metadataPageSize is the page size used when a client asks for something
	// silly, matching the sibling implementation's behaviour.
	metadataPageSize = 80
	metadataMaxSize  = 80
)

type uploadVersionInfo struct {
	UploadID  string    `json:"upload_id"`
	EventID   string    `json:"event_id"`
	FileName  string    `json:"file_name"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

type uploadVersionListResponse struct {
	Versions []uploadVersionInfo `json:"versions"`
}

// searchMetadataRequest is the sibling implementation's request contract.
type searchMetadataRequest struct {
	UserID   string `json:"user_id"`
	Count    int64  `json:"count"`
	Offset   int64  `json:"offset"`
	SortBy   string `json:"sort_by"`
	EventID  string `json:"event_id"`
	FileName string `json:"file_name"`
}

// searchMetadataItem is likewise its response contract, field for field.
type searchMetadataItem struct {
	UploadID      string    `json:"upload_id"`
	EventID       string    `json:"event_id"`
	UserID        string    `json:"user_id"`
	Bucket        string    `json:"bucket"`
	ObjectKey     string    `json:"object_key"`
	FileName      string    `json:"file_name"`
	FileSize      int64     `json:"file_size"`
	ETag          string    `json:"etag"`
	ContentType   string    `json:"content_type"`
	VersionID     string    `json:"version_id"`
	Status        string    `json:"status"`
	Summary       string    `json:"summary,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
	Description   string    `json:"description"`
	AnalysisState string    `json:"analysis_state,omitempty"`
}

type searchMetadataListResponse struct {
	Total int64                `json:"total"`
	List  []searchMetadataItem `json:"list"`
}

// callerOwnsWholeTenant reports whether the caller may see every Record owner's
// uploads in the tenant.
func callerOwnsWholeTenant(ctx *gin.Context, caller types.Caller) bool {
	return caller.Role.HasPermission(types.TenantRoleAdmin) ||
		types.IsSystemAdminFromContext(ctx.Request.Context())
}

// scopeToCaller narrows a listing to what the caller may see: everything in the
// tenant for admins and above, their own records for everybody else.
func scopeToCaller(ctx *gin.Context, caller types.Caller, query UploadQuery) UploadQuery {
	query.TenantID = caller.TenantID
	if !callerOwnsWholeTenant(ctx, caller) {
		query.OwnerUserID = caller.UserID
	}
	return query
}

// listUploadVersions answers "what have I uploaded before", which is what the
// upload form uses to chain versions. Only finished uploads count: an Upload
// that is still receiving parts is not a version of anything yet.
func (m *Module) listUploadVersions(c *gin.Context) {
	caller := types.CallerFromContext(c.Request.Context())
	query := scopeToCaller(c, caller, UploadQuery{
		Statuses: []UploadStatus{StatusMerged},
		Count:    maxVersionList,
	})

	page, err := m.store.ListUploads(c.Request.Context(), query)
	if err != nil {
		logger.Errorf(c.Request.Context(), "[Datahub] list upload versions failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to list upload versions")
		return
	}

	versions := make([]uploadVersionInfo, 0, len(page.Records))
	for _, record := range page.Records {
		versions = append(versions, uploadVersionInfo{
			UploadID:  record.UploadID,
			EventID:   record.EventID,
			FileName:  record.Filename,
			Version:   record.Version,
			CreatedAt: record.CreatedAt,
		})
	}
	respondOK(c, uploadVersionListResponse{Versions: versions})
}

// searchUploadMetadata is the metadata search the product metadata columns were
// added for: find my uploads by event, by filename fragment, or just page
// through them.
func (m *Module) searchUploadMetadata(c *gin.Context) {
	var req searchMetadataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid request body")
		return
	}

	count := int(req.Count)
	if count <= 0 || count > metadataMaxSize {
		count = metadataPageSize
	}
	offset := int(req.Offset)
	if offset < 0 {
		offset = 0
	}

	caller := types.CallerFromContext(c.Request.Context())
	query := scopeToCaller(c, caller, UploadQuery{
		EventID:  req.EventID,
		FileName: req.FileName,
		SortBy:   req.SortBy,
		Offset:   offset,
		Count:    count,
	})

	page, err := m.store.ListUploads(c.Request.Context(), query)
	if err != nil {
		logger.Errorf(c.Request.Context(), "[Datahub] search upload metadata failed: %v", err)
		respondError(c, http.StatusInternalServerError, "failed to search upload metadata")
		return
	}

	list := make([]searchMetadataItem, 0, len(page.Records))
	for _, record := range page.Records {
		list = append(list, searchMetadataItem{
			UploadID:    record.UploadID,
			EventID:     record.EventID,
			UserID:      record.UserID,
			Bucket:      record.Bucket,
			ObjectKey:   record.ObjectKey,
			FileName:    record.Filename,
			FileSize:    record.FileSize,
			ETag:        record.ETag,
			ContentType: record.ContentType,
			VersionID:   record.ObjectVersionID,
			Status:      string(record.Status),
			// Both come from the columns reserved for a future summarizer, so
			// they are empty until something writes them.
			Summary:       record.Headline,
			UpdatedAt:     record.UpdatedAt,
			Description:   record.Description,
			AnalysisState: record.AnalysisState,
		})
	}

	respondOK(c, searchMetadataListResponse{Total: page.Total, List: list})
}
