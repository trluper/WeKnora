package datahub

import "time"

// UploadStatus is where one Upload stands in its lifecycle.
//
//	init -> uploading -> merging -> merged
//
// with failed / cancelled / error as the terminal branches.
type UploadStatus string

const (
	StatusInit      UploadStatus = "init"
	StatusUploading UploadStatus = "uploading"
	StatusMerging   UploadStatus = "merging"
	StatusMerged    UploadStatus = "merged"
	StatusFailed    UploadStatus = "failed"
	StatusCancelled UploadStatus = "cancelled"
	StatusError     UploadStatus = "error"
)

// Scenarios separate uploads by the visibility rules they live under.
const (
	ScenarioExperiment = "experiment"
	ScenarioQA         = "qa"
)

// UploadRecord is one Upload: task state, object-storage facts, product
// metadata and the reserved semantic columns, all in one row. Column names are
// spelled out because several of them would not survive GORM's snake-casing
// (ETag would become e_tag).
type UploadRecord struct {
	ID       int64  `gorm:"column:id;primaryKey"`
	TenantID uint64 `gorm:"column:tenant_id"`
	UploadID string `gorm:"column:upload_id"`
	EventID  string `gorm:"column:event_id"`
	Scenario string `gorm:"column:scenario"`
	UserID   string `gorm:"column:user_id"`
	Filename string `gorm:"column:filename"`
	FileSize int64  `gorm:"column:file_size"`

	FileHash       string            `gorm:"column:file_hash"`
	ContentType    string            `gorm:"column:content_type"`
	Bucket         string            `gorm:"column:bucket"`
	ObjectKey      string            `gorm:"column:object_key"`
	StoragePath    string            `gorm:"column:storage_path"`
	ObjectUploadID string            `gorm:"column:object_upload_id"`
	Status         UploadStatus      `gorm:"column:status"`
	TotalParts     int               `gorm:"column:total_parts"`
	CompletedParts int               `gorm:"column:completed_parts"`
	PartSize       int64             `gorm:"column:part_size"`
	Metadata       map[string]string `gorm:"column:metadata;type:jsonb;serializer:json"`

	Description  string `gorm:"column:description"`
	Category     string `gorm:"column:category"`
	ImportantKey string `gorm:"column:important_key"`
	Version      string `gorm:"column:version"`

	ETag            string     `gorm:"column:etag"`
	ObjectVersionID string     `gorm:"column:object_version_id"`
	LastModified    *time.Time `gorm:"column:last_modified"`

	ExpireAt    time.Time  `gorm:"column:expire_at"`
	CompletedAt *time.Time `gorm:"column:completed_at"`
	CreatedAt   time.Time  `gorm:"column:created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at"`

	// Reserved: no writer today. A future summarizer owns these.
	SummaryMarkdown string     `gorm:"column:summary_markdown"`
	Headline        string     `gorm:"column:headline"`
	Keywords        []string   `gorm:"column:keywords;type:jsonb;serializer:json"`
	AnalysisState   string     `gorm:"column:analysis_state"`
	QueuedAt        *time.Time `gorm:"column:queued_at"`
	StartedAt       *time.Time `gorm:"column:started_at"`
	FinishedAt      *time.Time `gorm:"column:finished_at"`
	ErrorMessage    string     `gorm:"column:error_msg"`
}

// TableName pins the physical name; the module prefix is what makes Datahub's
// tables recognisable at a glance.
func (UploadRecord) TableName() string { return "datahub_uploads" }

// UploadParts is one Upload's part-registration state.
type UploadParts struct {
	TenantID       uint64    `gorm:"column:tenant_id;primaryKey"`
	UploadID       string    `gorm:"column:upload_id;primaryKey"`
	PartBitmap     []byte    `gorm:"column:part_bitmap"`
	PartMeta       []byte    `gorm:"column:part_meta"`
	CompletedParts int       `gorm:"column:completed_parts"`
	Version        int       `gorm:"column:version"`
	IsMerged       int16     `gorm:"column:is_merged"`
	CreatedAt      time.Time `gorm:"column:created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at"`
}

func (UploadParts) TableName() string { return "datahub_upload_parts" }

// Test verdicts. A board either passed or failed; there is no third state, so
// anything else a client sends is rejected rather than stored.
const (
	TestResultFailed = 0
	TestResultPassed = 1
)

// TestDetail is one board's verdict within an Event. Re-reporting a board is a
// correction, so (tenant, event, board) is unique.
type TestDetail struct {
	ID           int64     `gorm:"column:id;primaryKey"`
	TenantID     uint64    `gorm:"column:tenant_id"`
	EventID      string    `gorm:"column:event_id"`
	BoardID      string    `gorm:"column:board_id"`
	TestResult   int16     `gorm:"column:test_result"`
	FailedReason string    `gorm:"column:failed_reason"`
	UserID       string    `gorm:"column:user_id"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (TestDetail) TableName() string { return "datahub_test_details" }

// TestSummary is one Event's overall test progress, aggregated across everyone
// who recorded a board for it.
type TestSummary struct {
	ID          int64     `gorm:"column:id;primaryKey"`
	TenantID    uint64    `gorm:"column:tenant_id"`
	EventID     string    `gorm:"column:event_id"`
	TotalCount  int64     `gorm:"column:total_count"`
	PassedCount int64     `gorm:"column:passed_count"`
	FailedCount int64     `gorm:"column:failed_count"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

func (TestSummary) TableName() string { return "datahub_test_summaries" }
