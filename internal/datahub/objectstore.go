package datahub

import (
	"context"
	"time"
)

// ObjectStore is Datahub's one outbound port. Uploads go straight from the
// client to object storage over pre-signed multipart URLs, so the module needs
// the native multipart session — something WeKnora's FileService abstraction
// does not expose (see docs/adr/0004-direct-multipart-upload.md).
//
// Keeping this behind an interface is what lets the upload flow be exercised
// end to end without running MinIO.
type ObjectStore interface {
	// CreateMultipartUpload opens a multipart session and returns its id.
	CreateMultipartUpload(ctx context.Context, bucket, objectKey string) (string, error)
	// PresignUploadPart returns a URL the client can PUT one part to.
	PresignUploadPart(
		ctx context.Context,
		bucket, objectKey, uploadID string,
		partNumber int,
		ttl time.Duration,
	) (string, error)
	// CompletedParts returns the parts object storage has actually received,
	// used both to finish an upload and to reconcile a client's claims.
	CompletedParts(
		ctx context.Context,
		bucket, objectKey, uploadID string,
	) ([]CompletedPart, error)
	// CompleteMultipartUpload assembles the final object.
	CompleteMultipartUpload(
		ctx context.Context,
		bucket, objectKey, uploadID string,
		parts []CompletedPart,
	) (ObjectInfo, error)
	// AbortMultipartUpload discards the session and its uploaded parts.
	AbortMultipartUpload(ctx context.Context, bucket, objectKey, uploadID string) error
}

// CompletedPart is one uploaded part as object storage knows it.
type CompletedPart struct {
	PartNumber int
	ETag       string
	Size       int64
}

// ObjectInfo is what object storage reports about a finished object. These are
// the physical facts an Upload record keeps.
type ObjectInfo struct {
	ETag         string
	VersionID    string
	ContentType  string
	Size         int64
	LastModified time.Time
}
