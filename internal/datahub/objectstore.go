package datahub

import (
	"context"
	"errors"
	"time"
)

// ErrObjectNotFound means object storage does not hold the object or session the
// caller asked about. It is distinct from "object storage is unreachable", which
// is worth retrying.
var ErrObjectNotFound = errors.New("datahub: object not found")

// objectPrefix namespaces every object Datahub writes. Without it a reconciler
// could not tell this module's objects from the files the rest of WeKnora keeps
// in the same bucket, and orphan detection would have to guess.
const objectPrefix = "datahub/"

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
	// StatObject reports one object's physical facts, or ErrObjectNotFound.
	StatObject(ctx context.Context, bucket, objectKey string) (ObjectInfo, error)
	// ListObjects returns the objects under prefix, bounded by limit; the bool
	// reports whether the listing was truncated.
	ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]ObjectRef, bool, error)
}

// ObjectRef is one object found by listing a prefix.
type ObjectRef struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
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
