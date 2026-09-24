package datahub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// partListPageSize is how many parts one ListObjectParts page asks for.
const partListPageSize = 1000

// s3ObjectStore talks the S3 multipart protocol to whatever S3-compatible
// endpoint the deployment configured. Datahub owns this client rather than
// going through WeKnora's FileService, which has no multipart surface — see
// docs/adr/0004-direct-multipart-upload.md.
type s3ObjectStore struct {
	// client signs URLs (a client-level call); core drives the multipart
	// session (minio-go keeps those on Core rather than Client).
	client *minio.Client
	core   *minio.Core
	bucket string
}

// NewObjectStore builds the S3/MinIO client for the configured storage.
//
// The endpoint goes through WeKnora's SSRF validation exactly like the file
// service does: a deployment that points object storage at a restricted
// address is misconfigured, and failing here beats an upload that mysteriously
// cannot reach its parts.
func NewObjectStore(settings ObjectStorage) (ObjectStore, error) {
	endpoint := settings.Endpoint
	if endpoint == "" {
		return nil, fmt.Errorf("datahub: %s endpoint is not configured", settings.Provider)
	}
	if err := utils.ValidateURLForSSRF(endpoint); err != nil {
		return nil, fmt.Errorf("datahub: unsafe object storage endpoint %q: %w", endpoint, err)
	}

	options := &minio.Options{
		Creds:  credentials.NewStaticV4(settings.AccessKeyID, settings.SecretAccessKey, ""),
		Secure: settings.UseSSL,
		Region: settings.Region,
		Transport: &utils.SSRFValidatingRoundTripper{
			Base: utils.NewSSRFSafeTransport(utils.DefaultSSRFSafeHTTPClientConfig()),
		},
	}
	client, err := minio.New(endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("datahub: failed to initialize object storage client: %w", err)
	}
	core, err := minio.NewCore(endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("datahub: failed to initialize object storage multipart client: %w", err)
	}

	return &s3ObjectStore{client: client, core: core, bucket: settings.Bucket}, nil
}

func (s *s3ObjectStore) CreateMultipartUpload(
	ctx context.Context, bucket, objectKey string,
) (string, error) {
	uploadID, err := s.core.NewMultipartUpload(
		ctx, s.bucketName(bucket), objectKey, minio.PutObjectOptions{},
	)
	if err != nil {
		return "", fmt.Errorf("create multipart upload: %w", err)
	}
	return uploadID, nil
}

// PresignUploadPart signs PUT /<object>?uploadId=…&partNumber=N, the S3
// multipart part-upload form the client already speaks.
func (s *s3ObjectStore) PresignUploadPart(
	ctx context.Context,
	bucket, objectKey, uploadID string,
	partNumber int,
	ttl time.Duration,
) (string, error) {
	params := url.Values{}
	params.Set("uploadId", uploadID)
	params.Set("partNumber", strconv.Itoa(partNumber))

	signed, err := s.client.Presign(
		ctx, http.MethodPut, s.bucketName(bucket), objectKey, ttl, params,
	)
	if err != nil {
		return "", fmt.Errorf("presign part %d: %w", partNumber, err)
	}
	return signed.String(), nil
}

func (s *s3ObjectStore) CompletedParts(
	ctx context.Context, bucket, objectKey, uploadID string,
) ([]CompletedPart, error) {
	var parts []CompletedPart
	marker := 0
	for {
		page, err := s.core.ListObjectParts(
			ctx, s.bucketName(bucket), objectKey, uploadID, marker, partListPageSize,
		)
		if err != nil {
			return nil, fmt.Errorf("list upload parts: %w", err)
		}
		for _, part := range page.ObjectParts {
			parts = append(parts, CompletedPart{
				PartNumber: part.PartNumber,
				ETag:       part.ETag,
				Size:       part.Size,
			})
		}
		if !page.IsTruncated {
			return parts, nil
		}
		marker = page.NextPartNumberMarker
	}
}

func (s *s3ObjectStore) CompleteMultipartUpload(
	ctx context.Context, bucket, objectKey, uploadID string, parts []CompletedPart,
) (ObjectInfo, error) {
	completeParts := make([]minio.CompletePart, 0, len(parts))
	for _, part := range parts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}

	info, err := s.core.CompleteMultipartUpload(
		ctx, s.bucketName(bucket), objectKey, uploadID, completeParts, minio.PutObjectOptions{},
	)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("complete multipart upload: %w", err)
	}
	return ObjectInfo{
		ETag:         info.ETag,
		VersionID:    info.VersionID,
		Size:         info.Size,
		LastModified: info.LastModified,
	}, nil
}

func (s *s3ObjectStore) AbortMultipartUpload(
	ctx context.Context, bucket, objectKey, uploadID string,
) error {
	if err := s.core.AbortMultipartUpload(
		ctx, s.bucketName(bucket), objectKey, uploadID,
	); err != nil {
		return fmt.Errorf("abort multipart upload: %w", err)
	}
	return nil
}

func (s *s3ObjectStore) StatObject(
	ctx context.Context, bucket, objectKey string,
) (ObjectInfo, error) {
	info, err := s.client.StatObject(ctx, s.bucketName(bucket), objectKey, minio.StatObjectOptions{})
	if err != nil {
		if isNoSuchKey(err) {
			return ObjectInfo{}, fmt.Errorf("%w: %s", ErrObjectNotFound, objectKey)
		}
		return ObjectInfo{}, fmt.Errorf("stat object %s: %w", objectKey, err)
	}
	return ObjectInfo{
		ETag:         info.ETag,
		VersionID:    info.VersionID,
		ContentType:  info.ContentType,
		Size:         info.Size,
		LastModified: info.LastModified,
	}, nil
}

func (s *s3ObjectStore) ListObjects(
	ctx context.Context, bucket, prefix string, limit int,
) ([]ObjectRef, bool, error) {
	objects := make([]ObjectRef, 0, 64)

	// ListObjectsIter rather than the channel-returning ListObjects, on purpose:
	// minio runs the channel form on a goroutine that only stops once the
	// consumer drains it to the close, so stopping early — at limit, or on the
	// first error — leaves that goroutine parked on its channel for good. The
	// iterator form stops where the caller stops and spawns nothing.
	for object := range s.client.ListObjectsIter(ctx, s.bucketName(bucket), minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if object.Err != nil {
			return nil, false, fmt.Errorf("list objects under %s: %w", prefix, object.Err)
		}
		if limit > 0 && len(objects) >= limit {
			// The caller asked for at most limit objects; this is the whole
			// answer as far as it is concerned.
			return objects, true, nil
		}
		objects = append(objects, ObjectRef{
			Key:          object.Key,
			Size:         object.Size,
			LastModified: object.LastModified,
			ETag:         object.ETag,
		})
	}

	// The listing ran out without reaching limit. If the context was cancelled
	// along the way, the pages never fetched make this a partial listing, and
	// reporting it as a complete one would let a reconciler read "nothing left
	// in the bucket" into a listing it never finished. ListObjectsIter stops
	// silently on cancellation, where the channel form surfaced ctx.Err().
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("list objects under %s: %w", prefix, err)
	}
	return objects, false, nil
}

// isNoSuchKey recognises a missing object across the error shapes minio-go
// returns for the S3 and MinIO backends.
func isNoSuchKey(err error) bool {
	var responseErr minio.ErrorResponse
	if errors.As(err, &responseErr) {
		return responseErr.Code == "NoSuchKey" || responseErr.Code == "NoSuchBucket"
	}
	return false
}

func (s *s3ObjectStore) bucketName(bucket string) string {
	if bucket != "" {
		return bucket
	}
	return s.bucket
}
