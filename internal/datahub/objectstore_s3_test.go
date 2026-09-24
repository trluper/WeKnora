package datahub

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// fakeS3List answers the ListObjectsV2 XML an S3 backend would, with the page
// count under the test's control. It is a RoundTripper instead of a server so
// the tests need neither a socket nor a sandbox network exception.
type fakeS3List struct {
	pageSize int
	// pages is how many pages the bucket holds; 0 means the listing never ends,
	// which is what a caller that stops early has to cope with.
	pages int
	// failPage, when non-zero, answers that 1-based page with an S3 error.
	failPage int
	requests int
}

func (f *fakeS3List) RoundTrip(r *http.Request) (*http.Response, error) {
	f.requests++

	page := 1
	if token := r.URL.Query().Get("continuation-token"); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil {
			return nil, fmt.Errorf("fakeS3List: bad continuation token %q", token)
		}
		page = parsed
	}
	if f.failPage != 0 && page == f.failPage {
		// A 4xx so minio does not spend the test's time retrying.
		return fakeS3Response(r, http.StatusForbidden,
			`<?xml version="1.0" encoding="UTF-8"?>`+
				`<Error><Code>AccessDenied</Code><Message>listing exploded</Message></Error>`), nil
	}

	truncated := f.pages == 0 || page < f.pages
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	b.WriteString("<Name>weknora</Name><Prefix>datahub/</Prefix>")
	b.WriteString("<KeyCount>" + strconv.Itoa(f.pageSize) + "</KeyCount>")
	b.WriteString("<IsTruncated>" + strconv.FormatBool(truncated) + "</IsTruncated>")
	if truncated {
		b.WriteString("<NextContinuationToken>" + strconv.Itoa(page+1) + "</NextContinuationToken>")
	}
	for i := 0; i < f.pageSize; i++ {
		b.WriteString("<Contents><Key>" + fakeS3Key(page, i) + "</Key>")
		b.WriteString("<LastModified>2026-01-01T00:00:00.000Z</LastModified>")
		b.WriteString("<ETag>&quot;etag-" + strconv.Itoa(page) + "-" + strconv.Itoa(i) + "&quot;</ETag>")
		b.WriteString("<Size>" + strconv.Itoa(10+i) + "</Size>")
		b.WriteString("<StorageClass>STANDARD</StorageClass></Contents>")
	}
	b.WriteString("</ListBucketResult>")
	return fakeS3Response(r, http.StatusOK, b.String()), nil
}

func fakeS3Key(page, index int) string {
	return fmt.Sprintf("datahub/page-%02d/obj-%03d", page, index)
}

func fakeS3Response(r *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": []string{"application/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		Request:       r,
	}
}

func newTestObjectStore(t *testing.T, rt http.RoundTripper) *s3ObjectStore {
	t.Helper()
	client, err := minio.New("s3.test.invalid", &minio.Options{
		Creds:     credentials.NewStaticV4("test-key", "test-secret", ""),
		Secure:    false,
		Transport: rt,
	})
	require.NoError(t, err)
	return &s3ObjectStore{client: client, bucket: "weknora"}
}

func TestListObjectsStopsAtLimit(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 500})

	objects, truncated, err := store.ListObjects(context.Background(), "", objectPrefix, 3)
	require.NoError(t, err)
	require.True(t, truncated)
	require.Len(t, objects, 3)
	require.Equal(t, []string{fakeS3Key(1, 0), fakeS3Key(1, 1), fakeS3Key(1, 2)},
		[]string{objects[0].Key, objects[1].Key, objects[2].Key})
	require.Equal(t, int64(10), objects[0].Size)
	require.Equal(t, "etag-1-0", objects[0].ETag)
}

func TestListObjectsWalksEveryPageUnderTheLimit(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 4, pages: 3})

	objects, truncated, err := store.ListObjects(context.Background(), "", objectPrefix, 0)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, objects, 12)
	require.Equal(t, fakeS3Key(1, 0), objects[0].Key)
	require.Equal(t, fakeS3Key(3, 3), objects[11].Key)
}

func TestListObjectsIsNotTruncatedWhenTheLastPageFitsExactly(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 2, pages: 2})

	objects, truncated, err := store.ListObjects(context.Background(), "", objectPrefix, 4)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, objects, 4)
}

func TestListObjectsReportsListingErrors(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 4, pages: 3, failPage: 2})

	_, _, err := store.ListObjects(context.Background(), "", objectPrefix, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list objects under datahub/")
}

func TestListObjectsReportsCancelledContext(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 4, pages: 3})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := store.ListObjects(ctx, "", objectPrefix, 0)
	require.ErrorIs(t, err, context.Canceled)
}

// TestListObjectsDoesNotLeakGoroutines is the regression test for the leak:
// minio's channel-returning ListObjects runs the listing on a goroutine that
// parks forever unless the consumer drains the channel to its close, so every
// call that stops at the limit (or returns an error) would strand one.
func TestListObjectsDoesNotLeakGoroutines(t *testing.T) {
	store := newTestObjectStore(t, &fakeS3List{pageSize: 500})

	// Warm up first so one-off goroutines are already running in the baseline.
	for i := 0; i < 5; i++ {
		_, _, err := store.ListObjects(context.Background(), "", objectPrefix, 5)
		require.NoError(t, err)
	}
	settleGoroutines()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		objects, truncated, err := store.ListObjects(context.Background(), "", objectPrefix, 5)
		require.NoError(t, err)
		require.True(t, truncated)
		require.Len(t, objects, 5)
	}

	// A stranded listing goroutine never exits, so a bounded wait separates
	// "still winding down" from "leaked".
	deadline := time.Now().Add(3 * time.Second)
	for {
		leaked := runtime.NumGoroutine() - baseline
		if leaked <= 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ListObjects stranded %d goroutines after 20 bounded listings:\n%s",
				leaked, goroutineDump())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func settleGoroutines() {
	// Give anything with a scheduling delay a chance to exit before the baseline.
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
}

func goroutineDump() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}
