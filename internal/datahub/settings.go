// Package datahub implements WeKnora's file-upload and test-data archival
// module: chunked file upload straight to object storage, plus per-board test
// results with an event-level summary.
//
// The module is deliberately self-contained. It shares WeKnora's tenants,
// authentication and database, but owns its tables, routes and vocabulary.
// See CONTEXT.md for the glossary and docs/adr/0001..0005 for the decisions
// behind its shape.
package datahub

import (
	"fmt"
	"os"
	"strings"
)

// Datahub supports exactly one database driver and one family of storage
// backends — see docs/adr/0002-postgres-only-migrations.md and
// docs/adr/0004-direct-multipart-upload.md.
const (
	driverPostgres = "postgres"

	providerMinIO = "minio"
	providerS3    = "s3"

	// WeKnora defaults STORAGE_TYPE to local when it is unset, and local
	// storage cannot serve multipart uploads.
	defaultStorageType = "local"
)

// ObjectStorage carries the object-storage coordinates Datahub needs. The
// fields mirror the env vars WeKnora's own file service reads, so a
// deployment configures storage once and both services agree on it.
type ObjectStorage struct {
	Provider        string
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	UseSSL          bool
}

// Settings is the deployment configuration Datahub needs.
//
// Enabled is derived from existing configuration rather than a new env var:
// Datahub turns itself on exactly when the deployment already uses an
// S3-compatible object store, because that is the one thing it cannot run
// without. Deployments that store files locally — including the shipped
// lite mode — are left completely untouched.
type Settings struct {
	Enabled       bool
	DBDriver      string
	StorageType   string
	ObjectStorage ObjectStorage
}

// LoadSettingsFromEnv reads Datahub settings from the process environment.
func LoadSettingsFromEnv() (Settings, error) {
	return LoadSettings(os.Getenv)
}

// LoadSettings validates deployment configuration.
//
// getenv is injected so the rejection and disablement paths can be tested
// without mutating process state.
func LoadSettings(getenv func(string) string) (Settings, error) {
	storageType := strings.ToLower(strings.TrimSpace(getenv("STORAGE_TYPE")))
	if storageType == "" {
		storageType = defaultStorageType
	}
	driver := strings.ToLower(strings.TrimSpace(getenv("DB_DRIVER")))

	settings := Settings{
		DBDriver:    driver,
		StorageType: storageType,
	}

	switch storageType {
	case providerMinIO:
		settings.ObjectStorage = ObjectStorage{
			Provider:        providerMinIO,
			Endpoint:        strings.TrimSpace(getenv("MINIO_ENDPOINT")),
			AccessKeyID:     strings.TrimSpace(getenv("MINIO_ACCESS_KEY_ID")),
			SecretAccessKey: getenv("MINIO_SECRET_ACCESS_KEY"),
			Bucket:          strings.TrimSpace(getenv("MINIO_BUCKET_NAME")),
			UseSSL:          strings.EqualFold(getenv("MINIO_USE_SSL"), "true"),
		}
	case providerS3:
		settings.ObjectStorage = ObjectStorage{
			Provider:        providerS3,
			Endpoint:        strings.TrimSpace(getenv("S3_ENDPOINT")),
			Region:          strings.TrimSpace(getenv("S3_REGION")),
			AccessKeyID:     strings.TrimSpace(getenv("S3_ACCESS_KEY")),
			SecretAccessKey: getenv("S3_SECRET_KEY"),
			Bucket:          strings.TrimSpace(getenv("S3_BUCKET_NAME")),
			UseSSL:          true,
		}
	default:
		// Local storage, or an object store Datahub has no client for
		// (cos / tos / obs / oss / dummy). Stay out of the way rather
		// than refusing to boot a deployment that never asked for us.
		return settings, nil
	}

	settings.Enabled = true
	if err := settings.ObjectStorage.validate(); err != nil {
		return settings, fmt.Errorf("datahub: %s configuration is incomplete: %w", storageType, err)
	}
	if driver != driverPostgres {
		// Storage is S3-compatible, so Datahub is meant to be on — and
		// it only ships Postgres migrations.
		return Settings{}, fmt.Errorf(
			"datahub is enabled by STORAGE_TYPE=%s but requires DB_DRIVER=%s, got %q",
			storageType, driverPostgres, displayOrUnset(driver),
		)
	}
	return settings, nil
}

func (o ObjectStorage) validate() error {
	var missing []string
	require := func(value, name string) {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, name)
		}
	}
	if o.Provider == providerMinIO {
		require(o.Endpoint, "MINIO_ENDPOINT")
	} else {
		require(o.Region, "S3_REGION")
	}
	require(o.AccessKeyID, credentialEnvName(o.Provider, "ACCESS_KEY"))
	require(o.SecretAccessKey, credentialEnvName(o.Provider, "SECRET"))
	require(o.Bucket, bucketEnvName(o.Provider))

	if len(missing) > 0 {
		return fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func credentialEnvName(provider, kind string) string {
	if provider == providerMinIO {
		switch kind {
		case "ACCESS_KEY":
			return "MINIO_ACCESS_KEY_ID"
		default:
			return "MINIO_SECRET_ACCESS_KEY"
		}
	}
	if kind == "ACCESS_KEY" {
		return "S3_ACCESS_KEY"
	}
	return "S3_SECRET_KEY"
}

func bucketEnvName(provider string) string {
	if provider == providerMinIO {
		return "MINIO_BUCKET_NAME"
	}
	return "S3_BUCKET_NAME"
}

func displayOrUnset(value string) string {
	if value == "" {
		return "<unset>"
	}
	return value
}
