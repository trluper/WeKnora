package datahub

import (
	"context"
	"net/http"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// routePrefix is where Datahub hangs off the authenticated /api/v1 group.
const routePrefix = "/datahub"

// dependency timeout bounds every health probe.
const dependencyTimeout = 3 * time.Second

// Module is Datahub's assembled runtime. Later tickets hang the upload store,
// the test-data store and the background workers off this struct.
type Module struct {
	settings Settings
	db       *gorm.DB
	redis    *redis.Client
	objects  ObjectStore
	store    UploadStore
}

// Parts are the collaborators a Module runs on. Grouping them keeps the
// constructor readable as later tickets add to it.
type Parts struct {
	DB      *gorm.DB
	Redis   *redis.Client
	Objects ObjectStore
	Store   UploadStore
}

// New builds the Datahub module from the deployment environment.
//
// A deployment that is not configured for Datahub — local storage, or an
// object store this module has no client for — gets a disabled module that
// mounts nothing. A deployment that *is* configured for it but cannot support
// it (a non-Postgres database) is a hard startup error rather than a silent
// no-op.
func New(db *gorm.DB, redisClient *redis.Client) (*Module, error) {
	settings, err := LoadSettingsFromEnv()
	if err != nil {
		return nil, err
	}
	if !settings.Enabled {
		logger.Infof(context.Background(),
			"[Datahub] disabled: STORAGE_TYPE=%s is not an S3-compatible object store",
			settings.StorageType)
		return newModule(settings, Parts{DB: db, Redis: redisClient}), nil
	}

	objects, err := NewObjectStore(settings.ObjectStorage)
	if err != nil {
		return nil, err
	}
	logger.Infof(context.Background(),
		"[Datahub] enabled: provider=%s bucket=%s",
		settings.ObjectStorage.Provider, settings.ObjectStorage.Bucket)
	return newModule(settings, Parts{
		DB:      db,
		Redis:   redisClient,
		Objects: objects,
		Store:   NewPostgresUploadStore(db),
	}), nil
}

// newModule assembles a Module from already-validated parts. Tests inject fakes
// for the object store and the upload store.
func newModule(settings Settings, parts Parts) *Module {
	return &Module{
		settings: settings,
		db:       parts.DB,
		redis:    parts.Redis,
		objects:  parts.Objects,
		store:    parts.Store,
	}
}

// RegisterRoutes mounts Datahub on the authenticated API group. It is the only
// route-level touch point in the existing codebase.
func RegisterRoutes(group *gin.RouterGroup, module *Module) {
	if group == nil || module == nil || !module.settings.Enabled {
		return
	}
	datahub := group.Group(routePrefix)
	datahub.Use(requireCaller())
	datahub.GET("/health", module.health)
	datahub.POST("/upload/init", module.initUpload)
}

// requireCaller rejects requests that reached the module without an
// authenticated identity. In a full deployment the auth middleware has already
// rejected them; this makes the module fail closed on its own rather than
// depending on where it happens to be mounted.
func requireCaller() gin.HandlerFunc {
	return func(c *gin.Context) {
		caller := types.CallerFromContext(c.Request.Context())
		if caller.TenantID == 0 || caller.UserID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "authentication required",
			})
			return
		}
		c.Next()
	}
}

type dependencyState struct {
	Status string `json:"status"`
}

type objectStorageState struct {
	Status   string `json:"status"`
	Wired    bool   `json:"wired"`
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
}

type healthResponse struct {
	Module       string `json:"module"`
	Status       string `json:"status"`
	Enabled      bool   `json:"enabled"`
	Dependencies struct {
		Database      dependencyState    `json:"database"`
		Redis         dependencyState    `json:"redis"`
		ObjectStorage objectStorageState `json:"object_storage"`
	} `json:"dependencies"`
}

// health reports what Datahub is wired to. It is the walking skeleton's proof
// that configuration, dependency injection and routing all reach the module.
func (m *Module) health(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), dependencyTimeout)
	defer cancel()

	resp := healthResponse{
		Module:  "datahub",
		Enabled: m.settings.Enabled,
	}
	resp.Dependencies.Database = m.databaseState(ctx)
	resp.Dependencies.Redis = m.redisState(ctx)
	resp.Dependencies.ObjectStorage = m.objectStorageState()

	resp.Status = "ok"
	for _, state := range []string{
		resp.Dependencies.Database.Status,
		resp.Dependencies.Redis.Status,
		resp.Dependencies.ObjectStorage.Status,
	} {
		// Redis is legitimately absent in lite mode; a missing object
		// storage client is a real gap until the S3 client is wired.
		if state != "ok" && state != "disabled" {
			resp.Status = "degraded"
			break
		}
	}

	c.JSON(http.StatusOK, resp)
}

func (m *Module) databaseState(ctx context.Context) dependencyState {
	if m.db == nil {
		return dependencyState{Status: "not_wired"}
	}
	sqlDB, err := m.db.DB()
	if err != nil {
		return dependencyState{Status: "unhealthy"}
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return dependencyState{Status: "unhealthy"}
	}
	return dependencyState{Status: "ok"}
}

func (m *Module) redisState(ctx context.Context) dependencyState {
	if m.redis == nil {
		return dependencyState{Status: "disabled"}
	}
	if err := m.redis.Ping(ctx).Err(); err != nil {
		return dependencyState{Status: "unhealthy"}
	}
	return dependencyState{Status: "ok"}
}

func (m *Module) objectStorageState() objectStorageState {
	state := objectStorageState{
		Wired:    m.objects != nil,
		Provider: m.settings.ObjectStorage.Provider,
		Bucket:   m.settings.ObjectStorage.Bucket,
	}
	if state.Wired {
		state.Status = "ok"
		return state
	}
	state.Status = "not_wired"
	return state
}
