package middleware_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"campaign-broker/internal/api/middleware"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestAuthMiddleware(t *testing.T) {
	t.Run("missing token returns 401", func(t *testing.T) {
		r := gin.New()
		r.Use(middleware.AuthMiddleware("http://auth.local", nil))
		r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("valid x-user-id header bypasses remote auth", func(t *testing.T) {
		r := gin.New()
		r.Use(middleware.AuthMiddleware("http://auth.local", nil))
		r.GET("/test", func(c *gin.Context) {
			userID, _ := c.Get("user_id")
			c.JSON(http.StatusOK, gin.H{"user_id": userID})
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-User-Id", "user_123")
		req.Header.Set("X-Scope", "api/all:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "user_123")
	})

	t.Run("remote auth validation success", func(t *testing.T) {
		authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(middleware.TokenValidationResponse{
				UserID: "auth_user_456",
				Scopes: []string{"api/broker:write"},
			})
		}))
		defer authServer.Close()

		r := gin.New()
		r.Use(middleware.AuthMiddleware(authServer.URL, authServer.Client()))
		r.GET("/test", func(c *gin.Context) {
			userID, _ := c.Get("user_id")
			c.JSON(http.StatusOK, gin.H{"user_id": userID})
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "Bearer valid_jwt_token")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "auth_user_456")
	})

	t.Run("empty authURL sets anonymous direct", func(t *testing.T) {
		r := gin.New()
		r.Use(middleware.AuthMiddleware("", nil))
		r.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Api-Key", "dummy_key")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("cached token avoids second http call", func(t *testing.T) {
		callCount := 0
		authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(middleware.TokenValidationResponse{
				UserID: "cached_user_789",
				Scope:  "api/all:write",
			})
		}))
		defer authServer.Close()

		cache := middleware.NewTokenCache(10 * time.Second)
		r := gin.New()
		r.Use(middleware.AuthMiddlewareWithCache(authServer.URL, authServer.Client(), cache))
		r.GET("/test", func(c *gin.Context) {
			userID, _ := c.Get("user_id")
			c.JSON(http.StatusOK, gin.H{"user_id": userID})
		})

		// First call -> hits authServer
		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req1.Header.Set("Authorization", "Bearer cached_jwt_token")
		r.ServeHTTP(w1, req1)
		assert.Equal(t, http.StatusOK, w1.Code)
		assert.Equal(t, 1, callCount)

		// Second call -> hits cache
		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.Header.Set("Authorization", "Bearer cached_jwt_token")
		r.ServeHTTP(w2, req2)
		assert.Equal(t, http.StatusOK, w2.Code)
		assert.Equal(t, 1, callCount)
	})
}

func TestTokenCache_Unit(t *testing.T) {
	cache := middleware.NewTokenCache(50 * time.Millisecond)

	_, found := cache.Get("non_existent")
	assert.False(t, found)

	cache.Set("tok1", middleware.TokenValidationResponse{UserID: "u1"})
	val, found := cache.Get("tok1")
	assert.True(t, found)
	assert.Equal(t, "u1", val.UserID)

	time.Sleep(60 * time.Millisecond)
	_, foundAfterExpire := cache.Get("tok1")
	assert.False(t, foundAfterExpire)
}

func TestScopeValidator(t *testing.T) {
	t.Run("allowed scope grants access", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set("scopes", []string{"api/broker:write"})
			c.Next()
		})
		r.Use(middleware.ScopeValidator("api/all:write", "api/broker:write"))
		r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("direct scope grants access", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set("scopes", "direct")
			c.Next()
		})
		r.Use(middleware.ScopeValidator("api/all:write"))
		r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("missing scope returns 403", func(t *testing.T) {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set("scopes", []string{"unrelated:read"})
			c.Next()
		})
		r.Use(middleware.ScopeValidator("api/all:write", "api/broker:write"))
		r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusForbidden, w.Code)
	})
}
