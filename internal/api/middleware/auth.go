package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type TokenValidationRequest struct {
	Token string `json:"token"`
}

type TokenValidationResponse struct {
	UserID string   `json:"user_id"`
	Scopes []string `json:"scopes,omitempty"`
	Scope  string   `json:"scope,omitempty"`
}

type cachedToken struct {
	valResp   TokenValidationResponse
	expiresAt time.Time
}

type TokenCache struct {
	mu    sync.RWMutex
	items map[string]cachedToken
	ttl   time.Duration
}

func NewTokenCache(ttl time.Duration) *TokenCache {
	return &TokenCache{
		items: make(map[string]cachedToken),
		ttl:   ttl,
	}
}

func (c *TokenCache) Get(token string) (TokenValidationResponse, bool) {
	c.mu.RLock()
	item, found := c.items[token]
	c.mu.RUnlock()

	if !found || time.Now().After(item.expiresAt) {
		return TokenValidationResponse{}, false
	}
	return item.valResp, true
}

func (c *TokenCache) Set(token string, valResp TokenValidationResponse) {
	c.mu.Lock()
	c.items[token] = cachedToken{
		valResp:   valResp,
		expiresAt: time.Now().Add(c.ttl),
	}
	if len(c.items) > 10000 {
		now := time.Now()
		for k, v := range c.items {
			if now.After(v.expiresAt) {
				delete(c.items, k)
			}
		}
	}
	c.mu.Unlock()
}

func defaultAuthClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}
}

func AuthMiddleware(authURL string, client *http.Client) gin.HandlerFunc {
	return AuthMiddlewareWithCache(authURL, client, NewTokenCache(60*time.Second))
}

func AuthMiddlewareWithCache(authURL string, client *http.Client, cache *TokenCache) gin.HandlerFunc {
	if client == nil {
		client = defaultAuthClient()
	}
	if cache == nil {
		cache = NewTokenCache(60 * time.Second)
	}

	return func(c *gin.Context) {
		if userID := strings.TrimSpace(c.GetHeader("X-User-Id")); userID != "" {
			c.Set("user_id", userID)
			if scopeHeader := strings.TrimSpace(c.GetHeader("X-Scope")); scopeHeader != "" {
				c.Set("scopes", strings.Split(scopeHeader, " "))
			}
			c.Next()
			return
		}

		authHeader := strings.TrimSpace(c.GetHeader("Authorization"))
		apiKey := strings.TrimSpace(c.GetHeader("X-Api-Key"))

		token := ""
		if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			token = strings.TrimSpace(authHeader[7:])
		} else if apiKey != "" {
			token = apiKey
		}

		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required or invalid credentials"})
			return
		}

		if authURL == "" {
			c.Set("user_id", "anonymous")
			c.Set("scopes", []string{"direct"})
			c.Next()
			return
		}

		if cachedVal, ok := cache.Get(token); ok {
			scopes := cachedVal.Scopes
			if len(scopes) == 0 && cachedVal.Scope != "" {
				scopes = strings.Split(cachedVal.Scope, " ")
			}
			c.Set("user_id", cachedVal.UserID)
			c.Set("scopes", scopes)
			c.Next()
			return
		}

		reqBody, _ := json.Marshal(TokenValidationRequest{Token: token})
		req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, authURL, bytes.NewReader(reqBody))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"detail": "Authentication service error"})
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required or invalid credentials"})
			return
		}
		defer resp.Body.Close()

		var valResp TokenValidationResponse
		if err := json.NewDecoder(resp.Body).Decode(&valResp); err != nil || valResp.UserID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Invalid authentication service response"})
			return
		}

		cache.Set(token, valResp)

		scopes := valResp.Scopes
		if len(scopes) == 0 && valResp.Scope != "" {
			scopes = strings.Split(valResp.Scope, " ")
		}

		c.Set("user_id", valResp.UserID)
		c.Set("scopes", scopes)
		c.Next()
	}
}

func ScopeValidator(allowedScopes ...string) gin.HandlerFunc {
	scopeMap := make(map[string]struct{}, len(allowedScopes)+1)
	for _, s := range allowedScopes {
		scopeMap[s] = struct{}{}
	}
	scopeMap["direct"] = struct{}{}

	return func(c *gin.Context) {
		var userScopes []string
		if val, exists := c.Get("scopes"); exists {
			if sList, ok := val.([]string); ok {
				userScopes = sList
			} else if sStr, ok := val.(string); ok {
				userScopes = strings.Split(sStr, " ")
			}
		}

		for _, s := range userScopes {
			if _, exists := scopeMap[s]; exists {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"detail": "Insufficient permissions. Required scopes not found."})
	}
}
