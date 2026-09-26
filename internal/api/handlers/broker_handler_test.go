package handlers_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"campaign-broker/internal/api"
	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/service"
)

func init() {
	gin.SetMode(gin.TestMode)
}

type MockPublisher struct {
	Published []any
	Fail      bool
}

func (m *MockPublisher) Publish(ctx context.Context, routingKey string, message any) error {
	if m.Fail {
		return errors.New("amqp error")
	}
	m.Published = append(m.Published, message)
	return nil
}

func (m *MockPublisher) Close() error {
	return nil
}

type MockDownloader struct {
	Fail bool
}

func (m *MockDownloader) DownloadAndEncodeBase64(ctx context.Context, fileURL string, fileToken string) (string, error) {
	if m.Fail {
		return "", errors.New("download failed")
	}
	return "encoded_base64_content", nil
}

func setupTestRouter(pub *MockPublisher, dl *MockDownloader) *gin.Engine {
	logger := logrus.New()
	svc := service.NewBrokerService(pub, dl)
	handler := handlers.NewBrokerHandler(svc, logger)
	return api.SetupRouter(api.RouterConfig{
		AuthURL:       "",
		BrokerHandler: handler,
		Logger:        logger,
	})
}

func TestBrokerHandler_Endpoints(t *testing.T) {
	t.Run("GET /health returns healthy", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{})
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "healthy")
	})

	t.Run("POST /conversations/webhook/broker/message returns 200 OK", func(t *testing.T) {
		pub := &MockPublisher{}
		r := setupTestRouter(pub, &MockDownloader{})

		payload := `{
			"campaignAlias": "alias",
			"destinations": [{"destination": "+5511999999999"}],
			"message": {"messageText": "Hello"}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Payload received")
		assert.Contains(t, w.Body.String(), `"statusCode":200`)
		assert.Len(t, pub.Published, 1)
	})

	t.Run("POST /conversations/webhook/broker/messages returns 202 Accepted", func(t *testing.T) {
		pub := &MockPublisher{}
		r := setupTestRouter(pub, &MockDownloader{})

		payload := `{
			"destinations": [{"destination": "+5511999999999"}],
			"message": {"messageText": "Hello Multi"}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/messages", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/all:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusAccepted, w.Code)
		assert.Contains(t, w.Body.String(), "Payload received")
		assert.Contains(t, w.Body.String(), `"statusCode":202`)
		assert.Len(t, pub.Published, 1)
	})

	t.Run("invalid json body returns 400 with Failed to recieve payload", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(`invalid json`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Failed to recieve payload")
	})

	t.Run("failed document download returns 400 with detail message", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{Fail: true})

		payload := `{
			"destinations": [{"destination": "+5511999999999"}],
			"message": {
				"document": {
					"url": "https://fail.com/file.pdf",
					"type": "PDF"
				}
			}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Failed to download document")
	})

	t.Run("failed image download returns 400 with detail message", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{Fail: true})

		payload := `{
			"destinations": [{"destination": "+5511999999999"}],
			"message": {
				"image": {
					"url": "https://fail.com/file.png",
					"type": "PNG"
				}
			}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Failed to download image")
	})

	t.Run("failed video download returns 400 with detail message", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{Fail: true})

		payload := `{
			"destinations": [{"destination": "+5511999999999"}],
			"message": {
				"video": {
					"url": "https://fail.com/file.mp4",
					"type": "MP4"
				}
			}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Failed to download video")
	})

	t.Run("unauthenticated request returns 401", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("unauthorized scope returns 403", func(t *testing.T) {
		r := setupTestRouter(&MockPublisher{}, &MockDownloader{})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/read:only")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("service unavailable when message broker is down", func(t *testing.T) {
		logger := logrus.New()
		svc := service.NewBrokerService(nil, &MockDownloader{})
		handler := handlers.NewBrokerHandler(svc, logger)
		r := api.SetupRouter(api.RouterConfig{
			AuthURL:       "",
			BrokerHandler: handler,
			Logger:        logger,
		})

		payload := `{
			"destinations": [{"destination": "+5511999999999"}],
			"message": {"messageText": "Hello"}
		}`

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader([]byte(payload)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "user_1")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "Message broker unavailable")
	})
}

func TestBrokerHandler_TableDriven(t *testing.T) {
	testCases := []struct {
		name           string
		endpoint       string
		method         string
		payload        string
		tokenHeader    string
		authScope      string
		dlFail         bool
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "text message singular endpoint",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"messageText":"Text test"}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "text message plural endpoint",
			endpoint:       "/conversations/webhook/broker/messages",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"messageText":"Text test plural"}}`,
			authScope:      "api/all:write",
			expectedStatus: http.StatusAccepted,
			expectedMsg:    "Payload received",
		},
		{
			name:           "template message without campaign id",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"template":{"elementName":"hello_template"}}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "template message with campaign id",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"campaignId":"camp_123","destinations":[{"destination":"+5511999999999"}],"message":{"template":{"elementName":"camp_template"}}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "document message with file token",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"document":{"url":"https://files.com/doc.pdf","type":"PDF"}}}`,
			tokenHeader:    "Bearer secret_token",
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "document download failure returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"document":{"url":"https://files.com/fail.pdf","type":"PDF"}}}`,
			authScope:      "api/broker:write",
			dlFail:         true,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to download document",
		},
		{
			name:           "image download failure returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"image":{"url":"https://files.com/fail.png","type":"PNG"}}}`,
			authScope:      "api/broker:write",
			dlFail:         true,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to download image",
		},
		{
			name:           "video download failure returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"video":{"url":"https://files.com/fail.mp4","type":"MP4"}}}`,
			authScope:      "api/broker:write",
			dlFail:         true,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to download video",
		},
		{
			name:           "interactive reply button",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"interactive":{"messageInteractiveType":"REPLY_BUTTON","replyButtonAction":{"buttons":[{"type":"reply","reply":{"title":"OK","payload":"ok"}}]}}}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "interactive list action",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"interactive":{"messageInteractiveType":"LIST","listAction":{"button":"Choose","sections":[{"rows":[{"id":"1","title":"Item 1","description":"Desc 1"}]}]}}}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "interactive order action",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"interactive":{"messageInteractiveType":"ORDER","orderAction":{"orderId":"order_123"}}}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Payload received",
		},
		{
			name:           "empty destinations returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[],"message":{"messageText":"Empty dest"}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to recieve payload",
		},
		{
			name:           "empty message content returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{}}`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to recieve payload",
		},
		{
			name:           "malformed json returns 400",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations": [malformed`,
			authScope:      "api/broker:write",
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "Failed to recieve payload",
		},
		{
			name:           "unauthorized missing token returns 401",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"messageText":"Test"}}`,
			authScope:      "",
			expectedStatus: http.StatusUnauthorized,
			expectedMsg:    "Authentication required",
		},
		{
			name:           "forbidden scope returns 403",
			endpoint:       "/conversations/webhook/broker/message",
			method:         http.MethodPost,
			payload:        `{"destinations":[{"destination":"+5511999999999"}],"message":{"messageText":"Test"}}`,
			authScope:      "api/invalid:scope",
			expectedStatus: http.StatusForbidden,
			expectedMsg:    "Insufficient permissions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &MockPublisher{}
			dl := &MockDownloader{Fail: tc.dlFail}
			r := setupTestRouter(pub, dl)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.endpoint, bytes.NewReader([]byte(tc.payload)))
			req.Header.Set("Content-Type", "application/json")

			if tc.authScope != "" {
				req.Header.Set("X-User-Id", "test_user_id")
				req.Header.Set("X-Scope", tc.authScope)
			}
			if tc.tokenHeader != "" {
				req.Header.Set("X-BV-File-Token", tc.tokenHeader)
			}

			r.ServeHTTP(w, req)

			assert.Equal(t, tc.expectedStatus, w.Code)
			assert.Contains(t, w.Body.String(), tc.expectedMsg)
		})
	}
}

func BenchmarkBrokerHandler_HandleBrokerMessage(b *testing.B) {
	pub := &MockPublisher{}
	dl := &MockDownloader{}
	r := setupTestRouter(pub, dl)

	payload := []byte(`{
		"destinations": [{"destination": "+5511999999999"}],
		"message": {"messageText": "Benchmark text payload"}
	}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/conversations/webhook/broker/message", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-Id", "bench_user")
		req.Header.Set("X-Scope", "api/broker:write")
		r.ServeHTTP(w, req)
	}
}
