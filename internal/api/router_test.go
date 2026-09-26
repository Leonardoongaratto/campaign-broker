package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"campaign-broker/internal/api"
	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/service"
)

type MockPublisher struct{}

func (m *MockPublisher) Publish(ctx context.Context, routingKey string, message any) error {
	return nil
}

func (m *MockPublisher) Close() error {
	return nil
}

func TestSetupRouter(t *testing.T) {
	logger := logrus.New()
	svc := service.NewBrokerService(&MockPublisher{}, nil)
	handler := handlers.NewBrokerHandler(svc, logger)

	r := api.SetupRouter(api.RouterConfig{
		AuthURL:       "http://auth.local",
		BrokerHandler: handler,
		Logger:        logger,
	})

	require.NotNil(t, r)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
