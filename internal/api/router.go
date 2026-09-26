package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/api/middleware"
)

type RouterConfig struct {
	AuthURL       string
	BrokerHandler *handlers.BrokerHandler
	Logger        *logrus.Logger
}

func SetupRouter(cfg RouterConfig) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/health", cfg.BrokerHandler.Health)

	authorized := r.Group("")
	authorized.Use(middleware.AuthMiddleware(cfg.AuthURL, nil))
	authorized.Use(middleware.ScopeValidator("api/all:write", "api/broker:write"))
	{
		authorized.POST("/conversations/webhook/broker/message", cfg.BrokerHandler.HandleBrokerMessage)
		authorized.POST("/conversations/webhook/broker/messages", cfg.BrokerHandler.HandleBrokerMessages)
	}

	return r
}
