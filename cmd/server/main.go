package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"

	"campaign-broker/internal/amqp"
	"campaign-broker/internal/api"
	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/config"
	"campaign-broker/internal/downloader"
	"campaign-broker/internal/service"
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	cfg := config.LoadConfig()

	logger.Infof("Starting campaign-broker on port %s", cfg.Port)

	var publisher amqp.Publisher
	pub, err := amqp.NewChannelPoolPublisher(cfg.RabbitMQURL, cfg.RabbitMQChannelPool, logger)
	if err != nil {
		logger.Warnf("RabbitMQ initial connection failed (service starting in degraded mode): %v", err)
	} else {
		publisher = pub
		defer func() {
			if err := publisher.Close(); err != nil {
				logger.Errorf("Error closing AMQP publisher: %v", err)
			}
		}()
	}

	dl := downloader.NewHTTPDownloader(cfg.DownloadTimeoutSeconds)
	brokerService := service.NewBrokerService(publisher, dl)
	brokerHandler := handlers.NewBrokerHandler(brokerService, logger)

	router := api.SetupRouter(api.RouterConfig{
		AuthURL:       cfg.AuthURL,
		BrokerHandler: brokerHandler,
		Logger:        logger,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("Failed to listen and serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down broker service gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Errorf("Server forced to shutdown: %v", err)
	}

	logger.Info("Broker service exited successfully")
}
