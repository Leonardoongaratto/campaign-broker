package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"campaign-broker/internal/config"
)

func TestLoadConfig(t *testing.T) {
	t.Run("default configuration values", func(t *testing.T) {
		os.Clearenv()
		cfg := config.LoadConfig()

		assert.Equal(t, "8080", cfg.Port)
		assert.Equal(t, 10, cfg.RabbitMQChannelPool)
		assert.Equal(t, 15*time.Second, cfg.DownloadTimeoutSeconds)
		assert.Contains(t, cfg.RabbitMQURL, "amqp://guest:guest@localhost:5672/")
	})

	t.Run("custom environment values with ssl", func(t *testing.T) {
		os.Setenv("PORT", "9090")
		os.Setenv("RABBITMQ_USE_SSL", "true")
		os.Setenv("RABBITMQ_HOST", "rabbit.prod")
		os.Setenv("RABBITMQ_PORT", "5671")
		os.Setenv("RABBITMQ_CHANNEL_POOL_SIZE", "25")
		os.Setenv("DOWNLOAD_TIMEOUT_SECONDS", "30")
		defer os.Clearenv()

		cfg := config.LoadConfig()

		assert.Equal(t, "9090", cfg.Port)
		assert.Equal(t, 25, cfg.RabbitMQChannelPool)
		assert.Equal(t, 30*time.Second, cfg.DownloadTimeoutSeconds)
		assert.Contains(t, cfg.RabbitMQURL, "amqps://guest:guest@rabbit.prod:5671/")
	})
}
