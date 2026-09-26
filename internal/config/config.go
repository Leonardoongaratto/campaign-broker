package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                   string
	RabbitMQURL            string
	RabbitMQChannelPool    int
	AuthURL                string
	DownloadTimeoutSeconds time.Duration
}

func LoadConfig() *Config {
	_ = godotenv.Load()

	port := getEnv("PORT", "8080")
	authURL := getEnv("AUTH_URL", "")

	rabbitHost := getEnv("RABBITMQ_HOST", "localhost")
	rabbitPort := getEnv("RABBITMQ_PORT", "5672")
	rabbitUser := getEnv("RABBITMQ_USERNAME", "guest")
	rabbitPass := getEnv("RABBITMQ_PASSWORD", "guest")
	useSSL := getEnv("RABBITMQ_USE_SSL", "false") == "true"

	scheme := "amqp"
	if useSSL {
		scheme = "amqps"
	}
	rabbitURL := fmt.Sprintf("%s://%s:%s@%s:%s/", scheme, rabbitUser, rabbitPass, rabbitHost, rabbitPort)

	poolSize, err := strconv.Atoi(getEnv("RABBITMQ_CHANNEL_POOL_SIZE", "10"))
	if err != nil || poolSize <= 0 {
		poolSize = 10
	}

	timeoutSec, err := strconv.Atoi(getEnv("DOWNLOAD_TIMEOUT_SECONDS", "15"))
	if err != nil || timeoutSec <= 0 {
		timeoutSec = 15
	}

	return &Config{
		Port:                   port,
		RabbitMQURL:            rabbitURL,
		RabbitMQChannelPool:    poolSize,
		AuthURL:                authURL,
		DownloadTimeoutSeconds: time.Duration(timeoutSec) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}
