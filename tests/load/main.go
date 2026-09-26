package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"campaign-broker/internal/api"
	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/downloader"
	"campaign-broker/internal/service"
)

func main() {
	if _, err := runLoadApp(os.Args[1:]); err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		os.Exit(1)
	}
}

func runLoadApp(args []string) (PerformanceSummary, error) {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	targetFlag := fs.String("target", "", "Target URL of running broker service (leave empty to run embedded high-speed server)")
	rpsFlag := fs.Int("rps", 5000, "Target requests per second")
	durationFlag := fs.Duration("duration", 10*time.Second, "Load test duration")
	workersFlag := fs.Int("workers", 500, "Number of concurrent worker goroutines")
	warmupFlag := fs.Duration("warmup", 2*time.Second, "Warmup duration before measured run")
	if err := fs.Parse(args); err != nil {
		return PerformanceSummary{}, err
	}

	targetURL := *targetFlag
	mediaBaseURL := targetURL
	var cleanup func()

	if targetURL == "" {
		gin.SetMode(gin.ReleaseMode)
		logger := logrus.New()
		logger.SetLevel(logrus.ErrorLevel)

		mediaServer := NewMockMediaServer()
		mockPub := NewMockPublisher()
		dl := downloader.NewHTTPDownloader(5 * time.Second)
		brokerService := service.NewBrokerService(mockPub, dl)
		brokerHandler := handlers.NewBrokerHandler(brokerService, logger)

		router := api.SetupRouter(api.RouterConfig{
			AuthURL:       "",
			BrokerHandler: brokerHandler,
			Logger:        logger,
		})

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return PerformanceSummary{}, fmt.Errorf("failed to create listener: %w", err)
		}

		server := &http.Server{
			Handler: router,
		}

		go func() {
			_ = server.Serve(listener)
		}()

		targetURL = fmt.Sprintf("http://%s", listener.Addr().String())
		mediaBaseURL = mediaServer.URL()

		cleanup = func() {
			_ = server.Shutdown(context.Background())
			mediaServer.Close()
			_ = mockPub.Close()
		}
		defer cleanup()

		fmt.Printf("Started embedded broker server on %s (Media Mock on %s)\n", targetURL, mediaBaseURL)
	}

	fmt.Printf("Running load test against %s\n", targetURL)
	fmt.Printf("Configuration: %d RPS | %d Workers | Duration: %v | Warmup: %v\n",
		*rpsFlag, *workersFlag, *durationFlag, *warmupFlag)

	if *warmupFlag > 0 {
		fmt.Printf("Warming up for %v...\n", *warmupFlag)
		warmupRunner := NewLoadRunner(LoadConfig{
			TargetBaseURL: targetURL,
			MediaBaseURL:  mediaBaseURL,
			RPS:           *rpsFlag / 2,
			Duration:      *warmupFlag,
			Concurrency:   *workersFlag / 2,
		})
		_ = warmupRunner.Run(context.Background())
	}

	fmt.Printf("Executing main load plateau for %v at %d RPS...\n", *durationFlag, *rpsFlag)
	runner := NewLoadRunner(LoadConfig{
		TargetBaseURL: targetURL,
		MediaBaseURL:  mediaBaseURL,
		RPS:           *rpsFlag,
		Duration:      *durationFlag,
		Concurrency:   *workersFlag,
	})

	summary := runner.Run(context.Background())
	fmt.Print(summary.String())

	if errCount, exists := summary.StatusCodes[500]; exists && errCount > 0 {
		return summary, fmt.Errorf("observed %d HTTP 500 responses", errCount)
	}
	if summary.P99Latency > 50*time.Millisecond {
		fmt.Printf("WARNING: p99 latency (%v) exceeded 50ms threshold.\n", summary.P99Latency)
	}

	return summary, nil
}
