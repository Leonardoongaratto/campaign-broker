package amqp_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	brokerAmqp "campaign-broker/internal/amqp"
)

type MockChannel struct {
	mu         sync.Mutex
	PublishErr error
	Closed     bool
	Published  []amqp.Publishing
}

func (m *MockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PublishErr != nil {
		return m.PublishErr
	}
	m.Published = append(m.Published, msg)
	return nil
}

func (m *MockChannel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

func TestChannelPoolPublisher_Publish(t *testing.T) {
	logger := logrus.New()

	t.Run("successful publish using channel pool", func(t *testing.T) {
		mockCh := &MockChannel{}
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{mockCh}, logger)

		ctx := context.Background()
		err := pool.Publish(ctx, "channels_queue", map[string]string{"type": "text"})
		require.NoError(t, err)
		assert.Len(t, mockCh.Published, 1)

		err = pool.Close()
		require.NoError(t, err)
		assert.True(t, mockCh.Closed)
	})

	t.Run("publish error returns error", func(t *testing.T) {
		mockCh := &MockChannel{PublishErr: errors.New("amqp write failure")}
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{mockCh}, logger)

		ctx := context.Background()
		err := pool.Publish(ctx, "channels_queue", map[string]string{"type": "text"})
		require.Error(t, err)
	})

	t.Run("borrow channel timeout", func(t *testing.T) {
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{}, logger)

		ctx := context.Background()
		err := pool.Publish(ctx, "channels_queue", map[string]string{"type": "text"})
		require.ErrorIs(t, err, brokerAmqp.ErrBorrowTimeout)
	})

	t.Run("borrow channel with cancelled context", func(t *testing.T) {
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{}, logger)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := pool.Publish(ctx, "channels_queue", map[string]string{"type": "text"})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("publish on closed pool returns error", func(t *testing.T) {
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{}, logger)
		_ = pool.Close()

		err := pool.Publish(context.Background(), "channels_queue", map[string]string{"type": "text"})
		require.ErrorIs(t, err, brokerAmqp.ErrPublisherClosed)
	})

	t.Run("publish with unmarshalable payload returns error", func(t *testing.T) {
		mockCh := &MockChannel{}
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels([]brokerAmqp.AMQPChannel{mockCh}, logger)

		err := pool.Publish(context.Background(), "channels_queue", make(chan int))
		require.Error(t, err)
	})

	t.Run("massive concurrency stress test with 120 parallel goroutines", func(t *testing.T) {
		const poolSize = 10
		channels := make([]brokerAmqp.AMQPChannel, poolSize)
		for i := 0; i < poolSize; i++ {
			channels[i] = &MockChannel{}
		}
		pool := brokerAmqp.NewChannelPoolPublisherWithChannels(channels, logger)

		const concurrency = 120
		var wg sync.WaitGroup
		wg.Add(concurrency)
		errs := make(chan error, concurrency)

		for i := 0; i < concurrency; i++ {
			go func(idx int) {
				defer wg.Done()
				err := pool.Publish(context.Background(), "channels_queue", map[string]any{
					"type":  "text",
					"index": idx,
				})
				if err != nil {
					errs <- err
				}
			}(i)
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}

		err := pool.Close()
		require.NoError(t, err)
	})
}

func TestChannelPoolPublisher_FailDial(t *testing.T) {
	logger := logrus.New()
	_, err := brokerAmqp.NewChannelPoolPublisher("amqp://127.0.0.1:59999", 5, logger)
	assert.Error(t, err)

	_, err = brokerAmqp.NewChannelPoolPublisher("amqp://127.0.0.1:59999", -1, logger)
	assert.Error(t, err)
}

func BenchmarkChannelPoolPublisher_Publish(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	channels := make([]brokerAmqp.AMQPChannel, 30)
	for i := 0; i < 30; i++ {
		channels[i] = &MockChannel{}
	}
	pool := brokerAmqp.NewChannelPoolPublisherWithChannels(channels, logger)
	defer pool.Close()

	ctx := context.Background()
	msg := map[string]string{"type": "text", "body": "benchmark message content"}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pool.Publish(ctx, "channels_queue", msg); err != nil {
				b.Fatalf("benchmark publish error: %v", err)
			}
		}
	})
}
