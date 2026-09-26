package amqp

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type internalMockChannel struct {
	closed bool
}

func (m *internalMockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

func (m *internalMockChannel) Close() error {
	m.closed = true
	return nil
}

func TestChannelPoolPublisher_InternalEdgeCases(t *testing.T) {
	logger := logrus.New()

	t.Run("returnChannel when closed closes channel", func(t *testing.T) {
		p := &ChannelPoolPublisher{
			poolSize: 1,
			logger:   logger,
			channels: make(chan AMQPChannel, 1),
		}
		p.isClosed.Store(true)
		ch := &internalMockChannel{}
		p.returnChannel(ch, false)
		assert.True(t, ch.closed)
	})

	t.Run("returnChannel when channel is nil and error", func(t *testing.T) {
		p := &ChannelPoolPublisher{
			poolSize: 1,
			logger:   logger,
			channels: make(chan AMQPChannel, 1),
		}
		p.returnChannel(nil, true)
	})

	t.Run("returnChannel when buffer is full closes channel", func(t *testing.T) {
		p := &ChannelPoolPublisher{
			poolSize: 1,
			logger:   logger,
			channels: make(chan AMQPChannel, 1),
		}
		existingCh := &internalMockChannel{}
		p.channels <- existingCh

		extraCh := &internalMockChannel{}
		p.returnChannel(extraCh, false)
		assert.True(t, extraCh.closed)
	})

	t.Run("double close is idempotent", func(t *testing.T) {
		p := &ChannelPoolPublisher{
			poolSize: 1,
			logger:   logger,
			channels: make(chan AMQPChannel, 1),
		}
		ch := &internalMockChannel{}
		p.channels <- ch

		err := p.Close()
		assert.NoError(t, err)
		assert.True(t, ch.closed)

		err = p.Close()
		assert.NoError(t, err)
	})
}
