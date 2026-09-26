package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	ErrPublisherClosed = errors.New("amqp publisher is closed")
	ErrBorrowTimeout   = errors.New("timeout borrowing amqp channel from pool")
)

type AMQPChannel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type Publisher interface {
	Publish(ctx context.Context, routingKey string, message any) error
	Close() error
}

type ChannelPoolPublisher struct {
	amqpURL  string
	poolSize int
	logger   *logrus.Logger
	conn     *amqp.Connection
	channels chan AMQPChannel
	mu       sync.Mutex
	isClosed atomic.Bool
	dialFn   func(url string) (*amqp.Connection, error)
}

func NewChannelPoolPublisher(amqpURL string, poolSize int, logger *logrus.Logger) (*ChannelPoolPublisher, error) {
	if poolSize <= 0 {
		poolSize = 10
	}

	p := &ChannelPoolPublisher{
		amqpURL:  amqpURL,
		poolSize: poolSize,
		logger:   logger,
		channels: make(chan AMQPChannel, poolSize),
		dialFn: func(url string) (*amqp.Connection, error) {
			return amqp.DialConfig(url, amqp.Config{
				Dial: amqp.DefaultDial(2 * time.Second),
			})
		},
	}

	if err := p.connect(); err != nil {
		return nil, err
	}

	return p, nil
}

func NewChannelPoolPublisherWithChannels(channels []AMQPChannel, logger *logrus.Logger) *ChannelPoolPublisher {
	poolSize := len(channels)
	if poolSize <= 0 {
		poolSize = 1
	}
	chChan := make(chan AMQPChannel, poolSize)
	for _, ch := range channels {
		chChan <- ch
	}

	return &ChannelPoolPublisher{
		poolSize: poolSize,
		logger:   logger,
		channels: chChan,
	}
}

func (p *ChannelPoolPublisher) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, err := p.dialFn(p.amqpURL)
	if err != nil {
		return fmt.Errorf("failed to dial amqp: %w", err)
	}
	p.conn = conn

	for i := 0; i < p.poolSize; i++ {
		ch, err := conn.Channel()
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("failed to open amqp channel: %w", err)
		}
		p.channels <- ch
	}

	return nil
}

func (p *ChannelPoolPublisher) borrowChannel(ctx context.Context) (AMQPChannel, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch, ok := <-p.channels:
		if !ok {
			return nil, ErrPublisherClosed
		}
		return ch, nil
	default:
	}

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch, ok := <-p.channels:
		if !ok {
			return nil, ErrPublisherClosed
		}
		return ch, nil
	case <-timer.C:
		return nil, ErrBorrowTimeout
	}
}

func (p *ChannelPoolPublisher) returnChannel(ch AMQPChannel, isErr bool) {
	if p.isClosed.Load() {
		if ch != nil {
			_ = ch.Close()
		}
		return
	}

	if isErr || ch == nil {
		p.mu.Lock()
		defer p.mu.Unlock()

		if ch != nil {
			_ = ch.Close()
		}
		if p.conn != nil && !p.conn.IsClosed() {
			if newCh, err := p.conn.Channel(); err == nil {
				select {
				case p.channels <- newCh:
				default:
					_ = newCh.Close()
				}
			}
		}
		return
	}

	select {
	case p.channels <- ch:
	default:
		_ = ch.Close()
	}
}

func (p *ChannelPoolPublisher) Publish(ctx context.Context, routingKey string, message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal amqp message: %w", err)
	}

	ch, err := p.borrowChannel(ctx)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         data,
	}

	publishErr := ch.Publish("", routingKey, false, false, pub)
	p.returnChannel(ch, publishErr != nil)

	if publishErr != nil {
		p.logger.WithField("routing_key", routingKey).Errorf("failed to publish amqp message: %v", publishErr)
		return publishErr
	}

	return nil
}

func (p *ChannelPoolPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed.Swap(true) {
		return nil
	}

	close(p.channels)
	for ch := range p.channels {
		_ = ch.Close()
	}

	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Close()
	}

	return nil
}
