package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"campaign-broker/internal/domain"
	"campaign-broker/internal/service"
)

type MockPublisher struct {
	mu        sync.Mutex
	Published []PublishedItem
	Fail      bool
}

type PublishedItem struct {
	RoutingKey string
	Message    any
}

func (m *MockPublisher) Publish(ctx context.Context, routingKey string, message any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail {
		return errors.New("amqp publish failure")
	}
	m.Published = append(m.Published, PublishedItem{
		RoutingKey: routingKey,
		Message:    message,
	})
	return nil
}

func (m *MockPublisher) Close() error {
	return nil
}

type MockDownloader struct {
	ResultBase64 string
	Err          error
}

func (m *MockDownloader) DownloadAndEncodeBase64(ctx context.Context, fileURL string, fileToken string) (string, error) {
	if m.Err != nil {
		return "", m.Err
	}
	return m.ResultBase64, nil
}

func strPtr(s string) *string {
	return &s
}

func TestBrokerService_ProcessBrokerMessage(t *testing.T) {
	t.Run("empty destinations returns error", func(t *testing.T) {
		svc := service.NewBrokerService(&MockPublisher{}, &MockDownloader{})
		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", &domain.OutboundRequest{
			Destinations: []domain.Destination{},
		})
		require.ErrorIs(t, err, service.ErrNoDestinations)
	})

	t.Run("empty message returns error", func(t *testing.T) {
		svc := service.NewBrokerService(&MockPublisher{}, &MockDownloader{})
		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511999999999"}},
			Message:      domain.RawMessage{},
		})
		require.ErrorIs(t, err, service.ErrEmptyMessage)
	})

	t.Run("publish text message successfully", func(t *testing.T) {
		pub := &MockPublisher{}
		svc := service.NewBrokerService(pub, &MockDownloader{})

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{
				{Destination: "+5511999999999", CorrelationID: strPtr("corr_1")},
			},
			FlowID:        strPtr("flow_1"),
			CampaignAlias: strPtr("alias_1"),
			Message: domain.RawMessage{
				MessageText: strPtr("Hello Campaign Broker"),
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_123", "", req)
		require.NoError(t, err)
		require.Len(t, pub.Published, 1)

		item := pub.Published[0]
		assert.Equal(t, "channels_queue", item.RoutingKey)
		amqpMsg, ok := item.Message.(domain.AMQPChannelMessage)
		require.True(t, ok)
		assert.Equal(t, "text", amqpMsg.Type)
		assert.Equal(t, "Hello Campaign Broker", *amqpMsg.MessageText)
		assert.Equal(t, "+5511999999999", amqpMsg.PhoneNumber)
		assert.Equal(t, "owner_123", amqpMsg.OwnerID)
	})

	t.Run("publish template without campaignId to channels_queue", func(t *testing.T) {
		pub := &MockPublisher{}
		svc := service.NewBrokerService(pub, &MockDownloader{})

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511988888888"}},
			Message: domain.RawMessage{
				Template: &domain.TemplateModel{
					ElementName: "hello_tpl",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.NoError(t, err)
		require.Len(t, pub.Published, 1)
		assert.Equal(t, "channels_queue", pub.Published[0].RoutingKey)
	})

	t.Run("publish template with campaignId to campaign_queue", func(t *testing.T) {
		pub := &MockPublisher{}
		svc := service.NewBrokerService(pub, &MockDownloader{})

		req := &domain.OutboundRequest{
			CampaignID:   strPtr("camp_456"),
			Destinations: []domain.Destination{{Destination: "+5511988888888"}},
			Message: domain.RawMessage{
				Template: &domain.TemplateModel{
					ElementName: "promo_tpl",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.NoError(t, err)
		require.Len(t, pub.Published, 1)
		assert.Equal(t, "campaign_queue", pub.Published[0].RoutingKey)
	})

	t.Run("publish document message with download", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{ResultBase64: "base64_doc_stream"}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511977777777"}},
			Message: domain.RawMessage{
				Document: &domain.FileData{
					URL:     strPtr("https://files.com/doc.pdf"),
					Caption: strPtr("My Invoice"),
					Type:    "PDF",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "tok", req)
		require.NoError(t, err)
		require.Len(t, pub.Published, 1)

		amqpMsg := pub.Published[0].Message.(domain.AMQPChannelMessage)
		assert.Equal(t, "document", amqpMsg.Type)
		assert.Equal(t, "base64_doc_stream", *amqpMsg.DocumentData)
		assert.Equal(t, "My Invoice", *amqpMsg.Caption)
		assert.Equal(t, "PDF", *amqpMsg.DocType)
	})

	t.Run("document download failure returns error", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{Err: errors.New("network error")}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511977777777"}},
			Message: domain.RawMessage{
				Document: &domain.FileData{
					URL:  strPtr("https://files.com/fail.pdf"),
					Type: "PDF",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.ErrorIs(t, err, service.ErrFailedDownloadDocument)
	})

	t.Run("publish image message with download", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{ResultBase64: "base64_image_stream"}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511966666666"}},
			Message: domain.RawMessage{
				Image: &domain.FileData{
					URL:     strPtr("https://files.com/image.png"),
					Caption: strPtr("Photo"),
					Type:    "PNG",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.NoError(t, err)

		amqpMsg := pub.Published[0].Message.(domain.AMQPChannelMessage)
		assert.Equal(t, "image", amqpMsg.Type)
		assert.Equal(t, "base64_image_stream", *amqpMsg.ImageData)
	})

	t.Run("image download failure returns error", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{Err: errors.New("network error")}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511966666666"}},
			Message: domain.RawMessage{
				Image: &domain.FileData{
					URL:  strPtr("https://files.com/fail.png"),
					Type: "PNG",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.ErrorIs(t, err, service.ErrFailedDownloadImage)
	})

	t.Run("publish video message with download", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{ResultBase64: "base64_video_stream"}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511955555555"}},
			Message: domain.RawMessage{
				Video: &domain.FileData{
					URL:  strPtr("https://files.com/video.mp4"),
					Type: "MP4",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.NoError(t, err)

		amqpMsg := pub.Published[0].Message.(domain.AMQPChannelMessage)
		assert.Equal(t, "video", amqpMsg.Type)
		assert.Equal(t, "base64_video_stream", *amqpMsg.VideoData)
	})

	t.Run("video download failure returns error", func(t *testing.T) {
		pub := &MockPublisher{}
		dl := &MockDownloader{Err: errors.New("network error")}
		svc := service.NewBrokerService(pub, dl)

		req := &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511955555555"}},
			Message: domain.RawMessage{
				Video: &domain.FileData{
					URL:  strPtr("https://files.com/fail.mp4"),
					Type: "MP4",
				},
			},
		}

		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
		require.ErrorIs(t, err, service.ErrFailedDownloadVideo)
	})

	t.Run("publish interactive messages (reply button, list, order)", func(t *testing.T) {
		pub := &MockPublisher{}
		svc := service.NewBrokerService(pub, &MockDownloader{})

		types := []string{"REPLY_BUTTON", "LIST", "ORDER"}
		for _, it := range types {
			req := &domain.OutboundRequest{
				Destinations: []domain.Destination{{Destination: "+5511944444444"}},
				Message: domain.RawMessage{
					Interactive: &domain.InteractiveModel{
						MessageInteractiveType: it,
						ReplyButtonAction: &domain.ReplyButtonAction{
							Buttons: []domain.ReplyButton{{Type: "reply"}},
						},
						ListAction: &domain.ListAction{
							Button: "View",
						},
						OrderAction: map[string]any{"key": "val"},
					},
				},
			}

			err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", req)
			require.NoError(t, err)
		}

		assert.Len(t, pub.Published, 3)
	})

	t.Run("nil publisher returns ErrPublisherUnavailable", func(t *testing.T) {
		svc := service.NewBrokerService(nil, &MockDownloader{})
		err := svc.ProcessBrokerMessage(context.Background(), "owner_1", "", &domain.OutboundRequest{
			Destinations: []domain.Destination{{Destination: "+5511999999999"}},
			Message: domain.RawMessage{
				MessageText: strPtr("Hello"),
			},
		})
		require.ErrorIs(t, err, service.ErrPublisherUnavailable)
	})

	t.Run("concurrency stress test with 120 parallel goroutines", func(t *testing.T) {
		pub := &MockPublisher{}
		svc := service.NewBrokerService(pub, &MockDownloader{})

		const concurrency = 120
		var wg sync.WaitGroup
		wg.Add(concurrency)
		errs := make(chan error, concurrency)

		for i := 0; i < concurrency; i++ {
			go func(idx int) {
				defer wg.Done()
				req := &domain.OutboundRequest{
					Destinations: []domain.Destination{{Destination: "+5511999999999"}},
					Message: domain.RawMessage{
						MessageText: strPtr("Concurrent text payload"),
					},
				}
				if err := svc.ProcessBrokerMessage(context.Background(), "owner_123", "", req); err != nil {
					errs <- err
				}
			}(i)
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}

		assert.Len(t, pub.Published, concurrency)
	})
}

func BenchmarkBrokerService_ProcessBrokerMessage_Text(b *testing.B) {
	pub := &MockPublisher{}
	svc := service.NewBrokerService(pub, &MockDownloader{})
	ctx := context.Background()

	req := &domain.OutboundRequest{
		Destinations: []domain.Destination{{Destination: "+5511999999999"}},
		Message: domain.RawMessage{
			MessageText: strPtr("Benchmark text payload for Campaign Broker"),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = svc.ProcessBrokerMessage(ctx, "owner_bench", "", req)
	}
}

func BenchmarkBrokerService_ProcessBrokerMessage_Template(b *testing.B) {
	pub := &MockPublisher{}
	svc := service.NewBrokerService(pub, &MockDownloader{})
	ctx := context.Background()

	req := &domain.OutboundRequest{
		CampaignID:   strPtr("campaign_123"),
		Destinations: []domain.Destination{{Destination: "+5511999999999"}},
		Message: domain.RawMessage{
			Template: &domain.TemplateModel{
				Namespace:      "custom_ns",
				ElementName:    "welcome_tpl",
				LanguageCode:   "pt_BR",
				BodyParameters: []any{"Leonardo", "Broker"},
				Buttons: []domain.TemplateButton{
					{Type: "QUICK_REPLY", ReplyPayload: strPtr("opt_1")},
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = svc.ProcessBrokerMessage(ctx, "owner_bench", "", req)
	}
}

func BenchmarkBrokerService_ProcessBrokerMessage_Interactive(b *testing.B) {
	pub := &MockPublisher{}
	svc := service.NewBrokerService(pub, &MockDownloader{})
	ctx := context.Background()

	req := &domain.OutboundRequest{
		Destinations: []domain.Destination{{Destination: "+5511999999999"}},
		Message: domain.RawMessage{
			Interactive: &domain.InteractiveModel{
				MessageInteractiveType: "REPLY_BUTTON",
				AlternativeText:        strPtr("Alt text"),
				ReplyButtonAction: &domain.ReplyButtonAction{
					Buttons: []domain.ReplyButton{
						{Type: "reply", Reply: map[string]string{"title": "Yes", "payload": "1"}},
					},
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = svc.ProcessBrokerMessage(ctx, "owner_bench", "", req)
	}
}
