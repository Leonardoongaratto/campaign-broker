package service

import (
	"context"
	"errors"
	"strings"

	"campaign-broker/internal/amqp"
	"campaign-broker/internal/domain"
	"campaign-broker/internal/downloader"
)

var (
	ErrNoDestinations         = errors.New("destinations list cannot be empty")
	ErrEmptyMessage           = errors.New("message content cannot be empty")
	ErrFailedDownloadDocument = errors.New("failed to download document")
	ErrFailedDownloadImage    = errors.New("failed to download image")
	ErrFailedDownloadVideo    = errors.New("failed to download video")
	ErrPublisherUnavailable   = errors.New("message broker unavailable")

	defaultTemplateNamespace = "002abc39_6fbf_4da4_8674_29b782bf572a"
	defaultTemplateLanguage  = "pt_BR"
)

type BrokerService struct {
	publisher  amqp.Publisher
	downloader downloader.Downloader
}

func NewBrokerService(publisher amqp.Publisher, dl downloader.Downloader) *BrokerService {
	return &BrokerService{
		publisher:  publisher,
		downloader: dl,
	}
}

func (s *BrokerService) ProcessBrokerMessage(ctx context.Context, ownerID string, fileToken string, req *domain.OutboundRequest) error {
	if s.publisher == nil {
		return ErrPublisherUnavailable
	}

	if len(req.Destinations) == 0 {
		return ErrNoDestinations
	}

	dest := req.Destinations[0]
	phoneNumber := dest.Destination
	correlationID := dest.CorrelationID
	flowID := req.FlowID
	campaignAlias := req.CampaignAlias
	campaignID := req.CampaignID
	if campaignID == nil || *campaignID == "" {
		campaignID = campaignAlias
	}
	channelID := req.ChannelID
	extraInfo := req.DefaultExtraInfo

	msg := req.Message
	if msg.Template != nil {
		nsPtr := &defaultTemplateNamespace
		if trimmedNS := strings.TrimSpace(msg.Template.Namespace); trimmedNS != "" {
			nsPtr = &trimmedNS
		}
		langPtr := &defaultTemplateLanguage
		if trimmedLang := strings.TrimSpace(msg.Template.LanguageCode); trimmedLang != "" {
			langPtr = &trimmedLang
		}

		routingKey := "channels_queue"
		if req.CampaignID != nil && strings.TrimSpace(*req.CampaignID) != "" {
			routingKey = "campaign_queue"
		}

		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:       "whatsapp",
			Broker:            true,
			Action:            "send",
			Type:              "template",
			PhoneNumber:       phoneNumber,
			FlowID:            flowID,
			CampaignAlias:     campaignAlias,
			CampaignID:        campaignID,
			ChannelID:         channelID,
			OwnerID:           ownerID,
			ExtraInfo:         extraInfo,
			CorrelationID:     correlationID,
			TemplateNamespace: nsPtr,
			TemplateName:      &msg.Template.ElementName,
			LanguageCode:      langPtr,
			TemplateHeaders:   msg.Template.Header,
			TemplateBody:      msg.Template.BodyParameters,
			TemplateButtons:   msg.Template.Buttons,
		}

		return s.publisher.Publish(ctx, routingKey, amqpMsg)
	}

	if msg.MessageText != nil {
		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:   "whatsapp",
			Broker:        true,
			Action:        "send",
			Type:          "text",
			PhoneNumber:   phoneNumber,
			FlowID:        flowID,
			CampaignAlias: campaignAlias,
			CampaignID:    campaignID,
			ChannelID:     channelID,
			OwnerID:       ownerID,
			ExtraInfo:     extraInfo,
			CorrelationID: correlationID,
			MessageText:   msg.MessageText,
		}
		return s.publisher.Publish(ctx, "channels_queue", amqpMsg)
	}

	if msg.Document != nil {
		docURL := ""
		if msg.Document.URL != nil {
			docURL = *msg.Document.URL
		}
		encoded, err := s.downloader.DownloadAndEncodeBase64(ctx, docURL, fileToken)
		if err != nil {
			return ErrFailedDownloadDocument
		}

		docType := msg.Document.Type
		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:   "whatsapp",
			Broker:        true,
			Action:        "send",
			Type:          "document",
			PhoneNumber:   phoneNumber,
			FlowID:        flowID,
			CampaignAlias: campaignAlias,
			CampaignID:    campaignID,
			ChannelID:     channelID,
			OwnerID:       ownerID,
			ExtraInfo:     extraInfo,
			CorrelationID: correlationID,
			DocumentData:  &encoded,
			Caption:       msg.Document.Caption,
			DocType:       &docType,
		}
		return s.publisher.Publish(ctx, "channels_queue", amqpMsg)
	}

	if msg.Image != nil {
		imgURL := ""
		if msg.Image.URL != nil {
			imgURL = *msg.Image.URL
		}
		encoded, err := s.downloader.DownloadAndEncodeBase64(ctx, imgURL, fileToken)
		if err != nil {
			return ErrFailedDownloadImage
		}

		docType := msg.Image.Type
		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:   "whatsapp",
			Broker:        true,
			Action:        "send",
			Type:          "image",
			PhoneNumber:   phoneNumber,
			FlowID:        flowID,
			CampaignAlias: campaignAlias,
			CampaignID:    campaignID,
			ChannelID:     channelID,
			OwnerID:       ownerID,
			ExtraInfo:     extraInfo,
			CorrelationID: correlationID,
			ImageData:     &encoded,
			Caption:       msg.Image.Caption,
			DocType:       &docType,
		}
		return s.publisher.Publish(ctx, "channels_queue", amqpMsg)
	}

	if msg.Video != nil {
		vidURL := ""
		if msg.Video.URL != nil {
			vidURL = *msg.Video.URL
		}
		encoded, err := s.downloader.DownloadAndEncodeBase64(ctx, vidURL, fileToken)
		if err != nil {
			return ErrFailedDownloadVideo
		}

		docType := msg.Video.Type
		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:   "whatsapp",
			Broker:        true,
			Action:        "send",
			Type:          "video",
			PhoneNumber:   phoneNumber,
			FlowID:        flowID,
			CampaignAlias: campaignAlias,
			CampaignID:    campaignID,
			ChannelID:     channelID,
			OwnerID:       ownerID,
			ExtraInfo:     extraInfo,
			CorrelationID: correlationID,
			VideoData:     &encoded,
			Caption:       msg.Video.Caption,
			DocType:       &docType,
		}
		return s.publisher.Publish(ctx, "channels_queue", amqpMsg)
	}

	if msg.Interactive != nil {
		interType := msg.Interactive.MessageInteractiveType
		var content any
		switch interType {
		case "REPLY_BUTTON":
			content = msg.Interactive.ReplyButtonAction
		case "LIST":
			content = msg.Interactive.ListAction
		case "ORDER":
			content = msg.Interactive.OrderAction
		}

		amqpMsg := domain.AMQPChannelMessage{
			ChannelType:        "whatsapp",
			Broker:             true,
			Action:             "send",
			Type:               "interactive",
			PhoneNumber:        phoneNumber,
			FlowID:             flowID,
			CampaignAlias:      campaignAlias,
			CampaignID:         campaignID,
			ChannelID:          channelID,
			OwnerID:            ownerID,
			ExtraInfo:          extraInfo,
			CorrelationID:      correlationID,
			InteractiveType:    &interType,
			AlternativeText:    msg.Interactive.AlternativeText,
			InteractiveBody:    msg.Interactive.Body,
			InteractiveFooter:  msg.Interactive.Footer,
			InteractiveHeader:  msg.Interactive.Header,
			InteractiveContent: content,
		}
		return s.publisher.Publish(ctx, "channels_queue", amqpMsg)
	}

	return ErrEmptyMessage
}
