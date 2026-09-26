package domain

import "encoding/json"

type Destination struct {
	CorrelationID *string `json:"correlationId,omitempty"`
	Destination   string  `json:"destination" binding:"required"`
}

type FileData struct {
	Caption *string `json:"caption,omitempty"`
	URL     *string `json:"url,omitempty"`
	Data    *string `json:"data,omitempty"`
	Type    string  `json:"type,omitempty"`
}

type TemplateButton struct {
	Type         string      `json:"type" binding:"required"`
	ReplyPayload *string     `json:"replyPayload,omitempty"`
	URL          *string     `json:"url,omitempty"`
	OrderDetails interface{} `json:"orderDetails,omitempty"`
}

type TemplateModel struct {
	Namespace      string           `json:"namespace"`
	ElementName    string           `json:"elementName" binding:"required"`
	LanguagePolicy string           `json:"languagePolicy,omitempty"`
	LanguageCode   string           `json:"languageCode,omitempty"`
	Header         interface{}      `json:"header,omitempty"`
	BodyParameters []interface{}    `json:"bodyParameters,omitempty"`
	Buttons        []TemplateButton `json:"buttons,omitempty"`
}

type ReplyButton struct {
	Type  string            `json:"type"`
	Reply map[string]string `json:"reply"`
}

type ReplyButtonAction struct {
	Buttons []ReplyButton `json:"buttons"`
}

type ListRow struct {
	ID          string `json:"id"`
	Identifier  string `json:"identifier,omitempty"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

func (r *ListRow) UnmarshalJSON(data []byte) error {
	type Alias ListRow
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if r.ID == "" && r.Identifier != "" {
		r.ID = r.Identifier
	} else if r.Identifier == "" && r.ID != "" {
		r.Identifier = r.ID
	}
	return nil
}

type ListSection struct {
	Title *string   `json:"title,omitempty"`
	Rows  []ListRow `json:"rows"`
}

type ListAction struct {
	Button   string        `json:"button"`
	Sections []ListSection `json:"sections"`
}

type InteractiveModel struct {
	MessageInteractiveType string             `json:"messageInteractiveType" binding:"required"`
	AlternativeText        *string            `json:"alternativeText,omitempty"`
	Body                   map[string]any     `json:"body,omitempty"`
	Footer                 map[string]any     `json:"footer,omitempty"`
	Header                 map[string]any     `json:"header,omitempty"`
	ReplyButtonAction      *ReplyButtonAction `json:"replyButtonAction,omitempty"`
	ListAction             *ListAction        `json:"listAction,omitempty"`
	OrderAction            map[string]any     `json:"orderAction,omitempty"`
}

type RawMessage struct {
	MessageText *string           `json:"messageText,omitempty"`
	Template    *TemplateModel    `json:"template,omitempty"`
	Document    *FileData         `json:"document,omitempty"`
	Image       *FileData         `json:"image,omitempty"`
	Video       *FileData         `json:"video,omitempty"`
	Interactive *InteractiveModel `json:"interactive,omitempty"`
}

type OutboundRequest struct {
	CampaignAlias    *string       `json:"campaignAlias,omitempty"`
	CampaignID       *string       `json:"campaignId,omitempty"`
	Destinations     []Destination `json:"destinations" binding:"required,min=1"`
	FlowID           *string       `json:"flowId,omitempty"`
	ChannelID        *string       `json:"channelId,omitempty"`
	DefaultExtraInfo *string       `json:"defaultExtraInfo,omitempty"`
	Message          RawMessage    `json:"message" binding:"required"`
}

type WebhookResponseData struct {
	Timestamp float64 `json:"timestamp"`
}

type WebhookResponse struct {
	StatusCode int                 `json:"statusCode"`
	Message    string              `json:"message"`
	Data       WebhookResponseData `json:"data"`
}

type ErrorDetail struct {
	StatusCode int                 `json:"statusCode"`
	Message    string              `json:"message"`
	Data       WebhookResponseData `json:"data"`
}

type ErrorResponse struct {
	Detail ErrorDetail `json:"detail"`
}

type AMQPChannelMessage struct {
	ChannelType        string      `json:"channel_type"`
	Broker             bool        `json:"broker"`
	Action             string      `json:"action"`
	Type               string      `json:"type"`
	PhoneNumber        string      `json:"phone_number"`
	FlowID             *string     `json:"flow_id"`
	CampaignAlias      *string     `json:"campaign_alias"`
	CampaignID         *string     `json:"campaign_id"`
	ChannelID          *string     `json:"channel_id"`
	OwnerID            string      `json:"owner_id"`
	ExtraInfo          *string     `json:"extra_info"`
	CorrelationID      *string     `json:"correlation_id"`
	MessageText        *string     `json:"message_text,omitempty"`
	DocumentData       *string     `json:"document_data,omitempty"`
	ImageData          *string     `json:"image_data,omitempty"`
	VideoData          *string     `json:"video_data,omitempty"`
	Caption            *string     `json:"caption,omitempty"`
	DocType            *string     `json:"doc_type,omitempty"`
	TemplateNamespace  *string     `json:"template_namespace,omitempty"`
	TemplateName       *string     `json:"template_name,omitempty"`
	LanguageCode       *string     `json:"language_code,omitempty"`
	TemplateHeaders    interface{} `json:"template_headers,omitempty"`
	TemplateBody       interface{} `json:"template_body,omitempty"`
	TemplateButtons    interface{} `json:"template_buttons,omitempty"`
	InteractiveType    *string     `json:"interactive_type,omitempty"`
	AlternativeText    *string     `json:"alternative_text,omitempty"`
	InteractiveBody    interface{} `json:"interactive_body,omitempty"`
	InteractiveFooter  interface{} `json:"interactive_footer,omitempty"`
	InteractiveHeader  interface{} `json:"interactive_header,omitempty"`
	InteractiveContent interface{} `json:"interactive_content,omitempty"`
}
