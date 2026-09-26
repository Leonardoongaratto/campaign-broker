package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"campaign-broker/internal/domain"
)

func TestOutboundRequest_UnmarshalJSON(t *testing.T) {
	t.Run("unmarshal text message", func(t *testing.T) {
		payload := `{
			"campaignAlias": "test_alias",
			"campaignId": "camp_123",
			"flowId": "flow_1",
			"channelId": "chan_1",
			"defaultExtraInfo": "extra",
			"destinations": [
				{"destination": "+5511999999999", "correlationId": "corr_1"}
			],
			"message": {
				"messageText": "Hello from Campaign Broker"
			}
		}`

		var req domain.OutboundRequest
		err := json.Unmarshal([]byte(payload), &req)
		require.NoError(t, err)
		assert.Equal(t, "camp_123", *req.CampaignID)
		assert.Equal(t, "+5511999999999", req.Destinations[0].Destination)
		assert.Equal(t, "Hello from Campaign Broker", *req.Message.MessageText)
	})

	t.Run("unmarshal template message", func(t *testing.T) {
		payload := `{
			"destinations": [{"destination": "+5511888888888"}],
			"message": {
				"template": {
					"namespace": "custom_ns",
					"elementName": "welcome_template",
					"languageCode": "pt_BR",
					"bodyParameters": ["John", "Broker"],
					"buttons": [{"type": "QUICK_REPLY", "replyPayload": "btn_payload"}]
				}
			}
		}`

		var req domain.OutboundRequest
		err := json.Unmarshal([]byte(payload), &req)
		require.NoError(t, err)
		require.NotNil(t, req.Message.Template)
		assert.Equal(t, "welcome_template", req.Message.Template.ElementName)
		assert.Len(t, req.Message.Template.Buttons, 1)
	})

	t.Run("unmarshal document message", func(t *testing.T) {
		payload := `{
			"destinations": [{"destination": "+5511777777777"}],
			"message": {
				"document": {
					"url": "https://example.com/file.pdf",
					"caption": "invoice",
					"type": "PDF"
				}
			}
		}`

		var req domain.OutboundRequest
		err := json.Unmarshal([]byte(payload), &req)
		require.NoError(t, err)
		require.NotNil(t, req.Message.Document)
		assert.Equal(t, "https://example.com/file.pdf", *req.Message.Document.URL)
		assert.Equal(t, "PDF", req.Message.Document.Type)
	})

	t.Run("unmarshal interactive reply button", func(t *testing.T) {
		payload := `{
			"destinations": [{"destination": "+5511666666666"}],
			"message": {
				"interactive": {
					"messageInteractiveType": "REPLY_BUTTON",
					"alternativeText": "Choose an option",
					"body": {"text": "Select your choice"},
					"footer": {"text": "Powered by Campaign Broker"},
					"replyButtonAction": {
						"buttons": [
							{"type": "reply", "reply": {"title": "Yes", "payload": "1"}}
						]
					}
				}
			}
		}`

		var req domain.OutboundRequest
		err := json.Unmarshal([]byte(payload), &req)
		require.NoError(t, err)
		require.NotNil(t, req.Message.Interactive)
		assert.Equal(t, "REPLY_BUTTON", req.Message.Interactive.MessageInteractiveType)
		assert.NotNil(t, req.Message.Interactive.ReplyButtonAction)
	})

	t.Run("unmarshal interactive list with identifier fallback", func(t *testing.T) {
		payload := `{
			"destinations": [{"destination": "+5511666666666"}],
			"message": {
				"interactive": {
					"messageInteractiveType": "LIST",
					"listAction": {
						"button": "Select",
						"sections": [
							{
								"title": "Section 1",
								"rows": [
									{
										"identifier": "row_id_via_identifier",
										"title": "Item 1",
										"description": "Description 1"
									},
									{
										"id": "row_id_direct",
										"title": "Item 2",
										"description": "Description 2"
									}
								]
							}
						]
					}
				}
			}
		}`

		var req domain.OutboundRequest
		err := json.Unmarshal([]byte(payload), &req)
		require.NoError(t, err)
		require.NotNil(t, req.Message.Interactive)
		require.NotNil(t, req.Message.Interactive.ListAction)
		rows := req.Message.Interactive.ListAction.Sections[0].Rows
		assert.Equal(t, "row_id_via_identifier", rows[0].ID)
		assert.Equal(t, "row_id_via_identifier", rows[0].Identifier)
		assert.Equal(t, "row_id_direct", rows[1].ID)
		assert.Equal(t, "row_id_direct", rows[1].Identifier)
	})

	t.Run("list row invalid json returns error", func(t *testing.T) {
		var row domain.ListRow
		err := json.Unmarshal([]byte(`{"id": 12345}`), &row)
		assert.Error(t, err)
	})

	t.Run("unmarshal image and video messages", func(t *testing.T) {
		imagePayload := `{
			"destinations": [{"destination": "+5511555555555"}],
			"message": {
				"image": {
					"url": "https://example.com/photo.png",
					"caption": "profile photo",
					"type": "PNG"
				}
			}
		}`
		var imgReq domain.OutboundRequest
		err := json.Unmarshal([]byte(imagePayload), &imgReq)
		require.NoError(t, err)
		assert.Equal(t, "https://example.com/photo.png", *imgReq.Message.Image.URL)
		assert.Equal(t, "PNG", imgReq.Message.Image.Type)

		videoPayload := `{
			"destinations": [{"destination": "+5511555555555"}],
			"message": {
				"video": {
					"url": "https://example.com/clip.mp4",
					"caption": "intro clip",
					"type": "MP4"
				}
			}
		}`
		var vidReq domain.OutboundRequest
		err = json.Unmarshal([]byte(videoPayload), &vidReq)
		require.NoError(t, err)
		assert.Equal(t, "https://example.com/clip.mp4", *vidReq.Message.Video.URL)
		assert.Equal(t, "MP4", vidReq.Message.Video.Type)
	})

	t.Run("unmarshal interactive order", func(t *testing.T) {
		orderPayload := `{
			"destinations": [{"destination": "+5511444444444"}],
			"message": {
				"interactive": {
					"messageInteractiveType": "ORDER",
					"orderAction": {
						"name": "review_and_pay",
						"parameters": {"referenceId": "order_999"}
					}
				}
			}
		}`
		var orderReq domain.OutboundRequest
		err := json.Unmarshal([]byte(orderPayload), &orderReq)
		require.NoError(t, err)
		assert.Equal(t, "ORDER", orderReq.Message.Interactive.MessageInteractiveType)
		assert.NotNil(t, orderReq.Message.Interactive.OrderAction)
	})
}
