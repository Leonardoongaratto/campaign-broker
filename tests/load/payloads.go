package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync/atomic"

	"campaign-broker/internal/domain"
)

var payloadCounter atomic.Uint64

type RequestSpec struct {
	Endpoint string
	Body     []byte
	Headers  map[string]string
}

func GeneratePayload(mediaBaseURL string) RequestSpec {
	id := payloadCounter.Add(1)
	phone := fmt.Sprintf("+55119%08d", (id%90000000)+10000000)
	corrID := fmt.Sprintf("corr-%d", id)
	flowID := "flow-stress-test-100"
	channelID := "chan-wa-prod-01"

	headers := map[string]string{
		"Content-Type": "application/json",
		"X-User-Id":    "load-test-agent",
		"X-Scope":      "api/all:write api/broker:write",
	}

	roll := rand.Float64() * 100.0

	var req domain.OutboundRequest
	req.Destinations = []domain.Destination{
		{
			CorrelationID: &corrID,
			Destination:   phone,
		},
	}
	req.FlowID = &flowID
	req.ChannelID = &channelID

	endpoint := "/conversations/webhook/broker/message"

	switch {
	case roll < 60.0:
		// 60% Simple text message
		if roll < 30.0 {
			endpoint = "/conversations/webhook/broker/messages"
		}
		text := fmt.Sprintf("Hello! Stress test message sequence #%d", id)
		req.Message = domain.RawMessage{
			MessageText: &text,
		}

	case roll < 80.0:
		// 20% HSM Template
		elementName := "transaction_alert_v2"
		ns := "002abc39_6fbf_4da4_8674_29b782bf572a"
		lang := "pt_BR"
		campaignAlias := "stress_campaign_2026"
		req.CampaignAlias = &campaignAlias

		req.Message = domain.RawMessage{
			Template: &domain.TemplateModel{
				Namespace:    ns,
				ElementName:  elementName,
				LanguageCode: lang,
				BodyParameters: []any{
					"Cliente Especial",
					fmt.Sprintf("R$ %d,00", (id%500)+50),
					"18/09/2026",
				},
				Buttons: []domain.TemplateButton{
					{
						Type: "QUICK_REPLY",
					},
				},
			},
		}

	case roll < 95.0:
		// 15% Interactive
		if roll < 87.5 {
			interType := "REPLY_BUTTON"
			req.Message = domain.RawMessage{
				Interactive: &domain.InteractiveModel{
					MessageInteractiveType: interType,
					ReplyButtonAction: &domain.ReplyButtonAction{
						Buttons: []domain.ReplyButton{
							{
								Type: "reply",
								Reply: map[string]string{
									"id":    fmt.Sprintf("btn_yes_%d", id),
									"title": "Sim, confirmar",
								},
							},
							{
								Type: "reply",
								Reply: map[string]string{
									"id":    fmt.Sprintf("btn_no_%d", id),
									"title": "NÃ£o, cancelar",
								},
							},
						},
					},
				},
			}
		} else {
			interType := "LIST"
			secTitle := "OpÃ§Ãµes disponÃ­veis"
			req.Message = domain.RawMessage{
				Interactive: &domain.InteractiveModel{
					MessageInteractiveType: interType,
					ListAction: &domain.ListAction{
						Button: "Menu Principal",
						Sections: []domain.ListSection{
							{
								Title: &secTitle,
								Rows: []domain.ListRow{
									{
										ID:          "opt_1",
										Title:       "Consultar Saldo",
										Description: "Verifique seu limite disponÃ­vel",
									},
									{
										ID:          "opt_2",
										Title:       "Falar com Atendente",
										Description: "Transbordo humano imediato",
									},
								},
							},
						},
					},
				},
			}
		}

	default:
		// 5% Media message (document, image, or video)
		headers["X-BV-File-Token"] = "Bearer token-stress-test"
		mediaType := id % 3
		mediaURL := fmt.Sprintf("%s/media/file-%d", mediaBaseURL, id)

		caption := fmt.Sprintf("Media attachment file #%d", id)
		switch mediaType {
		case 0:
			docType := "pdf"
			req.Message = domain.RawMessage{
				Document: &domain.FileData{
					URL:     &mediaURL,
					Caption: &caption,
					Type:    docType,
				},
			}
		case 1:
			imgType := "jpg"
			req.Message = domain.RawMessage{
				Image: &domain.FileData{
					URL:     &mediaURL,
					Caption: &caption,
					Type:    imgType,
				},
			}
		default:
			vidType := "mp4"
			req.Message = domain.RawMessage{
				Video: &domain.FileData{
					URL:     &mediaURL,
					Caption: &caption,
					Type:    vidType,
				},
			}
		}
	}

	body, _ := json.Marshal(req)
	return RequestSpec{
		Endpoint: endpoint,
		Body:     body,
		Headers:  headers,
	}
}
