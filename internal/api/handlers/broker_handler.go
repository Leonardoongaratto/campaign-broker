package handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"campaign-broker/internal/domain"
	"campaign-broker/internal/service"
)

type BrokerHandler struct {
	brokerService *service.BrokerService
	logger        *logrus.Logger
}

func NewBrokerHandler(brokerService *service.BrokerService, logger *logrus.Logger) *BrokerHandler {
	return &BrokerHandler{
		brokerService: brokerService,
		logger:        logger,
	}
}

func (h *BrokerHandler) HandleBrokerMessage(c *gin.Context) {
	h.processMessage(c, http.StatusOK)
}

func (h *BrokerHandler) HandleBrokerMessages(c *gin.Context) {
	h.processMessage(c, http.StatusAccepted)
}

func (h *BrokerHandler) processMessage(c *gin.Context, successCode int) {
	now := float64(time.Now().UnixNano()) / 1e9

	var req domain.OutboundRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warnf("Invalid broker request payload: %v", err)
		c.JSON(http.StatusBadRequest, domain.ErrorResponse{
			Detail: domain.ErrorDetail{
				StatusCode: http.StatusBadRequest,
				Message:    "Failed to recieve payload",
				Data: domain.WebhookResponseData{
					Timestamp: now,
				},
			},
		})
		return
	}

	ownerID := c.GetString("user_id")
	fileToken := c.GetHeader("X-BV-File-Token")

	err := h.brokerService.ProcessBrokerMessage(c.Request.Context(), ownerID, fileToken, &req)
	if err != nil {
		h.logger.Errorf("Failed to process broker message: %v", err)
		if errors.Is(err, service.ErrPublisherUnavailable) {
			c.JSON(http.StatusServiceUnavailable, domain.ErrorResponse{
				Detail: domain.ErrorDetail{
					StatusCode: http.StatusServiceUnavailable,
					Message:    "Message broker unavailable",
					Data: domain.WebhookResponseData{
						Timestamp: now,
					},
				},
			})
			return
		}

		errMsg := "Failed to recieve payload"
		switch {
		case errors.Is(err, service.ErrFailedDownloadDocument):
			errMsg = "Failed to download document"
		case errors.Is(err, service.ErrFailedDownloadImage):
			errMsg = "Failed to download image"
		case errors.Is(err, service.ErrFailedDownloadVideo):
			errMsg = "Failed to download video"
		}

		c.JSON(http.StatusBadRequest, domain.ErrorResponse{
			Detail: domain.ErrorDetail{
				StatusCode: http.StatusBadRequest,
				Message:    errMsg,
				Data: domain.WebhookResponseData{
					Timestamp: now,
				},
			},
		})
		return
	}

	c.JSON(successCode, domain.WebhookResponse{
		StatusCode: successCode,
		Message:    "Payload received",
		Data: domain.WebhookResponseData{
			Timestamp: now,
		},
	})
}

func (h *BrokerHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"service":   "campaign-broker",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
