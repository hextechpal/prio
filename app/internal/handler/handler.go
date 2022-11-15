package handler

import (
	"context"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/core/commons"
	"github.com/hextechpal/prio/core/models"
	"net/http"
	"time"
)

type Handler struct {
	w      *core.Worker
	logger commons.Logger
}

func NewHandler(ctx context.Context, w *core.Worker, logger commons.Logger) (*Handler, error) {
	err := w.Start(ctx)
	if err != nil {
		return nil, err
	}
	return &Handler{w: w, logger: logger}, nil
}

func (h *Handler) Register(g *echo.Group) {
	g.POST("/topics", h.registerTopic())
	g.POST("/enqueue", h.enqueue())
	g.GET("/dequeue", h.dequeue())
	g.POST("/ack", h.ack())
}

func (h *Handler) enqueue() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := EnqueueRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		jobId, err := h.w.Enqueue(c.Request().Context(), toJob(&req))
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, EnqueueResponse{JobId: jobId})
	}
}

func (h *Handler) dequeue() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := DequeueRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		job, err := h.w.Dequeue(c.Request().Context(), req.Topic, req.Consumer)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, toDequeue(job))
	}
}

func (h *Handler) registerTopic() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := RegisterTopicRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		topicID, err := h.w.CreateTopic(c.Request().Context(), req.Name, req.Description)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, RegisterTopicResponse{TopicID: topicID})
	}
}

func (h *Handler) ack() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := AckRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		err := h.w.Ack(c.Request().Context(), req.Topic, req.JobId, req.Consumer)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, AckResponse{Acked: true})
	}
}

func toJob(r *EnqueueRequest) *models.Job {
	return &models.Job{
		Payload:   r.Payload,
		Priority:  r.Priority,
		Topic:     r.Topic,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		Status:    models.PENDING,
	}
}

func toDequeue(job *models.Job) DequeueResponse {
	return DequeueResponse{
		JobId:    job.ID,
		Topic:    job.Topic,
		Payload:  job.Payload,
		Priority: job.Priority,
	}
}
