package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/hextechpal/prio/internal/models"
	"github.com/hextechpal/prio/internal/store"
	"github.com/hextechpal/prio/internal/worker"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
)

type Handler struct {
	w      *worker.Worker
	logger *zerolog.Logger
}

func NewHandler(ctx context.Context, namespace string, s store.Storage, conn *zk.Conn, logger *zerolog.Logger) (*Handler, error) {
	w, err := worker.NewWorker(ctx, namespace, conn, s, logger)
	if err != nil {
		return nil, err
	}
	return &Handler{
		w:      w,
		logger: logger,
	}, nil
}

func (h *Handler) Register(g *echo.Group) {
	g.POST("/topics", h.registerTopic())
	g.POST("/enqueue", h.enqueue())
	g.GET("/dequeue", h.dequeue())
	g.POST("/ack", h.ack())
}

func (h *Handler) enqueue() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := worker.EnqueueRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		jobId, err := h.w.Enqueue(c.Request().Context(), toJob(&req))
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, worker.EnqueueResponse{JobId: jobId})
	}
}

func (h *Handler) dequeue() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := worker.DequeueRequest{}
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
		req := worker.RegisterTopicRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		topicID, err := h.w.CreateTopic(c.Request().Context(), req.Name, req.Description)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, worker.RegisterTopicResponse{TopicID: topicID})
	}
}

func (h *Handler) ack() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := worker.AckRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		err := h.w.Ack(c.Request().Context(), req.Topic, req.JobId, req.Consumer)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, worker.AckResponse{Acked: true})
	}
}

func toJob(r *worker.EnqueueRequest) *models.Job {
	return &models.Job{
		Payload:   r.Payload,
		Priority:  r.Priority,
		Topic:     r.Topic,
		CreatedAt: time.Now().UnixMilli(),
		UpdatedAt: time.Now().UnixMilli(),
		Status:    models.PENDING,
	}
}

func toDequeue(job *models.Job) worker.DequeueResponse {
	return worker.DequeueResponse{
		JobId:    job.ID,
		Topic:    job.Topic,
		Payload:  job.Payload,
		Priority: job.Priority,
	}
}
