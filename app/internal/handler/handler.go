package handler

import (
	"context"
	"github.com/hextechpal/prio/core"
	"github.com/hextechpal/prio/core/api"
	"github.com/hextechpal/prio/core/commons"
	"github.com/labstack/echo/v4"
	"net/http"
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
		req := api.EnqueueRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		res, err := h.w.Enqueue(c.Request().Context(), req)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, res)
	}
}

func (h *Handler) dequeue() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := api.DequeueRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		res, err := h.w.Dequeue(c.Request().Context(), req)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, res)
	}
}

func (h *Handler) registerTopic() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := api.RegisterTopicRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		res, err := h.w.RegisterTopic(c.Request().Context(), req)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, res)
	}
}

func (h *Handler) ack() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := api.AckRequest{}
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		res, err := h.w.Ack(c.Request().Context(), req)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		return c.JSON(http.StatusOK, res)
	}
}
