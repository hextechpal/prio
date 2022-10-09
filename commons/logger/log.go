package logger

import (
	"context"
	"os"

	"github.com/rs/zerolog"
)

type Logger struct {
	*zerolog.Logger
}

func NewLogger(ctx context.Context) *Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log := zerolog.New(os.Stdout).Level(zerolog.DebugLevel)
	log.WithContext(ctx)
	return &Logger{&log}
}
