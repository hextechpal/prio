package commons

import (
	"context"
	"os"

	"github.com/rs/zerolog"
)

type PLogger struct {
	// TODO : Convert this to an interface and use that implementation
	*zerolog.Logger
}

func NewLogger(ctx context.Context) *PLogger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)
	log.WithContext(ctx)
	return &PLogger{&log}
}
