package internal

import "github.com/rs/zerolog"

type Logger struct {
	*zerolog.Logger
}

func (l *Logger) Info(format string, v ...any) {
	l.Logger.Info().Msgf(format, v...)
}

func (l *Logger) Error(err error, format string, v ...any) {
	l.Logger.Error().Err(err).Msgf(format, v...)
}

func (l *Logger) Warn(format string, v ...any) {
	l.Logger.Warn().Msgf(format, v...)
}

func (l *Logger) Debug(format string, v ...any) {
	l.Logger.Warn().Msgf(format, v...)
}

func (l *Logger) Fatal(format string, v ...any) {
	l.Logger.Fatal().Msgf(format, v...)
}
