package logger

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const HeaderLogLevel = "x-log-level"

type contextKey struct{}

func ToContext(ctx context.Context, logger zerolog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

func FromContext(ctx context.Context) zerolog.Logger {
	logger, ok := ctx.Value(contextKey{}).(zerolog.Logger)
	if ok {
		return logger
	}

	return log.Logger
}
