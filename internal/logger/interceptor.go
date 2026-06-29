package logger

import (
	"context"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func UnaryLogLevelInterceptor(defaultLevel zerolog.Level) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		level := defaultLevel

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			values := md.Get(HeaderLogLevel)
			if len(values) > 0 && values[0] != "" {
				parsedLevel, err := zerolog.ParseLevel(strings.ToLower(values[0]))
				if err != nil {
					log.Warn().
						Str("header", HeaderLogLevel).
						Str("value", values[0]).
						Msg("invalid log level header")
				} else {
					level = parsedLevel
				}
			}
		}

		requestLogger := log.Logger.
			Level(level).
			With().
			Str("grpc_method", info.FullMethod).
			Str("log_level", level.String()).
			Logger()

		ctx = ToContext(ctx, requestLogger)

		return handler(ctx, req)
	}
}
