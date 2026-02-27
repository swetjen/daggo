package dag

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

type stepLoggerKey struct{}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if ctx == nil || logger == nil {
		return ctx
	}
	return context.WithValue(ctx, stepLoggerKey{}, logger)
}

func LoggerFromContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}
	logger, ok := ctx.Value(stepLoggerKey{}).(*slog.Logger)
	if !ok || logger == nil {
		return slog.Default()
	}
	return logger
}

func LogDebug(ctx context.Context, message string, args ...any) {
	LoggerFromContext(ctx).Debug(normalizeLogMessage(message), args...)
}

func LogInfo(ctx context.Context, message string, args ...any) {
	LoggerFromContext(ctx).Info(normalizeLogMessage(message), args...)
}

func LogWarn(ctx context.Context, message string, args ...any) {
	LoggerFromContext(ctx).Warn(normalizeLogMessage(message), args...)
}

func LogError(ctx context.Context, message string, args ...any) {
	LoggerFromContext(ctx).Error(normalizeLogMessage(message), args...)
}

type stepLogSink func(level string, message string, payload map[string]any)

type stepLogHandler struct {
	sink   stepLogSink
	attrs  []slog.Attr
	groups []string
	level  slog.Leveler
}

func NewStepLogHandler(sink stepLogSink) slog.Handler {
	return &stepLogHandler{
		sink:  sink,
		attrs: nil,
		level: slog.LevelDebug,
	}
}

type multiHandler struct {
	handlers []slog.Handler
}

func NewMultiHandler(handlers ...slog.Handler) slog.Handler {
	flattened := make([]slog.Handler, 0, len(handlers))
	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		flattened = append(flattened, handler)
	}
	if len(flattened) == 0 {
		return slog.NewTextHandler(nilWriter{}, nil)
	}
	if len(flattened) == 1 {
		return flattened[0]
	}
	return &multiHandler{handlers: flattened}
}

func (h *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h == nil || len(h.handlers) == 0 {
		return false
	}
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *multiHandler) Handle(ctx context.Context, record slog.Record) error {
	if h == nil || len(h.handlers) == 0 {
		return nil
	}
	var combined error
	for _, handler := range h.handlers {
		if !handler.Enabled(ctx, record.Level) {
			continue
		}
		if err := handler.Handle(ctx, record); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

func (h *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h == nil || len(h.handlers) == 0 {
		return h
	}
	next := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		next = append(next, handler.WithAttrs(attrs))
	}
	return &multiHandler{handlers: next}
}

func (h *multiHandler) WithGroup(name string) slog.Handler {
	if h == nil || len(h.handlers) == 0 {
		return h
	}
	next := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		next = append(next, handler.WithGroup(name))
	}
	return &multiHandler{handlers: next}
}

func (h *stepLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	if h == nil || h.sink == nil {
		return false
	}
	if h.level == nil {
		return true
	}
	return level >= h.level.Level()
}

func (h *stepLogHandler) Handle(_ context.Context, record slog.Record) error {
	if h == nil || h.sink == nil {
		return nil
	}

	payload := map[string]any{}
	if !record.Time.IsZero() {
		payload["timestamp"] = record.Time.UTC().Format(time.RFC3339Nano)
	}

	for _, attr := range h.attrs {
		appendSlogAttr(payload, h.groups, attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		appendSlogAttr(payload, h.groups, attr)
		return true
	})

	message := normalizeLogMessage(record.Message)
	h.sink(levelToEventLevel(record.Level), message, payload)
	return nil
}

func (h *stepLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &stepLogHandler{
		sink:   h.sink,
		attrs:  append([]slog.Attr{}, h.attrs...),
		groups: append([]string{}, h.groups...),
		level:  h.level,
	}
	next.attrs = append(next.attrs, attrs...)
	return next
}

func (h *stepLogHandler) WithGroup(name string) slog.Handler {
	trimmed := strings.TrimSpace(name)
	next := &stepLogHandler{
		sink:   h.sink,
		attrs:  append([]slog.Attr{}, h.attrs...),
		groups: append([]string{}, h.groups...),
		level:  h.level,
	}
	if trimmed != "" {
		next.groups = append(next.groups, trimmed)
	}
	return next
}

func appendSlogAttr(payload map[string]any, groups []string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) || strings.TrimSpace(attr.Key) == "" {
		return
	}
	key := attr.Key
	if len(groups) > 0 {
		key = strings.Join(append(append([]string{}, groups...), attr.Key), ".")
	}
	payload[key] = slogValueToAny(attr.Value)
}

func slogValueToAny(value slog.Value) any {
	switch value.Kind() {
	case slog.KindAny:
		return value.Any()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindString:
		return value.String()
	case slog.KindTime:
		return value.Time().UTC().Format(time.RFC3339Nano)
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindGroup:
		items := map[string]any{}
		for _, attr := range value.Group() {
			resolved := attr
			resolved.Value = resolved.Value.Resolve()
			if strings.TrimSpace(resolved.Key) == "" {
				continue
			}
			items[resolved.Key] = slogValueToAny(resolved.Value)
		}
		return items
	case slog.KindLogValuer:
		return slogValueToAny(value.Resolve())
	default:
		return value.String()
	}
}

func levelToEventLevel(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return "error"
	case level >= slog.LevelWarn:
		return "warn"
	case level >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}

func normalizeLogMessage(message string) string {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return "step log"
	}
	return trimmed
}

type nilWriter struct{}

func (nilWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
