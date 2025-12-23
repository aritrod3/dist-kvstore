package observability

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type Logger interface {
	Info(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
}

type StdLogger struct {
	logger *log.Logger
}

func NewStdLogger(logger *log.Logger) *StdLogger {
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	return &StdLogger{logger: logger}
}

func (l *StdLogger) Info(msg string, keyvals ...any) {
	l.logger.Println(format("info", msg, keyvals...))
}

func (l *StdLogger) Error(msg string, keyvals ...any) {
	l.logger.Println(format("error", msg, keyvals...))
}

func format(level string, msg string, keyvals ...any) string {
	var b strings.Builder
	b.WriteString("level=")
	b.WriteString(level)
	b.WriteString(" msg=")
	b.WriteString(msg)
	for i := 0; i < len(keyvals); i += 2 {
		key := fmt.Sprint(keyvals[i])
		var value any = "missing"
		if i+1 < len(keyvals) {
			value = keyvals[i+1]
		}
		b.WriteString(" ")
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(fmt.Sprint(value))
	}
	return b.String()
}
