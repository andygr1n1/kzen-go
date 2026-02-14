package golib

import (
	log "log"
	"os"
	"strings"
)

func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func ConsoleLog(format string, a ...any) {
	log.Printf(format+"\n", a...)
}
