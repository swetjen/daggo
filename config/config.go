package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port                    string
	AllowedOrigins          []string
	DatabaseURL             string
	RunQueueSize            int
	RunExecutionMode        string
	RunMaxConcurrentRuns    int
	RunMaxConcurrentSteps   int
	SchedulerEnabled        bool
	SchedulerKey            string
	SchedulerTickSeconds    int
	SchedulerMaxDuePerTick  int
	DeployLockPath          string
	DeployLockPollSeconds   int
	DeployDrainGraceSeconds int
}

func Load() Config {
	loadDotEnv(".env")
	return Config{
		Port:                    getEnv("PORT", "8000"),
		AllowedOrigins:          splitEnvList("CORS_ALLOW_ORIGINS", []string{"*"}),
		DatabaseURL:             getEnv("DATABASE_URL", "file:daggo.sqlite?cache=shared&mode=rwc"),
		RunQueueSize:            getEnvInt("RUN_QUEUE_SIZE", 128),
		RunExecutionMode:        getEnv("RUN_EXECUTION_MODE", "subprocess"),
		RunMaxConcurrentRuns:    getEnvInt("RUN_MAX_CONCURRENT_RUNS", 1),
		RunMaxConcurrentSteps:   getEnvInt("RUN_MAX_CONCURRENT_STEPS", 8),
		SchedulerEnabled:        getEnvBool("SCHEDULER_ENABLED", true),
		SchedulerKey:            getEnv("SCHEDULER_KEY", "monolith"),
		SchedulerTickSeconds:    getEnvInt("SCHEDULER_TICK_SECONDS", 15),
		SchedulerMaxDuePerTick:  getEnvInt("SCHEDULER_MAX_DUE_PER_TICK", 24),
		DeployLockPath:          getEnv("DEPLOY_LOCK_PATH", "runtime/WILL_DEPLOY"),
		DeployLockPollSeconds:   getEnvInt("DEPLOY_LOCK_POLL_SECONDS", 1),
		DeployDrainGraceSeconds: getEnvInt("DEPLOY_DRAIN_GRACE_SECONDS", 900),
	}
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func splitEnvList(key string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := parseEnvLine(line)
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func parseEnvLine(line string) (string, string, bool) {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	key := strings.TrimSpace(parts[0])
	if key == "" {
		return "", "", false
	}
	value := strings.TrimSpace(parts[1])
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}
	return key, value, true
}
