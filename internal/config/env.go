package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// getEnv reads key from the environment and returns its value, or
// defaultVal if the variable is unset or empty.
func getEnv(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return defaultVal
}

// getEnvInt reads key from the environment and parses it as an int.
// It returns defaultVal if the variable is unset or empty, and an
// error if it is set but not a valid integer.
func getEnvInt(key string, defaultVal int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q: %w", key, v, err)
	}
	return n, nil
}

// getEnvDuration reads key from the environment and parses it with
// time.ParseDuration (e.g. "15s", "500ms"). It returns defaultVal if
// the variable is unset or empty, and an error if it is set but not a
// valid duration.
func getEnvDuration(key string, defaultVal time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", key, v, err)
	}
	return d, nil
}
