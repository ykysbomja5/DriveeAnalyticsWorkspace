package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"drivee-self-service/internal/shared"
)

// providerSQLResponse описывает JSON-ответ, возвращаемый Qwen.
type providerSQLResponse struct {
	SQL           string        `json:"sql"`
	Intent        shared.Intent `json:"intent"`
	Clarification string        `json:"clarification"`
	Confidence    float64       `json:"confidence"`
}

// requestedProvider теперь фиксирует новую архитектуру: только qwen.
func requestedProvider() string {
	value := strings.ToLower(strings.TrimSpace(getenv("LLM_PROVIDER", "qwen")))
	if value == "" {
		return "qwen"
	}
	return value
}

// activeProvider возвращает активного провайдера или ошибку конфигурации.
func activeProvider() (string, error) {
	provider := requestedProvider()
	if provider != "qwen" && provider != "cerebras" {
		return "", fmt.Errorf("unsupported LLM_PROVIDER %q: allowed value is qwen", os.Getenv("LLM_PROVIDER"))
	}
	if err := validateQwenCredentials(); err != nil {
		return "", err
	}
	return "qwen", nil
}

// generateProviderSQL вызывает Qwen через Cerebras Inference API.
func generateProviderSQL(ctx context.Context, text string, layer shared.SemanticLayer) (providerSQLResponse, string, error) {
	provider, err := activeProvider()
	if err != nil {
		return providerSQLResponse{}, "", err
	}
	resp, err := callQwenSQL(ctx, text, layer)
	if err != nil {
		return providerSQLResponse{}, "", err
	}
	return resp, provider, nil
}

// validateProviderStartup проверяет конфигурацию на старте без сетевого запроса, чтобы сервис мог стартовать быстро.
func validateProviderStartup(ctx context.Context) error {
	_ = ctx
	if _, err := activeProvider(); err != nil {
		return err
	}
	if err := validateProviderConfig(); err != nil {
		return err
	}
	if strings.ToLower(getenv("LLM_VALIDATE_ON_STARTUP", "false")) == "true" {
		log.Printf("LLM_VALIDATE_ON_STARTUP=true: qwen config is present; network validation is skipped to avoid startup delays")
	}
	return nil
}

// validateProviderConfig проверяет ключ и наличие prompt-файла.
func validateProviderConfig() error {
	if err := validateQwenCredentials(); err != nil {
		return err
	}
	_, err := loadLLMSettingsFile()
	return err
}
