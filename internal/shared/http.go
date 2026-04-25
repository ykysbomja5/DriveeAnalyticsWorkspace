package shared

import (
	"encoding/json"
	"net/http"
)

// WriteJSON сериализует подготовленные данные в нужный формат ответа.
func WriteJSON(w http.ResponseWriter, status int, value any) {
	_ = WriteJSONWithError(w, status, value)
}

func WriteJSONWithError(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

// WriteError сериализует подготовленные данные в нужный формат ответа.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// DecodeJSON нормализует граничные значения перед дальнейшим использованием.
func DecodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

// HandlePreflight проверяет HTTP-запрос и запускает сценарий эндпоинта.
func HandlePreflight(w http.ResponseWriter, r *http.Request) bool {
	// Preflight-запросы браузера закрываем сразу, не передавая их в бизнес-обработчики.
	if r.Method != http.MethodOptions {
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	w.WriteHeader(http.StatusNoContent)
	return true
}
