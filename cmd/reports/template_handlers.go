package main

import (
	"net/http"
	"strconv"
	"strings"

	"drivee-self-service/internal/shared"
)

// handleTemplateActions проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleTemplateActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/templates/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		shared.WriteError(w, http.StatusNotFound, "template route not found")
		return
	}

	templateID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid template id")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPut:
			app.updateTemplate(w, r, templateID)
		case http.MethodDelete:
			app.deleteTemplate(w, r, templateID)
		default:
			shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	if len(parts) == 2 && parts[1] == "sharing" && r.Method == http.MethodPut {
		app.updateTemplateSharing(w, r, templateID)
		return
	}

	if len(parts) == 2 && parts[1] == "run" && r.Method == http.MethodPost {
		app.runTemplateNow(w, r, templateID)
		return
	}

	shared.WriteError(w, http.StatusNotFound, "template route not found")
}
