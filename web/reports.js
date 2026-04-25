const state = {
  reports: [],
  reportDepartmentFilter: "",
};

const elements = {
  ownerName: document.getElementById("archive-owner-name"),
  ownerDepartment: document.getElementById("archive-owner-department"),
  statusBanner: document.getElementById("archive-status"),
  myReportsList: document.getElementById("reports-page-list"),
  sharedReportsList: document.getElementById("shared-reports-page-list"),
  sharedReportsDepartmentFilter: document.getElementById("shared-reports-department-filter"),
};

// setStatus синхронизирует локальное состояние интерфейса и поля формы.
function setStatus(message) {
  if (elements.statusBanner) {
    elements.statusBanner.textContent = message;
  }
}

// getProfile выводит переиспользуемое состояние для рендера и действий.
function getProfile() {
  return {
    name: window.localStorage.getItem("drivee:templateOwnerName") || "Локальный пользователь",
    department: window.localStorage.getItem("drivee:templateOwnerDepartment") || "",
  };
}

// hydrateProfile синхронизирует локальное состояние интерфейса и поля формы.
function hydrateProfile() {
  const profile = getProfile();
  if (elements.ownerName) {
    elements.ownerName.value = profile.name;
  }
  if (elements.ownerDepartment) {
    elements.ownerDepartment.value = profile.department;
  }
}

// saveProfile выполняет действие пользователя и синхронизирует результат с backend.
function saveProfile() {
  window.localStorage.setItem("drivee:templateOwnerName", elements.ownerName?.value.trim() || "Локальный пользователь");
  window.localStorage.setItem("drivee:templateOwnerDepartment", elements.ownerDepartment?.value.trim() || "");
}

// encodeHeaderValue выравнивает отображаемые или транспортные значения в интерфейсе.
function encodeHeaderValue(value) {
  const normalized = String(value ?? "").trim();
  return normalized ? encodeURIComponent(normalized) : "";
}

// api отправляет авторизованные запросы и нормализует ошибки ответа.
async function api(path, options = {}) {
  const profile = getProfile();
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(profile.name ? { "X-Drivee-User": encodeHeaderValue(profile.name) } : {}),
      ...(profile.department ? { "X-Drivee-Department": encodeHeaderValue(profile.department) } : {}),
      ...(options.headers || {}),
    },
    ...options,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || "Ошибка запроса");
  }
  return data;
}

// escapeHtml выравнивает отображаемые или транспортные значения в интерфейсе.
function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// formatDate выравнивает отображаемые или транспортные значения в интерфейсе.
function formatDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

// viewerName выводит переиспользуемое состояние для рендера и действий.
function viewerName() {
  return (elements.ownerName?.value || "").trim().toLowerCase();
}

// isOwnedByViewer выводит переиспользуемое состояние для рендера и действий.
function isOwnedByViewer(item) {
  const viewer = viewerName();
  if (!viewer) return false;
  return String(item.owner_name || "").trim().toLowerCase() === viewer;
}

// refreshDepartmentSelect загружает данные сервера и обновляет видимое состояние.
function refreshDepartmentSelect(select, items) {
  if (!select) {
    return;
  }

  const departments = [...new Set(
    items
      .filter((item) => item.is_public && !isOwnedByViewer(item) && String(item.owner_department || "").trim())
      .map((item) => String(item.owner_department).trim())
  )].sort((left, right) => left.localeCompare(right, "ru"));

  const current = state.reportDepartmentFilter;
  select.innerHTML = [
    `<option value="">Все отделы</option>`,
    ...departments.map((department) => `<option value="${escapeHtml(department)}">${escapeHtml(department)}</option>`),
  ].join("");

  if (departments.includes(current)) {
    select.value = current;
  } else {
    state.reportDepartmentFilter = "";
    select.value = "";
  }
}

// getMyReports выводит переиспользуемое состояние для рендера и действий.
function getMyReports() {
  return state.reports.filter((report) => isOwnedByViewer(report));
}

// getSharedReports выводит переиспользуемое состояние для рендера и действий.
function getSharedReports() {
  const department = state.reportDepartmentFilter.trim().toLowerCase();
  return state.reports.filter((report) => {
    if (!report.is_public || isOwnedByViewer(report)) {
      return false;
    }
    if (!department) {
      return true;
    }
    return String(report.owner_department || "").trim().toLowerCase() === department;
  });
}

// exportSavedReport выполняет действие пользователя и синхронизирует результат с backend.
async function exportSavedReport(reportId, format) {
  const profile = getProfile();
  const response = await fetch(`/api/v1/reports/${reportId}/export?format=${format}`, {
    headers: {
      ...(profile.name ? { "X-Drivee-User": encodeHeaderValue(profile.name) } : {}),
      ...(profile.department ? { "X-Drivee-Department": encodeHeaderValue(profile.department) } : {}),
    },
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось скачать отчет");
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `report-${reportId}.${format}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

// deleteSavedReport выполняет действие пользователя и синхронизирует результат с backend.
async function deleteSavedReport(reportId) {
  await api(`/api/v1/reports/${reportId}`, { method: "DELETE" });
}

// toggleReportSharing выполняет действие пользователя и синхронизирует результат с backend.
async function toggleReportSharing(reportId, nextPublic, ownerDepartment) {
  await api(`/api/v1/reports/${reportId}/sharing`, {
    method: "PUT",
    body: JSON.stringify({
      is_public: nextPublic,
      owner_department: ownerDepartment,
    }),
  });
}

// renderReportsList обновляет связанную область страницы из текущего состояния.
function renderReportsList(container, reports, isOwnedSection) {
  if (!container) {
    return;
  }

  if (!reports.length) {
    container.innerHTML = `<div class="empty-state">${isOwnedSection ? "Личных отчетов пока нет." : "Открытых отчетов по выбранным условиям пока нет."}</div>`;
    return;
  }

  container.innerHTML = reports
    .map((report) => `
      <article class="report-card">
        <div class="report-card-head">
          <div>
            <strong>${escapeHtml(report.name)}</strong>
            <p>${escapeHtml(report.query_text)}</p>
          </div>
          <div class="template-badges">
            ${report.source === "scheduled" ? '<span class="report-badge scheduled">scheduled</span>' : ""}
            <span class="template-visibility ${report.is_public ? "public" : "private"}">${escapeHtml(report.is_public ? "Открытый" : "Личный")}</span>
          </div>
        </div>
        <div class="report-card-meta">
          <span>Автор: ${escapeHtml(report.owner_name || "—")}</span>
          <span>Отдел: ${escapeHtml(report.owner_department || "Не указан")}</span>
          <span>Обновлен: ${escapeHtml(formatDate(report.updated_at))}</span>
          <span>Строк: ${escapeHtml(report.result?.count ?? 0)}</span>
          ${report.template_name ? `<span>Шаблон: ${escapeHtml(report.template_name)}</span>` : ""}
        </div>
        <div class="button-row compact">
          <button class="mini-button" type="button" data-report-open="${report.id}">Открыть</button>
          <button class="mini-button" type="button" data-report-export="pdf" data-report-id="${report.id}">PDF</button>
          <button class="mini-button" type="button" data-report-export="docx" data-report-id="${report.id}">DOCX</button>
          ${isOwnedSection ? `<button class="mini-button" type="button" data-report-share="${report.id}" data-next-public="${String(!report.is_public)}" data-owner-department="${escapeHtml(report.owner_department || elements.ownerDepartment?.value.trim() || "")}">${report.is_public ? "Сделать личным" : "Открыть доступ"}</button>` : ""}
          ${isOwnedSection ? `<button class="mini-button danger" type="button" data-report-delete="${report.id}">Удалить</button>` : ""}
        </div>
      </article>
    `)
    .join("");
}

// renderReports обновляет связанную область страницы из текущего состояния.
function renderReports() {
  refreshDepartmentSelect(elements.sharedReportsDepartmentFilter, state.reports);
  renderReportsList(elements.myReportsList, getMyReports(), true);
  renderReportsList(elements.sharedReportsList, getSharedReports(), false);
}

// loadReports загружает данные сервера и обновляет видимое состояние.
async function loadReports() {
  state.reports = await api("/api/v1/reports");
  renderReports();
}

document.body.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button) return;

  try {
    if (button.dataset.reportOpen) {
      setStatus("Открываю отчет на главной панели...");
      window.localStorage.setItem("drivee:openReportId", String(button.dataset.reportOpen));
      window.location.href = "/";
      return;
    }

    if (button.dataset.reportExport && button.dataset.reportId) {
      setStatus(`Готовлю экспорт ${button.dataset.reportExport.toUpperCase()}...`);
      await exportSavedReport(button.dataset.reportId, button.dataset.reportExport);
      setStatus("Экспорт отчета готов.");
      return;
    }

    if (button.dataset.reportDelete) {
      setStatus("Удаляю отчет...");
      await deleteSavedReport(button.dataset.reportDelete);
      await loadReports();
      setStatus("Отчет удален.");
      return;
    }

    if (button.dataset.reportShare) {
      const nextPublic = button.dataset.nextPublic === "true";
      const department = String(button.dataset.ownerDepartment || elements.ownerDepartment?.value || "").trim();
      if (nextPublic && !department) {
        throw new Error("Для общего доступа к отчету укажите отдел.");
      }
      setStatus(nextPublic ? "Публикую отчет..." : "Снимаю общий доступ...");
      await toggleReportSharing(button.dataset.reportShare, nextPublic, department);
      await loadReports();
      setStatus(nextPublic ? "Отчет опубликован для команды." : "Отчет снова доступен только владельцу.");
    }
  } catch (error) {
    setStatus(error.message);
  }
});

if (elements.ownerName) {
  elements.ownerName.addEventListener("input", () => {
    saveProfile();
    loadReports().catch((error) => setStatus(error.message));
  });
}

if (elements.ownerDepartment) {
  elements.ownerDepartment.addEventListener("input", () => {
    saveProfile();
    renderReports();
  });
}

if (elements.sharedReportsDepartmentFilter) {
  elements.sharedReportsDepartmentFilter.addEventListener("change", () => {
    state.reportDepartmentFilter = elements.sharedReportsDepartmentFilter.value;
    renderReports();
  });
}

hydrateProfile();
loadReports().catch((error) => {
  setStatus(error.message);
  renderReportsList(elements.myReportsList, [], true);
  renderReportsList(elements.sharedReportsList, [], false);
});
