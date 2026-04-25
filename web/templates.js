const state = {
  templates: [],
  templateDepartmentFilter: "",
};

const elements = {
  ownerName: document.getElementById("template-owner-name"),
  ownerDepartment: document.getElementById("template-owner-department"),
  statusBanner: document.getElementById("templates-archive-status"),
  myTemplatesList: document.getElementById("templates-page-list"),
  sharedTemplatesList: document.getElementById("shared-templates-page-list"),
  sharedTemplatesDepartmentFilter: document.getElementById("shared-templates-department-filter"),
  templateForm: document.getElementById("template-form"),
  templateId: document.getElementById("template-id"),
  templateName: document.getElementById("template-name"),
  templateDescription: document.getElementById("template-description"),
  templateQuery: document.getElementById("template-query"),
  templateIsPublic: document.getElementById("template-is-public"),
  templateScheduleEnabled: document.getElementById("template-schedule-enabled"),
  templateScheduleDay: document.getElementById("template-schedule-day"),
  templateScheduleTime: document.getElementById("template-schedule-time"),
  templateCancelButton: document.getElementById("template-cancel-button"),
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

// templateRunStatusView выполняет отдельную часть сценария страницы.
function templateRunStatusView(template) {
  const status = String(template?.last_status || "idle").toLowerCase();
  const labels = {
    idle: "не запускался",
    running: "выполняется",
    ok: "успешно",
    failed: "ошибка",
  };
  return {
    className: ["idle", "running", "ok", "failed"].includes(status) ? status : "idle",
    label: labels[status] || status,
  };
}

// renderTemplateRunBadge обновляет связанную область страницы из текущего состояния.
function renderTemplateRunBadge(template) {
  const status = templateRunStatusView(template);
  return `<span class="template-status ${status.className}">${escapeHtml(status.label)}</span>`;
}

// renderTemplateRunMeta обновляет связанную область страницы из текущего состояния.
function renderTemplateRunMeta(template) {
  const status = String(template?.last_status || "").toLowerCase();
  const rows = Number(template?.last_result_count ?? 0);
  return [
    template.last_run_at ? `<span>Последний запуск: ${escapeHtml(formatDate(template.last_run_at))}</span>` : "",
    status === "ok" ? `<span>Строк: ${escapeHtml(rows)}</span>` : "",
    template.last_error_text ? `<span class="template-error-text">Ошибка: ${escapeHtml(template.last_error_text)}</span>` : "",
  ].filter(Boolean).join("");
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

  const current = state.templateDepartmentFilter;
  select.innerHTML = [
    `<option value="">Все отделы</option>`,
    ...departments.map((department) => `<option value="${escapeHtml(department)}">${escapeHtml(department)}</option>`),
  ].join("");

  if (departments.includes(current)) {
    select.value = current;
  } else {
    state.templateDepartmentFilter = "";
    select.value = "";
  }
}

// getMyTemplates выводит переиспользуемое состояние для рендера и действий.
function getMyTemplates() {
  return state.templates.filter((template) => isOwnedByViewer(template));
}

// getSharedTemplates выводит переиспользуемое состояние для рендера и действий.
function getSharedTemplates() {
  const department = state.templateDepartmentFilter.trim().toLowerCase();
  return state.templates.filter((template) => {
    if (!template.is_public || isOwnedByViewer(template)) {
      return false;
    }
    if (!department) {
      return true;
    }
    return String(template.owner_department || "").trim().toLowerCase() === department;
  });
}

// deleteTemplate выполняет действие пользователя и синхронизирует результат с backend.
async function deleteTemplate(templateId) {
  await api(`/api/v1/reports/templates/${templateId}`, { method: "DELETE" });
}

// toggleTemplateSharing выполняет действие пользователя и синхронизирует результат с backend.
async function toggleTemplateSharing(templateId, nextPublic, ownerDepartment) {
  await api(`/api/v1/reports/templates/${templateId}/sharing`, {
    method: "PUT",
    body: JSON.stringify({
      is_public: nextPublic,
      owner_department: ownerDepartment,
    }),
  });
}

// putTemplateIntoEditor синхронизирует локальное состояние интерфейса и поля формы.
function putTemplateIntoEditor(template, editMode) {
  window.localStorage.setItem("drivee:pendingTemplate", JSON.stringify({
    mode: editMode ? "edit" : "apply",
    template,
  }));
  window.location.href = editMode ? "/templates.html" : "/";
}

// resetTemplateForm синхронизирует локальное состояние интерфейса и поля формы.
function resetTemplateForm() {
  elements.templateId.value = "";
  elements.templateName.value = "";
  elements.templateDescription.value = "";
  elements.templateQuery.value = "";
  if (elements.templateIsPublic) {
    elements.templateIsPublic.checked = false;
  }
  elements.templateScheduleEnabled.checked = false;
  elements.templateScheduleDay.value = "1";
  elements.templateScheduleTime.value = "13:00";
  document.getElementById("template-save-button").textContent = "Сохранить шаблон";
}

// fillTemplateForm синхронизирует локальное состояние интерфейса и поля формы.
function fillTemplateForm(data) {
  elements.templateId.value = data.id || "";
  if (elements.ownerName) {
    elements.ownerName.value = data.owner_name || viewerName();
  }
  if (elements.ownerDepartment) {
    elements.ownerDepartment.value = data.owner_department || (elements.ownerDepartment.value || "").trim();
  }
  elements.templateName.value = data.name || "";
  elements.templateDescription.value = data.description || "";
  elements.templateQuery.value = data.query_text || "";
  if (elements.templateIsPublic) {
    elements.templateIsPublic.checked = Boolean(data.is_public);
  }
  elements.templateScheduleEnabled.checked = Boolean(data.schedule?.enabled);
  elements.templateScheduleDay.value = String(data.schedule?.day_of_week ?? 1);
  elements.templateScheduleTime.value = `${String(data.schedule?.hour ?? 13).padStart(2, "0")}:${String(data.schedule?.minute ?? 0).padStart(2, "0")}`;
  saveProfile();
  document.getElementById("template-save-button").textContent = data.id ? "Обновить шаблон" : "Сохранить шаблон";
}

// collectTemplatePayload выводит переиспользуемое состояние для рендера и действий.
function collectTemplatePayload() {
  const [hour, minute] = elements.templateScheduleTime.value.split(":").map((item) => Number(item));
  saveProfile();
  return {
    owner_name: elements.ownerName.value.trim(),
    owner_department: elements.ownerDepartment.value.trim(),
    name: elements.templateName.value.trim(),
    description: elements.templateDescription.value.trim(),
    query_text: elements.templateQuery.value.trim(),
    is_public: elements.templateIsPublic.checked,
    schedule: {
      enabled: elements.templateScheduleEnabled.checked,
      day_of_week: Number(elements.templateScheduleDay.value),
      hour: Number.isFinite(hour) ? hour : 13,
      minute: Number.isFinite(minute) ? minute : 0,
      timezone: "Europe/Moscow",
    },
  };
}

// submitTemplate выполняет действие пользователя и синхронизирует результат с backend.
async function submitTemplate(event) {
  event.preventDefault();
  const payload = collectTemplatePayload();
  if (!payload.owner_name || !payload.name || !payload.query_text) {
    setStatus("У шаблона должны быть автор, название и текст запроса.");
    return;
  }
  if (payload.is_public && !payload.owner_department) {
    setStatus("Для открытого шаблона укажите отдел, чтобы коллеги могли отфильтровать его.");
    return;
  }

  const templateId = elements.templateId.value.trim();
  const isEditing = Boolean(templateId);
  setStatus(isEditing ? "Обновляю шаблон..." : "Сохраняю шаблон...");

  try {
    if (isEditing) {
      await api(`/api/v1/reports/templates/${templateId}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
    } else {
      await api("/api/v1/reports/templates", {
        method: "POST",
        body: JSON.stringify(payload),
      });
    }
    await loadTemplates();
    resetTemplateForm();
    setStatus(isEditing ? "Шаблон обновлён." : "Шаблон сохранён.");
  } catch (error) {
    setStatus(error.message);
  }
}

// applyPendingTemplateAction выполняет действие пользователя и синхронизирует результат с backend.
function applyPendingTemplateAction() {
  const raw = window.localStorage.getItem("drivee:pendingTemplate");
  if (!raw) {
    return;
  }

  window.localStorage.removeItem("drivee:pendingTemplate");
  try {
    const payload = JSON.parse(raw);
    if (!payload?.template) {
      return;
    }

    if (payload.mode === "edit" || payload.mode === "create") {
      fillTemplateForm(payload.template);
      setStatus(payload.mode === "edit"
        ? `Шаблон «${payload.template.name}» открыт на редактирование.`
        : "Заполните форму шаблона и сохраните.");
      return;
    }
  } catch {
    // Игнорируем некорректные данные из localStorage.
  }
}

// renderTemplatesList обновляет связанную область страницы из текущего состояния.
function renderTemplatesList(container, templates, isOwnedSection) {
  if (!container) {
    return;
  }

  if (!templates.length) {
    container.innerHTML = `<div class="empty-state">${isOwnedSection ? "Личных шаблонов пока нет." : "Открытых шаблонов по выбранным условиям пока нет."}</div>`;
    return;
  }

  container.innerHTML = templates
    .map((template) => `
      <article class="template-card${isOwnedSection ? "" : " shared"}">
        <div class="template-card-head">
          <div>
            <strong>${escapeHtml(template.name)}</strong>
            <p>${escapeHtml(template.description || template.query_text)}</p>
          </div>
          <div class="template-badges">
            <span class="template-status ${template.schedule?.enabled ? "live" : "draft"}">${escapeHtml(template.schedule?.enabled ? "с расписанием" : "ручной")}</span>
            <span class="template-visibility ${template.is_public ? "public" : "private"}">${escapeHtml(template.is_public ? "Открытый" : "Личный")}</span>
            ${renderTemplateRunBadge(template)}
          </div>
        </div>
        <div class="template-query-preview">${escapeHtml(template.query_text)}</div>
        <div class="template-meta">
          <span>Автор: ${escapeHtml(template.owner_name || "—")}</span>
          <span>Отдел: ${escapeHtml(template.owner_department || "Не указан")}</span>
          <span>${escapeHtml(template.schedule?.label || "Без расписания")}</span>
          ${template.schedule?.next_run ? `<span>Следующий запуск: ${escapeHtml(formatDate(template.schedule.next_run))}</span>` : ""}
          ${renderTemplateRunMeta(template)}
        </div>
        <div class="button-row compact">
          <button class="mini-button" type="button" data-template-apply="${template.id}">Использовать</button>
          <button class="mini-button" type="button" data-template-run="${template.id}">Запустить</button>
          ${isOwnedSection ? `<button class="mini-button" type="button" data-template-edit="${template.id}">Редактировать</button>` : ""}
          ${isOwnedSection ? `<button class="mini-button" type="button" data-template-share="${template.id}" data-next-public="${String(!template.is_public)}" data-owner-department="${escapeHtml(template.owner_department || elements.ownerDepartment?.value.trim() || "")}">${template.is_public ? "Сделать личным" : "Открыть доступ"}</button>` : ""}
          ${isOwnedSection ? `<button class="mini-button danger" type="button" data-template-delete="${template.id}">Удалить</button>` : ""}
        </div>
      </article>
    `)
    .join("");
}

// renderTemplates обновляет связанную область страницы из текущего состояния.
function renderTemplates() {
  refreshDepartmentSelect(elements.sharedTemplatesDepartmentFilter, state.templates);
  renderTemplatesList(elements.myTemplatesList, getMyTemplates(), true);
  renderTemplatesList(elements.sharedTemplatesList, getSharedTemplates(), false);
}

// loadTemplates загружает данные сервера и обновляет видимое состояние.
async function loadTemplates() {
  state.templates = await api("/api/v1/reports/templates");
  renderTemplates();
}

document.body.addEventListener("click", async (event) => {
  const button = event.target.closest("button");
  if (!button) return;

  try {
    if (button.dataset.templateApply) {
      const template = state.templates.find((item) => String(item.id) === String(button.dataset.templateApply));
      if (!template) {
        throw new Error("Шаблон не найден.");
      }
      setStatus(`Открываю шаблон «${template.name}» в редакторе...`);
      putTemplateIntoEditor(template, false);
      return;
    }

    if (button.dataset.templateEdit) {
      const template = state.templates.find((item) => String(item.id) === String(button.dataset.templateEdit));
      if (!template) {
        throw new Error("Шаблон не найден.");
      }
      setStatus(`Открываю шаблон «${template.name}» на редактирование...`);
      putTemplateIntoEditor(template, true);
      return;
    }

    if (button.dataset.templateRun) {
      setStatus("Запускаю шаблон...");
      const response = await api(`/api/v1/reports/templates/${button.dataset.templateRun}/run`, { method: "POST" });
      window.localStorage.setItem("drivee:runResponse", JSON.stringify(response.run));
      if (response.report?.id) {
        window.localStorage.setItem("drivee:openReportId", String(response.report.id));
      }
      window.location.href = "/#results";
      return;
    }

    if (button.dataset.templateDelete) {
      setStatus("Удаляю шаблон...");
      await deleteTemplate(button.dataset.templateDelete);
      await loadTemplates();
      setStatus("Шаблон удален.");
      return;
    }

    if (button.dataset.templateShare) {
      const nextPublic = button.dataset.nextPublic === "true";
      const department = String(button.dataset.ownerDepartment || elements.ownerDepartment?.value || "").trim();
      if (nextPublic && !department) {
        throw new Error("Для общего доступа к шаблону укажите отдел.");
      }
      setStatus(nextPublic ? "Публикую шаблон..." : "Снимаю общий доступ...");
      await toggleTemplateSharing(button.dataset.templateShare, nextPublic, department);
      await loadTemplates();
      setStatus(nextPublic ? "Шаблон опубликован для команды." : "Шаблон снова доступен только владельцу.");
    }
  } catch (error) {
    setStatus(error.message);
  }
});

if (elements.templateForm) {
  elements.templateForm.addEventListener("submit", submitTemplate);
}
if (elements.templateCancelButton) {
  elements.templateCancelButton.addEventListener("click", resetTemplateForm);
}

if (elements.ownerName) {
  elements.ownerName.addEventListener("input", () => {
    saveProfile();
    loadTemplates().catch((error) => setStatus(error.message));
  });
}

if (elements.ownerDepartment) {
  elements.ownerDepartment.addEventListener("input", () => {
    saveProfile();
    renderTemplates();
  });
}

if (elements.sharedTemplatesDepartmentFilter) {
  elements.sharedTemplatesDepartmentFilter.addEventListener("change", () => {
    state.templateDepartmentFilter = elements.sharedTemplatesDepartmentFilter.value;
    renderTemplates();
  });
}

hydrateProfile();
applyPendingTemplateAction();
loadTemplates().catch((error) => {
  setStatus(error.message);
  renderTemplatesList(elements.myTemplatesList, [], true);
  renderTemplatesList(elements.sharedTemplatesList, [], false);
});
