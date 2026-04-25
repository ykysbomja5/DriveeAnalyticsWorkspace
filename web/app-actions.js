// refreshReports загружает данные сервера и обновляет видимое состояние.
async function refreshReports() {
  if (!elements.reportList) {
    return;
  }
  state.reports = await api("/api/v1/reports").catch(() => []);
  renderReports();
}

const queryHistoryStorageKey = "drivee:queryHistory";
const queryHistoryLimit = 10;

function loadQueryHistory() {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(queryHistoryStorageKey) || "[]");
    state.queryHistory = Array.isArray(parsed)
      ? parsed.map((item) => String(item || "").trim()).filter(Boolean).slice(0, queryHistoryLimit)
      : [];
  } catch {
    state.queryHistory = [];
  }
  renderQueryHistory();
}

function saveQueryHistory(text) {
  const normalized = String(text || "").trim().replace(/\s+/g, " ");
  if (!normalized) {
    return;
  }
  state.queryHistory = [
    normalized,
    ...state.queryHistory.filter((item) => item.toLowerCase() !== normalized.toLowerCase()),
  ].slice(0, queryHistoryLimit);
  try {
    window.localStorage.setItem(queryHistoryStorageKey, JSON.stringify(state.queryHistory));
  } catch {
    // История запросов не должна мешать выполнению аналитического запроса.
  }
  renderQueryHistory();
}

function queryHistoryMatches() {
  return state.queryHistory;
}

let queryHistoryHideTimer = null;

function renderQueryHistory() {
  if (!elements.queryHistory) {
    return;
  }

  // Отменяем отложенное скрытие, если список снова показывается
  if (queryHistoryHideTimer) {
    clearTimeout(queryHistoryHideTimer);
    queryHistoryHideTimer = null;
  }

  const matches = queryHistoryMatches();
  if (!state.queryHistoryVisible || !matches.length) {
    elements.queryHistory.classList.remove("visible");
    // Ждём завершения CSS-перехода (opacity/transform/max-height — 0.18s),
    // прежде чем очистить содержимое
    queryHistoryHideTimer = setTimeout(() => {
      elements.queryHistory.innerHTML = "";
      queryHistoryHideTimer = null;
    }, 200);
    return;
  }

  elements.queryHistory.innerHTML = [
    `<div class="query-history-head">Последние запросы</div>`,
    ...matches.map((query, index) => `
      <button class="query-history-item" type="button" data-query-history-index="${index}">
        ${escapeHtml(query)}
      </button>
    `),
  ].join("");
  elements.queryHistory.classList.add("visible");
}

function applyQueryHistoryItem(index) {
  const matches = queryHistoryMatches();
  const query = matches[index];
  if (!query) {
    return;
  }
  elements.queryInput.value = query;
  state.queryHistoryVisible = false;
  renderQueryHistory();
  elements.queryInput.focus();
  setBanner("Запрос из истории подставлен в редактор.");
}

// refreshTemplates загружает данные сервера и обновляет видимое состояние.
async function refreshTemplates() {
  state.templates = await api("/api/v1/reports/templates").catch(() => []);
  refreshDepartmentFilter();
  renderTemplates();
  renderTemplatePrompts();
}

// openReportById меняет активную область интерфейса без лишнего изменения состояния.
async function openReportById(reportId) {
  try {
    setBanner("Открываю сохранённый отчёт...");
    const run = await api(`/api/v1/reports/${reportId}/run`, { method: "POST" });
    state.run = run;
    state.parse = { intent: run.intent, preview: run.preview, provider: run.provider };
    renderParse();
    renderRun();
    setBanner("Отчёт успешно выполнен.");
  } catch (error) {
    setBanner(error.message);
  }
}

// exportCurrent выполняет действие пользователя и синхронизирует результат с backend.
async function exportCurrent(format) {
  if (!state.run?.sql && !state.run?.result?.rows?.length) {
    setBanner("Сначала получите результат, затем можно скачать отчёт.");
    return;
  }

  const response = await fetch(`/api/v1/reports/export?format=${format}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: buildReportName(),
      query_text: elements.queryInput.value.trim(),
      run: state.run,
    }),
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось подготовить экспорт");
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${buildReportName().replace(/[^\p{L}\p{N}\- ]/gu, "").trim() || "report"}.${format}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

// exportSavedReport выполняет действие пользователя и синхронизирует результат с backend.
async function exportSavedReport(reportId, format) {
  const profile = getTemplateProfile();
  const response = await fetch(`/api/v1/reports/${reportId}/export?format=${format}`, {
    headers: {
      ...(profile.name ? { "X-Drivee-User": encodeHeaderValue(profile.name) } : {}),
      ...(profile.department ? { "X-Drivee-Department": encodeHeaderValue(profile.department) } : {}),
    },
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось скачать отчёт");
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

// renderTemplatePrompts обновляет связанную область страницы из текущего состояния.
function renderTemplatePrompts() {
  if (!elements.samplePrompts) {
    return;
  }

  elements.samplePrompts.innerHTML = "";
  const promptTemplates = [...getMyTemplates(), ...getSharedTemplates()].slice(0, 12);
  if (!promptTemplates.length) {
    elements.samplePrompts.innerHTML = `<span class="empty-inline">Шаблонов пока нет. Создай первый шаблон через кнопку «Добавить шаблон».</span>`;
    return;
  }

  promptTemplates.forEach((template) => {
    const button = document.createElement("button");
    button.className = "prompt-chip";
    button.type = "button";
    button.textContent = template.is_public && !isOwnedTemplate(template) ? `${template.name} · ${template.owner_department || "общий"}` : template.name;
    button.addEventListener("click", () => {
      elements.queryInput.value = template.query_text;
      setBanner(`Шаблон «${template.name}» подставлен в редактор запроса.`);
    });
    elements.samplePrompts.appendChild(button);
  });
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

    if (payload.mode === "apply") {
      elements.queryInput.value = payload.template.query_text || "";
      document.getElementById("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
      setBanner(`Шаблон «${payload.template.name}» подставлен в редактор запроса.`);
      return;
    }

    if (payload.mode === "edit" || payload.mode === "create") {
      window.localStorage.setItem("drivee:pendingTemplate", raw);
      window.location.href = "/templates.html";
    }
  } catch {
    // Игнорируем некорректные данные из localStorage.
  }
}

// applyPendingRunResponse выполняет действие пользователя и синхронизирует результат с backend.
function applyPendingRunResponse() {
  const raw = window.localStorage.getItem("drivee:runResponse");
  if (!raw) {
    return;
  }

  window.localStorage.removeItem("drivee:runResponse");
  try {
    const run = JSON.parse(raw);
    if (!run) {
      return;
    }
    state.run = run;
    state.parse = { intent: run.intent, preview: run.preview, provider: run.provider };
    renderParse();
    renderRun();
    document.getElementById("results").scrollIntoView({ behavior: "smooth", block: "start" });
    setBanner("Результат из архива загружен на главную панель.");
  } catch {
    // Игнорируем некорректные данные из localStorage.
  }
}

// loadInitialData загружает данные сервера и обновляет видимое состояние.
async function loadInitialData() {
  try {
    hydrateTemplateProfile();
    loadQueryHistory();
    const [semantic] = await Promise.all([api("/api/v1/meta/schema"), refreshTemplates(), refreshReports()]);
    state.semantic = semantic;
    renderGlossary();
    applyPendingTemplateAction();
    applyPendingRunResponse();

    const pendingReportId = window.localStorage.getItem("drivee:openReportId");
    if (pendingReportId) {
      window.localStorage.removeItem("drivee:openReportId");
      await openReportById(pendingReportId);
    }
  } catch (error) {
    setBanner(error.message);
  }
}

// parseQuery выравнивает отображаемые или транспортные значения в интерфейсе.
async function parseQuery() {
  if (state.queryRequestInFlight) {
    return;
  }
  const text = elements.queryInput.value.trim();
  if (!text) {
    setBanner("Введите текстовый запрос.");
    return;
  }

  setBanner("Понимаю запрос и сверяю его с бизнес-словариём...");
  saveQueryHistory(text);
  setQueryControlsBusy(true);
  try {
    state.parse = await api("/api/v1/query/parse", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    renderParse();
    setBanner("Запрос понятен. Можно сразу запускать результат или сохранить его как шаблон.");
  } catch (error) {
    setBanner(error.message);
  } finally {
    setQueryControlsBusy(false);
  }
}

// runQuery выполняет действие пользователя и синхронизирует результат с backend.
async function runQuery() {
  if (state.queryRequestInFlight) {
    return;
  }
  const text = elements.queryInput.value.trim();
  if (!text) {
    setBanner("Введите текстовый запрос.");
    return;
  }

  setBanner("Собираю результат, визуализацию и управленческое резюме...");
  saveQueryHistory(text);
  setQueryControlsBusy(true);
  try {
    state.run = await api("/api/v1/query/run", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    state.parse = {
      intent: state.run.intent,
      preview: state.run.preview,
      provider: state.run.provider,
    };
    renderParse();
    renderRun();
    if (!state.run.sql) {
      setBanner(state.run.preview?.clarification || state.run.intent?.clarification || "Нужно уточнить метрику или период, чтобы выполнить SQL.");
      return;
    }
    setBanner(resultReadyBanner(state.run));
  } catch (error) {
    setBanner(error.message);
  } finally {
    setQueryControlsBusy(false);
  }
}

function resultReadyBanner(run) {
  const result = normalizeQueryResult(run?.result);
  if (!result.count) {
    return "Запрос выполнен, но результат пустой.";
  }

  const isSingleMetricRow = result.count === 1 && result.columns.length >= 2 && result.rows[0]?.length >= 2;
  if (isSingleMetricRow) {
    const label = formatColumnValue(result.rows[0][0], result.columns[0], result.rows[0].length, 0);
    const value = formatColumnValue(result.rows[0][1], result.columns[1], result.rows[0].length, 1);
    return `Результат готов: ${label} — ${value}.`;
  }

  return `Результат готов. Получено строк: ${result.count}.`;
}

// saveReport выполняет действие пользователя и синхронизирует результат с backend.
async function saveReport() {
  const text = elements.queryInput.value.trim();
  if (!state.run?.sql) {
    setBanner("Сначала покажите результат, затем можно сохранить отчёт.");
    return;
  }

  const name = buildReportName();
  setBanner("Сохраняю отчёт...");
  try {
    await api("/api/v1/reports", {
      method: "POST",
      body: JSON.stringify({
        name,
        query_text: text,
        sql_text: state.run.sql,
        intent: state.run.intent,
        preview: state.run.preview,
        result: state.run.result,
        provider: state.run.provider,
        source: "manual",
      }),
    });
    await refreshReports();
    setBanner(`Отчёт «${name}» сохранён.`);
  } catch (error) {
    setBanner(error.message);
  }
}

// startTemplateFromCurrent выполняет отдельную часть сценария страницы.
function startTemplateFromCurrent() {
  const profile = getTemplateProfile();
  window.localStorage.setItem("drivee:pendingTemplate", JSON.stringify({
    mode: "create",
    template: {
      owner_name: profile.name,
      owner_department: profile.department,
      name: buildReportName(),
      description: state.parse?.preview?.summary || "Пользовательский шаблон для повторного запуска",
      query_text: elements.queryInput.value.trim(),
      is_public: false,
      schedule: { enabled: false, day_of_week: 1, hour: 13, minute: 0 },
    },
  }));
  window.location.href = "/templates.html";
}

// collectTemplatePayload выводит переиспользуемое состояние для рендера и действий.
function collectTemplatePayload() {
  const [hour, minute] = elements.templateScheduleTime.value.split(":").map((item) => Number(item));
  saveTemplateProfile();
  return {
    owner_name: elements.templateOwnerName.value.trim(),
    owner_department: elements.templateOwnerDepartment.value.trim(),
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
    setBanner("У шаблона должны быть автор, название и текст запроса.");
    return;
  }
  if (payload.is_public && !payload.owner_department) {
    setBanner("Для открытого шаблона укажите отдел, чтобы коллеги могли отфильтровать его.");
    return;
  }

  const templateId = elements.templateId.value.trim();
  const isEditing = Boolean(templateId);
  setBanner(isEditing ? "Обновляю шаблон..." : "Сохраняю шаблон...");

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
    await refreshTemplates();
    resetTemplateForm();
    setBanner(isEditing ? "Шаблон обновлён." : "Шаблон сохранён.");
  } catch (error) {
    setBanner(error.message);
  }
}

// runTemplate выполняет действие пользователя и синхронизирует результат с backend.
async function runTemplate(templateId) {
  setBanner("Запускаю шаблон и сохраняю результат...");
  try {
    const response = await api(`/api/v1/reports/templates/${templateId}/run`, { method: "POST" });
    state.run = response.run;
    state.parse = {
      intent: response.run.intent,
      preview: response.run.preview,
      provider: response.run.provider,
    };
    renderParse();
    renderRun();
    await Promise.all([refreshReports(), refreshTemplates()]);
    setBanner("Шаблон выполнен. Новый снимок отчёта сохранён.");
  } catch (error) {
    setBanner(error.message);
  }
}

// deleteTemplate выполняет действие пользователя и синхронизирует результат с backend.
async function deleteTemplate(templateId) {
  setBanner("Удаляю шаблон...");
  try {
    await api(`/api/v1/reports/templates/${templateId}`, { method: "DELETE" });
    await refreshTemplates();
    if (elements.templateId.value === String(templateId)) {
      resetTemplateForm();
    }
    setBanner("Шаблон удалён.");
  } catch (error) {
    setBanner(error.message);
  }
}

// handleTemplateAction выполняет отдельную часть сценария страницы.
function handleTemplateAction(event) {
  const target = event.target.closest("button");
  if (!target) return;

  const applyId = target.dataset.templateApply;
  const runId = target.dataset.templateRun;
  const editId = target.dataset.templateEdit;
  const deleteId = target.dataset.templateDelete;

  if (applyId) {
    const template = state.templates.find((item) => String(item.id) === applyId);
    if (template) {
      elements.queryInput.value = template.query_text;
      setBanner(`Шаблон «${template.name}» подставлен в редактор запроса.`);
      document.getElementById("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
    }
    return;
  }

  if (runId) {
    runTemplate(runId);
    return;
  }

  if (editId) {
    const template = state.templates.find((item) => String(item.id) === editId);
    if (template) {
      fillTemplateForm(template);
      document.getElementById("templates").scrollIntoView({ behavior: "smooth", block: "start" });
      setBanner(`Шаблон «${template.name}» открыт на редактирование.`);
    }
    return;
  }

  if (deleteId) {
    deleteTemplate(deleteId);
  }
}

// handleReportAction выполняет отдельную часть сценария страницы.
function handleReportAction(event) {
  const target = event.target.closest("button");
  if (!target) return;

  if (target.dataset.reportOpen) {
    openReportById(target.dataset.reportOpen);
    return;
  }

  if (target.dataset.reportExport && target.dataset.reportId) {
    exportSavedReport(target.dataset.reportId, target.dataset.reportExport).catch((error) => setBanner(error.message));
  }
}

if (elements.parseButton) {
  elements.parseButton.addEventListener("click", parseQuery);
}
if (elements.runButton) {
  elements.runButton.addEventListener("click", runQuery);
}
if (elements.queryInput) {
  elements.queryInput.addEventListener("focus", () => {
    state.queryHistoryVisible = true;
    renderQueryHistory();
  });
  elements.queryInput.addEventListener("input", () => {
    state.queryHistoryVisible = true;
    renderQueryHistory();
  });
  elements.queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      state.queryHistoryVisible = false;
      renderQueryHistory();
    }
  });
  elements.queryInput.addEventListener("blur", () => {
    window.setTimeout(() => {
      state.queryHistoryVisible = false;
      renderQueryHistory();
    }, 120);
  });
}
if (elements.queryHistory) {
  elements.queryHistory.addEventListener("pointerdown", (event) => {
    const button = event.target.closest("button[data-query-history-index]");
    if (!button) {
      return;
    }
    event.preventDefault();
    applyQueryHistoryItem(Number(button.dataset.queryHistoryIndex));
  });
}
if (elements.saveButton) {
  elements.saveButton.addEventListener("click", saveReport);
}
if (elements.addTemplateButton) {
  elements.addTemplateButton.addEventListener("click", startTemplateFromCurrent);
}
if (elements.exportPdfButton) {
  elements.exportPdfButton.addEventListener("click", () => exportCurrent("pdf").catch((error) => setBanner(error.message)));
}
if (elements.templateForm) {
  elements.templateForm.addEventListener("submit", submitTemplate);
}
if (elements.templateCancelButton) {
  elements.templateCancelButton.addEventListener("click", resetTemplateForm);
}
if (elements.templateList) {
  elements.templateList.addEventListener("click", handleTemplateAction);
}
if (elements.sharedTemplateList) {
  elements.sharedTemplateList.addEventListener("click", handleTemplateAction);
}
if (elements.templateOwnerName) {
  elements.templateOwnerName.addEventListener("input", () => {
    saveTemplateProfile();
    refreshTemplates().catch((error) => setBanner(error.message));
  });
}
if (elements.templateOwnerDepartment) {
  elements.templateOwnerDepartment.addEventListener("input", () => {
    saveTemplateProfile();
    refreshTemplates().catch((error) => setBanner(error.message));
  });
}
if (elements.sharedDepartmentFilter) {
  elements.sharedDepartmentFilter.addEventListener("change", () => {
    state.templateDepartmentFilter = elements.sharedDepartmentFilter.value;
    renderTemplates();
    renderTemplatePrompts();
  });
}
if (elements.reportList) {
  elements.reportList.addEventListener("click", handleReportAction);
}
if (elements.glossarySearch) {
  elements.glossarySearch.addEventListener("input", renderGlossary);
}
if (elements.glossaryFilters) {
  elements.glossaryFilters.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-kind]");
    if (!button) return;
    state.glossaryKind = button.dataset.kind;
    [...elements.glossaryFilters.querySelectorAll("button")].forEach((item) => item.classList.toggle("active", item === button));
    renderGlossary();
  });
}
if (elements.glossaryList) {
  elements.glossaryList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-glossary-term]");
    if (!button) return;
    const term = button.dataset.glossaryTerm;
    const current = elements.queryInput.value.trim();
    elements.queryInput.value = current ? `${current} ${term}` : term;
    document.getElementById("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

loadInitialData();
