const state = {
  parse: null,
  run: null,
  reports: [],
  templates: [],
  semantic: null,
  provider: "qwen",
  glossaryKind: "all",
  templateDepartmentFilter: "",
  queryRequestInFlight: false,
  queryHistory: [],
  queryHistoryVisible: false,
};

const elements = {
  queryInput: document.getElementById("query-input"),
  queryHistory: document.getElementById("query-history"),
  parseButton: document.getElementById("parse-button"),
  runButton: document.getElementById("run-button"),
  saveButton: document.getElementById("save-button"),
  addTemplateButton: document.getElementById("add-template-button"),
  exportPdfButton: document.getElementById("export-pdf-button"),
  responseBanner: document.getElementById("response-banner"),
  sidebarProvider: document.getElementById("sidebar-provider"),
  intentSummary: document.getElementById("intent-summary"),
  intentMetric: document.getElementById("intent-metric"),
  intentGroup: document.getElementById("intent-group"),
  intentPeriod: document.getElementById("intent-period"),
  intentConfidence: document.getElementById("intent-confidence"),
  intentProvider: document.getElementById("intent-provider"),
  intentFilters: document.getElementById("intent-filters"),
  clarificationBox: document.getElementById("clarification-box"),
  intentHighlightStrip: document.getElementById("intent-highlight-strip"),
  sqlPreview: document.getElementById("sql-preview"),
  chartSurface: document.getElementById("chart-surface"),
  chartInsights: document.getElementById("chart-insights"),
  rankingPanel: document.getElementById("ranking-panel"),
  businessSummary: document.getElementById("business-summary"),
  summaryStats: document.getElementById("summary-stats"),
  storyGrid: document.getElementById("story-grid"),
  resultTableHead: document.querySelector("#result-table thead"),
  resultTableBody: document.querySelector("#result-table tbody"),
  resultCount: document.getElementById("result-count"),
  reportList: document.getElementById("report-list"),
  samplePrompts: document.getElementById("sample-prompts"),
  glossarySearch: document.getElementById("glossary-search"),
  glossaryFilters: document.getElementById("glossary-filters"),
  glossaryList: document.getElementById("glossary-list"),
  templateForm: document.getElementById("template-form"),
  templateId: document.getElementById("template-id"),
  templateOwnerName: document.getElementById("template-owner-name"),
  templateOwnerDepartment: document.getElementById("template-owner-department"),
  templateName: document.getElementById("template-name"),
  templateDescription: document.getElementById("template-description"),
  templateQuery: document.getElementById("template-query"),
  templateIsPublic: document.getElementById("template-is-public"),
  templateScheduleEnabled: document.getElementById("template-schedule-enabled"),
  templateScheduleDay: document.getElementById("template-schedule-day"),
  templateScheduleTime: document.getElementById("template-schedule-time"),
  templateCancelButton: document.getElementById("template-cancel-button"),
  templateList: document.getElementById("template-list"),
  sharedTemplateList: document.getElementById("shared-template-list"),
  sharedDepartmentFilter: document.getElementById("shared-department-filter"),
};

// api отправляет авторизованные запросы и нормализует ошибки ответа.
async function api(path, options = {}) {
  const profile = getTemplateProfile();
  const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : 120000;
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  const { timeoutMs: _timeoutMs, signal: _signal, ...fetchOptions } = options;
  const headers = {
    "Content-Type": "application/json",
    ...(profile.name ? { "X-Drivee-User": encodeHeaderValue(profile.name) } : {}),
    ...(profile.department ? { "X-Drivee-Department": encodeHeaderValue(profile.department) } : {}),
    ...(fetchOptions.headers || {}),
  };
  let response;
  try {
    response = await fetch(path, {
      headers,
      ...fetchOptions,
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("Запрос выполняется слишком долго. Проверьте логи query-service и SQL к БД.");
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || "Ошибка запроса");
  }
  return data;
}

// setBanner синхронизирует локальное состояние интерфейса и поля формы.
function setBanner(message) {
  elements.responseBanner.textContent = message;
}

// setQueryControlsBusy синхронизирует локальное состояние интерфейса и поля формы.
function setQueryControlsBusy(isBusy) {
  state.queryRequestInFlight = isBusy;
  elements.parseButton.disabled = isBusy;
  elements.runButton.disabled = isBusy;
}

// getTemplateProfile выводит переиспользуемое состояние для рендера и действий.
function getTemplateProfile() {
  return {
    name: window.localStorage.getItem("drivee:templateOwnerName") || "Локальный пользователь",
    department: window.localStorage.getItem("drivee:templateOwnerDepartment") || "",
  };
}

// saveTemplateProfile выполняет действие пользователя и синхронизирует результат с backend.
function saveTemplateProfile() {
  const name = elements.templateOwnerName?.value.trim() || "";
  const department = elements.templateOwnerDepartment?.value.trim() || "";
  window.localStorage.setItem("drivee:templateOwnerName", name);
  window.localStorage.setItem("drivee:templateOwnerDepartment", department);
}

// encodeHeaderValue выравнивает отображаемые или транспортные значения в интерфейсе.
function encodeHeaderValue(value) {
  const normalized = String(value ?? "").trim();
  return normalized ? encodeURIComponent(normalized) : "";
}

// hydrateTemplateProfile синхронизирует локальное состояние интерфейса и поля формы.
function hydrateTemplateProfile() {
  const profile = getTemplateProfile();
  if (elements.templateOwnerName) {
    elements.templateOwnerName.value = profile.name;
  }
  if (elements.templateOwnerDepartment) {
    elements.templateOwnerDepartment.value = profile.department;
  }
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

// formatConfidence выравнивает отображаемые или транспортные значения в интерфейсе.
function formatConfidence(intent) {
  if (!intent) return "—";
  const percent = `${Math.round((intent.confidence || 0) * 100)}%`;
  return `${percent} - ${state.parse?.preview?.confidence_label || "—"}`;
}

// formatNumber выравнивает отображаемые или транспортные значения в интерфейсе.
function formatNumber(value, options = {}) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value ?? "");
  }
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: options.minimumFractionDigits ?? 0,
    maximumFractionDigits: options.maximumFractionDigits ?? 2,
  }).format(numeric);
}

// formatCurrency выравнивает отображаемые или транспортные значения в интерфейсе.
function formatCurrency(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value ?? "");
  }
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "RUB",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(numeric);
}

// parseNumeric выравнивает отображаемые или транспортные значения в интерфейсе.
function parseNumeric(value) {
  if (typeof value === "number") return value;
  const numeric = Number(String(value ?? "").replace(/\s/g, "").replace(",", "."));
  return Number.isFinite(numeric) ? numeric : 0;
}

// isNumericLike выводит переиспользуемое состояние для рендера и действий.
function isNumericLike(value) {
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  const normalized = String(value ?? "").replace(/\s/g, "").replace(",", ".").trim();
  return /^-?\d+(?:\.\d+)?$/.test(normalized);
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

// formatDateOnly выравнивает отображаемые или транспортные значения в интерфейсе.
function formatDateOnly(value) {
  if (!value) return "";
  const date = /\d{2}:\d{2}/.test(String(value)) ? new Date(value) : new Date(`${value}T00:00:00`);
  if (Number.isNaN(date.getTime())) return String(value ?? "");
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(date);
}

// getViewerName выводит переиспользуемое состояние для рендера и действий.
function getViewerName() {
  return (window.localStorage.getItem("drivee:templateOwnerName") || "").trim();
}

// getViewerDepartment выводит переиспользуемое состояние для рендера и действий.
function getViewerDepartment() {
  return (window.localStorage.getItem("drivee:templateOwnerDepartment") || "").trim();
}

// isOwnedTemplate выводит переиспользуемое состояние для рендера и действий.
function isOwnedTemplate(template) {
  const viewerName = getViewerName().toLowerCase();
  if (!viewerName) return false;
  return String(template.owner_name || "").trim().toLowerCase() === viewerName;
}

// getMyTemplates выводит переиспользуемое состояние для рендера и действий.
function getMyTemplates() {
  return state.templates.filter((template) => isOwnedTemplate(template));
}

// getSharedTemplates выводит переиспользуемое состояние для рендера и действий.
function getSharedTemplates() {
  const department = state.templateDepartmentFilter.trim().toLowerCase();
  return state.templates.filter((template) => {
    if (!template.is_public || isOwnedTemplate(template)) {
      return false;
    }
    if (!department) {
      return true;
    }
    return String(template.owner_department || "").trim().toLowerCase() === department;
  });
}

// refreshDepartmentFilter загружает данные сервера и обновляет видимое состояние.
function refreshDepartmentFilter() {
  if (!elements.sharedDepartmentFilter) {
    return;
  }

  const departments = [...new Set(
    state.templates
      .filter((template) => template.is_public && !isOwnedTemplate(template) && String(template.owner_department || "").trim())
      .map((template) => String(template.owner_department).trim())
  )].sort((a, b) => a.localeCompare(b, "ru"));

  const current = state.templateDepartmentFilter;
  elements.sharedDepartmentFilter.innerHTML = [
    `<option value="">Все отделы</option>`,
    ...departments.map((department) => `<option value="${escapeHtml(department)}">${escapeHtml(department)}</option>`),
  ].join("");

  if (departments.includes(current)) {
    elements.sharedDepartmentFilter.value = current;
  } else {
    state.templateDepartmentFilter = "";
    elements.sharedDepartmentFilter.value = "";
  }
}

// inferProvider выводит переиспользуемое состояние для рендера и действий.
function inferProvider(run) {
  return run?.provider || state.provider || "qwen";
}

// formatProviderLabel выравнивает отображаемые или транспортные значения в интерфейсе.
function formatProviderLabel(provider) {
  const normalized = String(provider || "").trim().toLowerCase();
  switch (normalized) {
    case "qwen":
    case "cerebras":
      return "Qwen / Cerebras";
    default:
      return provider || "Qwen / Cerebras";
  }
}

function normalizeMetricId(metricId) {
  const normalized = String(metricId || "").trim().toLowerCase();
  switch (normalized) {
    case "price_threshold_share":
    case "order_price_threshold_share":
    case "price_threshold_rate":
    case "price_share":
    case "price_percent":
    case "price_threshold_percent":
      return "order_price_threshold_rate";
    case "completed_rides":
    case "unique_completed_rides":
    case "completed_trips":
      return "completed_orders";
    case "orders":
    case "rides":
      return "total_orders";
    case "cancelled_orders":
    case "canceled_orders":
      return "cancellations";
    default:
      return normalized;
  }
}

function isTechnicalLabel(value) {
  return /^[a-z][a-z0-9_]*$/i.test(String(value || "").trim());
}

function humanizeIdentifier(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const words = text.replaceAll("_", " ").split(/\s+/).filter(Boolean);
  if (!words.length) return "";
  return words
    .map((word, index) => {
      const lowered = word.toLowerCase();
      return index === 0 ? lowered.charAt(0).toUpperCase() + lowered.slice(1) : lowered;
    })
    .join(" ");
}

function normalizeGroupLabel(value) {
  const text = String(value || "").trim();
  const normalized = text.toLowerCase();
  if (!text || ["none", "null", "no", "без", "без разбивки", "нет", "не задано"].includes(normalized)) {
    return "Без разбивки";
  }
  const labels = {
    day: "День",
    week: "Неделя",
    month: "Месяц",
    city: "Город",
    city_id: "Город",
    status_order: "Статус заказа",
    status_tender: "Статус тендера",
  };
  return labels[normalized] || (isTechnicalLabel(text) ? humanizeIdentifier(text) : text);
}


const KNOWN_ORDER_STATUSES = {
  done: "Завершён",
  completed: "Завершён",
  complete: "Завершён",
  accept: "Принят",
  accepted: "Принят",
  assigned: "Назначен",
  searching: "Поиск водителя",
  search: "Поиск водителя",
  cancelled: "Отменён",
  canceled: "Отменён",
  cancel: "Отменён",
  clientcancel: "Отменён клиентом",
  drivercancel: "Отменён водителем",
  in_progress: "В поездке",
  started: "В поездке",
};

const KNOWN_TENDER_STATUSES = {
  done: "Завершён",
  accept: "Принят",
  accepted: "Принят",
  created: "Создан",
  new: "Новый",
  searching: "Поиск водителя",
  cancel: "Отменён",
  cancelled: "Отменён",
  canceled: "Отменён",
  expired: "Истёк",
  failed: "Неуспешно",
};

function inferGroupValueKind(result, index = 1) {
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const sample = rows
    .map((row) => String(row?.[index] ?? "").trim().toLowerCase())
    .filter(Boolean)
    .slice(0, 12);
  if (!sample.length) {
    return "group";
  }
  if (sample.every((value) => Object.prototype.hasOwnProperty.call(KNOWN_ORDER_STATUSES, value))) {
    return "status_order";
  }
  if (sample.every((value) => Object.prototype.hasOwnProperty.call(KNOWN_TENDER_STATUSES, value))) {
    return "status_tender";
  }
  if (sample.every((value) => looksLikeDateLabel(value))) {
    return "period";
  }
  return "group";
}

function inferGroupColumnLabel(result, index = 1) {
  const explicit = state.parse?.preview?.group_by_label || state.parse?.intent?.group_by || "";
  const explicitLabel = normalizeGroupLabel(explicit);
  const hasDedicatedPeriodColumn = Array.isArray(result?.columns) && result.columns.some((column, columnIndex) => {
    if (columnIndex === index) {
      return false;
    }
    return ["period_value", "period_label", "stat_date"].includes(String(column || "").trim().toLowerCase());
  });
  if (explicitLabel && explicitLabel !== "Без разбивки" && !(hasDedicatedPeriodColumn && ["День", "Неделя", "Месяц"].includes(explicitLabel))) {
    return explicitLabel;
  }
  switch (inferGroupValueKind(result, index)) {
    case "status_order":
      return "Статус заказа";
    case "status_tender":
      return "Статус тендера";
    case "period":
      return "Период";
    default:
      return "Категория";
  }
}

function humanizeGroupValue(value, result, index = 1) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  switch (inferGroupValueKind(result, index)) {
    case "status_order":
      return KNOWN_ORDER_STATUSES[text.toLowerCase()] || text;
    case "status_tender":
      return KNOWN_TENDER_STATUSES[text.toLowerCase()] || text;
    case "period":
      return formatDateOnly(text) || text;
    default:
      return text;
  }
}

function humanPeriodLabel(value) {
  const text = String(value || "").trim();
  const normalized = text.toLowerCase();
  const labels = {
    "последний месяц от последней даты в бд": "последний месяц в данных",
    "последний месяц от последней даты в данных": "последний месяц в данных",
    "последние 30 дней от последней даты в бд": "последние 30 дней в данных",
    "последние 30 дней от последней даты в данных": "последние 30 дней в данных",
  };
  return labels[normalized] || text;
}

function currentFilters() {
  return state.run?.intent?.filters || state.parse?.intent?.filters || [];
}

function normalizeFilterField(field) {
  const normalized = String(field || "").trim().toLowerCase();
  switch (normalized) {
    case "price":
    case "price_local":
    case "order_price":
    case "price_order":
    case "price_order_local":
    case "стоимость":
    case "цена":
      return "final_price_local";
    default:
      return normalized;
  }
}

function filterFieldLabel(field) {
  switch (normalizeFilterField(field)) {
    case "final_price_local":
    case "price_order_local":
    case "price_tender_local":
    case "price_start_local":
    case "avg_price_local":
    case "gross_revenue_local":
      return "Стоимость заказа";
    case "completed_orders":
      return "Завершённые заказы";
    case "cancelled_orders":
      return "Отменённые заказы";
    case "total_orders":
      return "Все заказы";
    case "city":
    case "city_id":
      return "Город";
    case "status_order":
      return "Статус заказа";
    case "status_tender":
      return "Статус тендера";
    case "stat_date":
      return "Дата";
    default:
      return isTechnicalLabel(field) ? humanizeIdentifier(field) : String(field || "").trim();
  }
}

function filterOperatorLabel(operator) {
  switch (String(operator || "").trim()) {
    case ">":
      return "выше";
    case ">=":
      return "не ниже";
    case "<":
      return "ниже";
    case "<=":
      return "не выше";
    case "=":
    case "==":
      return "равно";
    case "!=":
    case "<>":
      return "не равно";
    case "like":
    case "ilike":
      return "содержит";
    default:
      return String(operator || "").trim();
  }
}

function formatFilterValueForLabel(field, value) {
  const cleaned = String(value ?? "").trim().replace(/^['"]|['"]$/g, "");
  if (!cleaned) return "";
  switch (normalizeFilterField(field)) {
    case "final_price_local":
    case "price_order_local":
    case "price_tender_local":
    case "price_start_local":
    case "avg_price_local":
    case "gross_revenue_local":
      return `${cleaned} ₽`;
    default:
      return cleaned;
  }
}

function parseLooseFilterText(value) {
  const text = String(value || "").trim();
  const match = text.match(/^([a-zA-Z0-9_]+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$/);
  if (!match) return { value: text };
  return {
    field: match[1],
    operator: match[2],
    value: match[3].replace(/^['"]|['"]$/g, ""),
  };
}

function humanFilterLabel(filter) {
  const normalized = typeof filter === "string" ? parseLooseFilterText(filter) : filter || {};
  const fieldLabel = filterFieldLabel(normalized.field || normalized.column || normalized.name);
  const operatorLabel = filterOperatorLabel(normalized.operator || normalized.op);
  const valueLabel = formatFilterValueForLabel(normalized.field || normalized.column || normalized.name, normalized.value ?? normalized.val ?? normalized.threshold ?? normalized.value);
  if (!fieldLabel && !operatorLabel) {
    return String(normalized.value || "").trim();
  }
  return [fieldLabel, operatorLabel, valueLabel].filter(Boolean).join(" ");
}

function priceThresholdMetricTitle(filters = currentFilters()) {
  const priceFilter = (filters || [])
    .map((item) => (typeof item === "string" ? parseLooseFilterText(item) : item || {}))
    .find((item) => normalizeFilterField(item.field || item.column || item.name) === "final_price_local");

  if (!priceFilter) {
    return "Доля заказов по порогу стоимости";
  }

  const value = formatFilterValueForLabel("final_price_local", priceFilter.value ?? priceFilter.val ?? priceFilter.threshold);
  switch (String(priceFilter.operator || priceFilter.op || "").trim()) {
    case ">":
      return `Доля заказов дороже ${value}`;
    case ">=":
      return `Доля заказов не дешевле ${value}`;
    case "<":
      return `Доля заказов дешевле ${value}`;
    case "<=":
      return `Доля заказов не дороже ${value}`;
    case "=":
    case "==":
      return `Доля заказов стоимостью ${value}`;
    default:
      return "Доля заказов по порогу стоимости";
  }
}

function humanMetricLabel(metricId) {
  const normalized = normalizeMetricId(metricId);
  switch (normalized) {
    case "revenue":
      return "Выручка";
    case "avg_price":
      return "Средняя стоимость";
    case "cancellation_rate":
      return "Соотношение отмен к завершённым";
    case "order_price_threshold_rate":
      return priceThresholdMetricTitle();
    case "avg_duration_minutes":
      return "Средняя длительность";
    case "avg_distance_meters":
      return "Средняя дистанция";
    case "completed_orders":
      return "Завершённые заказы";
    case "total_orders":
      return "Все заказы";
    case "cancellations":
      return "Отмены";
    case "custom_sql":
    case "qwen_sql":
      return "Показатель по запросу";
    default:
      return humanizeIdentifier(metricId) || "Показатель";
  }
}

// metricTitle выводит переиспользуемое состояние для рендера и действий.
function metricTitle() {
  const metricId = currentMetricId();
  const label = state.parse?.preview?.metric_label || "";
  if (normalizeMetricId(metricId) === "order_price_threshold_rate") {
    return priceThresholdMetricTitle();
  }
  if (!label || isTechnicalLabel(label)) {
    return humanMetricLabel(metricId);
  }
  return label;
}

// groupTitle выводит переиспользуемое состояние для рендера и действий.
function groupTitle() {
  return normalizeGroupLabel(state.parse?.preview?.group_by_label || state.parse?.intent?.group_by || "");
}

// periodTitle выводит переиспользуемое состояние для рендера и действий.
function periodTitle() {
  return humanPeriodLabel(state.parse?.intent?.period?.label || "выбранный период");
}

// currentMetricId выводит переиспользуемое состояние для рендера и действий.
function currentMetricId() {
  return normalizeMetricId(state.run?.intent?.metric || state.parse?.intent?.metric || "");
}

// metricUnitLabel выводит переиспользуемое состояние для рендера и действий.
function metricUnitLabel(metricId = currentMetricId()) {
  switch (normalizeMetricId(metricId)) {
    case "revenue":
    case "avg_price":
      return "₽";
    case "cancellation_rate":
    case "order_price_threshold_rate":
      return "%";
    case "avg_duration_minutes":
      return "мин";
    case "avg_distance_meters":
      return "м";
    case "completed_orders":
    case "total_orders":
    case "cancellations":
      return "шт.";
    default:
      return "";
  }
}

// metricTitleWithUnit выводит переиспользуемое состояние для рендера и действий.
function metricTitleWithUnit() {
  const unit = metricUnitLabel();
  if (!unit || unit === "%") {
    return metricTitle();
  }
  return `${metricTitle()}, ${unit}`;
}

// formatMetricValue выравнивает отображаемые или транспортные значения в интерфейсе.
function formatMetricValue(value, metricId = currentMetricId()) {
  switch (normalizeMetricId(metricId)) {
    case "revenue":
    case "avg_price":
      return formatCurrency(value);
    case "cancellation_rate":
      return formatPercent(Number(value), 1);
    case "order_price_threshold_rate":
      return formatPercent(normalizePercentRatio(Number(value)), 1);
    case "avg_duration_minutes":
      return `${formatNumber(value)} мин`;
    case "avg_distance_meters":
      return `${formatNumber(value)} м`;
    case "completed_orders":
    case "total_orders":
    case "cancellations":
      return `${formatNumber(value)} шт.`;
    default:
      return formatNumber(value);
  }
}

function normalizePercentRatio(value) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.abs(value) > 1 ? value / 100 : value;
}

// formatColumnValue выравнивает отображаемые или транспортные значения в интерфейсе.
function formatColumnValue(value, column, rowLength = 0, index = 0, result = null) {
  if (value == null || value === "") {
    return "";
  }

  if (index === 0 && rowLength > 1) {
    if (column === "period_value" || column === "stat_date") {
      return formatDateOnly(value);
    }
    if (/timestamp|_local$|created_at$|updated_at$/.test(column)) {
      return formatDate(value) || String(value);
    }
    return String(value);
  }

  switch (column) {
    case "metric_value":
      if (state.run?.chart?.type === "histogram") {
        return formatMetricValue(value, "total_orders");
      }
      return formatMetricValue(value);
    case "revenue_value":
    case "avg_price_value":
      return formatCurrency(value);
    case "cancellation_rate_value":
      return formatPercent(normalizePercentRatio(Number(value)), 1);
    case "completed_orders_value":
    case "cancellations_value":
    case "total_orders_value":
      return formatNumber(value);
    case "group_value":
      return humanizeGroupValue(value, result, index);
    case "gross_revenue_local":
    case "price_order_local":
    case "price_tender_local":
    case "price_start_local":
    case "final_price_local":
    case "avg_price_local":
      return formatCurrency(value);
    case "avg_duration_seconds":
    case "duration_in_seconds":
      return `${formatNumber(value)} сек`;
    case "avg_distance_meters":
    case "distance_in_meters":
      return `${formatNumber(value)} м`;
    default:
      return formatNumber(value);
  }
}

// buildReportName выводит переиспользуемое состояние для рендера и действий.
function buildReportName() {
  if (state.parse?.preview?.metric_label) {
    const metric = metricTitle();
    const group = state.parse?.preview?.group_by_label;
    return group ? `${metric} по ${group.toLowerCase()} - ${periodTitle()}` : `${metric} - ${periodTitle()}`;
  }
  const fallback = elements.queryInput.value.trim();
  return fallback ? fallback.slice(0, 60) : "Аналитический отчёт";
}

// displayColumnName выравнивает отображаемые или транспортные значения в интерфейсе.
function displayColumnName(column, result = null, index = -1) {
  switch (column) {
    case "group_value":
      return result ? inferGroupColumnLabel(result, index) : groupTitle();
    case "period_value":
    case "period_label":
      return "Период";
    case "bucket_value":
    case "bucket_label":
      return "Интервал";
    case "metric_value":
      if (state.run?.chart?.type === "histogram") {
        return "Количество заказов, шт.";
      }
      return metricTitleWithUnit();
    case "revenue_value":
      return "Выручка, ₽";
    case "completed_orders_value":
      return "Завершённые поездки";
    case "cancellations_value":
      return "Отмены";
    case "avg_price_value":
      return "Средняя стоимость, ₽";
    case "cancellation_rate_value":
      return "Доля отмен";
    case "total_orders_value":
      return "Все заказы";
    case "city_id":
      return "Город (ID)";
    case "order_id":
      return "Заказ";
    case "tender_id":
      return "Тендер";
    case "user_id":
      return "Клиент";
    case "driver_id":
      return "Водитель";
    case "status_order":
      return "Статус заказа";
    case "status_tender":
      return "Статус тендера";
    case "order_timestamp":
      return "Создание заказа";
    case "tender_timestamp":
      return "Создание тендера";
    case "driveraccept_timestamp":
      return "Принят водителем";
    case "driverarrived_timestamp":
      return "Водитель прибыл";
    case "driverstarttheride_timestamp":
      return "Начало поездки";
    case "driverdone_timestamp":
      return "Завершение поездки";
    case "clientcancel_timestamp":
      return "Отмена клиентом";
    case "drivercancel_timestamp":
      return "Отмена водителем";
    case "order_modified_local":
      return "Изменён локально";
    case "cancel_before_accept_local":
      return "Отмена до принятия";
    case "distance_in_meters":
      return "Дистанция, м";
    case "duration_in_seconds":
      return "Длительность, сек";
    case "gross_revenue_local":
      return "Выручка, ₽";
    case "avg_price_local":
      return "Средняя стоимость, ₽";
    case "avg_distance_meters":
      return "Средняя дистанция, м";
    case "avg_duration_seconds":
      return "Средняя длительность, сек";
    case "completed_orders":
      return "Завершенные заказы";
    case "cancelled_orders":
      return "Отмены";
    case "total_orders":
      return "Все заказы";
    case "price_threshold_share":
    case "order_price_threshold_rate":
      return priceThresholdMetricTitle();
    case "final_price_local":
      return "Стоимость заказа, ₽";
    default:
      return isTechnicalLabel(column) ? humanizeIdentifier(column) : column;
  }
}

// getSeries выводит переиспользуемое состояние для рендера и действий.
function getSeries(result) {
  const normalized = normalizeQueryResult(result);
  if (!normalized.rows.length || normalized.columns.length < 2) {
    return [];
  }
  const indexes = inferSeriesColumnIndexes(normalized);
  if (!indexes) {
    return [];
  }
  return normalized.rows
    .map((row) => ({
      label: String(row[indexes.labelIndex] ?? "").trim(),
      value: parseNumeric(row[indexes.metricIndex]),
      raw: row,
    }))
    .filter((item) => item.label && Number.isFinite(item.value));
}

// canRenderSeries выводит переиспользуемое состояние для рендера и действий.
function canRenderSeries(result) {
  const normalized = normalizeQueryResult(result);
  return Boolean(
    normalized.rows.length &&
    normalized.columns.length >= 2 &&
    inferSeriesColumnIndexes(normalized)
  );
}

function inferSeriesColumnIndexes(result) {
  const columns = (result?.columns || []).map((column) => String(column || "").trim().toLowerCase());
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  if (!columns.length || !rows.length || columns.length < 2) {
    return null;
  }

  let metricIndex = columns.lastIndexOf("metric_value");
  if (metricIndex < 0 || !rows.every((row) => isNumericLike(row?.[metricIndex]))) {
    metricIndex = -1;
    for (let index = columns.length - 1; index >= 0; index -= 1) {
      if (rows.every((row) => isNumericLike(row?.[index]))) {
        metricIndex = index;
        break;
      }
    }
  }
  if (metricIndex < 0) {
    return null;
  }

  let labelIndex = columns.findIndex((column, index) => index !== metricIndex && ["period_value", "period_label", "stat_date", "group_value"].includes(column));
  if (labelIndex < 0) {
    labelIndex = columns.findIndex((_, index) => index !== metricIndex);
  }
  if (labelIndex < 0) {
    return null;
  }

  return { labelIndex, metricIndex };
}

// normalizeQueryResult выравнивает отображаемые или транспортные значения в интерфейсе.
function normalizeQueryResult(result) {
  const columns = Array.isArray(result?.columns) ? result.columns : [];
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const count = Number.isFinite(Number(result?.count)) ? Number(result.count) : rows.length;
  return {
    ...result,
    columns,
    rows,
    count,
  };
}

// looksLikeDateLabel выводит переиспользуемое состояние для рендера и действий.
function looksLikeDateLabel(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return false;
  }
  return /^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/.test(text);
}

// isTimeSeriesResult выводит переиспользуемое состояние для рендера и действий.
function isTimeSeriesResult(result, chart) {
  const firstColumn = String(result?.columns?.[0] ?? "").trim().toLowerCase();
  if (["area-line", "line"].includes(chart?.type)) {
    return true;
  }
  if (!result?.rows?.length || result.columns?.length < 2) {
    return false;
  }
  if (firstColumn === "period_value" || firstColumn === "stat_date") {
    return result.rows.every((row) => looksLikeDateLabel(row?.[0]));
  }
  if (firstColumn === "period_label") {
    return result.rows.every((row) => looksLikeDateLabel(row?.[0]));
  }
  return false;
}

// summarizeSeries выводит переиспользуемое состояние для рендера и действий.
function summarizeSeries(series) {
  if (!series.length) {
    return null;
  }
  const total = series.reduce((sum, item) => sum + item.value, 0);
  const top = series.reduce((best, current) => (current.value > best.value ? current : best), series[0]);
  const bottom = series.reduce((best, current) => (current.value < best.value ? current : best), series[0]);
  const average = total / series.length;
  return { total, top, bottom, average };
}
