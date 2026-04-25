const state = {
  semantic: null,
  glossaryKind: "all",
};

const elements = {
  glossarySearch: document.getElementById("glossary-search"),
  glossaryFilters: document.getElementById("glossary-filters"),
  glossaryList: document.getElementById("glossary-list"),
};

// api отправляет авторизованные запросы и нормализует ошибки ответа.
async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
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

// getGlossaryEntries выводит переиспользуемое состояние для рендера и действий.
function getGlossaryEntries() {
  if (!state.semantic) {
    return [];
  }

  const metricEntries = (state.semantic.metrics || []).map((metric) => ({
    term: metric.title,
    kind: "metric",
    canonical: metric.id,
    description: metric.description,
  }));
  const dimensionEntries = (state.semantic.dimensions || []).map((dimension) => ({
    term: dimension.title,
    kind: "dimension",
    canonical: dimension.id,
    description: dimension.description,
  }));

  return [...metricEntries, ...dimensionEntries, ...(state.semantic.terms || [])];
}

// kindLabel выравнивает отображаемые или транспортные значения в интерфейсе.
function kindLabel(kind) {
  switch (kind) {
    case "metric":
      return "Метрика";
    case "dimension":
      return "Измерение";
    case "filter":
      return "Фильтр";
    default:
      return kind;
  }
}

// renderGlossary обновляет связанную область страницы из текущего состояния.
function renderGlossary() {
  const search = elements.glossarySearch.value.trim().toLowerCase();
  const entries = getGlossaryEntries().filter((item) => {
    if (state.glossaryKind !== "all" && item.kind !== state.glossaryKind) {
      return false;
    }
    if (!search) {
      return true;
    }
    const haystack = [item.term, item.description, item.canonical, item.kind].join(" ").toLowerCase();
    return haystack.includes(search);
  });

  if (!entries.length) {
    elements.glossaryList.innerHTML = `<div class="empty-state">По вашему фильтру ничего не найдено.</div>`;
    return;
  }

  elements.glossaryList.innerHTML = entries
    .map(
      (item) => `
        <article class="glossary-card">
          <span class="glossary-kind">${escapeHtml(kindLabel(item.kind))}</span>
          <strong>${escapeHtml(item.term)}</strong>
          <p>${escapeHtml(item.description)}</p>
          <div class="glossary-meta">
            <span>${escapeHtml(item.canonical)}</span>
            <a class="mini-button" href="/">${escapeHtml("Перейти к запросам")}</a>
          </div>
        </article>
      `
    )
    .join("");
}

// loadGlossaryPage загружает данные сервера и обновляет видимое состояние.
async function loadGlossaryPage() {
  try {
    state.semantic = await api("/api/v1/meta/schema");
    renderGlossary();
  } catch (error) {
    elements.glossaryList.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

elements.glossarySearch.addEventListener("input", renderGlossary);
elements.glossaryFilters.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-kind]");
  if (!button) return;
  state.glossaryKind = button.dataset.kind;
  [...elements.glossaryFilters.querySelectorAll("button")].forEach((item) => item.classList.toggle("active", item === button));
  renderGlossary();
});

loadGlossaryPage();
