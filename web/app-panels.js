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

// renderGlossary обновляет связанную область страницы из текущего состояния.
function renderGlossary() {
  if (!elements.glossaryList || !elements.glossarySearch) {
    return;
  }
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
          <span class="glossary-kind">${escapeHtml(item.kind)}</span>
          <strong>${escapeHtml(item.term)}</strong>
          <p>${escapeHtml(item.description)}</p>
          <div class="glossary-meta">
            <span>${escapeHtml(item.canonical)}</span>
            <button class="mini-button" type="button" data-glossary-term="${escapeHtml(item.term)}">Вставить в запрос</button>
          </div>
        </article>
      `
    )
    .join("");
}

// renderReports обновляет связанную область страницы из текущего состояния.
function renderReports() {
  if (!elements.reportList) {
    return;
  }
  if (!state.reports.length) {
    elements.reportList.innerHTML = `
      <div class="empty-state">
        Пока нет сохранённых отчётов. После первого сохранения или регулярного запуска архив появится здесь.
      </div>
    `;
    return;
  }

  elements.reportList.innerHTML = state.reports
    .slice(0, 8)
    .map(
      (report) => `
        <article class="report-card">
          <div class="report-card-head">
            <div>
              <strong>${escapeHtml(report.name)}</strong>
              <p>${escapeHtml(report.query_text)}</p>
            </div>
            ${report.source === "scheduled" ? '<span class="report-badge scheduled">scheduled</span>' : ""}
          </div>
          <div class="report-card-meta">
            <span>Обновлён: ${escapeHtml(formatDate(report.updated_at))}</span>
            <span>Строк: ${escapeHtml(report.result?.count ?? 0)}</span>
            ${report.template_name ? `<span>Шаблон: ${escapeHtml(report.template_name)}</span>` : ""}
          </div>
          <div class="button-row compact">
            <button class="mini-button" type="button" data-report-open="${report.id}">Открыть</button>
            <button class="mini-button" type="button" data-report-export="pdf" data-report-id="${report.id}">PDF</button>
            <button class="mini-button" type="button" data-report-export="docx" data-report-id="${report.id}">DOCX</button>
          </div>
        </article>
      `
    )
    .join("");
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
  if (elements.templateOwnerName) {
    elements.templateOwnerName.value = data.owner_name || getViewerName();
  }
  if (elements.templateOwnerDepartment) {
    elements.templateOwnerDepartment.value = data.owner_department || getViewerDepartment();
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
  saveTemplateProfile();
  document.getElementById("template-save-button").textContent = data.id ? "Обновить шаблон" : "Сохранить шаблон";
}

// renderTemplates обновляет связанную область страницы из текущего состояния.
function renderTemplates() {
  if (!elements.templateList) {
    return;
  }

  const myTemplates = getMyTemplates();
  const sharedTemplates = getSharedTemplates();

  if (!myTemplates.length) {
    elements.templateList.innerHTML = `
      <div class="empty-state">
        ${getViewerName() ? "У вас пока нет личных шаблонов. Сохраните первый сценарий через форму выше." : "Чтобы видеть свои шаблоны, укажите имя пользователя в форме выше."}
      </div>
    `;
  } else {
    elements.templateList.innerHTML = myTemplates
      .map(
        (template) => `
          <article class="template-card">
            <div class="template-card-head">
              <div>
                <strong>${escapeHtml(template.name)}</strong>
                <p>${escapeHtml(template.description || template.query_text)}</p>
              </div>
              <div class="template-badges">
                <span class="template-status ${template.schedule?.enabled ? "live" : "draft"}">${escapeHtml(template.schedule?.enabled ? "регулярный" : "ручной")}</span>
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
              <button class="mini-button" type="button" data-template-run="${template.id}">Запустить сейчас</button>
              <button class="mini-button" type="button" data-template-edit="${template.id}">Редактировать</button>
              <button class="mini-button danger" type="button" data-template-delete="${template.id}">Удалить</button>
            </div>
          </article>
        `
      )
      .join("");
  }

  if (!elements.sharedTemplateList) {
    return;
  }

  if (!sharedTemplates.length) {
    elements.sharedTemplateList.innerHTML = `
      <div class="empty-state">
        ${state.templateDepartmentFilter ? "По выбранному отделу открытых шаблонов пока нет." : "Открытых шаблонов пока нет. Пользователи смогут публиковать их для коллег."}
      </div>
    `;
    return;
  }

  elements.sharedTemplateList.innerHTML = sharedTemplates
    .map(
      (template) => `
        <article class="template-card shared">
          <div class="template-card-head">
            <div>
              <strong>${escapeHtml(template.name)}</strong>
              <p>${escapeHtml(template.description || template.query_text)}</p>
            </div>
            <div class="template-badges">
              <span class="template-visibility public">Открытый</span>
              <span class="template-status ${template.schedule?.enabled ? "live" : "draft"}">${escapeHtml(template.schedule?.enabled ? "с расписанием" : "ручной")}</span>
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
            <button class="mini-button" type="button" data-template-run="${template.id}">Запустить сейчас</button>
          </div>
        </article>
      `
    )
    .join("");
}


