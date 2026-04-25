// renderParse обновляет связанную область страницы из текущего состояния.
function renderParse() {
  const parse = state.parse;
  if (!parse) {
    return;
  }

  state.provider = inferProvider(parse);
  if (elements.providerPill) {
    elements.providerPill.textContent = `LLM: ${formatProviderLabel(state.provider)} - ${Math.round((parse.intent.confidence || 0) * 100)}%`;
  }
  if (elements.sidebarProvider) {
    elements.sidebarProvider.textContent = `LLM: ${formatProviderLabel(state.provider)}`;
  }

  elements.intentSummary.textContent = parse.preview.summary || "Не удалось интерпретировать запрос.";
  elements.intentMetric.textContent = metricTitle();
  elements.intentGroup.textContent = groupTitle();
  elements.intentPeriod.textContent = periodTitle();
  elements.intentConfidence.textContent = formatConfidence(parse.intent);
  if (elements.intentProvider) {
    elements.intentProvider.textContent = formatProviderLabel(parse.provider);
  }

  const stripItems = [
    ["Метрика", metricTitle() || "Требует уточнения"],
    ["Разрез", groupTitle()],
    ["Период", periodTitle() || "—"],
  ];
  elements.intentHighlightStrip.innerHTML = stripItems
    .map(
      ([label, value]) => `
        <div class="mini-stat-card">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `
    )
    .join("");

  const filters = parse.preview.applied_filters?.length ? parse.preview.applied_filters : parse.intent.filters || [];
  elements.intentFilters.innerHTML = filters.length
    ? filters.map((item) => `<span>${escapeHtml(humanFilterLabel(item))}</span>`).join("")
    : `<span>Дополнительные фильтры не заданы</span>`;

  const messages = [];
  if (parse.preview.clarification) messages.push(parse.preview.clarification);
  (parse.preview.assumptions || []).forEach((item) => messages.push(item));
  elements.clarificationBox.textContent = messages.length
    ? messages.join(" ")
    : "Система готова выполнить только безопасный и разрешённый вариант запроса.";
}

// renderSummary обновляет связанную область страницы из текущего состояния.
function renderSummary(run, context) {
  elements.summaryStats.innerHTML = "";
  const result = normalizeQueryResult(run?.result);

  if (!result.rows.length) {
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>По запросу пока нет данных</strong>
        <p>Попробуйте изменить формулировку, период или уточнить метрику. Все guardrails и интерпретация при этом сохранятся.</p>
      </div>
    `;
    elements.storyGrid.innerHTML = "";
    return;
  }

  if (context.tabularOnly) {
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>Ответ выведен в табличном формате</strong>
        <p>Запрос обращается к исходным полям и возвращает ${escapeHtml(String(result.count))} строк. Для такого сценария основной акцент перенесён на таблицу ниже без искусственной инфографики.</p>
      </div>
    `;
    elements.storyGrid.innerHTML = `
      <div class="story-card highlight">
        <span>Источник</span>
        <strong>${escapeHtml(inferProvider(run))}</strong>
        <p>Сырые поля показаны напрямую из запроса без агрегирования.</p>
      </div>
      <div class="story-card">
        <span>Колонки</span>
        <strong>${escapeHtml(String(result.columns.length))}</strong>
        <p>${escapeHtml(result.columns.map(displayColumnName).slice(0, 3).join(", "))}${result.columns.length > 3 ? "…" : ""}</p>
      </div>
    `;
    return;
  }

  const series = context.fullSeries;
  const summary = summarizeSeries(series);
  const analytics = buildSeriesAnalytics(series);

  if (!series.length) {
    const value = result.rows[0]?.[0] ?? "—";
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>${escapeHtml(metricTitleWithUnit())}: ${escapeHtml(formatMetricValue(value))}</strong>
        <p>Итоговое значение рассчитано без дополнительной разбивки за период «${escapeHtml(periodTitle())}».</p>
      </div>
    `;
    elements.storyGrid.innerHTML = `
      <div class="story-card">
        <span>Главный KPI</span>
        <strong>${escapeHtml(formatMetricValue(value))}</strong>
        <p>Это итоговая цифра по запросу без сравнительного ряда.</p>
      </div>
    `;
    return;
  }

  if (context.timeSeries && analytics) {
    const latest = analytics.last;
    const first = analytics.first;
    const deltaValue = analytics.delta;
    const deltaText = `${deltaValue >= 0 ? "+" : "−"}${formatMetricValue(Math.abs(deltaValue))}`;
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>${escapeHtml(metricTitleWithUnit())}: ${escapeHtml(formatMetricValue(latest.value))}</strong>
        <p>Последняя точка ряда приходится на ${escapeHtml(displaySeriesLabel(latest.label))}. За период «${escapeHtml(periodTitle())}» показатель изменился на ${escapeHtml(deltaText)} относительно первой точки и достиг пика ${escapeHtml(formatMetricValue(analytics.top.value))}.</p>
      </div>
    `;

    elements.storyGrid.innerHTML = `
      <div class="story-card highlight">
        <span>Последняя точка</span>
        <strong>${escapeHtml(displaySeriesLabel(latest.label))}</strong>
        <p>${escapeHtml(formatMetricValue(latest.value))}</p>
      </div>
      <div class="story-card">
        <span>Пик</span>
        <strong>${escapeHtml(displaySeriesLabel(analytics.top.label))}</strong>
        <p>${escapeHtml(formatMetricValue(analytics.top.value))}</p>
      </div>
      <div class="story-card">
        <span>Изменение</span>
        <strong>${escapeHtml(deltaText)}</strong>
        <p>От ${escapeHtml(displaySeriesLabel(first.label))} до ${escapeHtml(displaySeriesLabel(latest.label))}</p>
      </div>
    `;
    return;
  }

  if (context.comparison && analytics) {
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>Лучший период: ${escapeHtml(displaySeriesLabel(analytics.top.label))}</strong>
        <p>Сравнение за период «${escapeHtml(periodTitle())}» показывает максимум у ${escapeHtml(displaySeriesLabel(analytics.top.label))} со значением ${escapeHtml(formatMetricValue(analytics.top.value))}. Минимум пришёлся на ${escapeHtml(displaySeriesLabel(analytics.bottom.label))}, а разница между ними составила ${escapeHtml(formatMetricValue(analytics.range))}.</p>
      </div>
    `;

    elements.storyGrid.innerHTML = `
      <div class="story-card highlight">
        <span>Лидер периода</span>
        <strong>${escapeHtml(displaySeriesLabel(analytics.top.label))}</strong>
        <p>${escapeHtml(formatMetricValue(analytics.top.value))}</p>
      </div>
      <div class="story-card">
        <span>Минимум</span>
        <strong>${escapeHtml(displaySeriesLabel(analytics.bottom.label))}</strong>
        <p>${escapeHtml(formatMetricValue(analytics.bottom.value))}</p>
      </div>
      <div class="story-card">
        <span>Разница</span>
        <strong>${escapeHtml(formatMetricValue(analytics.range))}</strong>
        <p>${escapeHtml(context.rowCount)} наблюдений в сравнении</p>
      </div>
    `;
    return;
  }

  elements.businessSummary.innerHTML = `
    <div class="summary-hero">
      <span class="summary-label">Главный вывод</span>
      <strong>Лидер: ${escapeHtml(displaySeriesLabel(summary.top.label))}</strong>
      <p>${escapeHtml(metricTitleWithUnit())} за период «${escapeHtml(periodTitle())}» сильнее всего выглядит у ${escapeHtml(displaySeriesLabel(summary.top.label))}. Значение лидера составляет ${escapeHtml(formatMetricValue(summary.top.value))}, а вклад топ-3 категорий достигает ${escapeHtml(formatPercent(analytics?.concentration || 0, 0))}.</p>
    </div>
  `;

  elements.storyGrid.innerHTML = `
    <div class="story-card highlight">
      <span>Лидер</span>
      <strong>${escapeHtml(displaySeriesLabel(summary.top.label))}</strong>
      <p>${escapeHtml(formatMetricValue(summary.top.value))}</p>
    </div>
    <div class="story-card">
      <span>Контраст</span>
      <strong>${escapeHtml(displaySeriesLabel(summary.bottom.label))}</strong>
      <p>${escapeHtml(formatMetricValue(summary.bottom.value))}</p>
    </div>
    <div class="story-card">
      <span>Разрез</span>
      <strong>${escapeHtml(groupTitle())}</strong>
      <p>${escapeHtml(periodTitle())}</p>
    </div>
  `;
}

// renderTable обновляет связанную область страницы из текущего состояния.
function renderTable(result) {
  const normalized = normalizeQueryResult(result);
  elements.resultTableHead.innerHTML = "";
  elements.resultTableBody.innerHTML = "";

  if (!normalized.columns.length) {
    elements.resultCount.textContent = "0 строк";
    elements.resultTableBody.innerHTML = `<tr><td class="empty-state">Нет данных для отображения.</td></tr>`;
    return;
  }

  elements.resultCount.textContent = `${normalized.count} строк`;
  const headerRow = document.createElement("tr");
  normalized.columns.forEach((column, index) => {
    const th = document.createElement("th");
    th.textContent = displayColumnName(column, normalized, index);
    headerRow.appendChild(th);
  });
  elements.resultTableHead.appendChild(headerRow);

  normalized.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((value, index) => {
      const td = document.createElement("td");
      td.textContent = formatColumnValue(value, normalized.columns[index], row.length, index, normalized);
      tr.appendChild(td);
    });
    elements.resultTableBody.appendChild(tr);
  });
}

// createInsightChip выполняет действие пользователя и синхронизирует результат с backend.
function createInsightChip(label, value) {
  return `<span class="insight-chip"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`;
}

// displaySeriesLabel выравнивает отображаемые или транспортные значения в интерфейсе.
function displaySeriesLabel(value) {
  return looksLikeDateLabel(value) ? formatDateOnly(value) : String(value ?? "");
}

// isComparisonPattern выводит переиспользуемое состояние для рендера и действий.
function isComparisonPattern(pattern) {
  const normalized = String(pattern || "").trim().toLowerCase();
  return normalized.includes("comparison");
}


