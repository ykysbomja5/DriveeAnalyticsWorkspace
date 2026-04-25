// aggregateSeriesByLabel собирает пригодный для визуализации ряд из сырых строк результата.
function aggregateSeriesByLabel(result, labelIndex, valueIndex, formatter = (value) => String(value ?? "").trim()) {
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const bucket = new Map();
  rows.forEach((row) => {
    const label = formatter(row?.[labelIndex]);
    const value = parseNumeric(row?.[valueIndex]);
    if (!label || !Number.isFinite(value)) {
      return;
    }
    bucket.set(label, (bucket.get(label) || 0) + value);
  });
  return Array.from(bucket.entries()).map(([label, value]) => ({ label, value }));
}

// sortSeriesChronologically упорядочивает временной ряд по возрастанию даты.
function sortSeriesChronologically(series) {
  return [...series].sort((left, right) => {
    const leftDate = Date.parse(left.label);
    const rightDate = Date.parse(right.label);
    if (Number.isFinite(leftDate) && Number.isFinite(rightDate)) {
      return leftDate - rightDate;
    }
    return left.label.localeCompare(right.label, 'ru');
  });
}

function isPeriodColumn(column) {
  return ['period_value', 'period_label', 'stat_date'].includes(String(column || '').trim().toLowerCase());
}

function multiMetricColumnIndexes(result) {
  const columns = (result?.columns || []).map((column) => String(column || '').trim().toLowerCase());
  const labelIndex = columns.findIndex((column) => ['period_value', 'period_label', 'stat_date', 'group_value'].includes(column));
  const metricIndexes = columns
    .map((column, index) => ({ column, index }))
    .filter((item) => item.column.endsWith('_value') && item.column !== 'metric_value')
    .map((item) => item.index);
  return {
    labelIndex: labelIndex >= 0 ? labelIndex : 0,
    metricIndexes,
  };
}

function isMultiMetricResult(result) {
  return multiMetricColumnIndexes(result).metricIndexes.length >= 2;
}

// buildVisualizationProjection нормализует разные формы SQL-ответа под единый UI.
function buildVisualizationProjection(result) {
  const columns = (result?.columns || []).map((column) => String(column || '').trim().toLowerCase());
  const metricIndex = columns.lastIndexOf('metric_value');
  const periodIndex = columns.findIndex((column) => ['period_value', 'period_label', 'stat_date'].includes(column));
  const groupIndex = columns.findIndex((column, index) => column === 'group_value' && index !== metricIndex);

  if (result.columns.length === 1 && result.rows.length === 1 && isNumericLike(result.rows[0]?.[0])) {
    return {
      tabularOnly: false,
      fullSeries: [],
      chartSeries: [],
      rankingSeries: [],
      breakdownSeries: [],
      groupLabel: groupTitle(),
      timeSeries: false,
      structure: false,
      groupedResult: false,
    };
  }

  if (isMultiMetricResult(result)) {
    return {
      tabularOnly: true,
      multiMetric: true,
      fullSeries: [],
      chartSeries: [],
      rankingSeries: [],
      breakdownSeries: [],
      groupLabel: groupTitle(),
      timeSeries: false,
      structure: false,
      groupedResult: false,
    };
  }

  if (result.columns.length === 3 && metricIndex >= 0 && periodIndex >= 0 && groupIndex >= 0) {
    const timeSeries = sortSeriesChronologically(aggregateSeriesByLabel(result, periodIndex, metricIndex, (value) => String(value ?? '').trim()));
    const breakdownSeries = aggregateSeriesByLabel(result, groupIndex, metricIndex, (value) => humanizeGroupValue(value, result, groupIndex))
      .sort((left, right) => right.value - left.value || left.label.localeCompare(right.label, 'ru'));
    return {
      tabularOnly: false,
      fullSeries: timeSeries,
      chartSeries: timeSeries,
      rankingSeries: breakdownSeries,
      breakdownSeries,
      groupLabel: inferGroupColumnLabel(result, groupIndex),
      timeSeries: true,
      structure: breakdownSeries.length > 0,
      groupedResult: true,
    };
  }

  if (result.columns.length >= 2 && canRenderSeries(result)) {
    const indexes = inferSeriesColumnIndexes(result);
    const series = getSeries(result);
    const labelColumn = columns[indexes?.labelIndex ?? 0];
    return {
      tabularOnly: false,
      fullSeries: series,
      chartSeries: series,
      rankingSeries: series,
      breakdownSeries: [],
      groupLabel: inferGroupColumnLabel(result, indexes?.labelIndex ?? 0),
      timeSeries: series.length > 0 && (isPeriodColumn(labelColumn) || (labelColumn !== 'group_value' && series.every((item) => looksLikeDateLabel(item.label)))),
      structure: false,
      groupedResult: false,
    };
  }

  return {
    tabularOnly: true,
    fullSeries: [],
    chartSeries: [],
    rankingSeries: [],
    breakdownSeries: [],
    groupLabel: groupTitle(),
    timeSeries: false,
    structure: false,
    groupedResult: false,
  };
}

// detectVisualizationContext выводит переиспользуемое состояние для рендера и действий.
function detectVisualizationContext(run) {
  const result = normalizeQueryResult(run?.result);
  const projection = buildVisualizationProjection(result);
  const fullSeries = projection.fullSeries;
  const timeSeries = projection.timeSeries;
  const comparison = isComparisonPattern(run?.intent?.pattern);
  const structure = projection.structure || (
    fullSeries.length > 0 && (
      run?.intent?.group_by === 'status' ||
      String(result.columns?.[0] || '').toLowerCase().includes('status')
    )
  );
  const rankedSeries = timeSeries
    ? fullSeries
    : [...fullSeries].sort((left, right) => right.value - left.value || left.label.localeCompare(right.label, 'ru'));

  let chartType = 'metric';
  let mode = 'metric';
  if (run?.chart?.type === 'table' || projection.tabularOnly) {
    chartType = 'table';
    mode = 'table';
  } else if (run?.chart?.type === 'histogram') {
    chartType = 'histogram';
    mode = 'distribution';
  } else if (timeSeries) {
    chartType = 'area-line';
    mode = comparison ? 'time-comparison' : 'time-series';
  } else if (fullSeries.length > 0) {
    chartType = fullSeries.length > 8 ? 'lollipop' : 'bar';
    if (comparison) {
      mode = 'comparison';
    } else if (structure) {
      mode = 'structure';
    } else {
      mode = 'category';
    }
  }

  const chartSeries = timeSeries ? rankedSeries : rankedSeries.slice(0, 8);
  const hiddenCount = Math.max(rankedSeries.length - chartSeries.length, 0);
  const labels = {
    metric: 'KPI-карточка',
    table: 'Табличный режим',
    'time-series': projection.groupedResult ? 'Линейный график + структура' : 'Линейный график',
    'time-comparison': 'Линейное сравнение',
    comparison: 'Сравнительный бар-чарт',
    distribution: 'Гистограмма',
    structure: 'Категориальный график',
    category: chartType === 'lollipop' ? 'Рейтинг категорий' : 'Столбчатый график',
  };

  return {
    mode,
    chartType,
    tabularOnly: run?.chart?.type === 'table' || projection.tabularOnly,
    multiMetric: projection.multiMetric,
    timeSeries,
    comparison,
    structure,
    fullSeries,
    chartSeries,
    rankingSeries: projection.rankingSeries,
    breakdownSeries: projection.breakdownSeries,
    groupedResult: projection.groupedResult,
    groupLabel: projection.groupLabel,
    hiddenCount,
    rowCount: Number(result.count ?? result.rows?.length ?? 0),
    displayLabel: labels[mode] || 'Визуализация',
  };
}

// renderChartInsights обновляет связанную область страницы из текущего состояния.
function renderChartInsights(run, context, analytics, breakdownAnalytics = null) {
  const chips = [
    createInsightChip("Визуализация", context.displayLabel),
    createInsightChip("Получено", `${context.rowCount} строк${context.rowCount === 1 ? "а" : context.rowCount >= 2 && context.rowCount <= 4 ? "и" : ""}`),
    createInsightChip("Период", periodTitle()),
  ];

  const effectiveGroupLabel = context.groupLabel || groupTitle();
  if (!context.tabularOnly && (!context.timeSeries || context.groupedResult) && effectiveGroupLabel !== "Без разбивки") {
    chips.push(createInsightChip("Разрез", effectiveGroupLabel));
  }
  if (context.hiddenCount > 0) {
    chips.push(createInsightChip("На графике", `${context.chartSeries.length} из ${context.fullSeries.length} категорий`));
  }
  if (context.comparison) {
    chips.push(createInsightChip("Режим", "Сравнение периодов"));
  }
  if (analytics?.top && !context.timeSeries) {
    chips.push(createInsightChip("Лидер", displaySeriesLabel(analytics.top.label)));
  }
  if (analytics && context.timeSeries) {
    const deltaValue = analytics.delta;
    const deltaText = `${deltaValue >= 0 ? "+" : "−"}${formatMetricValue(Math.abs(deltaValue))}`;
    chips.push(createInsightChip("Изменение", deltaText));
  }
  if (breakdownAnalytics?.top) {
    chips.push(createInsightChip("Лидер структуры", displaySeriesLabel(breakdownAnalytics.top.label)));
  }
  (run?.chart?.highlights || []).slice(0, 2).forEach((item) => {
    chips.push(`<span class="insight-chip muted">${escapeHtml(item)}</span>`);
  });

  elements.chartInsights.innerHTML = chips.join("");
}

// trimLabel выравнивает отображаемые или транспортные значения в интерфейсе.
function trimLabel(value, maxLength = 14) {
  const text = String(value ?? "");
  return text.length > maxLength ? `${text.slice(0, Math.max(1, maxLength - 1))}…` : text;
}

// clamp выравнивает отображаемые или транспортные значения в интерфейсе.
function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

// formatPercent выравнивает отображаемые или транспортные значения в интерфейсе.
function formatPercent(value, digits = 1) {
  if (!Number.isFinite(value)) {
    return "0%";
  }
  return `${new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value * 100)}%`;
}

// formatSignedPercent выравнивает отображаемые или транспортные значения в интерфейсе.
function formatSignedPercent(value, digits = 1) {
  if (!Number.isFinite(value)) {
    return "0%";
  }
  const formatted = new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(Math.abs(value) * 100);
  return `${value >= 0 ? "+" : "-"}${formatted}%`;
}

// buildSeriesAnalytics выводит переиспользуемое состояние для рендера и действий.
function buildSeriesAnalytics(series) {
  if (!series.length) {
    return null;
  }

  const total = series.reduce((sum, item) => sum + item.value, 0);
  const average = total / series.length;
  const ordered = [...series].sort((a, b) => b.value - a.value);
  const top = ordered[0];
  const bottom = ordered.at(-1);
  const first = series[0];
  const last = series.at(-1);
  const range = top.value - bottom.value;
  const variance = series.reduce((sum, item) => sum + (item.value - average) ** 2, 0) / series.length;
  const stdDev = Math.sqrt(variance);
  const topShare = total > 0 ? top.value / total : 0;
  const topThreeValue = ordered.slice(0, 3).reduce((sum, item) => sum + item.value, 0);
  const concentration = total > 0 ? topThreeValue / total : 0;
  const delta = last.value - first.value;
  const deltaRatio = first.value ? delta / first.value : 0;
  const averageGap = average ? (top.value - average) / average : 0;

  return {
    total,
    average,
    ordered: ordered.map((item) => ({ ...item, share: total > 0 ? item.value / total : 0 })),
    top,
    bottom,
    first,
    last,
    range,
    stdDev,
    topShare,
    concentration,
    delta,
    deltaRatio,
    averageGap,
  };
}

// renderLineChart обновляет связанную область страницы из текущего состояния.
function renderLineChart(series, width, height, padding, analytics) {
  const max = Math.max(...series.map((item) => item.value), 1);
  const min = Math.min(...series.map((item) => item.value), 0);
  const span = Math.max(max - min, 1);
  const step = series.length > 1 ? (width - padding * 2) / (series.length - 1) : 0;
  const averageY = height - padding - ((analytics.average - min) / span) * (height - padding * 2);

  const points = series.map((item, index) => {
    const x = padding + index * step;
    const y = height - padding - ((item.value - min) / span) * (height - padding * 2);
    return { ...item, x, y };
  });

  const line = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const area = `${line} L ${points.at(-1).x} ${height - padding} L ${points[0].x} ${height - padding} Z`;
  const spotlightValues = new Set([analytics.top.label, analytics.bottom.label, analytics.last.label]);
  
  // Умно прореживаем подписи: показываем до 10, чтобы избежать наложений.
  const maxLabels = 10;
  const labelStep = series.length > maxLabels ? Math.ceil(series.length / maxLabels) : 1;
  
  const dots = points
    .map(
      (point, index) => {
        const showLabel = index % labelStep === 0 || spotlightValues.has(point.label);
        return `
          <circle class="line-dot ${spotlightValues.has(point.label) ? "emphasis" : ""}" cx="${point.x}" cy="${point.y}" r="${spotlightValues.has(point.label) ? 6 : 4.5}"></circle>
          ${showLabel ? `<text class="axis-label" x="${point.x}" y="${height - 10}" text-anchor="middle">${escapeHtml(trimLabel(point.label))}</text>` : ''}
        `;
      }
    )
    .join("");
    
  const annotations = points
    .filter((point) => spotlightValues.has(point.label))
    .map(
      (point, index) => `
        <line class="annotation-line" x1="${point.x}" x2="${point.x}" y1="${point.y - 8}" y2="${Math.max(28, point.y - 44 - index * 10)}"></line>
        <text class="annotation-text" x="${point.x}" y="${Math.max(20, point.y - 50 - index * 10)}" text-anchor="middle">${escapeHtml(trimLabel(`${point.label}: ${formatMetricValue(point.value)}`, 24))}</text>
      `
    )
    .join("");

  return `
    <line class="average-line" x1="${padding}" x2="${width - padding}" y1="${averageY}" y2="${averageY}"></line>
    <text class="average-label" x="${width - padding}" y="${Math.max(18, averageY - 8)}" text-anchor="end">Среднее ${escapeHtml(formatMetricValue(analytics.average))}</text>
    <path class="area-path" d="${area}"></path>
    <path class="line-path" d="${line}"></path>
    ${dots}
    ${annotations}
  `;
}

// renderBarChart обновляет связанную область страницы из текущего состояния.
function renderBarChart(series, width, height, padding, analytics) {
  const max = Math.max(...series.map((item) => item.value), 1);
  const slotWidth = (width - padding * 2) / series.length;
  const barWidth = Math.max(slotWidth - 18, 18);

  return series
    .map((item, index) => {
      const barHeight = (item.value / max) * (height - padding * 2);
      const x = padding + index * slotWidth + (slotWidth - barWidth) / 2;
      const y = height - padding - barHeight;
      const barClass = item.label === analytics.top.label ? "bar top" : "bar";
      return `
        <rect class="bar-shadow" x="${x}" y="${y + 6}" width="${barWidth}" height="${Math.max(barHeight - 6, 0)}" rx="18"></rect>
        <rect class="${barClass}" x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="18"></rect>
        <text class="value-label" x="${x + barWidth / 2}" y="${Math.max(y - 8, 16)}" text-anchor="middle">${escapeHtml(formatMetricValue(item.value))}</text>
        <text class="axis-label" x="${x + barWidth / 2}" y="${height - 10}" text-anchor="middle">${escapeHtml(trimLabel(item.label))}</text>
      `;
    })
    .join("");
}

// renderLollipopChart обновляет связанную область страницы из текущего состояния.
function renderLollipopChart(series, width, height, padding, analytics) {
  const ordered = [...series].sort((a, b) => b.value - a.value).slice(0, 8);
  const usableHeight = height - padding * 2;
  const step = usableHeight / Math.max(ordered.length, 1);
  const max = Math.max(...ordered.map((item) => item.value), 1);

  return ordered
    .map((item, index) => {
      const y = padding + step * index + step / 2;
      const dotX = padding + (item.value / max) * (width - padding * 2);
      const isTop = item.label === analytics.top.label;
      return `
        <text class="axis-label axis-label-left" x="${padding}" y="${y - 10}">${escapeHtml(trimLabel(item.label, 28))}</text>
        <line class="lollipop-line" x1="${padding}" x2="${dotX}" y1="${y}" y2="${y}"></line>
        <circle class="lollipop-dot ${isTop ? "top" : ""}" cx="${dotX}" cy="${y}" r="${isTop ? 10 : 8}"></circle>
        <text class="value-label value-label-end" x="${Math.min(width - padding, dotX + 16)}" y="${y + 4}">${escapeHtml(formatMetricValue(item.value))}</text>
      `;
    })
    .join("");
}

function renderHistogramChart(series, width, height, padding) {
  const max = Math.max(...series.map((item) => item.value), 1);
  const slotWidth = (width - padding * 2) / Math.max(series.length, 1);
  const barWidth = Math.max(slotWidth - 6, 16);

  return series
    .map((item, index) => {
      const barHeight = (item.value / max) * (height - padding * 2);
      const x = padding + index * slotWidth + (slotWidth - barWidth) / 2;
      const y = height - padding - barHeight;
      return `
        <rect class="bar-shadow" x="${x}" y="${y + 6}" width="${barWidth}" height="${Math.max(barHeight - 6, 0)}" rx="4"></rect>
        <rect class="bar" x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="4"></rect>
        <text class="value-label" x="${x + barWidth / 2}" y="${Math.max(y - 8, 16)}" text-anchor="middle">${escapeHtml(formatMetricValue(item.value, "total_orders"))}</text>
        <text class="axis-label" x="${x + barWidth / 2}" y="${height - 10}" text-anchor="middle">${escapeHtml(trimLabel(item.label, 16))}</text>
      `;
    })
    .join("");
}

// renderShareDonut обновляет связанную область страницы из текущего состояния.
function renderShareDonut(analytics) {
  const radius = 54;
  const circumference = 2 * Math.PI * radius;
  const dashOffset = circumference * (1 - clamp(analytics.topShare, 0, 1));

  return `
    <section class="viz-card donut-card">
      <div class="viz-card-head">
        <span>Доля лидера</span>
        <strong>${escapeHtml(analytics.top.label)}</strong>
      </div>
      <div class="donut-shell">
        <svg viewBox="0 0 160 160" class="donut-svg" aria-hidden="true">
          <circle class="share-arc-bg" cx="80" cy="80" r="${radius}"></circle>
          <circle class="share-arc-fg" cx="80" cy="80" r="${radius}" stroke-dasharray="${circumference}" stroke-dashoffset="${dashOffset}"></circle>
        </svg>
        <div class="donut-center">
          <strong>${escapeHtml(formatPercent(analytics.topShare, 0))}</strong>
          <span>от суммы</span>
        </div>
      </div>
      <p>${escapeHtml(analytics.top.label)} формирует самый заметный вклад в результирующую выборку.</p>
    </section>
  `;
}

// buildCancellationMixSnapshot выводит переиспользуемое состояние для рендера и действий.
function buildCancellationMixSnapshot(series, analytics) {
  const ratio = Number(analytics?.average ?? series.at(-1)?.value ?? 0);
  const safeRatio = Number.isFinite(ratio) && ratio >= 0 ? ratio : 0;
  const total = 1 + safeRatio;
  return {
    cancellationShare: total > 0 ? safeRatio / total : 0,
    completedShare: total > 0 ? 1 / total : 0,
  };
}

// renderCancellationMixDonut обновляет связанную область страницы из текущего состояния.
function renderCancellationMixDonut(series, analytics) {
  const snapshot = buildCancellationMixSnapshot(series, analytics);
  const radius = 54;
  const circumference = 2 * Math.PI * radius;
  const cancellationArc = circumference * clamp(snapshot.cancellationShare, 0, 1);

  return `
    <section class="viz-card donut-card split-donut-card">
      <div class="viz-card-head">
        <span>Структура показателя</span>
        <strong>Отмены vs завершённые</strong>
      </div>
      <div class="donut-shell">
        <svg viewBox="0 0 160 160" class="donut-svg" aria-hidden="true">
          <circle class="ratio-arc-bg" cx="80" cy="80" r="${radius}"></circle>
          <circle class="ratio-arc-cancel" cx="80" cy="80" r="${radius}" stroke-dasharray="${cancellationArc} ${Math.max(circumference - cancellationArc, 0)}"></circle>
        </svg>
        <div class="donut-center">
          <strong>${escapeHtml(formatPercent(snapshot.cancellationShare, 0))}</strong>
          <span>средняя доля отмен</span>
        </div>
      </div>
      <div class="ratio-legend">
        <div class="ratio-legend-row">
          <span class="ratio-swatch cancel"></span>
          <span>Отмены</span>
          <strong>${escapeHtml(formatPercent(snapshot.cancellationShare, 1))}</strong>
        </div>
        <div class="ratio-legend-row">
          <span class="ratio-swatch complete"></span>
          <span>Завершённые</span>
          <strong>${escapeHtml(formatPercent(snapshot.completedShare, 1))}</strong>
        </div>
      </div>
      <p>Карточка показывает среднюю дневную структуру за период «${escapeHtml(periodTitle())}». Основной график выше сохраняет динамику по дням.</p>
    </section>
  `;
}

// renderPulseCard обновляет связанную область страницы из текущего состояния.
function renderPulseCard(analytics) {
  const topValue = analytics.top.value || 1;
  return `
    <section class="viz-card">
      <div class="viz-card-head">
        <span>Сигнал выборки</span>
        <strong>${escapeHtml(formatSignedPercent(analytics.averageGap, 0))}</strong>
      </div>
      <div class="pulse-stack">
        <div class="pulse-row">
          <span>Среднее</span>
          <div class="scale-track"><span class="scale-fill" style="width:${clamp((analytics.average / topValue) * 100, 4, 100)}%"></span></div>
          <strong>${escapeHtml(formatMetricValue(analytics.average))}</strong>
        </div>
        <div class="pulse-row">
          <span>Лидер</span>
          <div class="scale-track"><span class="scale-fill accent" style="width:100%"></span></div>
          <strong>${escapeHtml(formatMetricValue(analytics.top.value))}</strong>
        </div>
        <div class="pulse-row">
          <span>Разброс</span>
          <div class="scale-track"><span class="scale-fill muted" style="width:${clamp((analytics.range / topValue) * 100, 4, 100)}%"></span></div>
          <strong>${escapeHtml(formatMetricValue(analytics.range))}</strong>
        </div>
      </div>
    </section>
  `;
}

// renderDeltaBars обновляет связанную область страницы из текущего состояния.
function renderDeltaBars(series) {
  if (series.length < 2) {
    return `
      <section class="viz-card">
        <div class="viz-card-head">
          <span>Импульс</span>
          <strong>Недостаточно точек</strong>
        </div>
        <p>Для анализа ускорения нужны минимум две точки во временном ряду.</p>
      </section>
    `;
  }

  const deltas = series.slice(1).map((item, index) => ({
    label: `${trimLabel(series[index].label, 10)} → ${trimLabel(item.label, 10)}`,
    value: item.value - series[index].value,
  }));
  const maxAbs = Math.max(...deltas.map((item) => Math.abs(item.value)), 1);

  return `
    <section class="viz-card">
      <div class="viz-card-head">
        <span>Импульс</span>
        <strong>${escapeHtml(formatMetricValue(deltas.at(-1).value))}</strong>
      </div>
      <div class="delta-list">
        ${deltas
          .slice(-5)
          .map(
            (item) => `
              <div class="delta-item">
                <span>${escapeHtml(item.label)}</span>
                <div class="delta-bar"><span class="${item.value >= 0 ? "delta-fill positive" : "delta-fill negative"}" style="width:${clamp((Math.abs(item.value) / maxAbs) * 100, 6, 100)}%"></span></div>
                <strong>${escapeHtml(formatMetricValue(item.value))}</strong>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

// renderCategoryBreakdown обновляет связанную область страницы из текущего состояния.
function renderCategoryBreakdown(analytics) {
  return `
    <section class="viz-card viz-card-wide">
      <div class="viz-card-head">
        <span>Структура</span>
        <strong>${escapeHtml(formatPercent(analytics.concentration, 0))} в топ-3</strong>
      </div>
      <div class="composition-list">
        ${analytics.ordered
          .slice(0, 5)
          .map(
            (item) => `
              <div class="composition-row">
                <span>${escapeHtml(trimLabel(item.label, 24))}</span>
                <div class="composition-track"><span style="width:${clamp(item.share * 100, 4, 100)}%"></span></div>
                <strong>${escapeHtml(formatPercent(item.share, 0))}</strong>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

// renderCategoryDashboard обновляет связанную область страницы из текущего состояния.
function renderCategoryDashboard(run, series, analytics) {
  const width = 820;
  const height = run.chart?.type === "lollipop" ? 360 : 400;
  const padding = run.chart?.type === "lollipop" ? 88 : 54;
  const chartBody = run.chart?.type === "histogram"
    ? renderHistogramChart(series, width, height, padding)
    : run.chart?.type === "lollipop"
      ? renderLollipopChart(series, width, height, padding, analytics)
      : renderBarChart(series, width, height, padding, analytics);

  return `
    <div class="visual-dashboard">
      <section class="viz-card viz-card-main">
        <div class="chart-caption">
          <strong>${escapeHtml(run.chart?.headline || metricTitleWithUnit())}</strong>
          <p>${escapeHtml(run.chart?.subtitle || `${groupTitle()} - ${periodTitle()}`)}</p>
        </div>
        <svg class="chart-svg tall" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
          <defs>
            <linearGradient id="drivee-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#8de80f"></stop>
              <stop offset="100%" stop-color="#2f7f1d"></stop>
            </linearGradient>
          </defs>
          ${chartBody}
        </svg>
      </section>
      <div class="viz-grid">
        ${renderShareDonut(analytics)}
        ${renderPulseCard(analytics)}
        ${renderCategoryBreakdown(analytics)}
      </div>
    </div>
  `;
}

// renderTimeSeriesDashboard обновляет связанную область страницы из текущего состояния.
function renderTimeSeriesDashboard(run, series, analytics, breakdownAnalytics = null) {
  // Рассчитываем ширину по количеству точек данных.
  // Берём 50px на точку для читаемости и минимум 820px до 16 точек.
  // Включаем горизонтальный скролл, если точек больше 16.
  const minWidth = 820;
  const pointsThreshold = 16;
  const pointWidth = series.length > pointsThreshold ? series.length * 50 : minWidth;
  const width = Math.max(minWidth, pointWidth);
  const height = 420;
  const padding = 60;
  
  const max = Math.max(...series.map((item) => item.value), 1);
  const min = Math.min(...series.map((item) => item.value), 0);
  const span = Math.max(max - min, 1);
  
  // Создаём линии сетки оси Y с подписями.
  const gridLines = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const value = min + ratio * span;
      const y = height - padding - ratio * (height - padding * 2);
      return `
        <line class="chart-grid-line ${ratio === 0.5 ? "strong" : ""}" x1="${padding}" x2="${width - padding}" y1="${y}" y2="${y}"></line>
        <text class="y-axis-label" x="${padding - 8}" y="${y + 4}" text-anchor="end">${formatMetricValue(value)}</text>
      `;
    })
    .join("");

  return `
    <div class="visual-dashboard">
      <section class="viz-card viz-card-main">
        <div class="chart-caption">
          <strong>${escapeHtml(run.chart?.headline || metricTitleWithUnit())}</strong>
          <p>${escapeHtml(run.chart?.subtitle || `${groupTitle()} - ${periodTitle()}`)}</p>
        </div>
        <div class="chart-scroll-container">
          <svg class="chart-svg tall" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
            <defs>
              <linearGradient id="drivee-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stop-color="#8de80f"></stop>
                <stop offset="100%" stop-color="#2f7f1d"></stop>
              </linearGradient>
            </defs>
            ${gridLines}
            ${renderLineChart(series, width, height, padding, analytics)}
          </svg>
        </div>
      </section>
      <div class="viz-grid">
        ${renderDeltaBars(series)}
        ${renderPulseCard(analytics)}
        ${currentMetricId() === "cancellation_rate" ? renderCancellationMixDonut(series, analytics) : renderShareDonut(breakdownAnalytics || analytics)}
      </div>
    </div>
  `;
}

// renderRankingPanel обновляет связанную область страницы из текущего состояния.
function renderRankingPanel(series, options = {}) {
  if (!series.length) {
    elements.rankingPanel.innerHTML = `<div class="ranking-empty">Рейтинг появится после запуска запроса.</div>`;
    return;
  }

  const analytics = buildSeriesAnalytics(series);
  // Показываем все элементы рейтинга; переполнение возьмёт на себя скролл.
  const ranking = analytics.ordered;
  const maxValue = Math.max(...ranking.map((item) => item.value), 1);
  elements.rankingPanel.innerHTML = `
    <div class="ranking-head">
      <span>Рейтинг и вклад</span>
      <strong>${escapeHtml(metricTitleWithUnit())}</strong>
      <p>${escapeHtml(options.label || groupTitle())} · ${escapeHtml(periodTitle())}</p>
    </div>
    <div class="ranking-list">
      ${ranking
        .map(
          (item, index) => `
            <div class="ranking-item">
              <span class="ranking-index">#${index + 1}</span>
              <div class="ranking-copy">
                <strong>${escapeHtml(item.label)}</strong>
                <small>${escapeHtml(formatMetricValue(item.value))} · ${escapeHtml(formatPercent(item.share || 0, 0))} от суммы</small>
                <div class="ranking-meter"><span style="width:${clamp((item.value / maxValue) * 100, 4, 100)}%"></span></div>
              </div>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

// renderChart обновляет связанную область страницы из текущего состояния.
function renderMultiMetricDashboard(result) {
  const normalized = normalizeQueryResult(result);
  const { labelIndex, metricIndexes } = multiMetricColumnIndexes(normalized);
  const rows = normalized.rows || [];
  const firstRow = rows[0] || [];

  const headline = metricIndexes.slice(0, 5).map((columnIndex) => `
    <div class="multi-metric-card">
      <span>${escapeHtml(displayColumnName(normalized.columns[columnIndex], normalized, columnIndex))}</span>
      <strong>${escapeHtml(formatColumnValue(firstRow[columnIndex], normalized.columns[columnIndex], firstRow.length, columnIndex, normalized))}</strong>
      <small>${escapeHtml(formatColumnValue(firstRow[labelIndex], normalized.columns[labelIndex], firstRow.length, labelIndex, normalized))}</small>
    </div>
  `).join("");

  const headerCells = [
    `<th>${escapeHtml(displayColumnName(normalized.columns[labelIndex], normalized, labelIndex))}</th>`,
    ...metricIndexes.map((columnIndex) => `<th>${escapeHtml(displayColumnName(normalized.columns[columnIndex], normalized, columnIndex))}</th>`),
  ].join("");

  const bodyRows = rows.map((row) => `
    <tr>
      <th>${escapeHtml(formatColumnValue(row[labelIndex], normalized.columns[labelIndex], row.length, labelIndex, normalized))}</th>
      ${metricIndexes.map((columnIndex) => `
        <td>${escapeHtml(formatColumnValue(row[columnIndex], normalized.columns[columnIndex], row.length, columnIndex, normalized))}</td>
      `).join("")}
    </tr>
  `).join("");

  return `
    <div class="multi-metric-dashboard">
      <div class="multi-metric-head">
        <span>${escapeHtml(String(rows.length))} строк</span>
        <strong>Все запрошенные метрики</strong>
      </div>
      <div class="multi-metric-cards">
        ${headline}
      </div>
      <div class="multi-metric-table-wrap">
        <table class="multi-metric-table">
          <thead><tr>${headerCells}</tr></thead>
          <tbody>${bodyRows}</tbody>
        </table>
      </div>
    </div>
  `;
}

function renderChart(run, context) {
  const result = normalizeQueryResult(run?.result);
  elements.chartInsights.innerHTML = "";

  if (!result.rows.length) {
    elements.chartSurface.innerHTML = `<div class="chart-placeholder">Результат визуализации появится после выполнения запроса. Здесь будет основной график, структурная разбивка и рейтинг.</div>`;
    renderRankingPanel([]);
    return;
  }

  if (context.multiMetric) {
    renderChartInsights(run, context, null);
    elements.chartSurface.innerHTML = renderMultiMetricDashboard(result);
    renderRankingPanel([]);
    return;
  }

  if (context.tabularOnly) {
    renderChartInsights(run, context, null);
    elements.chartSurface.innerHTML = `
      <div class="chart-placeholder">
        Запрос вернул многоколоночный табличный ответ. Здесь сохранён табличный режим без упрощения структуры, а основной акцент перенесён на таблицу и SQL ниже.
      </div>
    `;
    renderRankingPanel([]);
    return;
  }

  const series = context.chartSeries;
  const analytics = buildSeriesAnalytics(context.fullSeries);
  const breakdownAnalytics = buildSeriesAnalytics(context.breakdownSeries || []);
  if (!series.length) {
    const value = result.rows[0]?.[0] ?? "—";
    renderChartInsights(run, context, null);
    elements.chartSurface.innerHTML = `
      <div class="kpi-card">
        <span>KPI</span>
        <strong>${escapeHtml(formatMetricValue(value))}</strong>
        <p>${escapeHtml(metricTitleWithUnit())} за период «${periodTitle()}». Для такого вопроса визуальный акцент сделан на одном показателе.</p>
      </div>
    `;
    renderRankingPanel([]);
    return;
  }

  renderChartInsights(run, context, analytics, breakdownAnalytics);
  elements.chartSurface.innerHTML = context.timeSeries
    ? renderTimeSeriesDashboard(run, series, analytics, breakdownAnalytics)
    : renderCategoryDashboard({ ...run, chart: { ...run.chart, type: context.chartType } }, series, analytics);

  renderRankingPanel((context.rankingSeries && context.rankingSeries.length ? context.rankingSeries : context.fullSeries), { label: context.groupLabel || groupTitle() });
}

// renderRun обновляет связанную область страницы из текущего состояния.
function renderRun() {
  if (!state.run) {
    return;
  }
  state.run.result = normalizeQueryResult(state.run.result);
  state.provider = inferProvider(state.run);
  if (elements.providerPill) {
    elements.providerPill.textContent = `LLM: ${formatProviderLabel(state.provider)}`;
  }
  if (elements.sidebarProvider) {
    elements.sidebarProvider.textContent = `LLM: ${formatProviderLabel(state.provider)}`;
  }
  elements.sqlPreview.textContent = state.run.sql || "SQL не выполнялся: системе потребовалось уточнение запроса.";
  const vizContext = detectVisualizationContext(state.run);
  renderSummary(state.run, vizContext);
  renderTable(state.run.result);
  renderChart(state.run, vizContext);
}
