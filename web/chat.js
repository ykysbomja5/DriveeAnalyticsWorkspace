const state = {
  rooms: [],
  users: [],
  activeRoomId: 0,
  lastMessageId: 0,
  renderedMessageIds: new Set(),
  events: null,
  assets: { reports: [], templates: [] },
  pendingAttachment: null,
  roomDialogMode: "create",
  selectedMemberIds: new Set(),
  memberFilter: "",
};

const el = {
  rooms: document.getElementById("room-list"),
  messages: document.getElementById("message-list"),
  title: document.getElementById("active-room-title"),
  form: document.getElementById("message-form"),
  input: document.getElementById("message-input"),
  status: document.getElementById("chat-status"),
  assetButton: document.getElementById("asset-button"),
  assetPicker: document.getElementById("asset-picker"),
  addMembersButton: document.getElementById("add-members-button"),
  roomDialog: document.getElementById("room-dialog"),
  roomForm: document.getElementById("room-form"),
  roomDialogKicker: document.getElementById("room-dialog-kicker"),
  roomDialogTitle: document.getElementById("room-dialog-title"),
  roomDialogClose: document.getElementById("room-dialog-close"),
  roomDialogCancel: document.getElementById("room-dialog-cancel"),
  roomDialogSubmit: document.getElementById("room-dialog-submit"),
  roomTitleField: document.getElementById("room-title-field"),
  roomTitleInput: document.getElementById("room-title-input"),
  memberSearchInput: document.getElementById("member-search-input"),
  memberList: document.getElementById("member-list"),
  memberSelectedCount: document.getElementById("member-selected-count"),
};

// token выводит переиспользуемое состояние для рендера и действий.
function token() {
  return window.localStorage.getItem("drivee:token") || "";
}

// api отправляет авторизованные запросы и нормализует ошибки ответа.
async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || "Ошибка запроса");
  return data;
}

// escapeHtml выравнивает отображаемые или транспортные значения в интерфейсе.
function escapeHtml(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}

// setStatus синхронизирует локальное состояние интерфейса и поля формы.
function setStatus(message) {
  el.status.textContent = message;
}

// getCurrentUserId выводит переиспользуемое состояние для рендера и действий.
function getCurrentUserId() {
  try {
    return Number(JSON.parse(window.localStorage.getItem("drivee:user") || "{}").id) || 0;
  } catch {
    return 0;
  }
}

// activeRoom выводит переиспользуемое состояние для рендера и действий.
function activeRoom() {
  return state.rooms.find((item) => item.id === state.activeRoomId);
}

// userDisplayName выполняет отдельную часть сценария страницы.
function userDisplayName(user) {
  return user.full_name || user.email || `Пользователь #${user.id}`;
}

// pluralMembers выравнивает отображаемые или транспортные значения в интерфейсе.
function pluralMembers(count) {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return "выбран";
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return "выбрано";
  return "выбрано";
}

// loadData загружает данные сервера и обновляет видимое состояние.
async function loadData() {
  const [rooms, users, assets] = await Promise.all([
    api("/api/v1/chats"),
    api("/api/v1/auth/users").catch(() => []),
    api("/api/v1/chats/assets").catch(() => ({ reports: [], templates: [] })),
  ]);
  state.rooms = rooms;
  state.users = users;
  state.assets = assets;
  renderRooms();
}

// renderRooms обновляет связанную область страницы из текущего состояния.
function renderRooms() {
  el.rooms.innerHTML = state.rooms.length
    ? state.rooms.map((room) => `<button class="room-item${room.id === state.activeRoomId ? " active" : ""}" data-room-id="${room.id}"><strong>${escapeHtml(room.title)}</strong><small>${room.members?.length || 0} участников</small></button>`).join("")
    : `<div class="empty-inline">Чатов пока нет.</div>`;
  el.addMembersButton.disabled = !state.activeRoomId;
}

// openRoom меняет активную область интерфейса без лишнего изменения состояния.
async function openRoom(roomId) {
  state.activeRoomId = Number(roomId);
  state.lastMessageId = 0;
  state.renderedMessageIds = new Set();
  if (state.events) state.events.close();
  const room = activeRoom();
  el.title.textContent = room?.title || "Чат";
  el.messages.innerHTML = "";
  renderRooms();
  const messages = await api(`/api/v1/chats/${state.activeRoomId}/messages`);
  messages.forEach(renderMessage);
  state.events = new EventSource(`/api/v1/chats/${state.activeRoomId}/events?access_token=${encodeURIComponent(token())}`);
  state.events.addEventListener("message", (event) => renderMessage(JSON.parse(event.data)));
  state.events.addEventListener("ready", () => setStatus("Realtime подключен."));
  state.events.onerror = () => setStatus("Realtime переподключается...");
}

// renderMessage обновляет связанную область страницы из текущего состояния.
function renderMessage(message) {
  const messageId = Number(message?.id) || 0;
  if (!messageId || state.renderedMessageIds.has(messageId)) return;
  state.renderedMessageIds.add(messageId);
  state.lastMessageId = Math.max(state.lastMessageId, messageId);
  const item = document.createElement("article");
  item.className = "message-item";
  item.dataset.messageId = String(messageId);
  const attachment = message.attachment_type
    ? `<a class="message-attachment" href="${message.attachment_type === "report" ? "/reports.html" : "/templates.html"}">${message.attachment_type === "report" ? "Отчет" : "Шаблон"}: ${escapeHtml(message.attachment_title)}</a>`
    : "";
  item.innerHTML = `<strong>${escapeHtml(message.sender?.full_name || "Пользователь")}</strong><p>${escapeHtml(message.body || "")}</p>${attachment}<small>${new Date(message.created_at).toLocaleString("ru-RU")}</small>`;
  const nextItem = [...el.messages.querySelectorAll("[data-message-id]")]
    .find((node) => Number(node.dataset.messageId) > messageId);
  el.messages.insertBefore(item, nextItem || null);
  el.messages.scrollTop = el.messages.scrollHeight;
}

// openRoomDialog меняет активную область интерфейса без лишнего изменения состояния.
function openRoomDialog(mode) {
  state.roomDialogMode = mode;
  state.selectedMemberIds = new Set();
  state.memberFilter = "";
  el.memberSearchInput.value = "";
  el.roomTitleInput.value = "";

  const isCreate = mode === "create";
  el.roomDialogKicker.textContent = isCreate ? "Комната" : "Участники";
  el.roomDialogTitle.textContent = isCreate ? "Новый чат" : "Добавить коллег";
  el.roomDialogSubmit.textContent = isCreate ? "Создать чат" : "Добавить";
  el.roomTitleField.classList.toggle("hidden", !isCreate);
  el.roomTitleInput.required = isCreate;
  renderMemberPicker();
  el.roomDialog.classList.remove("hidden");
  document.body.classList.add("dialog-open");
  (isCreate ? el.roomTitleInput : el.memberSearchInput).focus();
}

// closeRoomDialog меняет активную область интерфейса без лишнего изменения состояния.
function closeRoomDialog() {
  el.roomDialog.classList.add("hidden");
  document.body.classList.remove("dialog-open");
}

// availableUsers выводит переиспользуемое состояние для рендера и действий.
function availableUsers() {
  const currentUserId = getCurrentUserId();
  const currentMemberIds = new Set((activeRoom()?.members || []).map((member) => Number(member.id)));
  return state.users
    .filter((user) => Number(user.id) !== currentUserId)
    .filter((user) => state.roomDialogMode === "create" || !currentMemberIds.has(Number(user.id)))
    .sort((left, right) => userDisplayName(left).localeCompare(userDisplayName(right), "ru"));
}

// renderMemberPicker обновляет связанную область страницы из текущего состояния.
function renderMemberPicker() {
  const query = state.memberFilter.trim().toLowerCase();
  const users = availableUsers();
  const visibleUsers = query
    ? users.filter((user) => [user.full_name, user.email, user.department_name, user.role].some((value) => String(value || "").toLowerCase().includes(query)))
    : users;

  el.memberSelectedCount.textContent = `${state.selectedMemberIds.size} ${pluralMembers(state.selectedMemberIds.size)}`;
  el.memberList.innerHTML = visibleUsers.length
    ? visibleUsers.map((user) => {
        const id = Number(user.id);
        const checked = state.selectedMemberIds.has(id) ? " checked" : "";
        const department = user.department_name ? `<small>${escapeHtml(user.department_name)}</small>` : `<small>${escapeHtml(user.email || "")}</small>`;
        return `
          <label class="member-option">
            <input type="checkbox" value="${id}" data-user-checkbox${checked} />
            <span>
              <strong>${escapeHtml(userDisplayName(user))}</strong>
              ${department}
            </span>
          </label>
        `;
      }).join("")
    : `<div class="empty-inline">${users.length ? "Коллеги не найдены." : "Нет доступных коллег."}</div>`;
}

// submitRoomDialog выполняет действие пользователя и синхронизирует результат с backend.
async function submitRoomDialog() {
  if (state.roomDialogMode === "create") {
    await createRoomFromDialog();
    return;
  }
  await addMembersFromDialog();
}

// createRoomFromDialog выполняет действие пользователя и синхронизирует результат с backend.
async function createRoomFromDialog() {
  const title = el.roomTitleInput.value.trim();
  if (!title) {
    el.roomTitleInput.focus();
    return;
  }
  const room = await api("/api/v1/chats", { method: "POST", body: JSON.stringify({ title, member_ids: [...state.selectedMemberIds] }) });
  state.rooms.unshift(room);
  renderRooms();
  closeRoomDialog();
  await openRoom(room.id);
  setStatus("Чат создан.");
}

// addMembersFromDialog выполняет действие пользователя и синхронизирует результат с backend.
async function addMembersFromDialog() {
  if (!state.activeRoomId) return;
  const memberIds = [...state.selectedMemberIds];
  if (!memberIds.length) return;
  const room = await api(`/api/v1/chats/${state.activeRoomId}/members`, { method: "POST", body: JSON.stringify({ member_ids: memberIds }) });
  state.rooms = state.rooms.map((item) => item.id === room.id ? room : item);
  renderRooms();
  closeRoomDialog();
  setStatus("Коллеги добавлены.");
}

// renderAssets обновляет связанную область страницы из текущего состояния.
function renderAssets() {
  const reports = state.assets.reports || [];
  const templates = state.assets.templates || [];
  el.assetPicker.innerHTML = [
    ...reports.map((item) => `<button type="button" data-asset-type="report" data-asset-id="${item.id}" data-asset-title="${escapeHtml(item.name)}">Отчет: ${escapeHtml(item.name)}</button>`),
    ...templates.map((item) => `<button type="button" data-asset-type="template" data-asset-id="${item.id}" data-asset-title="${escapeHtml(item.name)}">Шаблон: ${escapeHtml(item.name)}</button>`),
  ].join("") || `<div class="empty-inline">Нет сохраненных материалов.</div>`;
}

document.getElementById("new-room-button").addEventListener("click", () => openRoomDialog("create"));
el.addMembersButton.addEventListener("click", () => {
  if (!state.activeRoomId) return;
  openRoomDialog("add");
});
el.roomDialogClose.addEventListener("click", closeRoomDialog);
el.roomDialogCancel.addEventListener("click", closeRoomDialog);
el.roomDialog.addEventListener("click", (event) => {
  if (event.target === el.roomDialog) closeRoomDialog();
});
el.roomForm.addEventListener("submit", (event) => {
  event.preventDefault();
  submitRoomDialog().catch((error) => setStatus(error.message));
});
el.memberSearchInput.addEventListener("input", () => {
  state.memberFilter = el.memberSearchInput.value;
  renderMemberPicker();
});
el.memberList.addEventListener("change", (event) => {
  const checkbox = event.target.closest("[data-user-checkbox]");
  if (!checkbox) return;
  const id = Number(checkbox.value);
  if (checkbox.checked) {
    state.selectedMemberIds.add(id);
  } else {
    state.selectedMemberIds.delete(id);
  }
  renderMemberPicker();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !el.roomDialog.classList.contains("hidden")) {
    closeRoomDialog();
  }
});
el.rooms.addEventListener("click", (event) => {
  const button = event.target.closest("[data-room-id]");
  if (button) openRoom(button.dataset.roomId).catch((e) => setStatus(e.message));
});
el.assetButton.addEventListener("click", () => {
  renderAssets();
  el.assetPicker.classList.toggle("hidden");
});
el.assetPicker.addEventListener("click", (event) => {
  const button = event.target.closest("[data-asset-type]");
  if (!button) return;
  state.pendingAttachment = { attachment_type: button.dataset.assetType, attachment_id: Number(button.dataset.assetId), title: button.dataset.assetTitle };
  setStatus(`Прикреплено: ${state.pendingAttachment.title}`);
  el.assetPicker.classList.add("hidden");
});
el.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.activeRoomId) return;
  const body = el.input.value.trim();
  const payload = { body, ...(state.pendingAttachment || {}) };
  const message = await api(`/api/v1/chats/${state.activeRoomId}/messages`, { method: "POST", body: JSON.stringify(payload) });
  renderMessage(message);
  el.input.value = "";
  state.pendingAttachment = null;
  setStatus("Сообщение отправлено.");
});

loadData().catch((error) => setStatus(error.message));
