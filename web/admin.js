const usersBox = document.getElementById("admin-users");
const pendingBox = document.getElementById("admin-pending");
const accessBox = document.getElementById("admin-access");
const statusBox = document.getElementById("admin-status");
const grantForm = document.getElementById("grant-form");

let currentUser = null;
let users = [];

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
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

// setStatus синхронизирует локальное состояние интерфейса и поля формы.
function setStatus(message) {
  statusBox.textContent = message;
}

// renderUsers обновляет связанную область страницы из текущего состояния.
function renderUsers() {
  usersBox.innerHTML = users.filter(u => u.is_approved).map((user) => `
    <article class="admin-row">
      <div><strong>#${user.id} ${escapeHtml(user.full_name)}</strong><small>${escapeHtml(user.email)} · ${escapeHtml(user.department_name || "без отдела")}</small></div>
      <div class="button-row compact">
        <select data-role-user="${user.id}" ${currentUser.role !== "root" ? "disabled" : ""}>
          <option value="user" ${user.role === "user" ? "selected" : ""}>user</option>
          <option value="manager" ${user.role === "manager" ? "selected" : ""}>manager</option>
          <option value="root" ${user.role === "root" ? "selected" : ""}>root</option>
        </select>
      </div>
    </article>
  `).join("") || `<div class="empty-inline">Нет подтвержденных пользователей.</div>`;
}

// renderPending обновляет связанную область страницы из текущего состояния.
function renderPending(pending) {
  pendingBox.innerHTML = pending.map((user) => `
    <article class="admin-row">
      <div>
        <strong>#${user.id} ${escapeHtml(user.full_name)}</strong>
        <small>${escapeHtml(user.email)} · ${escapeHtml(user.department_name || "без отдела")}</small>
      </div>
      <div class="button-row compact">
        <button class="approve-btn" data-approve-user="${user.id}" ${currentUser.role !== "root" && currentUser.role !== "manager" ? "disabled" : ""}>Подтвердить</button>
        <button class="reject-btn" data-reject-user="${user.id}" ${currentUser.role !== "root" && currentUser.role !== "manager" ? "disabled" : ""}>Отклонить</button>
      </div>
    </article>
  `).join("") || `<div class="empty-inline">Нет ожидающих подтверждения пользователей.</div>`;
}

// renderAccess обновляет связанную область страницы из текущего состояния.
function renderAccess(access) {
  accessBox.innerHTML = access.map((item) => `
    <article class="admin-row">
      <div><strong>${escapeHtml(item.full_name)}</strong><small>${escapeHtml(item.email)} получил доступ к ${escapeHtml(item.department_name)}</small></div>
      <small>${escapeHtml(item.granted_by || "system")}</small>
    </article>
  `).join("") || `<div class="empty-inline">Дополнительных доступов пока нет.</div>`;
}

// loadAdmin загружает данные сервера и обновляет видимое состояние.
async function loadAdmin() {
  currentUser = await api("/api/v1/auth/me");
  if (currentUser.role !== "root" && currentUser.role !== "manager") {
    setStatus("Панель доступна только root и начальникам отделов.");
    grantForm.hidden = true;
    return;
  }
  users = await api("/api/v1/auth/users");
  const pending = await api("/api/v1/auth/users/pending");
  const access = await api("/api/v1/auth/department-access");
  renderPending(pending);
  renderUsers();
  renderAccess(access);
}

usersBox.addEventListener("change", async (event) => {
  const select = event.target.closest("[data-role-user]");
  if (!select) return;
  const user = users.find((item) => String(item.id) === select.dataset.roleUser);
  await api(`/api/v1/auth/users/${user.id}`, {
    method: "PATCH",
    body: JSON.stringify({ full_name: user.full_name, role: select.value, department_name: user.department_name }),
  });
  setStatus("Роль обновлена.");
  await loadAdmin();
});

grantForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await api("/api/v1/auth/department-access", {
    method: "POST",
    body: JSON.stringify({
      user_id: Number(document.getElementById("grant-user-id").value),
      department: document.getElementById("grant-department").value.trim(),
    }),
  });
  setStatus("Доступ выдан.");
  await loadAdmin();
});

pendingBox.addEventListener("click", async (event) => {
  const approveBtn = event.target.closest("[data-approve-user]");
  const rejectBtn = event.target.closest("[data-reject-user]");
  
  if (approveBtn) {
    const userId = Number(approveBtn.dataset.approveUser);
    await api("/api/v1/auth/users/approve", {
      method: "POST",
      body: JSON.stringify({ user_id: userId, approve: true }),
    });
    setStatus("Пользователь подтвержден.");
    await loadAdmin();
  }
  
  if (rejectBtn) {
    const userId = Number(rejectBtn.dataset.rejectUser);
    await api("/api/v1/auth/users/approve", {
      method: "POST",
      body: JSON.stringify({ user_id: userId, approve: false }),
    });
    setStatus("Пользователь отклонен.");
    await loadAdmin();
  }
});

loadAdmin().catch((error) => setStatus(error.message));
