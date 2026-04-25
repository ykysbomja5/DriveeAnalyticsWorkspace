const card = document.getElementById("profile-card");

// roleLabel выравнивает отображаемые или транспортные значения в интерфейсе.
function roleLabel(role) {
  if (role === "root") return "root, администратор системы";
  if (role === "manager") return "начальник отдела";
  return "обычный пользователь";
}

// escapeHtml выравнивает отображаемые или транспортные значения в интерфейсе.
function escapeHtml(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

// loadProfile загружает данные сервера и обновляет видимое состояние.
async function loadProfile() {
  const response = await fetch("/api/v1/auth/me");
  const user = await response.json();
  card.innerHTML = `
    <div class="profile-grid">
      <div><span class="field-label">ФИО</span><strong>${escapeHtml(user.full_name)}</strong></div>
      <div><span class="field-label">Почта</span><strong>${escapeHtml(user.email)}</strong></div>
      <div><span class="field-label">Права</span><strong>${escapeHtml(roleLabel(user.role))}</strong></div>
      <div><span class="field-label">Отдел</span><strong>${escapeHtml(user.department_name || "Не указан")}</strong></div>
    </div>
  `;
}

loadProfile().catch(() => {
  card.textContent = "Не удалось загрузить профиль.";
});
