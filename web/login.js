const statusBox = document.getElementById("auth-status");
const loginForm = document.getElementById("login-form");
const registerForm = document.getElementById("register-form");

// setStatus синхронизирует локальное состояние интерфейса и поля формы.
function setStatus(message) {
  statusBox.textContent = message;
}

// authRequest выполняет отдельную часть сценария страницы.
async function authRequest(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || "Не удалось выполнить запрос");
  
  // Проверяем, ожидает ли пользователь подтверждения.
  if (data.pending) {
    window.localStorage.setItem("drivee:user", JSON.stringify(data.user));
    window.location.href = "/pending-approval.html";
    return;
  }
  
  window.localStorage.setItem("drivee:token", data.token);
  window.localStorage.setItem("drivee:user", JSON.stringify(data.user));
  window.localStorage.setItem("drivee:templateOwnerName", data.user.full_name || "");
  window.localStorage.setItem("drivee:templateOwnerDepartment", data.user.department_name || "");
  window.location.href = "/";
}

document.querySelectorAll("[data-auth-tab]").forEach((button) => {
  button.addEventListener("click", () => {
    const mode = button.dataset.authTab;
    document.querySelectorAll("[data-auth-tab]").forEach((item) => item.classList.toggle("active", item === button));
    loginForm.classList.toggle("hidden", mode !== "login");
    registerForm.classList.toggle("hidden", mode !== "register");
  });
});

loginForm.addEventListener("submit", (event) => {
  event.preventDefault();
  setStatus("Проверяю доступ...");
  authRequest("/api/v1/auth/login", {
    email: document.getElementById("login-email").value.trim(),
    password: document.getElementById("login-password").value,
  }).catch((error) => setStatus(error.message));
});

registerForm.addEventListener("submit", (event) => {
  event.preventDefault();
  setStatus("Создаю аккаунт...");
  
  fetch("/api/v1/auth/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      full_name: document.getElementById("register-name").value.trim(),
      email: document.getElementById("register-email").value.trim(),
      department_name: document.getElementById("register-department").value.trim(),
      password: document.getElementById("register-password").value,
    }),
  })
  .then(async (response) => {
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || "Не удалось создать аккаунт");
    
    setStatus(data.message || "Регистрация успешна!");
    
    // Сохраняем данные пользователя и переводим на ожидание подтверждения.
    if (data.user) {
      window.localStorage.setItem("drivee:user", JSON.stringify(data.user));
      setTimeout(() => {
        window.location.href = "/pending-approval.html";
      }, 1500);
    }
  })
  .catch((error) => setStatus(error.message));
});
