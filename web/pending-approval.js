const statusBox = document.getElementById("pending-status");
const checkButton = document.getElementById("check-status-button");
const backButton = document.getElementById("back-to-login-button");

// setStatus синхронизирует локальное состояние интерфейса и поля формы.
function setStatus(message) {
  statusBox.textContent = message;
}

// checkApprovalStatus выполняет отдельную часть сценария страницы.
async function checkApprovalStatus() {
  const user = JSON.parse(window.localStorage.getItem("drivee:user") || "{}");
  if (!user.id) {
    setStatus("Пользователь не найден. Пожалуйста, войдите снова.");
    return;
  }

  setStatus("Проверяю статус...");
  
  try {
    const response = await fetch("/api/v1/auth/me", {
      headers: {
        "Authorization": `Bearer ${window.localStorage.getItem("drivee:token")}`
      }
    });
    
    if (!response.ok) {
      setStatus("Сессия истекла. Пожалуйста, войдите снова.");
      setTimeout(() => {
        window.location.href = "/login.html";
      }, 2000);
      return;
    }

    const userData = await response.json();
    
    if (userData.is_approved) {
      setStatus("✓ Ваш аккаунт подтвержден! Перенаправляю...");
      window.location.href = "/";
    } else {
      setStatus("Ваш аккаунт еще ожидает подтверждения администратора.");
    }
  } catch (error) {
    setStatus("Ошибка проверки статуса. Попробуйте снова.");
  }
}

checkButton.addEventListener("click", checkApprovalStatus);

backButton.addEventListener("click", () => {
  window.localStorage.removeItem("drivee:token");
  window.localStorage.removeItem("drivee:user");
  window.location.href = "/login.html";
});

// Автоматически проверяем статус каждые 30 секунд.
setInterval(checkApprovalStatus, 30000);
