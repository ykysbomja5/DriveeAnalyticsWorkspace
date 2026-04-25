(function () {
  const publicPages = ["/login.html"];
  const path = window.location.pathname;
  if (publicPages.includes(path)) return;

  const token = window.localStorage.getItem("drivee:token");
  if (!token) {
    window.location.href = "/login.html";
    return;
  }

  const originalFetch = window.fetch.bind(window);
  window.fetch = (input, init = {}) => {
    const url = typeof input === "string" ? input : input.url;
    const headers = new Headers(init.headers || (typeof input !== "string" ? input.headers : undefined));
    if (String(url).startsWith("/api/") && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${token}`);
    }
    return originalFetch(input, { ...init, headers });
  };

  async function hydrateUser() {
    const response = await fetch("/api/v1/auth/me");
    if (!response.ok) throw new Error("auth failed");
    const user = await response.json();
    
    // Проверяем, подтверждён ли пользователь.
    if (!user.is_approved) {
      window.location.href = "/pending-approval.html";
      return;
    }
    
    window.localStorage.setItem("drivee:user", JSON.stringify(user));
    window.localStorage.setItem("drivee:templateOwnerName", user.full_name || "");
    window.localStorage.setItem("drivee:templateOwnerDepartment", user.department_name || "");
    renderUserNav(user);
  }

  function renderUserNav(user) {
    document.querySelectorAll(".sidebar-nav").forEach((nav) => {
      const links = [
        ["/chat.html", "Чаты"],
        ["/profile.html", "Кабинет"],
      ];
      if (user.role === "root" || user.role === "manager") {
        links.push(["/admin.html", "Панель"]);
      }
      for (const [href, label] of links) {
        if (nav.querySelector(`a[href="${href}"]`)) continue;
        const link = document.createElement("a");
        link.href = href;
        link.className = `nav-item${window.location.pathname === href ? " active" : ""}`;
        link.textContent = label;
        nav.appendChild(link);
      }
    });

    const topbar = document.querySelector(".topbar");
    if (topbar && !document.querySelector(".auth-user-chip")) {
      const box = document.createElement("div");
      box.className = "auth-user-chip";
      box.innerHTML = `
        <span>${escapeHtml(user.full_name || user.email)}</span>
        <small>${escapeHtml(roleLabel(user.role))}${user.department_name ? " · " + escapeHtml(user.department_name) : ""}</small>
        <button class="mini-button" type="button" data-auth-logout>Выйти</button>
      `;
      topbar.appendChild(box);
    }
  }

  function roleLabel(role) {
    if (role === "root") return "root";
    if (role === "manager") return "начальник отдела";
    return "пользователь";
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  document.addEventListener("click", async (event) => {
    if (!event.target.closest("[data-auth-logout]")) return;
    await fetch("/api/v1/auth/logout", { method: "POST" }).catch(() => {});
    window.localStorage.removeItem("drivee:token");
    window.localStorage.removeItem("drivee:user");
    window.location.href = "/login.html";
  });

  hydrateUser().catch(() => {
    window.localStorage.removeItem("drivee:token");
    window.location.href = "/login.html";
  });
})();
