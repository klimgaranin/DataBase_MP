const state = {
  token: localStorage.getItem("dbmp_api_token") || "",
  tab: "admin",
  marketplace: "wb",
};

const apiToken = document.querySelector("#apiToken");
const authNotice = document.querySelector("#authNotice");
const pageTitle = document.querySelector("#pageTitle");
const pageSubtitle = document.querySelector("#pageSubtitle");

apiToken.value = state.token;

function headers() {
  return {
    Authorization: `Bearer ${state.token}`,
  };
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("ru-RU");
}

function statusPill(value) {
  const text = value || "-";
  return `<span class="status ${String(text).toLowerCase()}">${text}</span>`;
}

function escapeHtml(value) {
  return String(value ?? "-")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function requestJson(path) {
  const response = await fetch(path, { headers: headers() });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function setNotice(message) {
  authNotice.textContent = message;
  authNotice.classList.add("visible");
}

function clearNotice() {
  authNotice.classList.remove("visible");
}

function renderJobs(items) {
  const body = document.querySelector("#jobsBody");
  body.innerHTML = items.map((job) => `
    <tr>
      <td>${escapeHtml(job.job_name)}</td>
      <td>${statusPill(job.status)}</td>
      <td>${escapeHtml(formatDate(job.started_at))}</td>
      <td>${escapeHtml(job.api_rows)}</td>
      <td>${escapeHtml(job.norm_upserted)}</td>
      <td>${escapeHtml(job.duplicates)}</td>
    </tr>
  `).join("");
}

function renderSecrets(items) {
  const list = document.querySelector("#secretsList");
  const entries = Object.entries(items);
  list.innerHTML = entries.map(([name, ok]) => `
    <div class="secret">
      <span>${escapeHtml(name)}</span>
      ${statusPill(ok ? "задан" : "не задан")}
    </div>
  `).join("");
}

function renderOverview(data) {
  document.querySelector("#dbStatus").textContent = data.db?.ok ? "OK" : "Ошибка";
  document.querySelector("#dbDetail").textContent = data.db?.dsn || "-";

  const missing = data.alerts?.missing_required_secrets || [];
  document.querySelector("#secretsStatus").textContent = missing.length ? "Проверить" : "OK";
  document.querySelector("#secretsDetail").textContent = missing.length ? missing.join(", ") : "Ключевые секреты заданы";

  const failedJobs = data.alerts?.failed_jobs || 0;
  document.querySelector("#jobsStatus").textContent = failedJobs ? `${failedJobs} с ошибкой` : "OK";
  document.querySelector("#jobsDetail").textContent = `${data.jobs?.length || 0} последних запусков`;

  renderJobs(data.jobs || []);
  renderSecrets(data.secrets || {});
}

function renderOrders(items) {
  const body = document.querySelector("#ordersBody");
  body.innerHTML = items.map((order) => `
    <tr>
      <td>${escapeHtml(order.marketplace)}</td>
      <td>${escapeHtml(order.order_key)}</td>
      <td>${statusPill(order.status)}</td>
      <td>${escapeHtml(formatDate(order.order_date))}</td>
      <td>${escapeHtml(order.warehouse_name)}</td>
      <td>${escapeHtml(order.article)}</td>
      <td>${escapeHtml(order.product_name)}</td>
      <td>${escapeHtml(order.quantity)}</td>
      <td>${escapeHtml(order.price)}</td>
    </tr>
  `).join("");
}

async function loadAdmin() {
  if (!state.token) {
    setNotice("Вставьте API token. Данные не загрузятся без авторизации.");
    return;
  }
  try {
    const data = await requestJson("/api/v1/admin/overview");
    renderOverview(data);
    clearNotice();
  } catch (error) {
    setNotice(`Админка не загрузилась: ${error.message}`);
  }
}

async function loadOrders() {
  if (!state.token) {
    setNotice("Вставьте API token. Данные не загрузятся без авторизации.");
    return;
  }
  try {
    const data = await requestJson(`/api/v1/admin/orders?marketplace=${state.marketplace}&limit=100`);
    renderOrders(data.items || []);
    document.querySelector("#ordersHint").textContent = `Последние строки ${state.marketplace.toUpperCase()}`;
    clearNotice();
  } catch (error) {
    setNotice(`Лента заказов не загрузилась: ${error.message}`);
  }
}

function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll(".nav-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.tab === tab);
  });
  document.querySelectorAll(".tab").forEach((panel) => {
    panel.classList.toggle("active", panel.id === `${tab}Tab`);
  });
  pageTitle.textContent = tab === "admin" ? "Админка" : "Лента заказов";
  pageSubtitle.textContent = tab === "admin"
    ? "Состояние системы, секретов и последних jobs"
    : "WB и Ozon заказы из рабочей базы";
  refresh();
}

function refresh() {
  if (state.tab === "admin") {
    loadAdmin();
  } else {
    loadOrders();
  }
}

document.querySelector("#saveToken").addEventListener("click", () => {
  state.token = apiToken.value.trim();
  localStorage.setItem("dbmp_api_token", state.token);
  refresh();
});

document.querySelector("#refresh").addEventListener("click", refresh);

document.querySelectorAll(".nav-button").forEach((button) => {
  button.addEventListener("click", () => switchTab(button.dataset.tab));
});

document.querySelectorAll(".segment").forEach((button) => {
  button.addEventListener("click", () => {
    state.marketplace = button.dataset.marketplace;
    document.querySelectorAll(".segment").forEach((item) => {
      item.classList.toggle("active", item === button);
    });
    loadOrders();
  });
});

refresh();
