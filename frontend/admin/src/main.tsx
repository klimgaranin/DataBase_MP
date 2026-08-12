import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { AnimatePresence, motion } from "framer-motion";
import {
  Activity,
  AlertTriangle,
  Check,
  Database,
  ImageOff,
  KeyRound,
  LayoutDashboard,
  ListOrdered,
  Lock,
  Play,
  RefreshCw,
  Save,
  Server,
  ShieldAlert,
} from "lucide-react";
import "./styles.css";

type Tab = "admin" | "orders";
type Marketplace = "wb" | "ozon";

type JobRun = {
  job_name?: string;
  started_at?: string;
  status?: string;
  api_rows?: number | string | null;
  norm_upserted?: number | string | null;
  duplicates?: number | string | null;
};

type AdminOverview = {
  db?: { ok?: boolean; dsn?: string };
  alerts?: { failed_jobs?: number; missing_required_secrets?: string[] };
  jobs?: JobRun[];
  secrets?: Record<string, boolean>;
};

type JobAction = {
  key: string;
  title: string;
  description: string;
  marketplace: string;
  group: string;
  available: boolean;
};

type OrderRow = {
  marketplace?: string;
  order_key?: string;
  order_group_key?: string;
  order_number?: string;
  status?: string;
  status_label?: string;
  order_date?: string;
  warehouse_name?: string;
  article?: string;
  product_name?: string;
  image_url?: string;
  quantity?: number | string;
  price?: number | string;
};

type OrderGroup = {
  key: string;
  marketplace?: string;
  orderNumber?: string;
  status?: string;
  statusLabel?: string;
  date?: string;
  warehouseName?: string;
  rows: OrderRow[];
  totalQuantity: number;
  totalAmount: number;
};

type Toast = {
  id: number;
  title: string;
  text: string;
  tone: "good" | "bad";
};

const variants = {
  hidden: { opacity: 0, y: 12 },
  show: { opacity: 1, y: 0 },
};

function cls(...values: Array<string | false | undefined>) {
  return values.filter(Boolean).join(" ");
}

function formatDate(value?: string) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
}

function formatMoney(value?: number | string) {
  const number = Number(value);
  if (!Number.isFinite(number)) return value ?? "-";
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(number);
}

function asNumber(value?: number | string) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function statusTone(value?: string) {
  const text = String(value || "").toLowerCase();
  if (["ok", "success", "active", "delivered", "задан", "доставлен", "активный"].includes(text)) return "good";
  if (["error", "failed", "fail", "cancelled", "не задан", "отменён", "отменен"].includes(text)) return "bad";
  if (text.includes("awaiting") || text.includes("ожидает") || text.includes("process") || text.includes("running") || text.includes("идёт")) return "warn";
  return "info";
}

function StatusPill({ value }: { value?: string }) {
  const tone = statusTone(value);
  return (
    <span
      className={cls(
        "inline-flex min-h-6 max-w-full items-center rounded-full px-2 py-0.5 text-xs font-bold",
        tone === "good" && "bg-emerald-50 text-emerald-700",
        tone === "bad" && "bg-rose-50 text-rose-700",
        tone === "warn" && "bg-amber-50 text-amber-700",
        tone === "info" && "bg-blue-50 text-blue-700",
      )}
    >
      {value || "-"}
    </span>
  );
}

function groupOrders(rows: OrderRow[]): OrderGroup[] {
  const map = new Map<string, OrderGroup>();
  for (const row of rows) {
    const key = row.order_group_key || row.order_number || row.order_key || "Без номера";
    const quantity = asNumber(row.quantity);
    const amount = quantity * asNumber(row.price);
    const current = map.get(key);
    if (current) {
      current.rows.push(row);
      current.totalQuantity += quantity;
      current.totalAmount += amount;
      if (!current.date && row.order_date) current.date = row.order_date;
      continue;
    }
    map.set(key, {
      key,
      marketplace: row.marketplace,
      orderNumber: row.order_number,
      status: row.status,
      statusLabel: row.status_label || row.status,
      date: row.order_date,
      warehouseName: row.warehouse_name,
      rows: [row],
      totalQuantity: quantity,
      totalAmount: amount,
    });
  }
  return Array.from(map.values());
}

function App() {
  const [token, setToken] = useState(() => localStorage.getItem("dbmp_api_token") || "");
  const [tokenDraft, setTokenDraft] = useState(token);
  const [tab, setTab] = useState<Tab>("admin");
  const [marketplace, setMarketplace] = useState<Marketplace>("wb");
  const [limit, setLimit] = useState(100);
  const [overview, setOverview] = useState<AdminOverview | null>(null);
  const [actions, setActions] = useState<JobAction[]>([]);
  const [orders, setOrders] = useState<OrderRow[]>([]);
  const [notice, setNotice] = useState("Вставьте API token.");
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState<Set<string>>(new Set());
  const [lastRefresh, setLastRefresh] = useState("-");
  const [toasts, setToasts] = useState<Toast[]>([]);

  const page = tab === "admin"
    ? ["Система", "Админка", "Состояние базы, секретов и последних jobs"]
    : ["Заказы", "Лента заказов", "WB и Ozon заказы из рабочей базы"];

  const authHeaders = useMemo(() => ({
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  }), [token]);

  function pushToast(title: string, text: string, tone: Toast["tone"] = "good") {
    const id = Date.now() + Math.random();
    setToasts((items) => [...items, { id, title, text, tone }]);
    window.setTimeout(() => {
      setToasts((items) => items.filter((item) => item.id !== id));
    }, 5200);
  }

  async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(path, {
      ...init,
      headers: { ...authHeaders, ...(init?.headers || {}) },
    });
    if (!response.ok) {
      let message = `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload.detail) message = typeof payload.detail === "string" ? payload.detail : JSON.stringify(payload.detail);
      } catch {
        // Keep HTTP status as message.
      }
      throw new Error(message);
    }
    return response.json() as Promise<T>;
  }

  async function loadAdmin() {
    if (!token) {
      setNotice("Вставьте API token.");
      return;
    }
    setLoading(true);
    try {
      const [overviewData, actionsData] = await Promise.all([
        requestJson<AdminOverview>("/api/v1/admin/overview"),
        requestJson<{ items: JobAction[] }>("/api/v1/admin/actions"),
      ]);
      setOverview(overviewData);
      setActions(actionsData.items || []);
      setNotice("");
      setLastRefresh(new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }));
    } catch (error) {
      const text = error instanceof Error ? error.message : "Ошибка";
      setNotice(`Админка не загрузилась: ${text}`);
      pushToast("Ошибка загрузки", text, "bad");
    } finally {
      setLoading(false);
    }
  }

  async function loadOrders() {
    if (!token) {
      setNotice("Вставьте API token.");
      return;
    }
    setLoading(true);
    try {
      const data = await requestJson<{ items: OrderRow[] }>(`/api/v1/admin/orders?marketplace=${marketplace}&limit=${limit}`);
      setOrders(data.items || []);
      setNotice("");
      setLastRefresh(new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }));
    } catch (error) {
      const text = error instanceof Error ? error.message : "Ошибка";
      setNotice(`Лента заказов не загрузилась: ${text}`);
      pushToast("Заказы не загрузились", text, "bad");
    } finally {
      setLoading(false);
    }
  }

  async function runAction(action: JobAction) {
    setRunning((items) => new Set(items).add(action.key));
    try {
      const result = await requestJson<{ title: string; pid: number }>(
        `/api/v1/admin/actions/${encodeURIComponent(action.key)}/run`,
        { method: "POST" },
      );
      pushToast("Job запущен", `${result.title || action.title}: pid ${result.pid}`);
      window.setTimeout(loadAdmin, 900);
    } catch (error) {
      pushToast("Job не запущен", error instanceof Error ? error.message : "Ошибка", "bad");
    } finally {
      setRunning((items) => {
        const next = new Set(items);
        next.delete(action.key);
        return next;
      });
    }
  }

  function saveToken() {
    const clean = tokenDraft.trim();
    setToken(clean);
    localStorage.setItem("dbmp_api_token", clean);
    pushToast("Token сохранён", "Можно обновлять данные");
  }

  function refresh() {
    if (tab === "admin") void loadAdmin();
    if (tab === "orders") void loadOrders();
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, marketplace, limit, token]);

  const failedJobs = overview?.alerts?.failed_jobs || 0;
  const missingSecrets = overview?.alerts?.missing_required_secrets || [];

  return (
    <div className="min-h-screen bg-[#f4f6f8] text-ink">
      <div className="grid min-h-screen grid-cols-[288px_minmax(0,1fr)] max-[820px]:block">
        <aside className="sticky top-0 flex h-screen flex-col gap-6 bg-[#1f2933] p-6 text-white max-[820px]:relative max-[820px]:h-auto">
          <div className="grid grid-cols-[44px_minmax(0,1fr)] items-center gap-3">
            <div className="grid h-11 w-11 place-items-center rounded-ui bg-[#e7eef6] text-primary">
              <Database size={21} />
            </div>
            <div>
              <div className="text-lg font-extrabold">DataBase_MP</div>
              <div className="text-xs text-[#b8c3cf]">Marketplace control</div>
            </div>
          </div>

          <nav className="grid gap-2">
            <NavButton active={tab === "admin"} icon={<LayoutDashboard size={18} />} onClick={() => setTab("admin")}>Админка</NavButton>
            <NavButton active={tab === "orders"} icon={<ListOrdered size={18} />} onClick={() => setTab("orders")}>Лента заказов</NavButton>
          </nav>

          <div className="mt-auto grid gap-3 border-t border-white/15 pt-5">
            <label className="text-xs font-bold text-[#b8c3cf]" htmlFor="apiToken">API token</label>
            <div className="grid h-10 grid-cols-[20px_minmax(0,1fr)] items-center gap-2 rounded-ui border border-[#3d4b59] bg-[#18222c] px-3 transition focus-within:border-primary">
              <KeyRound size={17} className="text-[#b8c3cf]" />
              <input
                id="apiToken"
                type="password"
                value={tokenDraft}
                onChange={(event) => setTokenDraft(event.target.value)}
                className="min-w-0 bg-transparent text-white outline-none"
                placeholder="Вставить токен"
              />
            </div>
            <button className="button-primary" onClick={saveToken}>
              <Save size={17} />
              <span>Сохранить</span>
            </button>
          </div>
        </aside>

        <main className="min-w-0 p-8 max-[820px]:p-5">
          <header className="mb-5 flex items-start justify-between gap-4 max-[820px]:grid">
            <div>
              <div className="mb-1 text-xs font-extrabold uppercase text-primary">{page[0]}</div>
              <h1 className="text-[38px] font-black leading-none tracking-normal max-[820px]:text-[30px]">{page[1]}</h1>
              <p className="mt-2 text-muted">{page[2]}</p>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs text-muted">{lastRefresh === "-" ? "-" : `Обновлено ${lastRefresh}`}</span>
              <button className="button-secondary" onClick={refresh}>
                <RefreshCw size={17} className={loading ? "animate-spin" : ""} />
                <span>Обновить</span>
              </button>
            </div>
          </header>

          <AnimatePresence>
            {notice && (
              <motion.div
                className="mb-5 flex items-center gap-3 rounded-ui border border-[#ead8aa] bg-[#fff8e6] px-4 py-3 text-[#7a4f12]"
                initial={{ opacity: 0, y: -8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
              >
                <ShieldAlert size={18} />
                <span>{notice}</span>
              </motion.div>
            )}
          </AnimatePresence>

          <AnimatePresence mode="wait">
            {tab === "admin" ? (
              <motion.section key="admin" variants={variants} initial="hidden" animate="show" exit="hidden">
                <div className="mb-5 grid grid-cols-[1.08fr_0.92fr_1fr] gap-4 max-[1180px]:grid-cols-1">
                  <Kpi icon={<Server />} label="База данных" value={overview?.db?.ok ? "OK" : overview ? "Ошибка" : "-"} detail={overview?.db?.dsn || "-"} tone="teal" />
                  <Kpi icon={<Lock />} label="Секреты" value={missingSecrets.length ? "Проверить" : overview ? "OK" : "-"} detail={missingSecrets.length ? missingSecrets.join(", ") : "Ключевые секреты заданы"} tone="violet" />
                  <Kpi icon={<Activity />} label="Jobs" value={failedJobs ? `${failedJobs} с ошибкой` : overview ? "OK" : "-"} detail={`${overview?.jobs?.length || 0} последних запусков`} tone="amber" />
                </div>

                <Panel title="Запуск jobs" subtitle="Доступные команды" action={<IconButton onClick={() => void loadAdmin()}><RefreshCw size={17} /></IconButton>}>
                  <div className="grid grid-cols-[repeat(auto-fit,minmax(230px,1fr))] gap-3 p-4">
                    {actions.length ? actions.map((action) => (
                      <ActionCard
                        key={action.key}
                        action={action}
                        running={running.has(action.key)}
                        onRun={() => void runAction(action)}
                      />
                    )) : <Empty text="Команды не найдены" />}
                  </div>
                </Panel>

                <div className="grid grid-cols-[minmax(0,1.7fr)_minmax(280px,0.9fr)] gap-5 max-[1180px]:grid-cols-1">
                  <Panel title="Последние запуски" subtitle="За последние 24 часа">
                    <JobsTable items={overview?.jobs || []} />
                  </Panel>
                  <Panel title="Секреты" subtitle="Только статус">
                    <SecretsList items={overview?.secrets || {}} />
                  </Panel>
                </div>
              </motion.section>
            ) : (
              <motion.section key="orders" variants={variants} initial="hidden" animate="show" exit="hidden">
                <div className="mb-4 flex items-center justify-between gap-4 max-[820px]:grid">
                  <div className="inline-flex rounded-ui border border-line bg-white p-1">
                    <Segment active={marketplace === "wb"} onClick={() => setMarketplace("wb")}>WB</Segment>
                    <Segment active={marketplace === "ozon"} onClick={() => setMarketplace("ozon")}>Ozon</Segment>
                  </div>
                  <label className="flex items-center gap-2 text-xs font-bold text-muted">
                    Строк
                    <select className="h-9 rounded-ui border border-line bg-white px-3 text-ink" value={limit} onChange={(event) => setLimit(Number(event.target.value))}>
                      <option value={100}>100</option>
                      <option value={250}>250</option>
                      <option value={500}>500</option>
                      <option value={1000}>1000</option>
                    </select>
                  </label>
                </div>
                <Panel title="Лента заказов" subtitle={`Сгруппировано по заказам ${marketplace.toUpperCase()}`} action={<span className="grid h-8 min-w-10 place-items-center rounded-full bg-[#edf4fb] px-3 text-sm font-extrabold text-primary">{groupOrders(orders).length}</span>}>
                  <OrdersFeed items={orders} />
                </Panel>
              </motion.section>
            )}
          </AnimatePresence>
        </main>
      </div>

      <div className="fixed bottom-5 right-5 z-20 grid w-[min(420px,calc(100vw-40px))] gap-3">
        <AnimatePresence>
          {toasts.map((item) => (
            <motion.div
              key={item.id}
              className={cls("grid grid-cols-[22px_minmax(0,1fr)] gap-3 rounded-ui border bg-white p-4 shadow-panel", item.tone === "bad" ? "border-rose-200" : "border-emerald-200")}
              initial={{ opacity: 0, y: 12, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 12, scale: 0.98 }}
            >
              {item.tone === "bad" ? <AlertTriangle size={20} className="text-rose-700" /> : <Check size={20} className="text-emerald-700" />}
              <div>
                <div className="font-extrabold">{item.title}</div>
                <div className="mt-1 text-sm text-muted">{item.text}</div>
              </div>
            </motion.div>
          ))}
        </AnimatePresence>
      </div>
    </div>
  );
}

function NavButton({ active, icon, children, onClick }: { active: boolean; icon: React.ReactNode; children: React.ReactNode; onClick: () => void }) {
  return (
    <button
      className={cls(
        "grid h-11 grid-cols-[20px_minmax(0,1fr)] items-center gap-3 rounded-ui px-3 text-left transition duration-200 hover:translate-x-0.5",
        active ? "bg-[#e7eef6] text-[#1e3a5f]" : "bg-transparent text-[#d3dce6] hover:bg-[#2c3844] hover:text-white",
      )}
      onClick={onClick}
    >
      {icon}
      <span>{children}</span>
    </button>
  );
}

function Kpi({ icon, label, value, detail, tone }: { icon: React.ReactNode; label: string; value: string; detail: string; tone: "teal" | "violet" | "amber" }) {
  const colors = {
    teal: "bg-[#e7eef6] text-primary",
    violet: "bg-[#f0edf7] text-plum",
    amber: "bg-[#fff3d8] text-sun",
  };
  return (
    <motion.article className="card grid min-h-[132px] grid-cols-[42px_minmax(0,1fr)] items-center gap-x-3 p-5" whileHover={{ y: -2 }}>
      <div className={cls("row-span-3 grid h-[42px] w-[42px] place-items-center rounded-ui", colors[tone])}>{icon}</div>
      <span className="text-sm font-bold text-muted">{label}</span>
      <strong className="min-w-0 text-[27px] leading-tight">{value}</strong>
      <small className="min-w-0 truncate text-muted">{detail}</small>
    </motion.article>
  );
}

function Panel({ title, subtitle, action, children }: { title: string; subtitle: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <motion.section className="card mb-5 overflow-hidden" whileHover={{ y: -2 }}>
      <div className="flex min-h-[66px] items-center justify-between gap-4 border-b border-slate-100 px-4 py-3">
        <div>
          <h2 className="text-lg font-extrabold">{title}</h2>
          <span className="text-sm text-muted">{subtitle}</span>
        </div>
        {action}
      </div>
      {children}
    </motion.section>
  );
}

function IconButton({ children, onClick }: { children: React.ReactNode; onClick: () => void }) {
  return (
    <button className="grid h-10 w-10 place-items-center rounded-ui bg-slate-100 text-muted transition hover:-translate-y-0.5 hover:bg-white hover:shadow-soft" onClick={onClick}>
      {children}
    </button>
  );
}

function ActionCard({ action, running, onRun }: { action: JobAction; running: boolean; onRun: () => void }) {
  const Icon = action.marketplace === "WB" ? ListOrdered : action.marketplace === "Ozon" ? Server : action.marketplace === "Sheets" ? LayoutDashboard : Database;
  return (
    <motion.article
      className="card grid min-h-[148px] grid-rows-[auto_1fr_auto] gap-3 p-4"
      whileHover={{ y: -3 }}
      transition={{ type: "spring", stiffness: 320, damping: 24 }}
    >
      <div className="flex items-center justify-between gap-3">
        <div className="grid h-10 w-10 place-items-center rounded-ui bg-[#edf4fb] text-primary"><Icon size={18} /></div>
        <span className="rounded-full bg-[#f0edf7] px-2 py-1 text-xs font-extrabold text-plum">{action.group}</span>
      </div>
      <div>
        <div className="font-extrabold">{action.title}</div>
        <div className="mt-1 text-sm leading-snug text-muted">{action.description}</div>
      </div>
      <div className="flex items-center justify-between gap-3">
        <span className="rounded-full bg-[#f1f4f7] px-2 py-1 text-xs font-extrabold text-muted">{action.marketplace}</span>
        <button className="button-dark" disabled={!action.available || running} onClick={onRun}>
          {running ? <RefreshCw size={15} className="animate-spin" /> : <Play size={15} />}
          <span>{running ? "Запуск..." : "Запустить"}</span>
        </button>
      </div>
    </motion.article>
  );
}

function JobsTable({ items }: { items: JobRun[] }) {
  return (
    <div className="max-h-[420px] overflow-auto">
      <table className="min-w-[860px] w-full border-collapse">
        <thead>
          <tr>
            <TableHead>Job</TableHead>
            <TableHead>Статус</TableHead>
            <TableHead>Старт</TableHead>
            <TableHead>API</TableHead>
            <TableHead>Норм.</TableHead>
            <TableHead>Дубли</TableHead>
          </tr>
        </thead>
        <tbody>
          {items.length ? items.map((job, index) => (
            <tr key={`${job.job_name}-${job.started_at}-${index}`} className="transition hover:bg-slate-50">
              <TableCell><strong>{job.job_name || "-"}</strong></TableCell>
              <TableCell><StatusPill value={job.status} /></TableCell>
              <TableCell>{formatDate(job.started_at)}</TableCell>
              <TableCell>{job.api_rows ?? "-"}</TableCell>
              <TableCell>{job.norm_upserted ?? "-"}</TableCell>
              <TableCell>{job.duplicates ?? "-"}</TableCell>
            </tr>
          )) : <tr><td className="empty-cell" colSpan={6}>Запусков пока нет</td></tr>}
        </tbody>
      </table>
    </div>
  );
}

function SecretsList({ items }: { items: Record<string, boolean> }) {
  const entries = Object.entries(items);
  return (
    <div className="grid gap-2 p-4">
      {entries.length ? entries.map(([name, ok]) => (
        <motion.div key={name} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3 rounded-ui border border-slate-100 px-3 py-2 transition hover:translate-x-0.5 hover:bg-slate-50" whileHover={{ x: 2 }}>
          <span className="truncate text-sm">{name}</span>
          <StatusPill value={ok ? "задан" : "не задан"} />
        </motion.div>
      )) : <Empty text="Статусы не получены" />}
    </div>
  );
}

function OrdersFeed({ items }: { items: OrderRow[] }) {
  const groups = groupOrders(items);
  return (
    <div className="grid max-h-[680px] gap-3 overflow-auto p-4">
      {groups.length ? groups.map((group) => (
        <motion.article
          key={group.key}
          className="rounded-ui border border-line bg-white p-4 transition hover:border-[#b8c7d8] hover:shadow-soft"
          whileHover={{ y: -2 }}
        >
          <div className="mb-3 grid grid-cols-[minmax(0,1fr)_auto] items-start gap-4 max-[820px]:grid-cols-1">
            <div className="min-w-0">
              <div className="mb-2 flex flex-wrap items-center gap-2">
                <span className="rounded-full bg-[#f0edf7] px-2 py-1 text-xs font-extrabold text-plum">{group.marketplace || "-"}</span>
                <StatusPill value={group.statusLabel || group.status} />
              </div>
              <h3 className="truncate text-xl font-black leading-tight">{group.key}</h3>
              <div className="mt-1 text-sm text-muted">{formatDate(group.date)} · {group.warehouseName || "Склад не указан"}</div>
            </div>
            <div className="grid min-w-[170px] gap-1 rounded-ui bg-[#f6f8fa] px-3 py-2 text-right max-[820px]:text-left">
              <span className="text-xs font-bold text-muted">Итого</span>
              <strong>{group.totalQuantity} шт · {formatMoney(group.totalAmount)}</strong>
            </div>
          </div>

          <div className="grid gap-2">
            {group.rows.map((row, index) => (
              <OrderProductRow key={`${row.order_key}-${row.article}-${index}`} row={row} />
            ))}
          </div>
        </motion.article>
      )) : <Empty text="Заказы не найдены" />}
    </div>
  );
}

function OrderProductRow({ row }: { row: OrderRow }) {
  return (
    <div className="grid grid-cols-[58px_minmax(0,1fr)_auto] items-center gap-3 rounded-ui border border-[#edf1f5] bg-[#fbfcfd] p-2 max-[720px]:grid-cols-[58px_minmax(0,1fr)]">
      <ProductImage src={row.image_url} name={row.product_name} />
      <div className="min-w-0">
        <div className="line-clamp-2 font-bold leading-snug">{row.product_name || "Товар без названия"}</div>
        <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted">
          <span>Артикул: <strong className="text-ink">{row.article || "-"}</strong></span>
          <span>Отправление: {row.order_key || "-"}</span>
        </div>
      </div>
      <div className="text-right max-[720px]:col-span-2 max-[720px]:text-left">
        <div className="font-extrabold">{row.quantity ?? "-"} шт</div>
        <div className="text-sm text-muted">{formatMoney(row.price)}</div>
      </div>
    </div>
  );
}

function ProductImage({ src, name }: { src?: string; name?: string }) {
  const [failed, setFailed] = useState(false);
  if (!src || failed) {
    return (
      <div className="grid h-[58px] w-[58px] place-items-center rounded-ui bg-[#edf1f5] text-muted">
        <ImageOff size={20} />
      </div>
    );
  }
  return (
    <img
      src={src}
      alt={name || "Фото товара"}
      className="h-[58px] w-[58px] rounded-ui object-cover"
      loading="lazy"
      onError={() => setFailed(true)}
    />
  );
}

function Segment({ active, children, onClick }: { active: boolean; children: React.ReactNode; onClick: () => void }) {
  return (
    <button className={cls("h-9 min-w-[92px] rounded-md px-4 font-extrabold transition hover:-translate-y-0.5", active ? "bg-primary text-white" : "text-muted hover:bg-slate-50")} onClick={onClick}>
      {children}
    </button>
  );
}

function TableHead({ children }: { children: React.ReactNode }) {
  return <th className="sticky top-0 z-[1] border-b border-slate-100 bg-slate-50 px-3 py-3 text-left text-xs font-extrabold text-muted">{children}</th>;
}

function TableCell({ children }: { children: React.ReactNode }) {
  return <td className="border-b border-slate-100 px-3 py-3 align-top text-sm">{children}</td>;
}

function Empty({ text }: { text: string }) {
  return <div className="col-span-full px-4 py-8 text-center text-muted">{text}</div>;
}

createRoot(document.getElementById("root")!).render(<App />);
