import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { AnimatePresence, motion } from "framer-motion";
import {
  Activity,
  AlertTriangle,
  Check,
  Clipboard,
  Database,
  ImageOff,
  KeyRound,
  LayoutDashboard,
  ListOrdered,
  Lock,
  Play,
  RefreshCw,
  Save,
  Search,
  Server,
  ShieldAlert,
  X,
} from "lucide-react";
import "./styles.css";

type Tab = "admin" | "orders";
type Marketplace = "wb" | "ozon";

type JobRun = {
  job_name?: string;
  action_key?: string | null;
  started_at?: string;
  finished_at?: string | null;
  status?: string;
  api_rows?: number | string | null;
  norm_upserted?: number | string | null;
  duplicates?: number | string | null;
  error?: string | null;
};

type JobFilter = "all" | "bad" | "running" | "ok";

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

type BatchRunResult = {
  title?: string;
  count?: number;
  status?: string;
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
  image_urls?: string[];
  quantity?: number | string;
  price?: number | string;
  payout?: number | string;
  substatus?: string | null;
};

type OrderHistoryRow = {
  order_key?: string;
  changed_at?: string;
  status?: string;
  status_label?: string;
  previous_status?: string;
  previous_status_label?: string;
  substatus?: string;
  previous_substatus?: string;
  warehouse_name?: string;
  previous_warehouse_name?: string;
  cancel_type?: string;
  destination_city?: string;
  status_changed?: boolean;
  substatus_changed?: boolean;
  warehouse_changed?: boolean;
};

type OrderDetail = {
  marketplace?: string;
  key?: string;
  rows?: Array<OrderRow & Record<string, unknown>>;
  history?: OrderHistoryRow[];
  raw_payload?: Record<string, unknown> | null;
};

type OrdersSummary = {
  marketplace?: string;
  orders_count?: number | string;
  articles_count?: number | string;
  quantity?: number | string;
  amount?: number | string;
  cancelled_orders_count?: number | string;
};

type ProductGroup = {
  article: string;
  productName?: string;
  imageUrls: string[];
  rows: OrderRow[];
  totalQuantity: number;
  totalAmount: number;
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

function imageUrlsFromRow(row: OrderRow) {
  const urls: string[] = [];
  if (row.image_url) urls.push(row.image_url);
  for (const url of row.image_urls || []) {
    if (url && !urls.includes(url)) urls.push(url);
  }
  return urls;
}

function copyText(value?: string) {
  const text = String(value || "").trim();
  if (!text) return;
  void navigator.clipboard?.writeText(text);
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

function groupProducts(rows: OrderRow[]): ProductGroup[] {
  const map = new Map<string, ProductGroup>();
  for (const row of rows) {
    const key = row.article || row.product_name || "Без артикула";
    const quantity = asNumber(row.quantity);
    const amount = quantity * asNumber(row.price);
    const current = map.get(key);
    if (current) {
      current.rows.push(row);
      current.totalQuantity += quantity;
      current.totalAmount += amount;
      for (const url of imageUrlsFromRow(row)) {
        if (url && !current.imageUrls.includes(url)) current.imageUrls.push(url);
      }
      continue;
    }
    map.set(key, {
      article: key,
      productName: row.product_name,
      imageUrls: imageUrlsFromRow(row),
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
  const [overview, setOverview] = useState<AdminOverview | null>(null);
  const [actions, setActions] = useState<JobAction[]>([]);
  const [orders, setOrders] = useState<OrderRow[]>([]);
  const [ordersSummary, setOrdersSummary] = useState<OrdersSummary | null>(null);
  const [selectedOrder, setSelectedOrder] = useState<{ marketplace: Marketplace; key: string } | null>(null);
  const [orderDetail, setOrderDetail] = useState<OrderDetail | null>(null);
  const [orderDetailLoading, setOrderDetailLoading] = useState(false);
  const [ordersOffset, setOrdersOffset] = useState(0);
  const [ordersHasMore, setOrdersHasMore] = useState(true);
  const [ordersLoadingMore, setOrdersLoadingMore] = useState(false);
  const [notice, setNotice] = useState("Вставьте API token.");
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState<Set<string>>(new Set());
  const [runningBatch, setRunningBatch] = useState<"failed" | "all" | null>(null);
  const [lastRefresh, setLastRefresh] = useState("-");
  const [toasts, setToasts] = useState<Toast[]>([]);
  const ordersSentinelRef = useRef<HTMLDivElement | null>(null);
  const ordersPageSize = 50;

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

  const loadOrders = useCallback(async ({ reset = false }: { reset?: boolean } = {}) => {
    if (!token) {
      setNotice("Вставьте API token.");
      return;
    }
    const nextOffset = reset ? 0 : ordersOffset;
    if (!reset && (!ordersHasMore || ordersLoadingMore)) return;
    if (reset) {
      setOrders([]);
      setOrdersOffset(0);
      setOrdersHasMore(true);
      setLoading(true);
    } else {
      setOrdersLoadingMore(true);
    }
    try {
      const data = await requestJson<{ items: OrderRow[]; next_offset?: number; has_more?: boolean }>(
        `/api/v1/admin/orders?marketplace=${marketplace}&limit=${ordersPageSize}&offset=${nextOffset}`,
      );
      const items = data.items || [];
      setOrders((current) => (reset ? items : [...current, ...items]));
      setOrdersOffset(data.next_offset ?? nextOffset + ordersPageSize);
      setOrdersHasMore(Boolean(data.has_more && items.length > 0));
      setNotice("");
      setLastRefresh(new Date().toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" }));
    } catch (error) {
      const text = error instanceof Error ? error.message : "Ошибка";
      setNotice(`Лента заказов не загрузилась: ${text}`);
      pushToast("Заказы не загрузились", text, "bad");
    } finally {
      setLoading(false);
      setOrdersLoadingMore(false);
    }
  }, [marketplace, ordersHasMore, ordersLoadingMore, ordersOffset, token]);

  const loadOrdersSummary = useCallback(async () => {
    if (!token) return;
    try {
      const data = await requestJson<OrdersSummary>(`/api/v1/admin/orders/summary?marketplace=${marketplace}`);
      setOrdersSummary(data);
    } catch (error) {
      const text = error instanceof Error ? error.message : "Ошибка";
      pushToast("Итоги дня не загрузились", text, "bad");
    }
  }, [marketplace, token]);

  async function runAction(action: JobAction) {
    setRunning((items) => new Set(items).add(action.key));
    try {
      const result = await requestJson<{ title: string; pid: number }>(
        `/api/v1/admin/actions/${encodeURIComponent(action.key)}/run`,
        { method: "POST" },
      );
      pushToast("Job запущен", `${result.title || action.title}: pid ${result.pid}`);
      [900, 4000, 12000].forEach((delay) => window.setTimeout(loadAdmin, delay));
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

  async function runActionBatch(scope: "failed" | "all") {
    setRunningBatch(scope);
    try {
      const result = await requestJson<BatchRunResult>(
        `/api/v1/admin/actions/batch/${scope}/run`,
        { method: "POST" },
      );
      const count = result.count ?? 0;
      if (count > 0) {
        pushToast(result.title || "Jobs запущены", `В очереди: ${count}`);
      } else {
        pushToast("Запуск не нужен", scope === "failed" ? "Ошибочных jobs за 24 часа нет" : "Доступных jobs нет");
      }
      [900, 4000, 12000, 30000].forEach((delay) => window.setTimeout(loadAdmin, delay));
    } catch (error) {
      pushToast("Jobs не запущены", error instanceof Error ? error.message : "Ошибка", "bad");
    } finally {
      setRunningBatch(null);
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
    if (tab === "orders") {
      void loadOrders({ reset: true });
      void loadOrdersSummary();
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, marketplace, token]);

  useEffect(() => {
    if (tab !== "orders") return undefined;
    const node = ordersSentinelRef.current;
    if (!node) return undefined;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        void loadOrders();
      }
    }, { rootMargin: "360px" });
    observer.observe(node);
    return () => observer.disconnect();
  }, [loadOrders, tab]);

  useEffect(() => {
    if (tab !== "admin") return undefined;
    const hasRunningJobs = (overview?.jobs || []).some((job) => String(job.status || "").toLowerCase() === "running");
    if (!hasRunningJobs) return undefined;
    const interval = window.setInterval(() => void loadAdmin(), 5000);
    return () => window.clearInterval(interval);
  }, [overview?.jobs, tab]);

  const failedJobs = overview?.alerts?.failed_jobs || 0;
  const missingSecrets = overview?.alerts?.missing_required_secrets || [];
  const orderStats = {
    groups: asNumber(ordersSummary?.orders_count),
    articles: asNumber(ordersSummary?.articles_count),
    quantity: asNumber(ordersSummary?.quantity),
    amount: asNumber(ordersSummary?.amount),
    cancelled: asNumber(ordersSummary?.cancelled_orders_count),
  };

  async function openOrderDetail(target: { marketplace: Marketplace; key: string }) {
    setSelectedOrder(target);
    setOrderDetailLoading(true);
    setOrderDetail(null);
    try {
      const data = await requestJson<OrderDetail>(
        `/api/v1/admin/orders/detail?marketplace=${target.marketplace}&key=${encodeURIComponent(target.key)}`,
      );
      setOrderDetail(data);
    } catch (error) {
      pushToast("Заказ не загрузился", error instanceof Error ? error.message : "Ошибка", "bad");
    } finally {
      setOrderDetailLoading(false);
    }
  }

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

                <Panel
                  title="Запуск jobs"
                  subtitle="Доступные команды"
                  action={(
                    <MasterRunButton
                      running={runningBatch !== null}
                      onFailed={() => void runActionBatch("failed")}
                      onAll={() => void runActionBatch("all")}
                    />
                  )}
                >
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

                <Panel title="Последние запуски" subtitle="За последние 24 часа">
                  <JobsTable
                    items={overview?.jobs || []}
                    onRunAction={(key) => {
                      const action = actions.find((item) => item.key === key);
                      if (action) void runAction(action);
                    }}
                  />
                </Panel>

                <Panel title="Секреты" subtitle="Только статус">
                  <SecretsList items={overview?.secrets || {}} />
                </Panel>
              </motion.section>
            ) : (
              <motion.section key="orders" variants={variants} initial="hidden" animate="show" exit="hidden">
                <div className="mb-4 flex items-center justify-between gap-4 max-[820px]:grid">
                  <div className="inline-flex rounded-ui border border-line bg-white p-1">
                    <Segment active={marketplace === "wb"} onClick={() => setMarketplace("wb")}>WB</Segment>
                    <Segment active={marketplace === "ozon"} onClick={() => setMarketplace("ozon")}>Ozon</Segment>
                  </div>
                  <span className="rounded-full bg-white px-3 py-2 text-xs font-extrabold text-muted shadow-soft">
                    Загружено: {groupOrders(orders).length} заказов
                  </span>
                </div>
                <div className="mb-4 grid grid-cols-4 gap-3 max-[980px]:grid-cols-2 max-[560px]:grid-cols-1">
                  <MiniStat label="Заказов сегодня" value={orderStats.groups} />
                  <MiniStat label="Артикулов сегодня" value={orderStats.articles} />
                  <MiniStat label="Штук сегодня" value={orderStats.quantity} />
                  <MiniStat label="Сумма сегодня" value={formatMoney(orderStats.amount)} />
                </div>
                <Panel title="Лента заказов" subtitle={`Сгруппировано по заказам ${marketplace.toUpperCase()}`} action={<span className="grid h-8 min-w-10 place-items-center rounded-full bg-[#edf4fb] px-3 text-sm font-extrabold text-primary">{groupOrders(orders).length}</span>}>
                  <OrdersFeed
                    marketplace={marketplace}
                    items={orders}
                    hasMore={ordersHasMore}
                    loadingMore={ordersLoadingMore}
                    sentinelRef={ordersSentinelRef}
                    onOpenOrder={(key) => void openOrderDetail({ marketplace, key })}
                  />
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

      <OrderDetailModal
        selected={selectedOrder}
        detail={orderDetail}
        loading={orderDetailLoading}
        onClose={() => {
          setSelectedOrder(null);
          setOrderDetail(null);
        }}
      />
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

function MiniStat({ label, value }: { label: string; value: string | number }) {
  return (
    <motion.div className="card px-4 py-3" whileHover={{ y: -2 }}>
      <div className="text-xs font-extrabold uppercase text-muted">{label}</div>
      <div className="mt-1 text-xl font-black">{value}</div>
    </motion.div>
  );
}

function Panel({ title, subtitle, action, children }: { title: string; subtitle: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <motion.section className="card mb-5" whileHover={{ y: -2 }}>
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

function MasterRunButton({
  running,
  onFailed,
  onAll,
}: {
  running: boolean;
  onFailed: () => void;
  onAll: () => void;
}) {
  return (
    <div className="group relative">
      <button
        className="grid h-10 w-10 place-items-center rounded-ui bg-slate-100 text-muted transition hover:-translate-y-0.5 hover:bg-white hover:shadow-soft disabled:cursor-wait disabled:opacity-70"
        onClick={onFailed}
        disabled={running}
        title="Перезапустить ошибочные jobs"
      >
        <RefreshCw size={17} className={running ? "animate-spin" : ""} />
      </button>
      <button
        className="absolute right-0 top-10 z-20 hidden whitespace-nowrap rounded-ui bg-[#1f2933] px-3 py-2 text-xs font-extrabold text-white shadow-panel transition hover:bg-[#2d3a46] group-hover:block disabled:cursor-wait disabled:opacity-70"
        onClick={onAll}
        disabled={running}
      >
        Обновить всё
      </button>
    </div>
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

function JobsTable({ items, onRunAction }: { items: JobRun[]; onRunAction: (key: string) => void }) {
  const [filter, setFilter] = useState<JobFilter>("all");
  const [query, setQuery] = useState("");
  const filteredItems = items.filter((job) => {
    const status = String(job.status || "").toLowerCase();
    const isBad = !["ok", "running"].includes(status);
    const matchesFilter =
      filter === "all" ||
      (filter === "bad" && isBad) ||
      (filter === "running" && status === "running") ||
      (filter === "ok" && status === "ok");
    const text = `${job.job_name || ""} ${job.status || ""} ${job.error || ""}`.toLowerCase();
    return matchesFilter && text.includes(query.trim().toLowerCase());
  });
  return (
    <div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 p-3">
        <div className="inline-flex rounded-ui border border-line bg-white p-1">
          {(["all", "bad", "running", "ok"] as JobFilter[]).map((item) => (
            <button
              key={item}
              className={cls(
                "h-8 rounded-md px-3 text-xs font-extrabold transition hover:bg-slate-50",
                filter === item ? "bg-primary text-white hover:bg-primary" : "text-muted",
              )}
              onClick={() => setFilter(item)}
            >
              {item === "all" ? "Все" : item === "bad" ? "Ошибки" : item === "running" ? "В работе" : "OK"}
            </button>
          ))}
        </div>
        <label className="grid h-9 min-w-[240px] grid-cols-[18px_minmax(0,1fr)] items-center gap-2 rounded-ui border border-line bg-white px-3 text-muted">
          <Search size={16} />
          <input
            className="min-w-0 bg-transparent text-sm text-ink outline-none"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Найти job или ошибку"
          />
        </label>
      </div>
      <div className="max-h-[min(680px,62vh)] overflow-auto">
        <table className="min-w-[980px] w-full border-collapse">
          <thead>
            <tr>
              <TableHead>Job</TableHead>
              <TableHead>Статус</TableHead>
              <TableHead>Старт</TableHead>
              <TableHead>Финиш</TableHead>
              <TableHead>API</TableHead>
              <TableHead>Норм.</TableHead>
              <TableHead>Дубли</TableHead>
              <TableHead> </TableHead>
            </tr>
          </thead>
          <tbody>
            {filteredItems.length ? filteredItems.map((job, index) => (
              <React.Fragment key={`${job.job_name}-${job.started_at}-${index}`}>
                <tr className="transition hover:bg-slate-50">
                  <TableCell><strong>{job.job_name || "-"}</strong></TableCell>
                  <TableCell><StatusPill value={job.status} /></TableCell>
                  <TableCell>{formatDate(job.started_at)}</TableCell>
                  <TableCell>{formatDate(job.finished_at || undefined)}</TableCell>
                  <TableCell>{job.api_rows ?? "-"}</TableCell>
                  <TableCell>{job.norm_upserted ?? "-"}</TableCell>
                  <TableCell>{job.duplicates ?? "-"}</TableCell>
                  <TableCell>
                    {job.action_key ? (
                      <button
                        className="inline-flex h-8 items-center gap-1 rounded-md border border-[#dce4eb] bg-white px-2 text-xs font-bold text-muted transition hover:border-primary hover:text-primary"
                        onClick={() => onRunAction(String(job.action_key))}
                      >
                        <Play size={13} />
                        <span>Запуск</span>
                      </button>
                    ) : null}
                  </TableCell>
                </tr>
                {job.error ? (
                  <tr>
                    <td className="border-b border-slate-100 bg-[#fff8e6] px-3 py-2 text-sm text-[#7a4f12]" colSpan={8}>
                      <strong>Ошибка:</strong> {job.error}
                    </td>
                  </tr>
                ) : null}
              </React.Fragment>
            )) : <tr><td className="empty-cell" colSpan={8}>Запусков по фильтру нет</td></tr>}
          </tbody>
        </table>
      </div>
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

function OrdersFeed({
  marketplace,
  items,
  hasMore,
  loadingMore,
  sentinelRef,
  onOpenOrder,
}: {
  marketplace: Marketplace;
  items: OrderRow[];
  hasMore: boolean;
  loadingMore: boolean;
  sentinelRef: React.RefObject<HTMLDivElement | null>;
  onOpenOrder: (key: string) => void;
}) {
  const groups = groupOrders(items);
  return (
    <div className="grid gap-3 p-4">
      {groups.length ? (
        <>
          {groups.map((group) => (
            <motion.article
              key={group.key}
              className="relative z-0 rounded-ui border border-line bg-white p-4 transition hover:z-30 hover:border-[#b8c7d8] hover:shadow-soft"
              whileHover={{ y: -2 }}
            >
              <div className="mb-3 grid grid-cols-[minmax(0,1fr)_auto] items-start gap-4 max-[820px]:grid-cols-1">
                <div className="min-w-0">
                  <div className="mb-2 flex flex-wrap items-center gap-2">
                    <span className="rounded-full bg-[#f0edf7] px-2 py-1 text-xs font-extrabold text-plum">{group.marketplace || "-"}</span>
                    <StatusPill value={group.statusLabel || group.status} />
                  </div>
                  <h3 className="break-words text-xl font-black leading-tight">{group.key}</h3>
                  <div className="mt-1 text-sm text-muted">{formatDate(group.date)} · {group.warehouseName || "Склад не указан"}</div>
                </div>
                <div className="grid min-w-[170px] gap-1 rounded-ui bg-[#f6f8fa] px-3 py-2 text-right max-[820px]:text-left">
                  <span className="text-xs font-bold text-muted">Итого</span>
                  <strong>{group.totalQuantity} шт · {formatMoney(group.totalAmount)}</strong>
                </div>
              </div>

              <div className="grid gap-2">
                {groupProducts(group.rows).map((product) => (
                  <OrderProductRow key={product.article} product={product} marketplace={marketplace} onOpenOrder={onOpenOrder} />
                ))}
              </div>
            </motion.article>
          ))}
          <div ref={sentinelRef} className="py-4 text-center text-sm text-muted">
            {loadingMore ? "Загружаю ещё заказы..." : hasMore ? "Прокрутите ниже, чтобы загрузить ещё" : "Все видимые заказы загружены"}
          </div>
        </>
      ) : <Empty text="Заказы не найдены" />}
    </div>
  );
}

function OrderProductRow({
  product,
  marketplace,
  onOpenOrder,
}: {
  product: ProductGroup;
  marketplace: Marketplace;
  onOpenOrder: (key: string) => void;
}) {
  const firstOrderKey = product.rows.find((row) => row.order_group_key || row.order_number || row.order_key);
  const orderLabel = firstOrderKey?.order_group_key || firstOrderKey?.order_number || firstOrderKey?.order_key;
  return (
    <div className="relative z-0 grid min-h-[132px] grid-cols-[104px_minmax(0,1fr)_auto] items-stretch gap-3 rounded-ui border border-[#edf1f5] bg-[#fbfcfd] p-3 transition hover:z-[120] max-[720px]:grid-cols-[104px_minmax(0,1fr)]">
      <ProductImage urls={product.imageUrls} name={product.productName} />
      <div className="min-w-0">
        <div className="line-clamp-2 font-bold leading-snug" title={product.productName || ""}>
          {product.productName || "Товар без названия"}
        </div>
        <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted">
          <button className="transition hover:text-primary" title="Скопировать артикул" onClick={() => copyText(product.article)}>
            Артикул: <strong className="text-ink">{product.article || "Без артикула"}</strong>
          </button>
          {orderLabel ? (
            <button className="transition hover:text-primary" title="Скопировать номер заказа" onClick={() => copyText(orderLabel)}>
              Заказ: <strong className="text-ink">{orderLabel}</strong>
            </button>
          ) : null}
          <span>Строк: {product.rows.length}</span>
        </div>
        <CopyChips rows={product.rows} onOpenOrder={onOpenOrder} />
      </div>
      <div className="text-right max-[720px]:col-span-2 max-[720px]:text-left">
        <div className="font-extrabold">{product.totalQuantity} шт</div>
        <div className="text-sm text-muted">{formatMoney(product.totalAmount)}</div>
      </div>
    </div>
  );
}

function CopyChips({
  rows,
  onOpenOrder,
}: {
  rows: OrderRow[];
  onOpenOrder: (key: string) => void;
}) {
  const uniqueKeys = Array.from(new Set(rows.map((row) => row.order_key).filter(Boolean))) as string[];
  if (!uniqueKeys.length) return null;
  return (
    <div className="mt-2 flex flex-wrap gap-1.5">
      {uniqueKeys.map((key) => (
        <span key={key} className="inline-flex max-w-full overflow-hidden rounded-md border border-[#dce4eb] bg-white text-xs font-bold text-muted transition hover:border-primary">
          <button
            className="min-w-0 px-2 py-1 transition hover:text-primary"
            title="Открыть детали заказа"
            onClick={() => onOpenOrder(key)}
          >
            <span className="truncate">{key}</span>
          </button>
          <button
            className="grid w-7 place-items-center border-l border-[#dce4eb] transition hover:text-primary"
            title="Скопировать"
            onClick={() => copyText(key)}
          >
            <Clipboard size={12} />
          </button>
        </span>
      ))}
    </div>
  );
}

function ProductImage({ urls, name }: { urls: string[]; name?: string }) {
  const [index, setIndex] = useState(0);
  const [attempt, setAttempt] = useState(0);
  const src = urls[index];
  const retrySrc = src ? `${src}${src.includes("?") ? "&" : "?"}r=${attempt}` : "";
  useEffect(() => {
    setIndex(0);
    setAttempt(0);
  }, [urls.join("|")]);

  function handleImageError() {
    if (attempt < 2) {
      window.setTimeout(() => setAttempt((current) => current + 1), 250 * (attempt + 1));
      return;
    }
    setAttempt(0);
    setIndex((current) => current + 1);
  }

  if (!src) {
    return (
      <div className="grid h-full min-h-[108px] w-[104px] place-items-center rounded-ui bg-[#edf1f5] text-muted">
        <ImageOff size={20} />
      </div>
    );
  }
  return (
    <div className="group relative h-full min-h-[108px] w-[104px]">
      <img
        src={retrySrc}
        alt={name || "Фото товара"}
        className="h-full min-h-[108px] w-[104px] rounded-ui border border-[#e8eef4] bg-white object-contain p-2"
        loading="lazy"
        onError={handleImageError}
      />
      <div className="pointer-events-none absolute left-[116px] top-0 z-[999] hidden h-[344px] w-[258px] rounded-ui border border-[#d9e0e7] bg-white p-2 shadow-panel group-hover:block max-[720px]:left-0 max-[720px]:top-[118px]">
        <img
          src={retrySrc}
          alt={name || "Фото товара"}
          className="h-full w-full rounded-md object-cover"
        />
      </div>
    </div>
  );
}

function OrderDetailModal({
  selected,
  detail,
  loading,
  onClose,
}: {
  selected: { marketplace: Marketplace; key: string } | null;
  detail: OrderDetail | null;
  loading: boolean;
  onClose: () => void;
}) {
  if (!selected) return null;
  const rows = detail?.rows || [];
  const first = rows[0] || {};
  const history = detail?.history || [];
  return (
    <div className="fixed inset-0 z-[200] grid place-items-center bg-[#0f1720]/45 p-5 backdrop-blur-sm" onClick={onClose}>
      <motion.section
        className="max-h-[92vh] w-[min(980px,100%)] overflow-hidden rounded-ui bg-white shadow-panel"
        initial={{ opacity: 0, scale: 0.98, y: 12 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-100 px-5 py-4">
          <div className="min-w-0">
            <div className="mb-1 text-xs font-extrabold uppercase text-primary">{selected.marketplace.toUpperCase()}</div>
            <h2 className="break-words text-2xl font-black leading-tight">{selected.key}</h2>
            <div className="mt-1 text-sm text-muted">
              {String(first.product_name || "Заказ")} · {String(first.warehouse_name || first.analytics_warehouse_name || "Склад не указан")}
            </div>
          </div>
          <button className="grid h-10 w-10 place-items-center rounded-ui bg-slate-100 text-muted transition hover:bg-white hover:shadow-soft" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="max-h-[calc(92vh-86px)] overflow-auto p-5">
          {loading ? (
            <div className="grid min-h-[220px] place-items-center text-muted">
              <RefreshCw size={22} className="animate-spin" />
            </div>
          ) : (
            <div className="grid gap-5">
              <div className="grid grid-cols-4 gap-3 max-[880px]:grid-cols-2 max-[560px]:grid-cols-1">
                <MiniStat label="Строк" value={rows.length} />
                <MiniStat label="Статус" value={String(first.status_label || first.status || "-")} />
                <MiniStat label="Кол-во" value={rows.reduce((sum, row) => sum + asNumber(row.quantity), 0)} />
                <MiniStat label="Сумма" value={formatMoney(rows.reduce((sum, row) => sum + asNumber(row.quantity) * asNumber(row.price), 0))} />
              </div>

              <div className="grid grid-cols-2 gap-4 max-[880px]:grid-cols-1">
                <DetailPanel title="Отгрузка и доставка">
                  <DetailLine label="Склад" value={first.warehouse_name || first.analytics_warehouse_name} />
                  <DetailLine label="Тип доставки" value={first.analytics_delivery_type || first.warehouse_type} />
                  <DetailLine label="Город" value={first.analytics_city || first.region_name} />
                  <DetailLine label="Кластер из" value={first.financial_cluster_from} />
                  <DetailLine label="Кластер в" value={first.financial_cluster_to} />
                </DetailPanel>
                <DetailPanel title="Отмена">
                  <DetailLine label="Причина" value={first.cancel_reason || first.cancel_type || first.cancellation_type} />
                  <DetailLine label="Инициатор" value={first.cancellation_initiator} />
                  <DetailLine label="Дата отмены" value={formatDate(String(first.cancel_date || ""))} />
                  <DetailLine label="Подстатус" value={first.substatus} />
                </DetailPanel>
              </div>

              <DetailPanel title="Товары">
                <div className="overflow-auto">
                  <table className="min-w-[760px] w-full border-collapse">
                    <thead>
                      <tr>
                        <TableHead>Артикул</TableHead>
                        <TableHead>Название</TableHead>
                        <TableHead>SKU/NM</TableHead>
                        <TableHead>Кол-во</TableHead>
                        <TableHead>Цена</TableHead>
                        <TableHead>Выплата</TableHead>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row, index) => (
                        <tr key={`${row.order_key}-${row.article}-${index}`} className="transition hover:bg-slate-50">
                          <TableCell><strong>{String(row.article || "-")}</strong></TableCell>
                          <TableCell>{String(row.product_name || "-")}</TableCell>
                          <TableCell>{String(row.marketplace_sku || "-")}</TableCell>
                          <TableCell>{row.quantity ?? "-"}</TableCell>
                          <TableCell>{formatMoney(row.price)}</TableCell>
                          <TableCell>{formatMoney(row.payout)}</TableCell>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </DetailPanel>

              <DetailPanel title="История">
                {history.length ? (
                  <div className="grid gap-2">
                    {history.map((item, index) => (
                      <div key={`${item.order_key}-${item.changed_at}-${index}`} className="rounded-ui border border-slate-100 bg-[#fbfcfd] px-3 py-2">
                        <div className="flex flex-wrap items-center gap-2">
                          <StatusPill value={item.status_label || item.status} />
                          <span className="text-sm text-muted">{formatDate(item.changed_at)}</span>
                        </div>
                        <div className="mt-1 text-sm text-muted">
                          {item.previous_status || item.previous_status_label ? `Было: ${item.previous_status_label || item.previous_status}. ` : ""}
                          {item.warehouse_name ? `Склад: ${item.warehouse_name}. ` : ""}
                          {item.destination_city ? `Город: ${item.destination_city}. ` : ""}
                          {item.cancel_type ? `Отмена: ${item.cancel_type}.` : ""}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-muted">История изменений пока не найдена.</div>
                )}
              </DetailPanel>
            </div>
          )}
        </div>
      </motion.section>
    </div>
  );
}

function DetailPanel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-ui border border-slate-100 bg-white p-4">
      <h3 className="mb-3 text-sm font-extrabold uppercase text-muted">{title}</h3>
      {children}
    </section>
  );
}

function DetailLine({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="grid grid-cols-[150px_minmax(0,1fr)] gap-3 border-b border-slate-100 py-2 text-sm last:border-b-0">
      <span className="font-bold text-muted">{label}</span>
      <span className="min-w-0 break-words">{value ? String(value) : "-"}</span>
    </div>
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
