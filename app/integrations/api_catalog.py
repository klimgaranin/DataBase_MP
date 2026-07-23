from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent.parent
MP_GAS_SHARED = ROOT.parent / "mp-gas" / "shared"


@dataclass(frozen=True)
class ApiMethodSpec:
    marketplace: str
    name: str
    method: str
    path: str
    base_url: str
    auth_secret_names: tuple[str, ...]
    spec_path: Path
    operation_id: str | None = None
    notes: str = ""

    @property
    def url(self) -> str:
        return self.base_url.rstrip("/") + self.path


API_METHODS: tuple[ApiMethodSpec, ...] = (
    ApiMethodSpec(
        marketplace="wb",
        name="wb_analytics_stocks_by_warehouse",
        method="POST",
        base_url="https://seller-analytics-api.wildberries.ru",
        path="/api/analytics/v1/stocks-report/wb-warehouses",
        operation_id="postV1StocksReportWbWarehouses",
        auth_secret_names=("WB_TOKEN", "WB_ANALYTICS_TOKEN"),
        spec_path=MP_GAS_SHARED / "wb-11-analytics.yaml",
        notes="Источник остатков WB по складам для аналитики и ЧЗ.",
    ),
    ApiMethodSpec(
        marketplace="wb",
        name="wb_supplies_list",
        method="POST",
        base_url="https://supplies-api.wildberries.ru",
        path="/api/v1/supplies",
        auth_secret_names=("WB_SUPPLIES_TOKEN", "WB_TOKEN"),
        spec_path=MP_GAS_SHARED / "wb-07-orders-fbw.yaml",
        notes="Список FBW-поставок WB; в mp-gas используется для ЧЗ и товаров в пути.",
    ),
    ApiMethodSpec(
        marketplace="wb",
        name="wb_adv_fullstats",
        method="GET",
        base_url="https://advert-api.wildberries.ru",
        path="/adv/v3/fullstats",
        auth_secret_names=("WB_TOKEN_CONTENT",),
        spec_path=MP_GAS_SHARED / "wb-08-promotion.yaml",
        notes="Расходы рекламы WB; текущий fullstats-модуль пока не рефакторится.",
    ),
    ApiMethodSpec(
        marketplace="ozon",
        name="ozon_analytics_stocks",
        method="POST",
        base_url="https://api-seller.ozon.ru",
        path="/v1/analytics/stocks",
        operation_id="AnalyticsAPI_AnalyticsStocks",
        auth_secret_names=("OZON_CLIENT_ID", "OZON_API_KEY"),
        spec_path=MP_GAS_SHARED / "ozon-seller-swagger.json",
        notes="Остатки Ozon; товары в пути считаются из requested/transit/returns.",
    ),
    ApiMethodSpec(
        marketplace="ozon",
        name="ozon_supply_order_list",
        method="POST",
        base_url="https://api-seller.ozon.ru",
        path="/v3/supply-order/list",
        operation_id="SupplyOrderList",
        auth_secret_names=("OZON_CLIENT_ID", "OZON_API_KEY"),
        spec_path=MP_GAS_SHARED / "ozon-seller-swagger.json",
        notes="Список заявок FBO Ozon для ЧЗ.",
    ),
    ApiMethodSpec(
        marketplace="ozon",
        name="ozon_supply_order_bundle",
        method="POST",
        base_url="https://api-seller.ozon.ru",
        path="/v1/supply-order/bundle",
        operation_id="SupplyOrderBundle",
        auth_secret_names=("OZON_CLIENT_ID", "OZON_API_KEY"),
        spec_path=MP_GAS_SHARED / "ozon-seller-swagger.json",
        notes="Состав поставки Ozon.",
    ),
)


def methods_by_marketplace(marketplace: str) -> list[ApiMethodSpec]:
    key = marketplace.lower().strip()
    return [method for method in API_METHODS if method.marketplace == key]


def missing_specs() -> list[ApiMethodSpec]:
    return [method for method in API_METHODS if not method.spec_path.exists()]
