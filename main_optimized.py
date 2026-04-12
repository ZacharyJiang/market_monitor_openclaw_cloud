"""
ETF NEXUS backend (Tencent Cloud oriented)
- Real market data only (Eastmoney primary + Sina fallback)
- No mock/demo auto generation
- Disk cache fallback
- Conservative rate limiting + circuit breaker
"""

import json
import logging
import math
import os
import random
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import hmac
import hashlib
import subprocess

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

BEIJING_TZ = timezone(timedelta(hours=8))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etf-nexus")


# ============================================================
# ENV / CONFIG
# ============================================================

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default


REFRESH_MINUTES = _env_int("REFRESH_MINUTES", 2)
KLINE_REFRESH_MINUTES = _env_int("KLINE_REFRESH_MINUTES", 180)
KLINE_BATCH_SIZE = max(1, _env_int("KLINE_BATCH_SIZE", 2))
# KLINE_TOP_N: 控制每批 K 线刷新处理的 ETF 数量
# 0 或负数表示处理全部 ETF（按规模降序）
# 正数表示只处理前 N 只规模较大的 ETF
KLINE_TOP_N = _env_int("KLINE_TOP_N", 0)
FORCE_REFRESH = _env_bool("FORCE_REFRESH", False)

REQUEST_TIMEOUT_SECONDS = _env_float("REQUEST_TIMEOUT_SECONDS", 12.0)
API_BASE_INTERVAL = _env_float("API_BASE_INTERVAL", 1.2)
API_MAX_INTERVAL = _env_float("API_MAX_INTERVAL", 8.0)
SECONDARY_API_INTERVAL = _env_float("SECONDARY_API_INTERVAL", 0.45)
CIRCUIT_BREAKER_THRESHOLD = _env_int("CIRCUIT_BREAKER_THRESHOLD", 4)
CIRCUIT_BREAKER_COOLDOWN = _env_int("CIRCUIT_BREAKER_COOLDOWN", 180)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
SPOT_CACHE = DATA_DIR / "spot_cache.json"
FEE_CACHE_FILE = DATA_DIR / "fee_cache.json"
KLINE_DIR = DATA_DIR / "kline"
KLINE_DIR.mkdir(exist_ok=True)


# ============================================================
# HTTP SESSION
# ============================================================

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://quote.eastmoney.com/",
        "Connection": "close",
    }
)

_PROXY = (
    os.environ.get("MARKET_PROXY")
    or os.environ.get("AKSHARE_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or os.environ.get("HTTP_PROXY")
)
if _PROXY:
    SESSION.proxies.update({"http": _PROXY, "https": _PROXY})
    logger.info("Using proxy for market requests")
else:
    logger.info("No proxy configured — direct market requests")


# ============================================================
# GLOBAL STORE
# ============================================================

etf_spot: Dict[str, Dict] = {}
etf_stats: Dict[str, Dict] = {}
market_indices: List[Dict] = []
last_updated: Optional[str] = None
# live | cache | degraded
# degraded means live fetch and cache are both unavailable
data_source = "degraded"
# eastmoney | sina | cache | none
live_provider = "none"

# key: ETF code, value: date string yyyy-mm-dd
_last_kline_update: Dict[str, str] = {}

# fee cache format: {code: {"管理费": 0.5, "托管费": 0.1}}
_fee_cache: Dict[str, Dict[str, float]] = {}

_lock = threading.RLock()
_secondary_lock = threading.Lock()
_secondary_last_request_at = 0.0


# ============================================================
# RATE LIMITER (Eastmoney)
# ============================================================


class RequestController:
    def __init__(self) -> None:
        self.min_interval = max(0.2, API_BASE_INTERVAL)
        self.max_interval = max(self.min_interval, API_MAX_INTERVAL)
        self.current_interval = self.min_interval

        self.failure_streak = 0
        self.breaker_until = 0.0
        self.last_request_at = 0.0

        self._lock = threading.Lock()

    def wait_for_slot(self) -> None:
        with self._lock:
            now = time.time()
            if now < self.breaker_until:
                raise RuntimeError(
                    f"circuit_open:{round(self.breaker_until - now, 2)}s"
                )

            wait_time = self.current_interval - (now - self.last_request_at)
            if wait_time < 0:
                wait_time = 0
            self.last_request_at = now + wait_time

        if wait_time > 0:
            time.sleep(wait_time + random.uniform(0.02, 0.18))

    def record_success(self) -> None:
        with self._lock:
            self.failure_streak = 0
            self.current_interval = max(
                self.min_interval,
                self.current_interval * 0.9,
            )

    def record_failure(self) -> None:
        with self._lock:
            self.failure_streak += 1
            self.current_interval = min(
                self.max_interval,
                self.current_interval * 1.45,
            )
            if self.failure_streak >= CIRCUIT_BREAKER_THRESHOLD:
                self.breaker_until = time.time() + CIRCUIT_BREAKER_COOLDOWN
                logger.warning(
                    "Circuit breaker opened for %ss (failure streak=%s)",
                    CIRCUIT_BREAKER_COOLDOWN,
                    self.failure_streak,
                )

    def status(self) -> Dict:
        now = time.time()
        with self._lock:
            return {
                "state": "open" if now < self.breaker_until else "closed",
                "remaining": max(0, round(self.breaker_until - now, 2)),
                "interval": round(self.current_interval, 2),
                "failure_streak": self.failure_streak,
            }


request_controller = RequestController()


# ============================================================
# COMMON HELPERS
# ============================================================


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        raw = str(value).strip()
        if raw in {"", "-", "None", "nan"}:
            return default
        return float(raw)
    except Exception:
        return default


def _now_bj_str() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _today_bj_str() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")


def _mask_proxy(value: Optional[str]) -> str:
    if not value:
        return "none"
    if len(value) <= 12:
        return "***"
    return value[:8] + "***"


def _wait_secondary_slot() -> None:
    """
    Shared pacing for fallback providers (Sina/Tencent) to reduce anti-bot risk.
    """
    global _secondary_last_request_at

    interval = max(0.1, SECONDARY_API_INTERVAL)
    with _secondary_lock:
        now = time.time()
        wait_time = interval - (now - _secondary_last_request_at)
        if wait_time < 0:
            wait_time = 0
        _secondary_last_request_at = now + wait_time

    if wait_time > 0:
        time.sleep(wait_time + random.uniform(0.02, 0.12))


# ============================================================
# HTTP HELPERS
# ============================================================


def _request_json(url: str, params: Dict, retries: int = 2) -> Dict:
    """Eastmoney requests with shared rate limiting/circuit breaker."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            request_controller.wait_for_slot()
            resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, dict) and payload.get("rc") not in (None, 0):
                raise RuntimeError(f"remote_rc={payload.get('rc')}")
            request_controller.record_success()
            return payload
        except RuntimeError as exc:
            # circuit_open: do not keep increasing failure streak in a tight loop
            if str(exc).startswith("circuit_open"):
                last_exc = exc
                break
            last_exc = exc
            request_controller.record_failure()
        except Exception as exc:
            last_exc = exc
            request_controller.record_failure()

        if attempt < retries:
            cooldown = min(API_MAX_INTERVAL, request_controller.current_interval)
            time.sleep(cooldown + random.uniform(0.15, 0.55))

    raise last_exc if last_exc else RuntimeError("request failed")


def _request_text(
    url: str,
    params: Optional[Dict] = None,
    retries: int = 2,
    headers: Optional[Dict] = None,
) -> str:
    """Generic text GET request with shared rate limiting/circuit breaker."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            request_controller.wait_for_slot()
            resp = SESSION.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers=headers,
            )
            resp.raise_for_status()
            request_controller.record_success()
            return resp.text
        except RuntimeError as exc:
            if str(exc).startswith("circuit_open"):
                last_exc = exc
                break
            last_exc = exc
            request_controller.record_failure()
        except Exception as exc:
            last_exc = exc
            request_controller.record_failure()

        if attempt < retries:
            cooldown = min(API_MAX_INTERVAL, request_controller.current_interval)
            time.sleep(cooldown + random.uniform(0.15, 0.55))

    raise last_exc if last_exc else RuntimeError("text request failed")


def _request_text_sina(
    url: str,
    params: Optional[Dict] = None,
    retries: int = 2,
    headers: Optional[Dict] = None,
) -> str:
    """Sina requests with lightweight retry (independent from Eastmoney breaker)."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            _wait_secondary_slot()
            resp = SESSION.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers=headers,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep((attempt + 1) * 0.6 + random.uniform(0.1, 0.35))
    raise last_exc if last_exc else RuntimeError("sina request failed")


def _request_json_external(
    url: str,
    params: Optional[Dict] = None,
    retries: int = 2,
    headers: Optional[Dict] = None,
) -> Dict:
    """
    Generic lightweight JSON GET retry.
    Used for non-Eastmoney fallback providers (no shared circuit breaker).
    """
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            _wait_secondary_slot()
            resp = SESSION.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep((attempt + 1) * 0.6 + random.uniform(0.1, 0.35))
    raise last_exc if last_exc else RuntimeError("external json request failed")


# ============================================================
# CACHE
# ============================================================


def _load_fee_cache() -> None:
    global _fee_cache
    if not FEE_CACHE_FILE.exists():
        _fee_cache = {}
        return
    try:
        data = json.loads(FEE_CACHE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _fee_cache = data
            logger.info("Fee cache loaded: %s ETFs", len(_fee_cache))
        else:
            _fee_cache = {}
    except Exception:
        _fee_cache = {}


def _normalize_fee_detail(detail: Dict) -> Dict[str, float]:
    clean = {}
    if not isinstance(detail, dict):
        return clean
    for k, v in detail.items():
        fv = _safe_float(v, default=-1)
        if fv >= 0:
            clean[str(k)] = round(fv, 2)
    return clean


def _get_fee_detail(code: str) -> Dict[str, float]:
    detail = _fee_cache.get(code, {})
    return _normalize_fee_detail(detail)


def _format_fee_detail(detail: Dict[str, float]) -> str:
    if not detail:
        return ""
    return ", ".join(f"{k}{v:.2f}%" for k, v in detail.items())


def save_spot_cache() -> None:
    with _lock:
        payload = {
            "spot": etf_spot,
            "stats": etf_stats,
            "indices": market_indices,
            "updated": last_updated,
            "source": data_source,
            "provider": live_provider,
            "last_kline_update": _last_kline_update,
        }
    try:
        SPOT_CACHE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.error("Save spot cache failed: %s", exc)


def load_spot_cache() -> bool:
    global etf_spot, etf_stats, market_indices, last_updated, data_source, live_provider, _last_kline_update
    if not SPOT_CACHE.exists():
        return False
    try:
        data = json.loads(SPOT_CACHE.read_text(encoding="utf-8"))
        raw_source = str(data.get("source", "")).lower().strip()
        if raw_source in {"mock", "demo"}:
            logger.warning("Ignore legacy mock cache")
            return False

        etf_spot = data.get("spot", {}) or {}
        etf_stats = data.get("stats", {}) or {}
        market_indices = data.get("indices", []) or []
        last_updated = data.get("updated")
        _last_kline_update = data.get("last_kline_update", {}) or {}

        if etf_spot:
            data_source = "cache"
            live_provider = str(data.get("provider") or "cache")
        else:
            data_source = "degraded"
            live_provider = "none"

        logger.info(
            "Spot cache loaded: %s ETFs, %s stats, source=%s",
            len(etf_spot),
            len(etf_stats),
            data_source,
        )
        return bool(etf_spot)
    except Exception as exc:
        logger.warning("Load spot cache failed: %s", exc)
        return False


def _kline_path(code: str) -> Path:
    return KLINE_DIR / f"{code}.json"


def save_kline(code: str, kline: List[Dict]) -> None:
    try:
        _kline_path(code).write_text(json.dumps(kline, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.error("Save kline %s failed: %s", code, exc)


def load_kline(code: str) -> List[Dict]:
    path = _kline_path(code)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


# ============================================================
# MARKET DATA (EASTMONEY + SINA)
# ============================================================

SPOT_ENDPOINTS = [
    "https://88.push2.eastmoney.com/api/qt/clist/get",
    "https://48.push2.eastmoney.com/api/qt/clist/get",
]

INDEX_ENDPOINT = "https://push2.eastmoney.com/api/qt/stock/get"
KLINE_ENDPOINT = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

SINA_ETF_LIST_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/jsonp.php/"
    "IO.XSRV2.CallbackList['da_yPT46_Ll7K6WD']/Market_Center.getHQNodeDataSimple"
)
SINA_INDEX_URL = "https://hq.sinajs.cn/list=s_sh000001,s_sz399001,s_sh000300"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


def _calc_scale(row: Dict) -> float:
    iopv = _safe_float(row.get("f441"))
    shares = _safe_float(row.get("f38"))
    if iopv > 0 and shares > 0:
        return round(iopv * shares / 1e8, 2)

    total_mv = _safe_float(row.get("f20"))
    if total_mv > 1e6:
        return round(total_mv / 1e8, 2)
    if total_mv > 0:
        return round(total_mv, 2)

    flow_mv = _safe_float(row.get("f21"))
    if flow_mv > 1e6:
        return round(flow_mv / 1e8, 2)
    if flow_mv > 0:
        return round(flow_mv, 2)

    turnover = _safe_float(row.get("f6"))
    if turnover > 0:
        return round(turnover / 1e8, 2)

    return 0.0


def _parse_spot_row(row: Dict) -> Optional[Dict]:
    code = str(row.get("f12", "")).zfill(6)
    name = str(row.get("f14", "")).strip()
    price = _safe_float(row.get("f2"))

    if len(code) != 6 or not name or price <= 0:
        return None

    fee_detail = _get_fee_detail(code)
    fee_total = round(sum(fee_detail.values()), 2) if fee_detail else None

    return {
        "code": code,
        "name": name,
        "currentPrice": round(price, 4),
        "chgPct": round(_safe_float(row.get("f3")), 2),
        "scale": _calc_scale(row),
        "volume": int(_safe_float(row.get("f5"))),
        "turnover": round(_safe_float(row.get("f6")) / 1e8, 2),
        "fee": fee_total,
        "feeDetail": _format_fee_detail(fee_detail),
        "open": round(_safe_float(row.get("f17")), 4),
        "high": round(_safe_float(row.get("f15")), 4),
        "low": round(_safe_float(row.get("f16")), 4),
        "prevClose": round(_safe_float(row.get("f18")), 4),
    }


def _parse_spot_row_sina(row: Dict, scale_hints: Dict[str, float]) -> Optional[Dict]:
    code = str(row.get("code") or row.get("symbol", "")[-6:]).zfill(6)
    name = str(row.get("name", "")).strip()
    price = _safe_float(row.get("trade"))

    if len(code) != 6 or not name or price <= 0:
        return None

    turnover_yuan = _safe_float(row.get("amount"))
    turnover = round(turnover_yuan / 1e8, 2)

    scale_hint = _safe_float(scale_hints.get(code), 0.0)
    scale = round(scale_hint, 2) if scale_hint > 0 else turnover

    fee_detail = _get_fee_detail(code)
    fee_total = round(sum(fee_detail.values()), 2) if fee_detail else None

    return {
        "code": code,
        "name": name,
        "currentPrice": round(price, 4),
        "chgPct": round(_safe_float(row.get("changepercent")), 2),
        "scale": scale,
        "volume": int(_safe_float(row.get("volume"))),
        "turnover": turnover,
        "fee": fee_total,
        "feeDetail": _format_fee_detail(fee_detail),
        "open": round(_safe_float(row.get("open")), 4),
        "high": round(_safe_float(row.get("high")), 4),
        "low": round(_safe_float(row.get("low")), 4),
        "prevClose": round(_safe_float(row.get("settlement")), 4),
    }


def _fetch_spot_from_endpoint(url: str) -> Dict[str, Dict]:
    base_params = {
        "pn": "1",
        "pz": "200",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f12",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827",
        "fields": "f2,f3,f5,f6,f12,f14,f15,f16,f17,f18,f20,f21,f38,f441",
    }

    first = _request_json(url, base_params, retries=2)
    data = first.get("data") or {}
    first_rows = data.get("diff") or []
    if not first_rows:
        return {}

    total = int(data.get("total") or len(first_rows))
    page_size = max(1, len(first_rows))
    total_pages = max(1, math.ceil(total / page_size))

    rows = list(first_rows)
    for page in range(2, total_pages + 1):
        params = dict(base_params)
        params["pn"] = str(page)
        payload = _request_json(url, params, retries=1)
        rows.extend((payload.get("data") or {}).get("diff") or [])

    result: Dict[str, Dict] = {}
    for row in rows:
        parsed = _parse_spot_row(row)
        if not parsed:
            continue
        result[parsed["code"]] = parsed

    return result


def _fetch_spot_from_sina(scale_hints: Dict[str, float]) -> Dict[str, Dict]:
    text = _request_text_sina(
        SINA_ETF_LIST_URL,
        params={
            "page": "1",
            "num": "5000",
            "sort": "symbol",
            "asc": "0",
            "node": "etf_hq_fund",
            "[object HTMLDivElement]": "qvvne",
        },
        retries=2,
    )

    start = text.find("([")
    end = text.rfind("])")
    if start < 0 or end <= start:
        raise RuntimeError("invalid_sina_jsonp")

    rows = json.loads(text[start + 1 : end + 1])
    result: Dict[str, Dict] = {}
    for row in rows:
        parsed = _parse_spot_row_sina(row, scale_hints)
        if not parsed:
            continue
        result[parsed["code"]] = parsed

    return result


def _normalize_quote_num(value: float) -> float:
    if abs(value) > 100000:
        return value / 100
    return value


def _fetch_indices_from_eastmoney() -> List[Dict]:
    targets = [
        ("上证指数", "1.000001"),
        ("深证成指", "0.399001"),
        ("沪深300", "1.000300"),
    ]
    result = []

    for name, secid in targets:
        payload = _request_json(
            INDEX_ENDPOINT,
            {
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fltt": "2",
                "invt": "2",
                "fields": "f43,f170,f57,f58",
            },
            retries=1,
        )
        data = payload.get("data") or {}
        val = _normalize_quote_num(_safe_float(data.get("f43")))
        chg = _normalize_quote_num(_safe_float(data.get("f170")))
        if val <= 0:
            continue
        result.append({"name": name, "val": round(val, 2), "chg": round(chg, 2)})

    return result


def _fetch_indices_from_sina() -> List[Dict]:
    text = _request_text_sina(
        SINA_INDEX_URL,
        retries=2,
        headers={
            "Referer": "https://finance.sina.com.cn/",
            "User-Agent": SESSION.headers.get("User-Agent", "Mozilla/5.0"),
        },
    )

    name_map = {
        "s_sh000001": "上证指数",
        "s_sz399001": "深证成指",
        "s_sh000300": "沪深300",
    }

    result = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("var hq_str_"):
            continue
        left, _, right = line.partition("=")
        symbol = left.replace("var hq_str_", "").strip()
        raw = right.strip().strip(";").strip('"')
        parts = raw.split(",")
        if len(parts) < 4:
            continue
        display_name = name_map.get(symbol)
        if not display_name:
            continue
        val = _safe_float(parts[1])
        chg_pct = _safe_float(parts[3])
        if val <= 0:
            continue
        result.append({"name": display_name, "val": round(val, 2), "chg": round(chg_pct, 2)})

    return result


def fetch_spot_live(scale_hints: Dict[str, float]) -> Tuple[str, Dict[str, Dict]]:
    last_err = None

    # Primary: Eastmoney
    for endpoint in SPOT_ENDPOINTS:
        try:
            spot = _fetch_spot_from_endpoint(endpoint)
            if spot:
                logger.info("Spot fetched from %s, count=%s", endpoint, len(spot))
                return "eastmoney", spot
        except Exception as exc:
            last_err = exc
            logger.warning("Spot endpoint failed (%s): %s", endpoint, exc)

    # Fallback: Sina
    try:
        spot = _fetch_spot_from_sina(scale_hints)
        if spot:
            logger.info("Spot fetched from Sina, count=%s", len(spot))
            return "sina", spot
    except Exception as exc:
        last_err = exc
        logger.warning("Spot fallback (Sina) failed: %s", exc)

    if last_err:
        raise last_err
    return "none", {}


def fetch_indices_live() -> Tuple[str, List[Dict]]:
    try:
        indices = _fetch_indices_from_eastmoney()
        if indices:
            return "eastmoney", indices
    except Exception as exc:
        logger.warning("Index fetch (Eastmoney) failed: %s", exc)

    try:
        indices = _fetch_indices_from_sina()
        if indices:
            return "sina", indices
    except Exception as exc:
        logger.warning("Index fetch (Sina) failed: %s", exc)

    return "none", []


def _secid_candidates(code: str) -> List[str]:
    if code.startswith(("5", "6", "9")):
        return [f"1.{code}", f"0.{code}"]
    return [f"0.{code}", f"1.{code}"]


def _tencent_symbol(code: str) -> str:
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    return f"sz{code}"


def _fetch_kline_from_eastmoney(code: str, days: int = 1200) -> List[Dict]:
    start_date = (datetime.now(BEIJING_TZ) - timedelta(days=days)).strftime("%Y%m%d")
    end_date = datetime.now(BEIJING_TZ).strftime("%Y%m%d")

    for secid in _secid_candidates(code):
        try:
            payload = _request_json(
                KLINE_ENDPOINT,
                {
                    "secid": secid,
                    "ut": "7eea3edcaed734bea9cbfc24409ed989",
                    "klt": "101",
                    "fqt": "1",
                    "beg": start_date,
                    "end": end_date,
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
                },
                retries=1,
            )
            data = payload.get("data") or {}
            raw = data.get("klines") or []
            if not raw:
                continue

            result: List[Dict] = []
            for line in raw:
                parts = line.split(",")
                if len(parts) < 6:
                    continue
                result.append(
                    {
                        "date": parts[0],
                        "open": round(_safe_float(parts[1]), 4),
                        "close": round(_safe_float(parts[2]), 4),
                        "high": round(_safe_float(parts[3]), 4),
                        "low": round(_safe_float(parts[4]), 4),
                        "volume": int(_safe_float(parts[5])),
                    }
                )
            if result:
                return result
        except Exception as exc:
            logger.debug("Kline attempt failed (%s/%s): %s", code, secid, exc)

    return []


def _fetch_kline_from_tencent(code: str, days: int = 1200) -> List[Dict]:
    symbol = _tencent_symbol(code)
    start_cutoff = (datetime.now(BEIJING_TZ) - timedelta(days=days)).date()

    try:
        payload = _request_json_external(
            TENCENT_KLINE_URL,
            params={"param": f"{symbol},day,,,1500,qfq"},
            retries=2,
            headers={
                "User-Agent": SESSION.headers.get("User-Agent", "Mozilla/5.0"),
                "Referer": "https://stockapp.finance.qq.com/",
            },
        )
    except Exception as exc:
        logger.debug("Kline Tencent request failed (%s): %s", code, exc)
        return []

    if int(payload.get("code", -1)) != 0:
        return []

    data_map = payload.get("data") or {}
    node = data_map.get(symbol) or data_map.get(symbol.upper())
    if not node and data_map:
        node = next(iter(data_map.values()))
    if not isinstance(node, dict):
        return []

    raw_rows = node.get("qfqday") or node.get("day") or []
    if not isinstance(raw_rows, list):
        return []

    result: List[Dict] = []
    for row in raw_rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        date_str = str(row[0])[:10]
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if date_obj < start_cutoff:
            continue
        result.append(
            {
                "date": date_str,
                "open": round(_safe_float(row[1]), 4),
                "close": round(_safe_float(row[2]), 4),
                "high": round(_safe_float(row[3]), 4),
                "low": round(_safe_float(row[4]), 4),
                "volume": int(_safe_float(row[5])),
            }
        )

    return result


def _fetch_fee_from_eastmoney(code: str) -> bool:
    """Fetch management fee and other fees from Eastmoney fund page"""
    for secid in _secid_candidates(code):
        try:
            # Eastmoney fund details API
            url = "https://api.fund.eastmoney.com/fund/archives"
            params = {
                "fundcode": code,
                "pageIndex": 1,
                "pageSize": 1,
            }
            headers = {
                "Referer": f"http://fund.eastmoney.com/{code}.html",
            }
            payload = _request_json(url, params, retries=1, headers=headers)
            if not payload or not payload.get("data"):
                continue
            
            # 尝试从另一个接口获取费率信息
            info_url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
            text = _request_text(info_url, retries=1, headers={"Referer": f"http://fund.eastmoney.com/{code}.html"})
            if not text:
                continue
            
            #  parse fees
            import re
            fee_match = re.search(r'"fund_management_rate":\s*"([\d.]+)"', text)
            custody_match = re.search(r'"fund_custodian_rate":\s*"([\d.]+)"', text)
            sales_match = re.search(r'"fund_recurring_purchase_rate":\s*"([\d.]+)"', text)
            
            fees: Dict[str, float] = {}
            if fee_match:
                fees["管理费"] = _safe_float(fee_match.group(1))
            if custody_match:
                fees["托管费"] = _safe_float(custody_match.group(1))
            if sales_match:
                fees["销售服务费"] = _safe_float(sales_match.group(1))
            
            if fees:
                with _lock:
                    _fee_cache[code] = fees
                    # save to disk
                    try:
                        FEE_CACHE_FILE.write_text(json.dumps(_fee_cache, ensure_ascii=False), encoding="utf-8")
                    except Exception:
                        pass
                logger.debug("Updated fees for %s: %s", code, fees)
                return True
            
        except Exception as exc:
            logger.debug("Fetch fee failed for %s: %s", code, exc)
    
    return False


def fetch_kline_live(code: str, days: int = 1200) -> List[Dict]:
    # Fetch and update fees when fetching kline
    _fetch_fee_from_eastmoney(code)
    
    eastmoney_kline = _fetch_kline_from_eastmoney(code, days)
    if eastmoney_kline:
        return eastmoney_kline

    tencent_kline = _fetch_kline_from_tencent(code, days)
    if tencent_kline:
        logger.debug("Kline fallback hit (provider=tencent, code=%s)", code)
        return tencent_kline

    return []


# ============================================================
# METRICS / TRADING TIME
# ============================================================

# Required fields in a valid stats entry (added over time; used for migration check)
_REQUIRED_STATS_FIELDS = {
    "allTimeHigh", "allTimeHighDate", "dropFromHigh",
    "allTimeLow", "allTimeLowDate", "riseFromLow",
    "sparkline",
}


def _stats_is_complete(stats: Dict) -> bool:
    """Return True only if stats contains all required fields with non-None values."""
    if not stats:
        return False
    return all(stats.get(f) is not None for f in _REQUIRED_STATS_FIELDS)


def _max_drawdown(values: List[float]) -> float:
    if not values or len(values) < 2:
        return 0.0
    peak = values[0]
    mdd = 0.0
    for value in values:
        if value > peak:
            peak = value
        drawdown = (value - peak) / peak
        if drawdown < mdd:
            mdd = drawdown
    return round(mdd * 100, 2)


def compute_stats(kline: List[Dict]) -> Dict:
    if not kline or len(kline) < 10:
        return {}

    valid_kline = [k for k in kline if _safe_float(k.get("high")) > 0 and _safe_float(k.get("low")) > 0]
    if len(valid_kline) < 10:
        return {}

    closes = [k.get("close", 0) for k in valid_kline]
    current = closes[-1]

    # Find all-time high using the high field (not just close) + corresponding date
    all_high = -1.0
    all_high_date = ""
    for k in valid_kline:
        high = _safe_float(k.get("high"))
        if high > all_high:
            all_high = high
            all_high_date = k.get("date", "")

    # Find all-time low using the low field + corresponding date
    all_low = float("inf")
    all_low_date = ""
    for k in valid_kline:
        low = _safe_float(k.get("low"))
        if low > 0 and low < all_low:
            all_low = low
            all_low_date = k.get("date", "")

    one_year = closes[-250:] if len(closes) > 250 else closes
    three_year = closes[-750:] if len(closes) > 750 else closes

    return {
        "allTimeHigh": round(all_high, 4),
        "allTimeHighDate": all_high_date,
        "dropFromHigh": round((current - all_high) / all_high * 100, 2) if all_high > 0 else None,
        "allTimeLow": round(all_low, 4),
        "allTimeLowDate": all_low_date,
        "riseFromLow": round((current - all_low) / all_low * 100, 2) if all_low > 0 and all_low != float("inf") else None,
        "maxDD1Y": _max_drawdown(one_year),
        "maxDD3Y": _max_drawdown(three_year),
        "sparkline": [round(v, 4) for v in closes[-60:]],
    }


def is_trading_day() -> bool:
    """
    判断是否为交易日（周一到周五）。
    注意：此函数仅用于实时行情判断，K线历史数据采集不受此限制。
    """
    return datetime.now(BEIJING_TZ).weekday() < 5


def is_trading_time() -> bool:
    if not is_trading_day():
        return False
    now = datetime.now(BEIJING_TZ)
    hhmm = now.hour * 100 + now.minute
    return (930 <= hhmm <= 1130) or (1300 <= hhmm <= 1500)


def _should_refresh_spot(force: bool = False) -> bool:
    return force or FORCE_REFRESH or is_trading_time()


def _should_update_kline(code: str, force: bool = False) -> bool:
    """
    判断是否需要更新 K 线数据。
    
    Args:
        code: ETF 代码
        force: 如果为 True，强制更新（忽略今天已更新的检查）
    """
    if force:
        return True
    
    today = _today_bj_str()
    if _last_kline_update.get(code) != today:
        return True
    # Even if fetched today, re-fetch if stats are incomplete (e.g. after upgrade)
    with _lock:
        stats = etf_stats.get(code, {})
    return not _stats_is_complete(stats)


def backfill_stats_from_kline_files() -> None:
    """
    Startup-time job: scan all local kline JSON files and (re)compute stats for
    any ETF whose stats are missing or lack the required fields introduced in
    newer versions of compute_stats().  This ensures backward-compatibility when
    the server is upgraded without wiping the cache.
    """
    if not KLINE_DIR.exists():
        return

    files = list(KLINE_DIR.glob("*.json"))
    if not files:
        return

    updated = 0
    today = _today_bj_str()
    for path in files:
        code = path.stem
        with _lock:
            existing = etf_stats.get(code, {})
        if _stats_is_complete(existing):
            continue
        try:
            kline = load_kline(code)
            if len(kline) < 10:
                continue
            stats = compute_stats(kline)
            if not stats:
                continue
            with _lock:
                etf_stats[code] = stats
                if _last_kline_update.get(code) != today:
                    _last_kline_update[code] = today
            updated += 1
        except Exception as exc:
            logger.debug("Backfill stats failed (%s): %s", code, exc)

    if updated > 0:
        save_spot_cache()
        logger.info("Stats backfill complete: updated=%s / scanned=%s", updated, len(files))
    else:
        logger.info("Stats backfill: all %s kline files already have complete stats", len(files))


def _prioritized_codes(limit: int = 0) -> List[str]:
    """
    返回按规模排序的 ETF 代码列表。
    
    Args:
        limit: 限制返回数量，0 表示返回全部
    
    排序规则：
    1. 按规模（scale）降序排列
    2. 规模相同的按成交量（turnover）降序排列
    """
    with _lock:
        # 获取所有 ETF 并按规模降序排序
        all_etfs = []
        for code, info in etf_spot.items():
            scale = _safe_float(info.get("scale"))
            turnover = _safe_float(info.get("turnover"))
            all_etfs.append((code, scale, turnover))
        
        # 按规模降序，规模相同按成交量降序
        sorted_etfs = sorted(
            all_etfs,
            key=lambda x: (x[1], x[2]),  # (scale, turnover)
            reverse=True,
        )
        
        all_codes = [code for code, _, _ in sorted_etfs]
        
        # 如果指定了 limit 且大于 0，则限制返回数量
        if limit > 0 and len(all_codes) > limit:
            return all_codes[:limit]
        return all_codes


# ============================================================
# REFRESH JOBS
# ============================================================


def refresh_spot(force: bool = False) -> None:
    global last_updated, data_source, live_provider

    if not _should_refresh_spot(force=force):
        logger.debug("Skip spot refresh (not trading time)")
        return

    try:
        with _lock:
            scale_hints = {
                code: _safe_float(info.get("scale")) for code, info in etf_spot.items()
            }

        provider, new_spot = fetch_spot_live(scale_hints)
        if not new_spot:
            raise RuntimeError("empty spot data")

        index_provider, new_indices = fetch_indices_live()

        with _lock:
            etf_spot.clear()
            etf_spot.update(new_spot)

            for code in list(etf_stats.keys()):
                if code not in etf_spot:
                    etf_stats.pop(code, None)
            for code in list(_last_kline_update.keys()):
                if code not in etf_spot:
                    _last_kline_update.pop(code, None)

            if new_indices:
                market_indices.clear()
                market_indices.extend(new_indices)

            last_updated = _now_bj_str()
            data_source = "live"
            if provider != "none":
                live_provider = provider
            elif index_provider != "none":
                live_provider = index_provider
            else:
                live_provider = "live"

        save_spot_cache()
        logger.info("Spot refreshed: %s ETFs via %s", len(new_spot), live_provider)
    except Exception as exc:
        logger.error("Spot refresh failed: %s", exc)
        with _lock:
            if etf_spot:
                data_source = "cache"
                if live_provider == "none":
                    live_provider = "cache"
            else:
                data_source = "degraded"
                live_provider = "none"


def refresh_all_fees() -> None:
    """
    批量采集所有 ETF 的费率数据。
    独立于 K 线采集，确保费率数据完整。
    """
    if not etf_spot:
        logger.warning("No ETF spot data available, skipping fee refresh")
        return
    
    # 获取全部 ETF，按规模降序排列
    all_codes = _prioritized_codes(limit=0)
    if not all_codes:
        return
    
    # 找出费率数据缺失的 ETF
    missing_fee_codes = [code for code in all_codes if code not in _fee_cache]
    
    logger.info("Starting fee refresh: total_etfs=%s, missing_fees=%s", 
                len(all_codes), len(missing_fee_codes))
    
    done = 0
    failed = 0
    
    # 优先采集缺失费率的 ETF
    for code in missing_fee_codes:
        try:
            if _fetch_fee_from_eastmoney(code):
                done += 1
            else:
                failed += 1
        except Exception as exc:
            failed += 1
            logger.debug("Fee refresh failed for %s: %s", code, exc)
    
    # 再随机采集一部分已有费率的 ETF 进行更新（避免数据过期）
    existing_fee_codes = [code for code in all_codes if code in _fee_cache]
    import random
    sample_size = min(50, len(existing_fee_codes))  # 每天更新50个已有费率的 ETF
    sample_codes = random.sample(existing_fee_codes, sample_size) if existing_fee_codes else []
    
    for code in sample_codes:
        try:
            if _fetch_fee_from_eastmoney(code):
                done += 1
        except Exception as exc:
            logger.debug("Fee refresh (update) failed for %s: %s", code, exc)
    
    logger.info(
        "Fee refresh done: updated=%s failed=%s total_missing=%s",
        done,
        failed,
        len(missing_fee_codes),
    )


def refresh_kline_batch(force: bool = False) -> None:
    """
    批量刷新 K 线数据。
    采集全部 ETF（按规模降序），KLINE_TOP_N 控制每批处理数量。
    
    Args:
        force: 如果为 True，强制更新所有 ETF（忽略今天已更新的检查）
    """
    if not etf_spot:
        return

    # K线历史数据采集不受交易日限制，确保周末也能正常采集
    # 只在非强制模式下且非交易日时才跳过（定时任务使用非强制模式）
    if not force and not is_trading_day() and not FORCE_REFRESH:
        logger.debug("Skip kline refresh (non-trading day and not forced)")
        return

    # 获取全部 ETF，按规模降序排列
    all_codes = _prioritized_codes(limit=0)
    if not all_codes:
        return

    today = _today_bj_str()
    done = 0
    skipped = 0
    failed = 0
    
    # 计算实际需要处理的 ETF 数量
    # KLINE_TOP_N = 0 表示处理全部，否则只处理前 KLINE_TOP_N 只
    target_count = len(all_codes) if KLINE_TOP_N <= 0 else min(KLINE_TOP_N, len(all_codes))
    codes_to_process = all_codes[:target_count]

    logger.info("Starting kline refresh: total=%s, target=%s, batch_size=%s, force=%s", 
                len(all_codes), target_count, KLINE_BATCH_SIZE, force)

    for start in range(0, len(codes_to_process), KLINE_BATCH_SIZE):
        batch = codes_to_process[start : start + KLINE_BATCH_SIZE]

        for code in batch:
            if not _should_update_kline(code, force=force):
                skipped += 1
                continue

            try:
                kline = fetch_kline_live(code)
                if len(kline) < 10:
                    failed += 1
                    continue

                save_kline(code, kline)
                stats = compute_stats(kline)
                with _lock:
                    etf_stats[code] = stats
                    _last_kline_update[code] = today
                done += 1
            except Exception as exc:
                failed += 1
                logger.debug("Kline refresh failed (%s): %s", code, exc)

    if done > 0:
        save_spot_cache()

    logger.info(
        "Kline refresh done: updated=%s skipped=%s failed=%s target=%s total_etfs=%s",
        done,
        skipped,
        failed,
        target_count,
        len(all_codes),
    )


# ============================================================
# FASTAPI APP
# ============================================================

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global data_source, live_provider

    _load_fee_cache()
    cache_loaded = load_spot_cache()

    # Startup: always try one live refresh first.
    refresh_spot(force=True)

    with _lock:
        if not etf_spot:
            data_source = "degraded"
            live_provider = "none"
        elif data_source != "live":
            data_source = "cache"
            if live_provider == "none":
                live_provider = "cache"

    logger.info(
        "Startup state: source=%s provider=%s etfs=%s cache_loaded=%s",
        data_source,
        live_provider,
        len(etf_spot),
        cache_loaded,
    )

    scheduler.add_job(
        refresh_spot,
        "interval",
        minutes=REFRESH_MINUTES,
        id="spot_refresh",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        refresh_kline_batch,
        "interval",
        minutes=KLINE_REFRESH_MINUTES,
        id="kline_refresh",
        max_instances=1,
        coalesce=True,
    )
    # 添加独立的费率采集任务，每天执行一次
    scheduler.add_job(
        refresh_all_fees,
        "cron",
        hour=3,  # 每天凌晨3点执行
        minute=0,
        id="fee_refresh",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    # Warm up kline for all ETFs in background once (force=True to ensure all ETFs are fetched).
    # Use a lambda to pass force=True parameter.
    threading.Thread(target=lambda: refresh_kline_batch(force=True), daemon=True).start()
    
    # 启动时也在后台采集缺失的费率数据
    threading.Thread(target=refresh_all_fees, daemon=True).start()

    # Back-fill stats from any existing kline files that lack the new fields
    # (e.g. after a server upgrade where compute_stats gained new columns).
    threading.Thread(target=backfill_stats_from_kline_files, daemon=True).start()

    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="ETF NEXUS", lifespan=lifespan)


@app.get("/api/etf-data")
async def get_etf_data():
    with _lock:
        etfs = []
        for code, spot in etf_spot.items():
            stats = etf_stats.get(code, {})
            row = {
                **spot,
                "allTimeHigh": stats.get("allTimeHigh"),
                "allTimeHighDate": stats.get("allTimeHighDate"),
                "dropFromHigh": stats.get("dropFromHigh"),
                "allTimeLow": stats.get("allTimeLow"),
                "allTimeLowDate": stats.get("allTimeLowDate"),
                "riseFromLow": stats.get("riseFromLow"),
                "maxDD1Y": stats.get("maxDD1Y"),
                "maxDD3Y": stats.get("maxDD3Y"),
                "sparkline": stats.get("sparkline", []),
            }
            if row.get("fee") is None and code in _fee_cache:
                fee_detail = _get_fee_detail(code)
                if fee_detail:
                    row["fee"] = round(sum(fee_detail.values()), 2)
                    row["feeDetail"] = _format_fee_detail(fee_detail)
            row.setdefault("feeDetail", "")
            etfs.append(row)

        return JSONResponse(
            {
                "etfs": etfs,
                "indices": market_indices,
                "updated": last_updated,
                "source": data_source,
                "provider": live_provider,
            }
        )


@app.get("/api/kline/{code}")
async def get_kline(code: str, range: str = "1Y"):
    code = str(code).zfill(6)

    kline = load_kline(code)
    if not kline:
        kline = fetch_kline_live(code)
        if kline:
            save_kline(code, kline)
            stats = compute_stats(kline)
            with _lock:
                etf_stats[code] = stats
                _last_kline_update[code] = _today_bj_str()
            save_spot_cache()

    if not kline:
        return JSONResponse({"error": "Kline data not available"}, status_code=404)

    range_map = {"1M": 22, "3M": 66, "6M": 132, "1Y": 250, "3Y": 750, "全部": 999999}
    n = range_map.get(range, 250)
    sliced = kline[-min(n, len(kline)) :]

    spot = etf_spot.get(code, {})
    return JSONResponse(
        {
            "code": code,
            "name": spot.get("name", code),
            "fee": spot.get("fee"),
            "scale": spot.get("scale", 0),
            "kline": sliced,
        }
    )


@app.get("/api/health")
async def health():
    limiter = request_controller.status()
    return {
        "status": "ok",
        "etf_count": len(etf_spot),
        "stats_count": len(etf_stats),
        "source": data_source,
        "provider": live_provider,
        "updated": last_updated,
        "refresh_minutes": REFRESH_MINUTES,
        "kline_refresh_minutes": KLINE_REFRESH_MINUTES,
        "kline_top_n": KLINE_TOP_N,
        "rate_limiter": limiter,
        "proxy": _mask_proxy(_PROXY),
    }


@app.get("/api/diag")
async def diag():
    sample = []
    with _lock:
        for idx, (code, spot) in enumerate(etf_spot.items()):
            if idx >= 3:
                break
            sample.append({**spot, "stats": etf_stats.get(code, {})})

    return {
        "current_source": data_source,
        "provider": live_provider,
        "current_etf_count": len(etf_spot),
        "current_stats_count": len(etf_stats),
        "indices": market_indices,
        "updated": last_updated,
        "rate_limiter": request_controller.status(),
        "sample_etfs": sample,
    }


# ========== Webhook 自动更新路由 ==========
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
REPO_PATH = os.environ.get("REPO_PATH", "/app")

def verify_signature(payload: bytes, signature: str, secret: str) -> bool:
    """验证 GitHub webhook 签名"""
    if not secret:
        return True
    if not signature:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

@app.post("/webhook")
async def github_webhook(request: Request):
    """GitHub webhook endpoint - 接收 push 事件并自动更新服务（异步执行）"""
    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")
    
    if not verify_signature(body, signature, WEBHOOK_SECRET):
        return JSONResponse({"error": "Invalid signature"}, status_code=401)
    
    event = request.headers.get("X-GitHub-Event", "")
    if event != "push":
        return JSONResponse({"message": f"Ignored event: {event}"}, status_code=200)
    
    try:
        import subprocess
        # 使用 Popen 立即返回，后台执行更新脚本
        # 脚本会自己启动后台进程并立即返回
        process = subprocess.Popen(
            ["bash", f"{REPO_PATH}/auto-update.sh"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True  # 脱离父进程，避免被终止
        )
        
        # 等待短暂时间获取初始输出（脚本应该立即返回）
        try:
            stdout, stderr = process.communicate(timeout=5)
            return JSONResponse({
                "status": "success",
                "message": "Update task started in background",
                "stdout": stdout.decode('utf-8', errors='ignore') if stdout else "",
                "stderr": stderr.decode('utf-8', errors='ignore') if stderr else "",
                "pid": process.pid
            })
        except subprocess.TimeoutExpired:
            # 如果超时，说明脚本正在后台执行
            return JSONResponse({
                "status": "success",
                "message": "Update task is running in background",
                "pid": process.pid,
                "log_file": f"{REPO_PATH}/logs/auto-update.log"
            })
            
    except Exception as e:
        logger.error(f"Webhook execution failed: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
