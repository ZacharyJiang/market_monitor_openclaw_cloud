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
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import hmac
import hashlib
import subprocess

import requests
import httpx  # 添加异步HTTP客户端
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

# 溢价数据缓存
_premium_cache: Dict[str, float] = {}
_premium_cache_file = DATA_DIR / "premium_cache.json"

# 收盘溢价数据缓存
_close_premium_cache: Dict[str, Dict] = {}
_close_premium_cache_file = DATA_DIR / "close_premium_cache.json"


def _load_premium_cache():
    """加载溢价缓存"""
    global _premium_cache
    if _premium_cache_file.exists():
        try:
            _premium_cache = json.load(open(_premium_cache_file, encoding="utf-8"))
        except Exception:
            _premium_cache = {}
    return _premium_cache


def _save_premium_cache():
    """保存溢价缓存"""
    try:
        _premium_cache_file.write_text(json.dumps(_premium_cache, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _load_close_premium_cache():
    """加载收盘溢价缓存"""
    global _close_premium_cache
    if _close_premium_cache_file.exists():
        try:
            _close_premium_cache = json.load(open(_close_premium_cache_file, encoding="utf-8"))
        except Exception:
            _close_premium_cache = {}
    return _close_premium_cache


def _save_close_premium_cache():
    """保存收盘溢价缓存"""
    try:
        _close_premium_cache_file.write_text(json.dumps(_close_premium_cache, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def save_close_premium_at_market_close(session: str = "afternoon"):
    """
    收盘时保存当前溢价为收盘溢价。
    
    Args:
        session: "morning" 表示上午收盘(11:30)，"afternoon" 表示下午收盘(15:00)
    """
    global _close_premium_cache
    now = datetime.now(BEIJING_TZ)
    today = now.strftime("%Y-%m-%d")
    
    for code, premium in _premium_cache.items():
        _close_premium_cache[code] = {
            "premium": premium,
            "date": today,
            "session": session  # 标记是上午还是下午收盘
        }
    
    _save_close_premium_cache()
    logger.info(f"Saved {session} close premium for {len(_premium_cache)} ETFs on {today}")


def get_premium_for_display(code: str) -> Optional[float]:
    """
    获取用于展示的溢价数据。
    
    逻辑：
    - 交易时间（9:30-11:30, 13:00-15:00）：返回实时溢价
    - 午间休市（11:30-13:00）：返回上午收盘溢价
    - 收盘后（15:00-次日9:30）：返回下午收盘溢价
    - 非交易日：返回最近一个交易日的收盘溢价
    """
    now = datetime.now(BEIJING_TZ)
    hhmm = now.hour * 100 + now.minute
    is_trading_day_flag = is_trading_day()
    
    # 交易时间内：返回实时溢价
    if is_trading_day_flag and ((930 <= hhmm <= 1130) or (1300 <= hhmm <= 1500)):
        return _premium_cache.get(code)
    
    # 午间休市（11:30-13:00）：尝试返回上午收盘溢价
    if is_trading_day_flag and 1130 < hhmm < 1300:
        # 查找今天保存的收盘溢价（上午收盘时保存的）
        today = now.strftime("%Y-%m-%d")
        close_data = _close_premium_cache.get(code)
        if close_data and close_data.get("date") == today:
            return close_data.get("premium")
        # 如果没有今天的记录，返回实时溢价（可能是11:30前的最后数据）
        return _premium_cache.get(code)
    
    # 收盘后或周末：返回最近一个交易日的收盘溢价
    close_data = _close_premium_cache.get(code)
    if close_data:
        return close_data.get("premium")
    
    # 如果没有收盘溢价记录，返回实时溢价
    return _premium_cache.get(code)


# 初始化加载
try:
    _load_premium_cache()
    _load_close_premium_cache()
    logger.info("Cache loaded successfully")
except Exception as e:
    logger.error(f"Cache load failed: {e}", exc_info=True)

# 确保所有场内基金都在spot列表中
# 异步运行，避免阻塞启动
def _discover_funds_async():
    try:
        logger.info("Starting fund discovery in background")
        time.sleep(5)  # 等待启动完成
        _ensure_all_etfs_in_spot()
        logger.info("Fund discovery completed")
    except Exception as e:
        logger.error(f"Fund discovery failed: {e}", exc_info=True)

try:
    threading.Thread(target=_discover_funds_async, daemon=True).start()
    logger.info("Fund discovery thread started")
except Exception as e:
    logger.error(f"Failed to start fund discovery thread: {e}", exc_info=True)

# 服务启动完成日志
logger.info("✅ ETF monitor service started successfully, listening on port 8080")


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

# 异步HTTP客户端（用于批量溢价数据获取，不阻塞主线程）
_async_client: Optional[httpx.AsyncClient] = None

def _get_async_client() -> httpx.AsyncClient:
    global _async_client
    if _async_client is None:
        _async_client = httpx.AsyncClient(
            headers={
                "User-Agent": SESSION.headers.get("User-Agent", ""),
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": "https://quote.eastmoney.com/",
            },
            timeout=10.0,
            proxies={"all://": _PROXY} if _PROXY else None,
        )
    return _async_client


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

    # 不过滤价格为0的基金，即使非交易时间没有报价也要显示所有基金
    if len(code) != 6 or not name:
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

    # 不过滤价格为0的基金，即使非交易时间没有报价也要显示所有基金
    if len(code) != 6 or not name:
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


def _fetch_premium_from_eastmoney(code: str) -> Optional[float]:
    """
    从东方财富获取 ETF 溢价率数据。
    返回溢价率百分比（如 0.5 表示溢价 0.5%），None 表示获取失败。
    """
    for secid in _secid_candidates(code):
        try:
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": secid,
                "fields": "f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f60,f169,f170,f171,f172,f173,f177,f183,f184,f185,f186,f187,f188,f189,f190",
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
            }
            payload = _request_json(url, params, retries=1)
            data = payload.get("data")
            if not data:
                continue
            
            # f184 是溢价率字段（百分比）
            premium = data.get("f184")
            if premium is not None:
                return round(_safe_float(premium), 2)
            
            # 如果 f184 没有，尝试计算：溢价率 = (市价 - 净值) / 净值 * 100
            price = _safe_float(data.get("f43"))  # 当前价
            nav = _safe_float(data.get("f183"))   # 单位净值
            if price > 0 and nav > 0:
                premium = ((price - nav) / nav) * 100
                return round(premium, 2)
                
        except Exception as exc:
            logger.debug(f"Failed to fetch premium for {code} with secid {secid}: {exc}")
            continue
    
    return None


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


async def fetch_premium_for_spot_async(spot: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    异步批量获取spot中所有ETF的溢价数据。
    不阻塞主线程，使用并发请求。
    """
    codes = list(spot.keys())
    if not codes:
        return spot
    
    logger.info("Fetching premium data for %s ETFs asynchronously...", len(codes))
    start_time = time.time()
    
    # 分批处理，每批100个
    batch_size = 100
    total_success = 0
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        try:
            premiums = _fetch_premium_batch_sync(batch)
            success_count = len(premiums)
            total_success += success_count
            
            # 更新spot数据
            for code, premium in premiums.items():
                if code in spot:
                    spot[code]["premium"] = premium
            
            logger.info("Premium batch %s/%s completed: got %s/%s premiums", 
                       i//batch_size + 1, (len(codes)-1)//batch_size + 1, success_count, len(batch))
        except Exception as e:
            logger.warning(f"Premium batch {i//batch_size +1} failed: {e}")
        
        # 短暂延迟，避免触发限流
        await asyncio.sleep(0.3)
    
    elapsed = time.time() - start_time
    logger.info("Premium fetch completed: success=%s total=%s time=%.2fs", total_success, len(codes), elapsed)
    return spot

# 新增独立的溢价刷新任务，单独运行不依赖spot刷新
def refresh_all_premium() -> None:
    """刷新所有ETF的溢价数据，独立定时任务"""
    if not etf_spot:
        logger.warning("No ETF spot data available, skipping premium refresh")
        return
    
    all_codes = list(etf_spot.keys())
    logger.info("Starting full premium refresh for %s ETFs...", len(all_codes))
    
    batch_size = 100
    total_success = 0
    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i+batch_size]
        try:
            premiums = _fetch_premium_batch_sync(batch)
            total_success += len(premiums)
        except Exception as e:
            logger.warning(f"Premium refresh batch {i//batch_size +1} failed: {e}")
        time.sleep(0.3)
    
    # 更新last_updated时间戳
    global last_updated
    with _lock:
        last_updated = _now_bj_str()
    save_spot_cache()
    
    logger.info("Full premium refresh completed: updated=%s total=%s", total_success, len(all_codes))


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


def _fetch_premium_from_eastmoney(code: str) -> Optional[float]:
    """Fetch ETF premium/discount rate from Eastmoney"""
    for secid in _secid_candidates(code):
        try:
            # 使用分时数据接口获取溢价率
            url = f"https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": secid,
                "fields": "f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f60,f170,f171,f172,f173,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f193,f194,f195,f196,f197,f198,f199,f200,f201,f202,f203,f204,f205,f206,f207,f208,f209,f210,f211,f212,f213,f214,f215,f216,f217,f218,f219,f220,f221,f222,f223,f224,f225,f226,f227,f228,f229,f230,f231,f232,f233,f234,f235,f236,f237,f238,f239,f240,f241,f242,f243,f244,f245,f246,f247,f248,f249,f250,f251,f252,f253,f254,f255,f256,f257,f258,f259,f260,f261,f262,f263,f264,f265,f266,f267,f268,f269,f270,f271,f272,f273,f274,f275,f276,f277,f278,f279,f280,f281,f282,f283,f284,f285,f286,f287,f288,f289,f290,f291,f292,f293,f294,f295,f296,f297,f298,f299,f300",
            }
            payload = _request_json(url, params, retries=1)
            if not payload or not payload.get("data"):
                continue
            
            data = payload.get("data", {})
            # f184 是溢价率字段
            premium = data.get("f184")
            if premium is not None:
                premium_val = _safe_float(premium)
                if premium_val != 0 or data.get("f43"):  # 确保有数据
                    with _lock:
                        _premium_cache[code] = premium_val
                        _save_premium_cache()
                    logger.debug("Updated premium for %s: %.2f%%", code, premium_val)
                    return premium_val
            
        except Exception as exc:
            logger.debug("Fetch premium failed for %s: %s", code, exc)
    
    return None


def _fetch_premium_batch_sync(codes: List[str]) -> Dict[str, float]:
    """
    同步批量获取ETF溢价率数据。
    使用批量API请求，兼容后台线程环境。
    """
    results = {}
    if not codes:
        return results
    
    # 使用同步requests批量获取
    for i in range(0, len(codes), 200):  # 每批200只
        batch = codes[i:i+200]
        try:
            # 修复secid构造错误：5/6/9开头的沪市基金用1.前缀，其他用0.前缀
            secids = []
            for c in batch:
                if len(c) == 6 and c.startswith(("5", "6", "9")):
                    secids.append(f"1.{c}")
                else:
                    secids.append(f"0.{c}")
            codes_str = ",".join(secids)
            
            url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
            params = {
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2,
                "invt": 2,
                "fields": "f12,f2,f183,f184",  # f12=代码, f2=最新价, f183=单位净值, f184=溢价率(实际可能不对)
                "secids": codes_str,
                "_": int(time.time() * 1000)
            }
            resp = requests.get(url, params=params, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            })
            if resp.status_code == 200:
                data = resp.json()
                if data.get('data') and data['data'].get('diff'):
                    for item in data['data']['diff']:
                        code = item.get('f12')
                        price = _safe_float(item.get('f2'))
                        nav = _safe_float(item.get('f183'))
                        f184 = _safe_float(item.get('f184'))
                        if not code:
                            continue
                        try:
                            # 优先使用东方财富直接返回的f184溢价率，最准确，不需要计算
                            premium = None
                            if f184 is not None and abs(f184) < 20:  # 过滤异常值，溢价率超过20%的肯定是错的
                                premium = round(f184, 2)
                            # 只有当f184不存在的时候，才尝试通过最新价和净值计算
                            elif price > 0 and nav > 0 and abs(((price - nav) / nav) * 100) < 20:
                                premium = round(((price - nav) / nav) * 100, 2)
                            # 非交易时间价格为0的时候，不更新溢价率，保留缓存中的历史有效值
                            if premium is not None:
                                results[code] = premium
                                with _lock:
                                    _premium_cache[code] = premium
                        except Exception as e:
                            logger.debug(f"溢价计算失败 {code}: {e}")
            logger.info(f"Premium batch {i//200 +1}: fetched {len(results)} valid premiums")
        except Exception as e:
            logger.warning(f"Premium batch fetch failed: {e}")
        time.sleep(0.5)  # 批次间延迟避免限流
    
    if results:
        _save_premium_cache()
        logger.info(f"Total premium cache size: {len(_premium_cache)}")
    
    return results


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
    # 无论是否交易时间，都可以强制刷新
    if force or FORCE_REFRESH:
        return True
    # 交易时间必须刷新
    if is_trading_time():
        return True
    # 非交易时间每30分钟刷新一次（确保更新时间不会停在15点）
    now = datetime.now(BEIJING_TZ)
    return now.minute % 30 == 0  # 每小时的0分和30分刷新一次非交易时间数据


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


def _fetch_etf_name_from_eastmoney(code: str) -> Optional[str]:
    """从东方财富搜索 API 获取 ETF 真实名称"""
    try:
        # 使用东方财富搜索接口
        url = "https://searchapi.eastmoney.com/api/suggest/get"
        params = {
            "input": code,
            "type": 14,  # 14 表示 ETF/LOF
            "count": 5,
            "market": "",
            "source": "WEB"
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        resp = SESSION.get(url, params=params, headers=headers, timeout=5)
        data = resp.json()
        
        if data and data.get("QuotationCodeTable"):
            items = data["QuotationCodeTable"].get("Data", [])
            for item in items:
                # 匹配代码
                if item.get("Code") == code or item.get("SecurityCode") == code:
                    name = item.get("Name", "").strip()
                    if name and name != f"ETF {code}" and name != f"ETF-{code}":
                        return name
                        
        # 备用：尝试从基金详情页获取
        try:
            fund_url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
            resp = SESSION.get(fund_url, timeout=5)
            text = resp.text
            # 解析基金名称
            import re
            name_match = re.search(r'var\s+fS_name\s*=\s*"([^"]+)"', text)
            if name_match:
                return name_match.group(1).strip()
        except Exception:
            pass
            
    except Exception as e:
        logger.debug(f"Failed to fetch name for {code}: {e}")
    
    return None


def _fetch_all_exchange_funds() -> Dict[str, str]:
    """
    从东方财富获取所有场内基金（包括ETF、LOF、QDII、商品ETF、跨境ETF等）。
    返回 {code: name} 字典。
    """
    funds = {}
    try:
        logger.info("📥 Fetching all exchange funds from Eastmoney...")
        # 获取所有场内基金列表（包括ETF、LOF等），分页获取避免数据截断
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        base_params = {
            "pz": 1000,  # 每页1000条
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            # 全类型覆盖：ETF(10)/LOF(11)/QDII(12)/分级(13)/商品(14)/REITs(15)/股票封基(16)
            "fs": "m:0+t:10,m:1+t:10,m:0+t:11,m:1+t:11,m:0+t:12,m:1+t:12,m:0+t:13,m:1+t:13,m:0+t:14,m:1+t:14,m:0+t:15,m:1+t:15,m:0+t:16,m:1+t:16",
            "fields": "f12,f14,f2",  # f12=代码, f14=名称, f2=价格（过滤无效基金）
        }
        
        # 分页获取，最多获取10页（10000只，覆盖所有场内基金）
        for page in range(1, 11):
            params = dict(base_params)
            params["pn"] = page
            params["_"] = int(time.time() * 1000)
            
            try:
                payload = _request_json(url, params, retries=3)
                if not payload or not payload.get("data"):
                    logger.warning(f"⚠️ Page {page} empty, stop fetching")
                    break
                
                data = payload.get("data")
                diff = data.get("diff")
                if not diff or not isinstance(diff, list) or len(diff) == 0:
                    logger.warning(f"⚠️ Page {page} has no data, stop fetching")
                    break
                
                valid_page_count = 0
                for item in diff:
                    if not isinstance(item, dict):
                        continue
                    code = item.get("f12", "").strip()
                    name = item.get("f14", "").strip()
                    price = item.get("f2", 0.0)
                    # 不过滤价格为0的基金，保留所有合法的场内基金，包括非交易时间没有报价的QDII/LOF
                    if code and name and len(code) == 6 and not name.startswith(("ETF ", "基金")):
                        funds[code] = name
                        valid_page_count += 1
                
                logger.info(f"✅ Page {page} fetched: {valid_page_count} valid funds, total now: {len(funds)}")
                # 如果当前页返回数量小于每页最大数量，说明已经到最后一页
                if len(diff) < params["pz"]:
                    break
                    
                time.sleep(0.5)  # 分页请求间隔
            except Exception as e:
                logger.warning(f"⚠️ Page {page} fetch failed: {e}")
                break
        
        logger.info(f"✅ Successfully fetched {len(funds)} valid exchange funds from Eastmoney")
        # 验证特殊基金是否存在
        special_codes = ['164824', '501018', '160416', '162411', '513030', '513500']
        found_special = {k:v for k,v in funds.items() if k in special_codes}
        if found_special:
            logger.info(f"📋 特殊基金已包含: {list(found_special.items())}")
        else:
            logger.warning(f"⚠️ 未找到特殊基金: {special_codes}，将单独尝试获取")
            # 单独获取缺失的特殊基金
            for code in special_codes:
                if code not in funds:
                    name = _fetch_etf_name_from_eastmoney(code)
                    if name:
                        funds[code] = name
                        logger.info(f"✅ 单独获取到特殊基金 {code}: {name}")
    except Exception as e:
        logger.error(f"❌ Failed to fetch exchange funds: {e}", exc_info=True)
    
    return funds


def _ensure_all_etfs_in_spot() -> None:
    """
    确保所有已知的 ETF/LOF/QDII 都在 etf_spot 中。
    从多个数据源发现并添加缺失的基金。
    """
    global etf_spot
    
    with _lock:
        added_count = 0
        
        # 1. 从东方财富获取所有场内基金（最全面的数据源）
        exchange_funds = _fetch_all_exchange_funds()
        # 先保存原有spot里的有效数据，避免覆盖
        existing_etf_data = etf_spot.copy()
        # 清空spot，然后合并原有数据和新发现的基金
        etf_spot.clear()
        # 先添加原有基金数据
        for code, data in existing_etf_data.items():
            etf_spot[code] = data
        # 再添加新发现的基金，只加原来没有的
        for code, name in exchange_funds.items():
            if code not in etf_spot and len(code) == 6:
                etf_spot[code] = {
                    "code": code,
                    "name": name,
                    "currentPrice": 0.0,
                    "chgPct": 0.0,
                    "scale": 0.0,
                    "source": "exchange_list"
                }
                added_count += 1
                logger.info(f"Added fund {code} ({name}) from exchange list")
        
        # 2. 从 K 线文件中发现的 ETF（补充）
        if KLINE_DIR.exists():
            for kline_file in KLINE_DIR.glob("*.json"):
                code = kline_file.stem
                if code not in etf_spot and len(code) == 6:
                    try:
                        name = _fetch_etf_name_from_eastmoney(code) or f"ETF {code}"
                        etf_spot[code] = {
                            "code": code,
                            "name": name,
                            "currentPrice": 0.0,
                            "chgPct": 0.0,
                            "scale": 0.0,
                            "source": "kline_discovery"
                        }
                        added_count += 1
                        logger.info(f"Added ETF {code} ({name}) from kline file")
                    except Exception as e:
                        logger.debug(f"Failed to process kline for {code}: {e}")
        
        # 3. 从费率缓存中发现的 ETF（补充）
        if FEE_CACHE_FILE.exists():
            try:
                fee_data = json.loads(FEE_CACHE_FILE.read_text(encoding="utf-8"))
                for code in fee_data.keys():
                    if code not in etf_spot and len(code) == 6:
                        name = _fetch_etf_name_from_eastmoney(code) or f"ETF {code}"
                        etf_spot[code] = {
                            "code": code,
                            "name": name,
                            "currentPrice": 0.0,
                            "chgPct": 0.0,
                            "scale": 0.0,
                            "source": "fee_discovery"
                        }
                        added_count += 1
                        logger.info(f"Added ETF {code} ({name}) from fee cache")
            except Exception as e:
                logger.debug(f"Failed to read fee cache: {e}")
        
        if added_count > 0:
            logger.info(f"Added {added_count} funds from all discovery sources")


def _prioritized_codes(limit: int = 0) -> List[str]:
    """
    返回按规模排序的 ETF 代码列表。
    包括所有在 etf_spot 中的 ETF，确保没有遗漏。
    
    Args:
        limit: 限制返回数量，0 表示返回全部
    
    排序规则：
    1. 按规模（scale）降序排列
    2. 规模相同的按成交量（turnover）降序排列
    """
    # 首先确保所有已知的 ETF 都在 spot 中
    _ensure_all_etfs_in_spot()
    
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

        # 异步获取溢价数据（不阻塞主线程）
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果在运行中的事件循环中，使用create_task
                asyncio.create_task(fetch_premium_for_spot_async(new_spot))
                logger.info("Premium fetch scheduled as background task")
            else:
                # 否则直接运行
                new_spot = loop.run_until_complete(fetch_premium_for_spot_async(new_spot))
        except RuntimeError as e:
            logger.warning("Async premium fetch skipped: %s", e)

        index_provider, new_indices = fetch_indices_live()

        with _lock:
            # 先保留原有所有基金的基本信息
            all_funds = etf_spot.copy()
            # 用新获取的行情数据更新已有基金
            for code, data in new_spot.items():
                all_funds[code] = data
            # 确保所有已经发现的基金都在列表里，即使没有行情数据
            for code, fund_info in all_funds.items():
                # 没有行情数据的基金保留基本信息，其他字段设为默认值
                if not fund_info.get("currentPrice"):
                    fund_info["currentPrice"] = 0.0
                    fund_info["chgPct"] = 0.0
                    fund_info["volume"] = 0
                    fund_info["turnover"] = 0.0
            # 更新etf_spot
            etf_spot.clear()
            etf_spot.update(all_funds)

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
    global data_source, live_provider, _async_client

    _load_fee_cache()
    cache_loaded = load_spot_cache()

    # Startup: always try one live refresh first.
    # 在后台线程中执行，避免阻塞启动
    def startup_refresh():
        time.sleep(2)  # 等待服务完全启动
        refresh_spot(force=True)
    
    threading.Thread(target=startup_refresh, daemon=True).start()

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

    # 确保所有场内基金都在列表中（包括ETF、LOF、QDII等）
    # 使用延迟执行，避免启动时阻塞
    def delayed_discovery():
        time.sleep(5)
        _ensure_all_etfs_in_spot()
        logger.info("After ETF discovery: total etfs=%s", len(etf_spot))
    
    threading.Thread(target=delayed_discovery, daemon=True).start()

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
    # 添加收盘溢价保存任务 - 上午收盘 11:30
    scheduler.add_job(
        lambda: save_close_premium_at_market_close("morning"),
        "cron",
        hour=11,
        minute=30,
        id="save_morning_close_premium",
        max_instances=1,
        coalesce=True,
    )
    # 添加收盘溢价保存任务 - 下午收盘 15:00
    scheduler.add_job(
        lambda: save_close_premium_at_market_close("afternoon"),
        "cron",
        hour=15,
        minute=0,
        id="save_afternoon_close_premium",
        max_instances=1,
        coalesce=True,
    )
    # 添加独立溢价刷新任务，每5分钟运行一次
    scheduler.add_job(
        refresh_all_premium,
        "interval",
        minutes=5,
        id="premium_refresh",
        max_instances=1,
        coalesce=True,
    )
    # 添加每日基金发现任务，每天凌晨2点执行，发现新增ETF/LOF
    scheduler.add_job(
        _ensure_all_etfs_in_spot,
        "cron",
        hour=2,
        minute=0,
        id="etf_discovery",
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
    
    # 关闭异步HTTP客户端
    global _async_client
    if _async_client:
        await _async_client.aclose()
        _async_client = None


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
            # 添加溢价数据（如果存在）
            if "premium" not in row and code in _premium_cache:
                row["premium"] = _premium_cache[code]
            row.setdefault("premium", None)
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
