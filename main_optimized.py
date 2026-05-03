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
from apscheduler.triggers.cron import CronTrigger
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


REFRESH_MINUTES = _env_int("REFRESH_MINUTES", 5)
KLINE_REFRESH_MINUTES = _env_int("KLINE_REFRESH_MINUTES", 180)
KLINE_BATCH_SIZE = max(1, _env_int("KLINE_BATCH_SIZE", 2))
# KLINE_TOP_N: 控制每批 K 线刷新处理的 ETF 数量
# 0 或负数表示处理全部 ETF（按规模降序）
# 正数表示只处理前 N 只规模较大的 ETF
KLINE_TOP_N = _env_int("KLINE_TOP_N", 0)
FORCE_REFRESH = _env_bool("FORCE_REFRESH", False)

REQUEST_TIMEOUT_SECONDS = _env_float("REQUEST_TIMEOUT_SECONDS", 12.0)
API_BASE_INTERVAL = _env_float("API_BASE_INTERVAL", 5.0)      # 基础间隔 5s，最小化触发东方财富 IP 风控
API_MAX_INTERVAL = _env_float("API_MAX_INTERVAL", 30.0)       # 最大退避间隔 30s
SECONDARY_API_INTERVAL = _env_float("SECONDARY_API_INTERVAL", 1.0)  # 新浪接口间隔也提高
CIRCUIT_BREAKER_THRESHOLD = _env_int("CIRCUIT_BREAKER_THRESHOLD", 4)
CIRCUIT_BREAKER_COOLDOWN = _env_int("CIRCUIT_BREAKER_COOLDOWN", 180)
CIRCUIT_BREAKER_MAX_COOLDOWN = _env_int("CIRCUIT_BREAKER_MAX_COOLDOWN", 1800)  # 风控持续时冷却最长 30 分钟

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
SPOT_CACHE = DATA_DIR / "spot_cache.json"
CACHE_VERSION = "2"  # increment when data schema or calculation logic changes
FEE_CACHE_FILE = DATA_DIR / "fee_cache.json"
NAV_CACHE_FILE = DATA_DIR / "nav_cache.json"
KLINE_DIR = DATA_DIR / "kline"
KLINE_DIR.mkdir(exist_ok=True)

# 溢价数据缓存
_premium_cache: Dict[str, float] = {}
_premium_cache_file = DATA_DIR / "premium_cache.json"

# 收盘溢价数据缓存
_close_premium_cache: Dict[str, Dict] = {}
_close_premium_cache_file = DATA_DIR / "close_premium_cache.json"

# NAV缓存：LOF/QDII/REIT基金每日公布的单位净值
_nav_cache: Dict[str, Dict] = {}  # {code: {"nav": float, "date": str}}


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


def _load_nav_cache() -> None:
    global _nav_cache
    if NAV_CACHE_FILE.exists():
        try:
            _nav_cache = json.loads(NAV_CACHE_FILE.read_text(encoding="utf-8"))
            logger.info("NAV cache loaded: %d funds", len(_nav_cache))
        except Exception:
            _nav_cache = {}
    else:
        _nav_cache = {}


def _save_nav_cache() -> None:
    try:
        NAV_CACHE_FILE.write_text(json.dumps(_nav_cache, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        logger.debug("Save nav cache failed: %s", exc)


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

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

_REFERER_POOL = [
    "https://quote.eastmoney.com/",
    "https://quote.eastmoney.com/center/gridlist.html",
    "https://data.eastmoney.com/etf/default.html",
    "https://fund.eastmoney.com/",
    "https://www.eastmoney.com/",
]


def _rotate_headers():
    SESSION.headers["User-Agent"] = random.choice(_UA_POOL)
    SESSION.headers["Referer"] = random.choice(_REFERER_POOL)


SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": random.choice(_REFERER_POOL),
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

# 独立的费率采集 session，不经过共享限流器，避免拖慢行情请求
_FEE_SESSION = requests.Session()
_FEE_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://fundf10.eastmoney.com/",
})
_fee_rate_lock = threading.Lock()
_fee_last_call_at: float = 0.0
_FEE_INTERVAL = 0.8  # 费率页面请求间隔（秒）

# 独立的 pingzhongdata 采集 session（规模补充用），不占主限流器
_PINGZHONG_SESSION = requests.Session()
_PINGZHONG_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "http://fund.eastmoney.com/",
})
_pingzhong_rate_lock = threading.Lock()
_pingzhong_last_call_at: float = 0.0
_PINGZHONG_INTERVAL = 1.0  # pingzhong 请求间隔（秒）

# 独立的 gmbd (FundArchivesDatas) 采集 session，用于 QDII ETF 总规模
# fundf10.eastmoney.com 与 fee 页面同主机，使用独立限流
_GMBD_SESSION = requests.Session()
_GMBD_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://fundf10.eastmoney.com/",
})
_gmbd_rate_lock = threading.Lock()
_gmbd_last_call_at: float = 0.0
_GMBD_INTERVAL = 0.8  # gmbd 请求间隔（秒），与 _FEE_INTERVAL 一致

# QDII 规模缓存: {code: {"scale": float, "ftype": str, "is_qdii": bool, "date": str}}
_qdii_scale_cache: Dict[str, Dict] = {}

# 溢价刷新时间戳（非交易时段限频用）
_premium_last_full_refresh: float = 0.0


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

        _rotate_headers()
        if wait_time > 0:
            time.sleep(wait_time + random.uniform(2.0, 5.0))

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
                # 渐进冷却：streak 越高冷却越长，避免 IP 风控期间反复触发
                # base 180s → streak 5: 360s → streak 10: 540s → ... 上限 1800s (30min)
                multiplier = 1 + (self.failure_streak - CIRCUIT_BREAKER_THRESHOLD) // 2
                cooldown = min(CIRCUIT_BREAKER_MAX_COOLDOWN, CIRCUIT_BREAKER_COOLDOWN * multiplier)
                self.breaker_until = time.time() + cooldown
                logger.warning(
                    "Circuit breaker opened for %ss (failure streak=%s)",
                    cooldown,
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
            "version": CACHE_VERSION,
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
        if data.get("version") != CACHE_VERSION:
            logger.warning("Spot cache version mismatch (got %s, want %s), discarding", data.get("version"), CACHE_VERSION)
            return False

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


def _calc_scale(row: Dict, code: str = "") -> float:
    """从 spot 列表 API 一行计算总规模（亿元）。0 表示无法解析。

    优先 f20（列表 API 总市值，最稳定），其次 f441×f38，再次 NAV cache×shares。
    f21（流通市值）和 f6（成交额）都不代表总规模——已移除。
    f441 偶尔被 Eastmoney 以 10× 放大返回（部分 LOF），与 f20 交叉校验后取 f20。
    """
    total_mv = _safe_float(row.get("f20"))
    iopv = _safe_float(row.get("f441"))
    shares = _safe_float(row.get("f38"))

    # 1) f20 总市值——最稳定，Eastmoney 一致按净值×份额计算
    if total_mv > 1e7:
        scale_f20 = round(total_mv / 1e8, 2)
        # 与 f441×f38 交叉校验：相差 > 5× 时怀疑 f441 单位异常，信任 f20
        if iopv > 0 and shares > 0:
            scale_iopv = iopv * shares / 1e8
            if scale_iopv > 0 and (
                scale_iopv / scale_f20 > 5 or scale_f20 / scale_iopv > 5
            ):
                logger.warning(
                    "Scale unit anomaly %s: iopv×shares=%.2f 亿 vs f20=%.2f 亿, prefer f20",
                    code, scale_iopv, scale_f20,
                )
        return scale_f20

    # 2) f441 × f38（NAV × 份额）
    if iopv > 0 and shares > 0:
        return round(iopv * shares / 1e8, 2)

    # 3) NAV cache × shares（LOF/QDII：列表 API 当日 f441 缺失时）
    if shares > 0 and code:
        nav = _nav_cache.get(code, {}).get("nav", 0)
        if nav > 0:
            return round(nav * shares / 1e8, 2)

    # 4) f20 在「万元」单位（极少见兜底）
    if total_mv > 0:
        return round(total_mv / 1e4, 2)

    return 0.0


def _parse_spot_row(row: Dict, scale_hints: Optional[Dict[str, float]] = None) -> Optional[Dict]:
    code = str(row.get("f12", "")).zfill(6)
    name = str(row.get("f14", "")).strip()
    price = _safe_float(row.get("f2"))

    # 不过滤价格为0的基金，即使非交易时间没有报价也要显示所有基金
    if len(code) != 6 or not name:
        return None

    fee_detail = _get_fee_detail(code)
    fee_total = round(sum(fee_detail.values()), 2) if fee_detail else None

    # ============================================================
    # 溢价率 & 净值计算（2026-04-20 重大修复）
    # ============================================================
    # 关键发现（来自AKShare源码验证）：
    # - f184 在ETF列表API中是"主力净流入-净占比"，不是溢价率！
    # - f402 是"基金折价率"（= -溢价率），这才是正确的溢价率字段
    # - f441 是"IOPV实时估值"（ETF参考净值），已在_calc_scale中使用
    # - f183 在ETF列表API中含义不明确，不可作为净值
    #
    # 溢价率计算策略（优先级）：
    # 1. f402(基金折价率) - 最直接，折价率的负数就是溢价率
    # 2. f441(IOPV) + price 手动计算 - 溢价率 = (price - IOPV) / IOPV * 100
    # 3. premium_cache 缓存 - 非交易时间保留历史有效值

    f402_raw = _safe_float(row.get("f402"))  # 基金折价率(%)
    f441_raw = _safe_float(row.get("f441"))  # IOPV实时估值

    # 优先级1：使用f402折价率（折价率 = -溢价率）
    premium_value = None
    nav_value = None
    premium_source = None

    # 非交易时段：nav_cache+当日收盘价 比 Eastmoney 返回的陈旧 f402/f441 更准确，优先使用
    if not is_trading_time():
        _nt_nav = _nav_cache.get(code)
        if _nt_nav and _nt_nav.get("nav", 0) > 0:
            _nt_nav_val = _nt_nav["nav"]
            _nt_price = price if price > 0 else 0
            if _nt_price <= 0:
                with _lock:
                    _nt_price = etf_spot.get(code, {}).get("currentPrice") or 0
            if _nt_price <= 0:
                _nt_price = _safe_float(row.get("f18"))
            if _nt_nav_val > 0 and _nt_price > 0:
                _nt_calc = round(((_nt_price - _nt_nav_val) / _nt_nav_val) * 100, 2)
                if abs(_nt_calc) < 30:
                    premium_value = _nt_calc
                    premium_source = "nav_cache_nontrading"
                    with _lock:
                        _premium_cache[code] = premium_value

    if f402_raw is not None and f402_raw != 0 and abs(f402_raw) < 30 and premium_value is None:
        # f402是折价率，溢价率 = -折价率
        premium_value = round(-f402_raw, 2)
        premium_source = "f402_discount"
        # 同步更新缓存
        with _lock:
            _premium_cache[code] = premium_value

    # 优先级2：使用f441(IOPV) + price计算溢价率
    if premium_value is None and f441_raw is not None and f441_raw > 0 and price > 0:
        iopv = f441_raw
        # IOPV可能被放大1000倍（东方财富API特性），需判断
        if iopv > 100 and price < 100:
            iopv = iopv / 1000
        if iopv > 0 and abs(price - iopv) > 0.0001:
            calc_premium = round(((price - iopv) / iopv) * 100, 2)
            if abs(calc_premium) < 30:
                premium_value = calc_premium
                premium_source = "f441_iopv_calc"
                with _lock:
                    _premium_cache[code] = premium_value

    # 优先级3：用 NAV 缓存 + 实时价格计算（每日公布净值，比陈旧 premium_cache 更准）
    # 价格优先级：f2(实时价) > etf_spot currentPrice(今日收盘) > f18(昨收)
    if premium_value is None:
        nav_entry = _nav_cache.get(code)
        if nav_entry:
            nav_val = nav_entry.get("nav", 0)
            ref_price = price if price > 0 else 0
            if ref_price <= 0:
                with _lock:
                    ref_price = etf_spot.get(code, {}).get("currentPrice") or 0
            if ref_price <= 0:
                ref_price = _safe_float(row.get("f18"))
            if nav_val > 0 and ref_price > 0:
                calc = round(((ref_price - nav_val) / nav_val) * 100, 2)
                if abs(calc) < 30:
                    premium_value = calc
                    premium_source = "nav_cache"
                    with _lock:
                        _premium_cache[code] = premium_value

    # 优先级4：陈旧 premium 缓存兜底
    if premium_value is None:
        with _lock:
            cached_premium = _premium_cache.get(code)
        if cached_premium is not None and cached_premium != 0 and abs(cached_premium) < 30:
            premium_value = round(cached_premium, 2)
            premium_source = "cache"

    # 净值(IOPV)：f441即为IOPV参考净值
    if f441_raw is not None and f441_raw > 0:
        iopv_nav = f441_raw
        if iopv_nav > 100 and price < 100:
            iopv_nav = iopv_nav / 1000
        nav_value = round(iopv_nav, 4)
    else:
        # 保留历史净值
        with _lock:
            cached_nav = etf_spot.get(code, {}).get("nav") if code in etf_spot else None
        if cached_nav is not None and cached_nav != 0:
            nav_value = round(cached_nav, 4) if isinstance(cached_nav, float) else cached_nav

    # 规模：优先使用 scale_hints（refresh_all_scales 用 ulist f117 写入的权威值），
    # 仅当无 hint 时才回退到 _calc_scale (f441×f38)。否则每 2 分钟 spot 刷新
    # 会把 refresh_all_scales 的更新值覆盖回 f441×f38 的滞后估算。
    hinted_scale = _safe_float(scale_hints.get(code), 0.0) if scale_hints else 0.0
    scale_value = round(hinted_scale, 2) if hinted_scale > 0 else _calc_scale(row, code)

    result = {
        "code": code,
        "name": name,
        "currentPrice": round(price, 4),
        "chgPct": round(_safe_float(row.get("f3")), 2),
        "scale": scale_value,
        "volume": int(_safe_float(row.get("f5"))),
        "turnover": round(_safe_float(row.get("f6")) / 1e8, 2),
        "fee": fee_total,
        "feeDetail": _format_fee_detail(fee_detail),
        "open": round(_safe_float(row.get("f17")), 4),
        "high": round(_safe_float(row.get("f15")), 4),
        "low": round(_safe_float(row.get("f16")), 4),
        "prevClose": round(_safe_float(row.get("f18")), 4),
    }

    # 设置溢价率和净值
    if premium_value is not None:
        result["premium"] = premium_value
        result["_premium_source"] = premium_source
    if nav_value is not None:
        result["nav"] = nav_value

    return result


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


def _fetch_spot_from_endpoint(url: str, scale_hints: Optional[Dict[str, float]] = None) -> Dict[str, Dict]:
    base_params = {
        "pn": "1",
        "pz": "1000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f12",
        "fs": "m:0+t:10,m:1+t:10,m:0+t:11,m:1+t:11,m:0+t:12,m:1+t:12,m:0+t:13,m:1+t:13,m:0+t:14,m:1+t:14,m:0+t:15,m:1+t:15,m:0+t:16,m:1+t:16,m:0+t:17,m:1+t:17,m:0+t:18,m:1+t:18,m:0+t:20,m:1+t:20",
        "fields": "f2,f3,f5,f6,f12,f14,f15,f16,f17,f18,f20,f21,f38,f441,f402,f183,f184",
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
        parsed = _parse_spot_row(row, scale_hints=scale_hints)
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


def _fetch_indices_from_tencent() -> List[Dict]:
    """Tencent 指数源 (qt.gtimg.cn)，作为 Eastmoney/Sina 都失败时的兜底。"""
    targets = [("sh000001", "上证指数"), ("sz399001", "深证成指"), ("sh000300", "沪深300")]
    query = ",".join(s for s, _ in targets)
    text = _request_text_sina(
        f"https://qt.gtimg.cn/q={query}",
        params={},
        retries=2,
        headers={"Referer": "https://stockapp.finance.qq.com"},
    )
    name_map = dict(targets)
    result = []
    for line in text.strip().split("\n"):
        if "~" not in line:
            continue
        # 形如: v_sh000001="1~上证指数~000001~3300.12~prev_close~open~..."
        left, _, right = line.partition("=")
        symbol = left.replace("v_", "").strip()
        if symbol not in name_map:
            continue
        parts = right.strip().strip(';').strip('"').split("~")
        if len(parts) < 6:
            continue
        try:
            val = float(parts[3]) if parts[3] else 0.0
            prev = float(parts[4]) if parts[4] else 0.0
        except ValueError:
            continue
        if val <= 0:
            continue
        chg_pct = round((val - prev) / prev * 100, 2) if prev > 0 else 0.0
        result.append({"name": name_map[symbol], "val": round(val, 2), "chg": chg_pct})
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
    获取单个 ETF 溢价率数据（备用方法）。
    
    使用列表API的f402(基金折价率)和f441(IOPV)字段。
    优先使用批量方法_fetch_premium_batch_sync，此函数仅用于单条补全。
    
    修复记录(2026-04-20)：
    - f184在ETF列表API中是"主力净流入-净占比"，不是溢价率！
    - 东方财富单条股票API对ETF的f183/f184始终返回0
    - 改用列表API的f402(基金折价率)和f441(IOPV)字段
    """
    # 方法1：从列表API获取f402/f441
    for url in SPOT_ENDPOINTS:
        try:
            params = {
                "pn": "1", "pz": "10", "po": "1", "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2", "invt": "2", "fid": "f12",
                "fs": "m:0+t:10,m:1+t:10,m:0+t:11,m:1+t:11,m:0+t:12,m:1+t:12,m:0+t:13,m:1+t:13,m:0+t:14,m:1+t:14,m:0+t:15,m:1+t:15,m:0+t:16,m:1+t:16,m:0+t:17,m:1+t:17,m:0+t:18,m:1+t:18,m:0+t:20,m:1+t:20",
                "fields": "f2,f12,f402,f441",
            }
            payload = _request_json(url, params, retries=1)
            rows = (payload.get("data") or {}).get("diff") or []
            for row in rows:
                rcode = str(row.get("f12", "")).zfill(6)
                if rcode == code:
                    # 优先f402折价率
                    f402 = _safe_float(row.get("f402"))
                    if f402 is not None and f402 != 0 and abs(f402) < 30:
                        return round(-f402, 2)
                    # 备用f441 IOPV计算
                    f441 = _safe_float(row.get("f441"))
                    price = _safe_float(row.get("f2"))
                    with _lock:
                        spot_price = etf_spot.get(code, {}).get("currentPrice", 0) if price <= 0 else price
                    if f441 is not None and f441 > 0 and spot_price > 0:
                        iopv = f441 / 1000 if f441 > 100 and spot_price < 100 else f441
                        if iopv > 0:
                            premium = ((spot_price - iopv) / iopv) * 100
                            if abs(premium) < 30:
                                return round(premium, 2)
            break  # 第一个endpoint有数据就不再尝试
        except Exception:
            continue
    
    # 方法2：使用缓存
    with _lock:
        cached = _premium_cache.get(code)
    if cached is not None and cached != 0 and abs(cached) < 30:
        return round(cached, 2)
    
    return None


def fetch_spot_live(scale_hints: Dict[str, float]) -> Tuple[str, Dict[str, Dict]]:
    last_err = None

    # Primary: Eastmoney
    for endpoint in SPOT_ENDPOINTS:
        try:
            spot = _fetch_spot_from_endpoint(endpoint, scale_hints=scale_hints)
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


def _tencent_prefix(code: str) -> str:
    """Return Tencent exchange prefix for a fund code."""
    if code[0] == "5":
        return "sh"
    if code[:2] in ("15", "16"):
        return "sz"
    if code[0] == "0":
        return "sz"
    return "sh"


def _supplement_with_tencent(spot: Dict[str, Dict], fund_names: Dict[str, str]) -> int:
    """
    For codes missing price in `spot`, query Tencent real-time quote API and fill them in.
    Returns count of codes filled.
    """
    missing = [code for code, data in spot.items() if not data.get("currentPrice")]
    if not missing:
        return 0

    filled = 0
    batch_size = 80
    for i in range(0, len(missing), batch_size):
        batch = missing[i : i + batch_size]
        query = ",".join(_tencent_prefix(c) + c for c in batch)
        try:
            text = _request_text_sina(
                f"https://qt.gtimg.cn/q={query}",
                params={},
                headers={"Referer": "https://stockapp.finance.qq.com"},
            )
            for line in text.strip().split("\n"):
                if "~" not in line:
                    continue
                parts = line.split("~")
                if len(parts) < 35:
                    continue
                code = parts[2]
                if code not in spot:
                    continue
                try:
                    price = float(parts[3]) if parts[3] else 0.0
                    prev_close = float(parts[4]) if parts[4] else 0.0
                    if price <= 0:
                        continue
                    open_ = float(parts[5]) if parts[5] else 0.0
                    high = float(parts[33]) if parts[33] else price
                    low = float(parts[34]) if parts[34] else price
                    volume = int(float(parts[6])) if parts[6] else 0
                    turnover = float(parts[37]) if len(parts) > 37 and parts[37] else 0.0
                    chg_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0
                    spot[code].update({
                        "currentPrice": round(price, 4),
                        "chgPct": chg_pct,
                        "prevClose": round(prev_close, 4),
                        "open": round(open_, 4),
                        "high": round(high, 4),
                        "low": round(low, 4),
                        "volume": volume,
                        "turnover": round(turnover, 2),
                    })
                    filled += 1
                except (ValueError, IndexError):
                    continue
        except Exception as exc:
            logger.warning("Tencent quote supplement failed (batch %d): %s", i // batch_size, exc)
        time.sleep(0.3)

    logger.info("Tencent supplement filled %d/%d missing-price ETFs", filled, len(missing))
    return filled


# QDII 候选名称关键词（海外市场追踪）
_QDII_NAME_KEYWORDS = (
    "纳指", "标普", "纳斯达克", "道琼", "德国", "DAX", "法国", "英国",
    "欧洲", "日经", "日本", "印度", "越南", "东南亚", "原油", "油气",
    "QDII", "海外", "中概", "全球", "白银", "豆粕", "有色金属",
)


def _is_qdii_candidate(code: str, name: str) -> bool:
    """Quick name-based pre-screen for QDII/overseas ETF candidates.

    Intentionally broad — gmbd API 的 FTYPE 字段会做权威确认或排除。
    已确认 is_qdii=False 的（如黄金 ETF）后续直接跳过。
    """
    if not name:
        return False
    with _lock:
        if code in etf_spot:
            flag = etf_spot[code].get("is_qdii")
            if flag is True:
                return True
            if flag is False:
                return False
    return any(kw in name for kw in _QDII_NAME_KEYWORDS)


def _fetch_qdii_scale_from_gmbd(code: str) -> Optional[Dict]:
    """从 FundArchivesDatas gmbd API 获取 QDII ETF 净资产规模（含场外联接基金）。

    返回 {"scale": float(亿元), "ftype": str, "is_qdii": bool, "date": str}，
    失败返回 None。使用独立 _GMBD_SESSION 限流。
    """
    global _gmbd_last_call_at

    cached = _qdii_scale_cache.get(code)
    if cached and cached.get("date") == _today_bj_str():
        return cached

    try:
        with _gmbd_rate_lock:
            elapsed = time.time() - _gmbd_last_call_at
            wait = _GMBD_INTERVAL - elapsed
            if wait > 0:
                time.sleep(wait)
            _gmbd_last_call_at = time.time()

        url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
        params = {"type": "gmbd", "code": code}
        resp = _GMBD_SESSION.get(url, params=params, timeout=10)
        resp.raise_for_status()
        text = resp.text
        if not text:
            return None

        import re as _re

        # gmbd API 返回 JS 赋值：var gmbd_apidata={content:"...",summary:"...",data:[{...}]}
        # 取 data 数组第一条（最新季度）的 NETNAV 和 FTYPE
        netnav = 0.0
        ftype = ""
        is_qdii = False

        # 提取 data 数组第一个对象的 NETNAV 和 FTYPE
        data_m = _re.search(r'"data"\s*:\s*\[', text)
        if data_m:
            snippet = text[data_m.end():]
            # NETNAV — 净资产（元）
            netnav_m = _re.search(r'"NETNAV"\s*:\s*([\d.]+)', snippet)
            if netnav_m:
                netnav = _safe_float(netnav_m.group(1))
            # FTYPE — 基金类型
            ftype_m = _re.search(r'"FTYPE"\s*:\s*"([^"]+)"', snippet)
            if ftype_m:
                ftype = ftype_m.group(1).strip()

        is_qdii = "QDII" in ftype or "海外" in ftype

        scale_yi = round(netnav / 1e8, 2) if netnav > 1e6 else 0.0

        result = {
            "scale": scale_yi,
            "ftype": ftype,
            "is_qdii": is_qdii,
            "date": _today_bj_str(),
        }
        _qdii_scale_cache[code] = result
        return result

    except Exception as exc:
        logger.debug("gmbd scale fetch failed for %s: %s", code, exc)
        return None


def _refresh_qdii_scales(force: bool = False) -> None:
    """QDII ETF 规模修正：用 gmbd API 的 NETNAV 覆盖 ulist 场内规模。

    ulist f117/f38 对 QDII ETF 只返回场内份额/市值，而大部分份额通过场外联接基金持有。
    gmbd API 返回的 NETNAV 是包含场外的净资产规模，与 fundf10 页面一致。

    在 refresh_all_scales() 之后调用，同时设置 is_qdii 标志供后续识别。
    force=True 时无条件刷新（启动时）；否则当天已刷新过的 QDII 跳过。
    """
    if not etf_spot:
        return

    # 非 force 模式：检查今天是否已刷新过（gmbd 数据季度更新，无需高频采集）
    today = _today_bj_str()
    if not force:
        # 如果所有已知 QDII 今天都已缓存，直接跳过
        with _lock:
            known_qdii = [c for c, info in etf_spot.items() if info.get("is_qdii") is True]
        if known_qdii and all(
            _qdii_scale_cache.get(c, {}).get("date") == today for c in known_qdii
        ):
            logger.debug("QDII scales already refreshed today, skipping")
            return

    candidates = []
    with _lock:
        for code, info in etf_spot.items():
            flag = info.get("is_qdii")
            if flag is True:
                candidates.append(code)
            elif flag is False:
                continue
            elif _is_qdii_candidate(code, info.get("name", "")):
                candidates.append(code)

    if not candidates:
        logger.debug("No QDII candidates found, skip gmbd scale refresh")
        return

    logger.info("Starting QDII scale refresh for %d candidates via gmbd API...", len(candidates))

    overridden = 0
    confirmed = 0
    rejected = 0

    for code in candidates:
        result = _fetch_qdii_scale_from_gmbd(code)
        if result is None:
            continue

        is_qdii = result.get("is_qdii", False)
        gmbd_scale = result.get("scale", 0.0)

        with _lock:
            if code not in etf_spot:
                continue
            etf_spot[code]["is_qdii"] = is_qdii

            if is_qdii and gmbd_scale > 0:
                old_scale = _safe_float(etf_spot[code].get("scale"))
                etf_spot[code]["scale"] = gmbd_scale
                overridden += 1
                confirmed += 1
                if abs(gmbd_scale - old_scale) > 1.0:
                    logger.info(
                        "QDII scale override %s: ulist=%.2f亿 -> gmbd=%.2f亿 (FTYPE=%s)",
                        code, old_scale, gmbd_scale, result.get("ftype", ""),
                    )
            elif not is_qdii:
                rejected += 1

    if overridden > 0:
        save_spot_cache()

    logger.info(
        "QDII scale refresh done: candidates=%d overridden=%d confirmed=%d rejected=%d",
        len(candidates), overridden, confirmed, rejected,
    )


def _supplement_scale_from_pingzhong(codes: List[str]) -> int:
    """
    For ETFs with scale=0, fetch quarterly scale data from pingzhongdata JS
    (Data_fluctuationScale field) and update etf_spot in place.
    Uses dedicated _PINGZHONG_SESSION (1s interval), does not touch main rate limiter.
    Returns count of codes filled.
    """
    global _pingzhong_last_call_at
    import re as _re
    filled = 0
    for code in codes:
        try:
            with _pingzhong_rate_lock:
                elapsed = time.time() - _pingzhong_last_call_at
                wait = _PINGZHONG_INTERVAL - elapsed
                if wait > 0:
                    time.sleep(wait)
                _pingzhong_last_call_at = time.time()

            url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
            resp = _PINGZHONG_SESSION.get(url, timeout=10)
            resp.raise_for_status()
            text = resp.text
            if not text:
                continue
            m = _re.search(r'var\s+Data_fluctuationScale\s*=\s*(\{.*?\})\s*;', text, _re.DOTALL)
            if not m:
                continue
            data = json.loads(m.group(1))
            series = data.get('series', [])
            if not series:
                continue
            latest = series[-1].get('y')
            if latest and float(latest) > 0:
                scale_val = round(float(latest), 2)
                with _lock:
                    if code in etf_spot:
                        existing = etf_spot[code].get('scale') or 0
                        if not existing or existing == 0:
                            etf_spot[code]['scale'] = scale_val
                            filled += 1
        except Exception as exc:
            logger.debug("Scale supplement failed for %s: %s", code, exc)

    logger.info("Pingzhong scale supplement filled %d/%d zero-scale ETFs", filled, len(codes))
    return filled


def _fetch_scale_via_ulist_batch(codes: List[str]) -> Dict[str, float]:
    """
    使用 Eastmoney 单条行情后端（ulist.np/get）批量获取最新规模。
    与 APP/网页报价页一致——比列表 API 的 f441×f38 更新及时。

    多字段兜底（按优先级）：
      f117 (总市值)        — ETF 主力字段
      f164 (流通市值)      — 部分基金
      f20  (总市值，list)  — LOF/REIT 兜底
      f441×f38             — 最后兜底
    单位检测：> 1e7 视为元，> 0 视为万元；< 0 视为缺失。

    每批 50 个 secid，复用全局 _request_json 限流器/熔断器。
    """
    BATCH_SIZE = 50
    url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
    result: Dict[str, float] = {}
    sample_logged = False

    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i : i + BATCH_SIZE]
        secids = ",".join(
            f"1.{c}" if c.startswith(("5", "6", "9")) else f"0.{c}"
            for c in batch
        )
        params = {
            "secids": secids,
            "fields": "f12,f13,f14,f20,f38,f117,f164,f441",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
        }
        try:
            payload = _request_json(url, params, retries=2)
        except Exception as exc:
            logger.warning("Scale ulist batch failed (offset=%d): %s", i, exc)
            continue

        diff = (payload.get("data") or {}).get("diff") or []
        if not diff:
            logger.warning("Scale ulist batch empty (offset=%d, sent=%d)", i, len(batch))
            continue

        for row in diff:
            code = str(row.get("f12", "")).zfill(6)
            if not code:
                continue

            scale = _scale_from_row_fields(row, code)
            if scale > 0:
                result[code] = scale

            if not sample_logged:
                logger.info(
                    "ulist sample %s: f117=%s f164=%s f20=%s f38=%s f441=%s -> %s",
                    code, row.get("f117"), row.get("f164"), row.get("f20"),
                    row.get("f38"), row.get("f441"), scale,
                )
                sample_logged = True

    return result


def _scale_from_row_fields(row: Dict, code: str = "") -> float:
    """从一行 secid 返回中按优先级取规模（亿元）。0 表示无法解析。

    注意：f164 是「流通市值」，对货币 ETF / 大体量 LOF 远小于总规模
    （机构持有部分不计入流通），不能作为总规模 fallback——已移除。
    """
    f117 = _safe_float(row.get("f117"))
    f20 = _safe_float(row.get("f20"))
    iopv = _safe_float(row.get("f441"))
    shares = _safe_float(row.get("f38"))

    # 1) f117 总市值（主流 ETF 的权威字段）
    if f117 > 1e7:
        return round(f117 / 1e8, 2)

    # 2) f20 列表 API 总市值（LOF/QDII/REIT 多走这条；f117 常返回 "-"）
    if f20 > 1e7:
        return round(f20 / 1e8, 2)

    # 3) f441 × f38 (IOPV × 份额)。Eastmoney 偶尔对 LOF 返回 10× 放大值，
    #    若同时拿到 f20 则做 sanity check：相差 > 5× 时优先 f20。
    if iopv > 0 and shares > 0:
        scale_iopv = round(iopv * shares / 1e8, 2)
        if f20 > 0:
            scale_f20 = round(f20 / 1e8 if f20 > 1e6 else f20 / 1e4, 2)
            if scale_f20 > 0 and (
                scale_iopv / scale_f20 > 5 or scale_f20 / scale_iopv > 5
            ):
                logger.warning(
                    "Scale unit anomaly %s: iopv×shares=%s 亿 vs f20=%s 亿, prefer f20",
                    code or row.get("f12"), scale_iopv, scale_f20,
                )
                return scale_f20
        return scale_iopv

    # 4) NAV cache × shares（LOF/QDII：当日 f441 缺失时用每日公布净值）
    if shares > 0 and code:
        nav = _nav_cache.get(code, {}).get("nav", 0)
        if nav > 0:
            return round(nav * shares / 1e8, 2)

    # 5) 兜底：f117/f20 在「万元」级别（极少见）
    if f117 > 0:
        return round(f117 / 1e4, 2)
    if f20 > 0:
        return round(f20 / 1e4, 2)

    return 0.0


def refresh_all_scales(force: bool = False) -> None:
    """
    刷新所有 ETF/LOF/REIT 的最新规模，使用单条行情后端 f117（与 Eastmoney APP 一致）。
    覆盖 etf_spot[code]['scale']——比 _calc_scale 的 f441×f38 更新及时。

    频控：复用全局 _request_json 限流器（API_BASE_INTERVAL=5.0s/次），
          1500 只 / 50 一批 ≈ 30 次请求 ≈ 150s 完成。
    交易日：非交易日跳过（份额变化小，无需刷新）；force=True 时绕过此校验
            （用于启动时无条件修正）。
    """
    if not etf_spot:
        logger.warning("Skip scale refresh (no ETF spot data)")
        return

    if not force and not is_trading_day():
        logger.debug("Skip scale refresh (non-trading day, no force)")
        return

    with _lock:
        all_codes = list(etf_spot.keys())

    logger.info("Starting scale refresh for %d funds via ulist (force=%s)...", len(all_codes), force)
    scales = _fetch_scale_via_ulist_batch(all_codes)

    updated = 0
    unchanged = 0
    with _lock:
        for code, new_scale in scales.items():
            if code not in etf_spot or new_scale <= 0:
                continue
            old_scale = _safe_float(etf_spot[code].get("scale"))
            # 差异 > 0.1亿 或原值缺失/为0 时更新（之前 0.5亿 太宽松，
            # 像 161725 这种实际 21亿 但前端显示 258亿 的反向偏差也要能修正）
            if not old_scale or abs(new_scale - old_scale) > 0.1:
                etf_spot[code]["scale"] = new_scale
                updated += 1
            else:
                unchanged += 1

    if updated > 0:
        save_spot_cache()

    logger.info(
        "Scale refresh done: updated=%d unchanged=%d fetched=%d total=%d",
        updated, unchanged, len(scales), len(all_codes),
    )

    # QDII ETF：用 gmbd API 的 NETNAV 覆盖 ulist 场内规模
    # （大部分份额通过场外联接基金持有，ulist 仅返回场内值）
    # gmbd 数据为季度更新，非 force 时当天已刷新过就跳过
    try:
        _refresh_qdii_scales(force=force)
    except Exception as exc:
        logger.error("QDII scale refresh failed: %s", exc)


# 独立的溢价刷新任务，不依赖spot刷新
def refresh_all_premium() -> None:
    """
    刷新所有ETF的溢价数据。仅在交易时段运行；非交易时段溢价由 refresh_nav_batch 在启动时填充。
    """
    global last_updated
    if not etf_spot:
        logger.warning("No ETF spot data available, skipping premium refresh")
        return

    if not is_trading_time():
        logger.debug("Skip premium refresh (non-trading, handled by nav_batch)")
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

    with _lock:
        for code, premium in _premium_cache.items():
            if code in etf_spot and abs(premium) < 30:
                etf_spot[code]["premium"] = round(premium, 2)
        last_updated = _now_bj_str()
    save_spot_cache()

    logger.info("Full premium refresh completed: updated=%s total=%s", total_success, len(all_codes))


def fetch_indices_live() -> Tuple[str, List[Dict]]:
    # Sina/Tencent 优先：单请求取 3 指数，不消耗 push2 配额
    try:
        indices = _fetch_indices_from_sina()
        if len(indices) >= 2:
            return "sina", indices
    except Exception as exc:
        logger.warning("Index fetch (Sina) failed: %s", exc)

    try:
        indices = _fetch_indices_from_tencent()
        if len(indices) >= 2:
            return "tencent", indices
    except Exception as exc:
        logger.warning("Index fetch (Tencent) failed: %s", exc)

    try:
        indices = _fetch_indices_from_eastmoney()
        if indices:
            return "eastmoney", indices
    except Exception as exc:
        logger.warning("Index fetch (Eastmoney) failed: %s", exc)

    return "none", []


def _secid_candidates(code: str) -> List[str]:
    if code.startswith(("5", "6", "9")):
        return [f"1.{code}", f"0.{code}"]
    return [f"0.{code}", f"1.{code}"]


def _tencent_symbol(code: str) -> str:
    if code.startswith(("5", "6", "9")):
        return f"sh{code}"
    return f"sz{code}"


# 历史 K 线起始锚点：早于最早 ETF 上市日（华夏上证 50ETF, 2004-12-30），
# 用绝对日期而非相对天数，避免随时间漂移导致历史数据被截断。
KLINE_HISTORY_START = "20050101"


def _fetch_kline_from_eastmoney(code: str, days: Optional[int] = None, start_override: Optional[str] = None) -> List[Dict]:
    if start_override:
        start_date = start_override
    elif days is None:
        start_date = KLINE_HISTORY_START
    else:
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
                    "fqt": "0",
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


def _fetch_kline_from_tencent(code: str, days: Optional[int] = None, start_override: Optional[str] = None) -> List[Dict]:
    symbol = _tencent_symbol(code)
    if start_override:
        start_cutoff = datetime.strptime(start_override, "%Y%m%d").date()
    elif days is None:
        start_cutoff = datetime.strptime(KLINE_HISTORY_START, "%Y%m%d").date()
    else:
        start_cutoff = (datetime.now(BEIJING_TZ) - timedelta(days=days)).date()

    try:
        # Tencent 单次最多返回约 1500 条，覆盖最近 ~6 年。作为 fallback 够用——
        # Eastmoney 主路径已能直接给到全量历史。
        payload = _request_json_external(
            TENCENT_KLINE_URL,
            params={"param": f"{symbol},day,,,1500,"},
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

    raw_rows = node.get("day") or []
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





def _fetch_nav_from_fundgz(code: str, session: requests.Session = None) -> Optional[float]:
    """
    获取ETF的实时IOPV（参考净值），用于计算溢价率。
    
    策略优先级：
    1. 东方财富 trends2 接口 — 获取分时行情中的IOPV值（最后字段），在Docker容器内可访问
    2. fundgz 接口 — 备用，在部分网络环境可能不可用
    
    修复记录(2026-04-20)：
    - fundgz.1234567.com.cn 在Docker容器内无法访问（DNS解析/网络限制）
    - 东方财富 push2 单条股票API对ETF的f183/f184始终返回0
    - 发现 trends2 接口的分时数据最后一个字段是IOPV，可靠且在容器内可访问
    """
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        })
    
    # 方法1（主）：东方财富trends2接口获取IOPV
    try:
        secid = f"1.{code}" if code.startswith(("5", "6", "9")) else f"0.{code}"
        url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fltt": "2",
            "ndays": "1",  # 只获取当天数据
        }
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            trends = data.get("trends", [])
            if trends:
                # 取最后一条记录，最后一个字段是IOPV
                last_trend = trends[-1]
                parts = str(last_trend).split(",")
                if len(parts) >= 8:
                    iopv = _safe_float(parts[-1])
                    if iopv and iopv > 0 and iopv < 10000:
                        return iopv
    except Exception as e:
        logger.debug(f"trends2获取IOPV失败 {code}: {e}")
    
    # 方法2（备）：fundgz接口
    try:
        url = f"https://fundgz.1234567.com.cn/js/{code}.js"
        try:
            resp = session.get(url, timeout=5)
        except Exception:
            url = f"http://fundgz.1234567.com.cn/js/{code}.js"
            resp = session.get(url, timeout=5)
            
        if resp.status_code == 200:
            text = resp.text
            import re
            match = re.search(r'jsonpgz\((.+?)\)', text)
            if match:
                data = json.loads(match.group(1))
                gsz = _safe_float(data.get("gsz"))
                dwjz = _safe_float(data.get("dwjz"))
                if gsz and gsz > 0 and gsz < 10000:
                    return gsz
                if dwjz and dwjz > 0 and dwjz < 10000:
                    return dwjz
    except Exception as e:
        logger.debug(f"fundgz获取净值失败 {code}: {e}")
    
    return None


def _fetch_premium_batch_sync(codes: List[str]) -> Dict[str, float]:
    """
    同步批量获取ETF溢价率数据。

    核心策略(2026-04-20 重大修复)：
    使用列表API的 f402(基金折价率) 和 f441(IOPV实时估值) 字段计算溢价率。
    
    关键发现（AKShare源码验证）：
    - f402 是"基金折价率"，折价率 = -溢价率，是最直接的溢价率字段
    - f441 是"IOPV实时估值"，即ETF参考净值，可手动计算溢价率
    - f184 在ETF列表API中是"主力净流入-净占比"，不是溢价率！
    - fundgz.1234567.com.cn 在Docker容器内DNS解析失败
    - 东方财富单条股票API对ETF的f183/f184始终返回0
    
    溢价率来源优先级：
    1. f402(基金折价率) - 溢价率 = -折价率
    2. f441(IOPV) + price 手动计算 - 溢价率 = (price - IOPV) / IOPV * 100
    3. premium_cache 缓存 - 保留历史有效值
    """
    results = {}
    if not codes:
        return results
    
    f402_success = 0
    iopv_calc_success = 0
    cache_hit = 0
    
    try:
        # 使用列表API批量获取f402和f441（一次请求，批量获取）
        for url in SPOT_ENDPOINTS:
            try:
                params = {
                    "pn": "1",
                    "pz": "5000",
                    "po": "1",
                    "np": "1",
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": "2",
                    "invt": "2",
                    "wbp2u": "|0|0|0|web",
                    "fid": "f12",
                    "fs": "m:0+t:10,m:1+t:10,m:0+t:11,m:1+t:11,m:0+t:12,m:1+t:12,m:0+t:13,m:1+t:13,m:0+t:14,m:1+t:14,m:0+t:15,m:1+t:15,m:0+t:16,m:1+t:16,m:0+t:17,m:1+t:17,m:0+t:18,m:1+t:18,m:0+t:20,m:1+t:20",
                    "fields": "f2,f12,f18,f402,f441",
                }
                session = requests.Session()
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Referer": "https://quote.eastmoney.com/"
                })
                payload = _request_json(url, params, retries=2)
                data = payload.get("data") or {}
                rows = data.get("diff") or []
                
                if not rows:
                    continue
                
                # 构建code -> {f2, f402, f441} 映射
                row_map = {}
                for row in rows:
                    rcode = str(row.get("f12", "")).zfill(6)
                    if rcode in codes or rcode in etf_spot:
                        row_map[rcode] = {
                            "price": _safe_float(row.get("f2")),
                            "prevClose": _safe_float(row.get("f18")),
                            "f402": _safe_float(row.get("f402")),
                            "f441": _safe_float(row.get("f441")),
                        }
                
                logger.info(f"溢价批量采集: 从列表API获取到 {len(row_map)} 只ETF的f402/f441数据")
                
                # 处理每个code
                for code in codes:
                    if code in results:
                        continue
                    
                    rdata = row_map.get(code)
                    if not rdata:
                        # 列表API中没有该ETF数据，使用缓存
                        with _lock:
                            cached_val = _premium_cache.get(code)
                        if cached_val is not None and cached_val != 0 and abs(cached_val) < 30:
                            results[code] = cached_val
                            cache_hit += 1
                        continue
                    
                    # 优先级1：f402折价率 → 溢价率 = -折价率
                    f402_val = rdata["f402"]
                    if f402_val is not None and f402_val != 0 and abs(f402_val) < 30:
                        premium = round(-f402_val, 2)
                        results[code] = premium
                        with _lock:
                            _premium_cache[code] = premium
                        # 更新etf_spot中的premium
                        if code in etf_spot:
                            with _lock:
                                etf_spot[code]["premium"] = premium
                        f402_success += 1
                        continue
                    
                    # 优先级2：f441(IOPV) + price 计算
                    # 非交易时段 f2=0，改用 f18(prevClose) 计算上一交易日收盘溢价
                    f441_val = rdata["f441"]
                    spot_price = rdata["price"]
                    if spot_price <= 0:
                        with _lock:
                            spot_price = etf_spot.get(code, {}).get("currentPrice") or 0
                    if spot_price <= 0:
                        spot_price = rdata.get("prevClose", 0)
                    if spot_price <= 0:
                        with _lock:
                            spot_price = etf_spot.get(code, {}).get("prevClose") or 0
                    
                    if f441_val is not None and f441_val > 0 and spot_price > 0:
                        iopv = f441_val
                        # IOPV可能被放大1000倍
                        if iopv > 100 and spot_price < 100:
                            iopv = iopv / 1000
                        if iopv > 0 and abs(spot_price - iopv) > 0.0001:
                            premium = round(((spot_price - iopv) / iopv) * 100, 2)
                            if abs(premium) < 30:
                                results[code] = premium
                                with _lock:
                                    _premium_cache[code] = premium
                                    if code in etf_spot:
                                        etf_spot[code]["nav"] = round(iopv, 4)
                                        etf_spot[code]["premium"] = premium
                                iopv_calc_success += 1
                                continue
                    
                    # 优先级3：用 NAV 缓存 + 实时价格计算（每日公布净值，比陈旧 cache 更准）
                    # 价格优先级：今日收盘价(etf_spot currentPrice) > rdata prevClose > etf_spot prevClose
                    nav_entry = _nav_cache.get(code)
                    nav_calc = None
                    if nav_entry:
                        nav_val = nav_entry.get("nav", 0)
                        with _lock:
                            spot_info = etf_spot.get(code, {})
                        ref_price = spot_info.get("currentPrice") or 0
                        if ref_price <= 0:
                            ref_price = rdata.get("prevClose", 0) if rdata else 0
                        if ref_price <= 0:
                            ref_price = spot_info.get("prevClose") or 0
                        if nav_val > 0 and ref_price > 0:
                            calc = round(((ref_price - nav_val) / nav_val) * 100, 2)
                            if abs(calc) < 30:
                                nav_calc = calc

                    if nav_calc is not None:
                        results[code] = nav_calc
                        with _lock:
                            _premium_cache[code] = nav_calc
                            if code in etf_spot:
                                etf_spot[code]["premium"] = nav_calc
                        continue

                    # 优先级4：陈旧 premium 缓存兜底
                    with _lock:
                        cached_val = _premium_cache.get(code)
                    if cached_val is not None and cached_val != 0 and abs(cached_val) < 30:
                        results[code] = cached_val
                        cache_hit += 1
                        continue

                break  # 成功从某个endpoint获取数据，不再尝试其他
            
            except Exception as e:
                logger.warning(f"列表API溢价采集失败({url}): {e}")
                continue
    
    except Exception as e:
        logger.error(f"溢价批量采集异常: {e}")
    
    if results:
        _save_premium_cache()
    
    logger.info(f"溢价批量采集完成: f402折价率 {f402_success}/{len(codes)}, IOPV计算 {iopv_calc_success}/{len(codes)}, 缓存 {cache_hit}/{len(codes)}, 总缓存 {len(_premium_cache)} 只")
    return results


def _fetch_fee_from_eastmoney(code: str) -> bool:
    """Fetch management/custody/sales fees from Eastmoney fund fee detail HTML page."""
    global _fee_last_call_at
    try:
        # 独立限流：不占共享 request_controller，避免拖慢行情请求
        with _fee_rate_lock:
            elapsed = time.time() - _fee_last_call_at
            wait = _FEE_INTERVAL - elapsed
            if wait > 0:
                time.sleep(wait)
            _fee_last_call_at = time.time()

        url = f"https://fundf10.eastmoney.com/jjfl_{code}.html"
        resp = _FEE_SESSION.get(url, timeout=10)
        resp.raise_for_status()
        text = resp.text
        if not text:
            return False

        import re
        mgmt_m = re.search(r'管理费率</td><td[^>]*>([\d.]+)%', text)
        cust_m = re.search(r'托管费率</td><td[^>]*>([\d.]+)%', text)
        sales_m = re.search(r'销售服务费率</td><td[^>]*>([\d.]+)%', text)

        fees: Dict[str, float] = {}
        if mgmt_m:
            fees["管理费"] = _safe_float(mgmt_m.group(1))
        if cust_m:
            fees["托管费"] = _safe_float(cust_m.group(1))
        if sales_m and _safe_float(sales_m.group(1)) > 0:
            fees["销售服务费"] = _safe_float(sales_m.group(1))

        if fees:
            with _lock:
                _fee_cache[code] = fees
                try:
                    FEE_CACHE_FILE.write_text(json.dumps(_fee_cache, ensure_ascii=False), encoding="utf-8")
                except Exception:
                    pass
            logger.debug("Updated fees for %s: %s", code, fees)
            return True

    except Exception as exc:
        logger.debug("Fetch fee failed for %s: %s", code, exc)

    return False


def fetch_kline_live(code: str, days: Optional[int] = None) -> List[Dict]:
    _fetch_fee_from_eastmoney(code)

    existing = load_kline(code)

    # 增量拉取：有缓存且非指定天数模式时，只拉缓存末日之后的数据
    if existing and days is None:
        last_date = existing[-1].get("date", "")
        if last_date:
            incremental_start = last_date.replace("-", "")
            new_data = _fetch_kline_from_eastmoney(code, start_override=incremental_start)
            if not new_data:
                new_data = _fetch_kline_from_tencent(code, start_override=incremental_start)
            if new_data:
                date_set = {k["date"] for k in existing}
                merged = existing + [k for k in new_data if k["date"] not in date_set]
                merged.sort(key=lambda x: x["date"])
                return merged
            return existing

    # 无缓存或指定 days：全量拉取
    eastmoney_kline = _fetch_kline_from_eastmoney(code, days)
    if eastmoney_kline:
        return eastmoney_kline

    tencent_kline = _fetch_kline_from_tencent(code, days)
    if tencent_kline:
        logger.debug("Kline fallback hit (provider=tencent, code=%s)", code)
        return tencent_kline

    return existing or []


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
    判断是否为A股交易日。
    - 周六/日直接返回 False
    - 工作日：用 _nav_cache 的最近净值日期验证（能识别法定节假日）
      若 nav_cache 未加载则退化为纯工作日判断（最多误判节假日）
    """
    now = datetime.now(BEIJING_TZ)
    if now.weekday() >= 5:
        return False
    today = now.strftime("%Y-%m-%d")
    # 利用 NAV 缓存：净值日期 = 最近交易日。若今天 < 最近交易日，说明今天是节假日
    if _nav_cache:
        dates = [v.get("date", "") for v in _nav_cache.values() if v.get("date")]
        if dates:
            last_trading_date = max(dates)
            # 最近交易日比今天早超过3天，说明今天大概率是节假日
            try:
                delta = (
                    datetime.strptime(today, "%Y-%m-%d")
                    - datetime.strptime(last_trading_date, "%Y-%m-%d")
                ).days
                if delta > 3:
                    return False
            except ValueError:
                pass
    return True


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
    # 非交易时间：基于 last_updated 判断是否需要刷新
    # 如果 last_updated 为空，说明还没刷新过，需要刷新
    if not last_updated:
        return True
    # 如果距离上次刷新已超过30分钟，则需要刷新
    try:
        last_time = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BEIJING_TZ)
        elapsed = (datetime.now(BEIJING_TZ) - last_time).total_seconds()
        return elapsed >= 1800  # 30分钟 = 1800秒
    except (ValueError, TypeError):
        # 解析失败，默认需要刷新
        return True


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
            "pz": 1000,  # 每页1000条，东方财富接口最多支持每页1000条
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            # 全类型覆盖：所有类型的场内基金，确保没有遗漏（新增t:18商品、t:20债券）
            "fs": "m:0+t:10,m:1+t:10,m:0+t:11,m:1+t:11,m:0+t:12,m:1+t:12,m:0+t:13,m:1+t:13,m:0+t:14,m:1+t:14,m:0+t:15,m:1+t:15,m:0+t:16,m:1+t:16,m:0+t:17,m:1+t:17,m:0+t:18,m:1+t:18,m:0+t:20,m:1+t:20",
            "fields": "f12,f14",  # f12=代码, f14=名称，不再过滤价格
        }
        
        # 分页获取，最多获取40页（40000只，覆盖所有场内基金）
        for page in range(1, 41):
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
                    # 不过滤任何条件，只要是6位代码+有名称就收录，确保没有遗漏
                    if code and name and len(code) == 6:
                        funds[code] = name
                        valid_page_count += 1
                
                logger.info(f"✅ Page {page} fetched: {valid_page_count} valid funds, total now: {len(funds)}")
                # 如果当前页返回数量小于每页最大数量，说明已经到最后一页
                if len(diff) < params["pz"]:
                    break
                    
                time.sleep(1)  # 分页请求间隔，避免限流
            except Exception as e:
                logger.warning(f"⚠️ Page {page} fetch failed: {e}")
                break
        
        # 兜底补全：手动添加已知缺失的特殊基金
        special_funds = {
            "161129": "易方达原油LOF",
            "164824": "工银印度基金LOF",
            "501018": "南方原油LOF",
            "162411": "华宝油气LOF",
            "160416": "华安石油LOF",
            "513030": "德国30ETF",
            "513500": "标普500ETF",
            "159941": "纳指100ETF",
            "513100": "纳指ETF",
            "513060": "恒生医疗ETF",
            "159920": "恒生ETF",
            "510900": "H股ETF",
            "510300": "华泰柏瑞沪深300ETF",
            "562510": "华夏纳斯达克100ETF"
        }
        for code, name in special_funds.items():
            if code not in funds:
                funds[code] = name
                logger.info(f"✅ 兜底添加特殊基金 {code}: {name}")
        
        logger.info(f"✅ 最终收录场内基金总数: {len(funds)} 只")
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
                # 新基金自动触发费率和K线采集，不用等定时任务
                try:
                    threading.Thread(target=_fetch_fee_from_eastmoney, args=(code,), daemon=True).start()
                    threading.Thread(target=fetch_kline_live, args=(code,), daemon=True).start()
                except Exception as e:
                    logger.debug(f"触发新基金{code}采集失败: {e}")
        
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
            save_spot_cache()


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
            logger.warning("Got empty spot data from live source, keeping existing data")
            with _lock:
                last_updated = _now_bj_str()
            return

        # Supplement missing-price ETFs from Tencent quote API
        with _lock:
            fund_names = {c: d.get("name", "") for c, d in etf_spot.items()}
        for code, name in fund_names.items():
            if code not in new_spot:
                new_spot[code] = {"code": code, "name": name, "currentPrice": 0.0,
                                  "chgPct": 0.0, "scale": 0.0, "volume": 0, "turnover": 0.0}
        _supplement_with_tencent(new_spot, fund_names)

        index_provider, new_indices = fetch_indices_live()

        with _lock:
            # 先保留原有所有基金的基本信息
            all_funds = etf_spot.copy()
            # 用新获取的行情数据更新已有基金（智能合并：保留旧数据中的有效值）
            for code, new_data in new_spot.items():
                if code in all_funds:
                    old_data = all_funds[code]
                    # 智能合并：新数据字段为0/null时保留旧数据中的有效值
                    for field in ("currentPrice", "chgPct", "scale", "open", "high", "low", "prevClose", "nav", "premium"):
                        new_val = new_data.get(field)
                        old_val = old_data.get(field)
                        # 新数据无有效值但有旧数据有效值时，保留旧值
                        if (new_val is None or new_val == 0) and old_val is not None and old_val != 0:
                            new_data[field] = old_val
                    # 保留旧数据中的费率（新行情数据不包含费率时）
                    if not new_data.get("fee") and old_data.get("fee"):
                        new_data["fee"] = old_data["fee"]
                        new_data["feeDetail"] = old_data.get("feeDetail", "")
                all_funds[code] = new_data
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

            # premium 兜底：spot 解析后仍无溢价的 ETF，用缓存填充
            for code, info in etf_spot.items():
                if info.get("premium") in (None, 0):
                    cached = _premium_cache.get(code)
                    if cached is not None and cached != 0 and abs(cached) < 30:
                        info["premium"] = round(cached, 2)

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


def refresh_nav_batch() -> None:
    """
    每日批量采集所有ETF/LOF的单位净值（NAV）并缓存。
    用于非交易时段计算 LOF/QDII/REIT 基金的溢价率。
    每天收盘后（20:00）运行一次，采集当日公布的净值。
    """
    if not etf_spot:
        logger.warning("No ETF spot data, skip nav refresh")
        return

    all_codes = list(etf_spot.keys())
    today = _today_bj_str()
    done = 0
    skipped = 0
    nav_updated_premiums = 0

    logger.info("Starting NAV batch refresh for %d funds", len(all_codes))

    for code in all_codes:
        cached = _nav_cache.get(code, {})
        if cached.get("date") == today and cached.get("nav", 0) > 0:
            skipped += 1
            continue
        try:
            payload = _request_json_external(
                "https://api.fund.eastmoney.com/f10/lsjz",
                params={"fundCode": code, "pageIndex": 1, "pageSize": 1},
                retries=1,
                headers={"Referer": f"https://fund.eastmoney.com/{code}.html"},
            )
            lst = (payload.get("Data") or {}).get("LSJZList") or []
            if lst:
                nav = _safe_float(lst[0].get("DWJZ", 0))
                nav_date = lst[0].get("FSRQ", today)
                if nav > 0:
                    with _lock:
                        _nav_cache[code] = {"nav": nav, "date": nav_date}
                    done += 1
                    # 用当日收盘价（回退昨收）更新premium缓存，始终覆盖旧值保证准确
                    ref_price = etf_spot.get(code, {}).get("currentPrice") or 0
                    if ref_price <= 0:
                        ref_price = etf_spot.get(code, {}).get("prevClose") or 0
                    if ref_price > 0:
                        calc = round(((ref_price - nav) / nav) * 100, 2)
                        if abs(calc) < 30:
                            with _lock:
                                _premium_cache[code] = calc
                                if code in etf_spot:
                                    etf_spot[code]["premium"] = calc
                            nav_updated_premiums += 1
        except Exception as exc:
            logger.debug("NAV fetch failed for %s: %s", code, exc)

    _save_nav_cache()
    _save_premium_cache()
    logger.info(
        "NAV batch done: fetched=%d skipped=%d premium_filled=%d",
        done, skipped, nav_updated_premiums,
    )


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

    # Supplement scale data for ETFs still showing scale=0
    with _lock:
        zero_scale_codes = [
            code for code, info in etf_spot.items()
            if not info.get("scale") or info.get("scale") == 0
        ]
    if zero_scale_codes:
        logger.info("Supplementing scale from pingzhong for %d ETFs", len(zero_scale_codes))
        _supplement_scale_from_pingzhong(zero_scale_codes)


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


def check_and_fill_missing_data():
    """
    每小时检查缺失数据并补全：
    1. 价格为0的基金：单独拉取行情
    2. 溢价缺失的基金：触发溢价补采
    3. 费率缺失的基金：触发费率采集
    """
    try:
        global etf_spot
        with _lock:
            all_codes = list(etf_spot.keys())
        
        # 统计缺失数据
        missing_price = []
        missing_premium = []
        missing_fee = []
        for code in all_codes:
            etf = etf_spot.get(code, {})
            if etf.get("currentPrice", 0) == 0:
                missing_price.append(code)
            if etf.get("premium") is None and code not in _premium_cache:
                missing_premium.append(code)
            if code not in _fee_cache:
                missing_fee.append(code)
        
        logger.info(f"📊 数据完整性统计：总基金{len(all_codes)}只，价格缺失{len(missing_price)}只，溢价缺失{len(missing_premium)}只，费率缺失{len(missing_fee)}只")
        
        # 补全缺失价格
        if missing_price:
            logger.info(f"🔄 开始补全{len(missing_price)}只价格缺失基金的行情")
            success = 0
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            })
            for idx, code in enumerate(missing_price):
                try:
                    secid = f"1.{code}" if code.startswith(("5", "6", "9")) else f"0.{code}"
                    url = "https://push2.eastmoney.com/api/qt/stock/get"
                    params = {
                        "secid": secid,
                        "fields": "f43,f170",  # f43=最新价, f170=涨跌幅
                        "ut": "7eea3edcaed734bea9cbfc24409ed989",
                        "fltt": "2",  # 返回实际值
                    }
                    resp = session.get(url, params=params, timeout=5)
                    if resp.status_code != 200:
                        continue
                    data = resp.json().get("data", {})
                    price_raw = _safe_float(data.get("f43"))
                    # fltt=2 通常返回实际价格，但保险起见做判断
                    price = price_raw / 1000 if price_raw > 100 else price_raw
                    chg_raw = _safe_float(data.get("f170"))
                    chg = chg_raw / 100 if abs(chg_raw) > 100 else chg_raw
                    if price > 0:
                        with _lock:
                            if code in etf_spot:
                                etf_spot[code]["currentPrice"] = round(price, 4)
                                etf_spot[code]["chgPct"] = round(chg, 2)
                        success += 1
                except Exception as e:
                    logger.debug(f"补全价格失败 {code}: {e}")
                # 限流控制
                if (idx + 1) % 20 == 0:
                    time.sleep(1)
            logger.info(f"✅ 价格补全完成：成功{success}/{len(missing_price)}只")
        
        # 补全缺失溢价
        if missing_premium:
            logger.info(f"🔄 开始补全{len(missing_premium)}只溢价缺失基金的溢价率")
            _fetch_premium_batch_sync(missing_premium)
        
        # 补全缺失费率
        if missing_fee:
            logger.info(f"🔄 开始补全{len(missing_fee)}只费率缺失基金的费率")
            done = 0
            failed = 0
            for idx, code in enumerate(missing_fee):
                try:
                    if _fetch_fee_from_eastmoney(code):
                        done += 1
                except Exception as e:
                    failed += 1
                    logger.debug(f"补全费率失败 {code}: {e}")
                if (idx + 1) % 20 == 0:
                    time.sleep(1)
            logger.info(f"✅ 费率补全完成：成功{done}/{len(missing_fee)}只")

        # 补全缺失规模（pingzhongdata季度AUM，仅处理scale=0的基金）
        # 只在非交易时段运行，避免与行情请求竞争（已用独立session，但仍占带宽）
        if not is_trading_time():
            with _lock:
                missing_scale = [
                    code for code, info in etf_spot.items()
                    if not info.get("scale") or info.get("scale") == 0
                ]
            if missing_scale:
                logger.info(f"🔄 开始补全{len(missing_scale)}只规模缺失基金的规模（pingzhong）")
                _supplement_scale_from_pingzhong(missing_scale)

        # 保存最新数据
        save_spot_cache()
        logger.info("✅ 数据补全任务完成")
    except Exception as e:
        logger.error(f"❌ 数据补全任务失败: {e}", exc_info=True)


# ============================================================
# FASTAPI APP
# ============================================================

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global data_source, live_provider

    _load_fee_cache()
    _load_nav_cache()
    cache_loaded = load_spot_cache()

    # 启动初始化：先发现基金，再刷新行情
    # 在后台线程中执行，避免阻塞启动
    def startup_init():
        time.sleep(2)  # 等待服务完全启动
        # 先执行基金发现
        _ensure_all_etfs_in_spot()
        logger.info("After ETF discovery: total etfs=%s", len(etf_spot))
        # 首次行情刷新（_calc_scale 估算的规模可能滞后，下一步会修正）
        refresh_spot(force=True)
        # 用 ulist f117 修正规模（覆盖 _calc_scale 的滞后估算）。
        # 必须在 refresh_spot 之后串行执行，避免并发竞态导致 etf_spot 未填充就跳过。
        # force=True：忽略 is_trading_day 校验，确保部署后无条件跑一次。
        try:
            refresh_all_scales(force=True)
        except Exception as exc:
            logger.error("Initial scale refresh failed: %s", exc)

    threading.Thread(target=startup_init, daemon=True).start()

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
    # premium_refresh 已合并进 refresh_spot（spot 的 _parse_spot_row 已含完整 4 级溢价计算）
    # 添加每日基金发现定时任务，每天凌晨2点执行，发现新增ETF/LOF，补全缺失基金
    scheduler.add_job(
        _ensure_all_etfs_in_spot,
        "cron",
        hour=2,
        minute=0,
        id="etf_discovery",
        max_instances=1,
        coalesce=True,
    )
    # 添加数据完整性校验定时任务，每24小时运行一次，统计缺失数据并补全
    scheduler.add_job(
        check_and_fill_missing_data,
        "interval",
        hours=24,
        id="data_fill",
        max_instances=1,
        coalesce=True,
    )
    # NAV批量采集：每天20:00（北京时间）运行，覆盖当日收盘后公布的净值
    scheduler.add_job(
        refresh_nav_batch,
        "cron",
        hour=12,  # UTC 12:00 = 北京时间 20:00
        minute=0,
        id="nav_refresh",
        max_instances=1,
        coalesce=True,
    )
    # 规模刷新：用单条行情后端 f117(总市值) 覆盖 _calc_scale 的 f441×f38 估算。
    # 修复 511360 等快速申赎ETF出现 ~30% 偏差的问题。
    # 内部已校验 is_trading_day()——非交易日自动跳过；启动时已在 startup_init 串行跑一次。
    # 显式绑定北京时区，避免依赖服务器 local time（不同部署环境可能 UTC 或 BJT）。
    scheduler.add_job(
        refresh_all_scales,
        CronTrigger(hour=9, minute=35, timezone=BEIJING_TZ),
        id="scale_refresh_morning",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        refresh_all_scales,
        CronTrigger(hour=15, minute=5, timezone=BEIJING_TZ),
        id="scale_refresh_close",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()

    # Warm up kline for all ETFs in background once (force=True to ensure all ETFs are fetched).
    # Use a lambda to pass force=True parameter.
    threading.Thread(target=lambda: refresh_kline_batch(force=True), daemon=True).start()

    # 启动时也在后台采集缺失的费率数据
    threading.Thread(target=refresh_all_fees, daemon=True).start()

    # 启动时在后台采集NAV，用于填充非交易时段溢价
    threading.Thread(target=refresh_nav_batch, daemon=True).start()

    # 注：refresh_all_scales 不再独立启动线程——已在 startup_init 中
    # 串行执行（refresh_spot 之后），避免读 etf_spot 时的竞态。

    # Back-fill stats from any existing kline files that lack the new fields
    # (e.g. after a server upgrade where compute_stats gained new columns).
    threading.Thread(target=backfill_stats_from_kline_files, daemon=True).start()

    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="ETF NEXUS", lifespan=lifespan)


@app.get("/api/etf-data")
async def get_etf_data():
    with _lock:
        if not etf_spot:
            # 尝试加载缓存
            load_spot_cache()
            if not etf_spot:
                # 缓存也没有，执行基金发现
                _ensure_all_etfs_in_spot()
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
            # 溢价：优先使用 get_premium_for_display（含非交易时段收盘回退逻辑）
            display_premium = get_premium_for_display(code)
            if display_premium is not None and abs(display_premium) < 30:
                row["premium"] = round(display_premium, 2)
            else:
                spot_premium = spot.get("premium")
                if spot_premium is not None and abs(spot_premium) < 30:
                    row["premium"] = round(spot_premium, 2)
                else:
                    row["premium"] = None
            # 移除内部诊断字段，不暴露给前端
            row.pop("_premium_source", None)
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

    # 测试列表API的f402/f441字段值（溢价率核心字段诊断）
    clist_premium_test = None
    try:
        _diag_url = SPOT_ENDPOINTS[0] if SPOT_ENDPOINTS else "https://88.push2.eastmoney.com/api/qt/clist/get"
        _diag_params = {
            "pn": "1", "pz": "10", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2", "invt": "2", "fid": "f12",
            "fs": "m:1+t:10,m:0+t:10",
            "fields": "f2,f12,f14,f402,f441",
        }
        _diag_resp = requests.get(_diag_url, params=_diag_params, timeout=5)
        if _diag_resp.status_code == 200:
            _diag_data = _diag_resp.json().get("data", {}).get("diff", [])
            _diag_samples = []
            for _r in _diag_data[:5]:
                _diag_samples.append({
                    "code": _r.get("f12"),
                    "name": _r.get("f14"),
                    "price": _r.get("f2"),
                    "f402_discount": _r.get("f402"),
                    "f441_iopv": _r.get("f441"),
                })
            clist_premium_test = _diag_samples
        else:
            clist_premium_test = f"http_{_diag_resp.status_code}"
    except Exception as e:
        clist_premium_test = f"error: {type(e).__name__}: {e}"

    # 统计当前spot中有premium的ETF数量
    premium_count = 0
    premium_by_source = {}
    with _lock:
        for code, spot in etf_spot.items():
            if spot.get("premium") is not None and spot.get("premium") != 0 and abs(spot.get("premium", 0)) < 30:
                premium_count += 1
                src = spot.get("_premium_source", "unknown")
                premium_by_source[src] = premium_by_source.get(src, 0) + 1

    return {
        "current_source": data_source,
        "provider": live_provider,
        "current_etf_count": len(etf_spot),
        "current_stats_count": len(etf_stats),
        "indices": market_indices,
        "updated": last_updated,
        "rate_limiter": request_controller.status(),
        "sample_etfs": sample,
        "clist_premium_test": clist_premium_test,
        "premium_cache_size": len(_premium_cache),
        "premium_in_spot": premium_count,
        "premium_by_source": premium_by_source,
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
