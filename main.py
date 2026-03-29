"""
ETF NEXUS — A股全部场内ETF实时数据终端 Backend
AKShare + FastAPI + APScheduler
Architecture:
  - Spot refresh: every 1min via ak.fund_etf_spot_em() → ALL ETFs
  - Kline + stats: background thread gradually fetches for all ETFs
  - On-demand kline: /api/kline/:code fetches live if not cached
"""
import os, json, logging, threading, time, random
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etf-nexus")

# ============================================================
# PROXY CONFIG — route AKShare (requests/urllib) through a China proxy
# Set env var AKSHARE_PROXY to e.g. "http://your-cn-proxy:8080"
# or use standard HTTP_PROXY / HTTPS_PROXY vars.
# ============================================================
_PROXY = os.environ.get("AKSHARE_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
if _PROXY:
    os.environ.setdefault("HTTP_PROXY", _PROXY)
    os.environ.setdefault("HTTPS_PROXY", _PROXY)
    logger.info(f"Proxy configured: {_PROXY}")
else:
    logger.info("No proxy configured — AKShare will connect directly")

# ============================================================
# CONFIG
# ============================================================
REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "1"))
KLINE_BATCH_SIZE = int(os.environ.get("KLINE_BATCH_SIZE", "10"))
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
SPOT_CACHE = DATA_DIR / "spot_cache.json"
KLINE_DIR = DATA_DIR / "kline"
KLINE_DIR.mkdir(exist_ok=True)

# ============================================================
# IN-MEMORY STORE
# ============================================================
# etf_spot: { code: {name, code, currentPrice, scale, fee, chgPct, ...} }
etf_spot = {}
# etf_stats: { code: {dropFromHigh, riseFromLow, maxDD1Y, maxDD3Y, sparkline:[]} }
etf_stats = {}
# indices
market_indices = []
last_updated = None
data_source = "none"
_lock = threading.Lock()

# Default fee estimation (AKShare spot doesn't include fees)
DEFAULT_FEE = 0.20
KNOWN_FEES = {
    # Cross-border / QDII ETFs typically higher
    "513100": 0.35, "513050": 0.35, "159941": 0.35, "513130": 0.35,
    "159934": 0.15, "518880": 0.15, "511010": 0.10, "562340": 0.15,
}


# ============================================================
# AKSHARE FETCHERS
# ============================================================
def _safe_float(val, default=0):
    """Safely convert to float, return default on failure."""
    try:
        v = float(val) if val is not None and str(val).strip() not in ("", "-", "nan", "None") else default
        return v
    except (ValueError, TypeError):
        return default


def fetch_spot_akshare():
    """Fetch ALL ETF spot data via AKShare. Returns dict {code: row_dict}."""
    import akshare as ak
    logger.info("AKShare: fetching spot data for all ETFs...")
    df = ak.fund_etf_spot_em()
    cols = list(df.columns)
    logger.info(f"AKShare spot columns: {cols}")
    df["代码"] = df["代码"].astype(str).str.zfill(6)

    # Detect column names (AKShare changes these across versions)
    def _find_col(candidates, df_cols):
        for c in candidates:
            if c in df_cols:
                return c
        return None

    col_price = _find_col(["最新价", "现价", "收盘价"], cols)
    col_name = _find_col(["名称", "基金名称"], cols)
    col_chg = _find_col(["涨跌幅", "涨幅"], cols)
    col_vol = _find_col(["成交量"], cols)
    col_amt = _find_col(["成交额"], cols)
    col_scale = _find_col(["总市值", "市值", "基金规模"], cols)
    col_open = _find_col(["开盘价", "开盘", "今开"], cols)
    col_high = _find_col(["最高价", "最高"], cols)
    col_low = _find_col(["最低价", "最低"], cols)
    col_prev = _find_col(["昨收", "昨收价"], cols)

    logger.info(f"Column mapping: price={col_price}, name={col_name}, chg={col_chg}, "
                f"vol={col_vol}, amt={col_amt}, scale={col_scale}")

    result = {}
    for _, row in df.iterrows():
        code = row["代码"]
        try:
            price = _safe_float(row.get(col_price) if col_price else None)
            if price <= 0:
                continue
            # Scale: try 总市值 first, else estimate from 成交额/换手率 if available
            scale_raw = _safe_float(row.get(col_scale) if col_scale else None)
            if scale_raw > 1e6:  # Likely in yuan, convert to 亿
                scale = round(scale_raw / 1e8, 2)
            elif scale_raw > 0:  # Might already be in 亿
                scale = round(scale_raw, 2)
            else:
                # Estimate: daily turnover / turnover_rate ≈ total market cap
                amt = _safe_float(row.get(col_amt) if col_amt else None)
                scale = round(amt / 1e8, 2) if amt > 0 else 0

            result[code] = {
                "code": code,
                "name": str(row.get(col_name, "") if col_name else ""),
                "currentPrice": round(price, 4),
                "chgPct": round(_safe_float(row.get(col_chg) if col_chg else None), 2),
                "scale": scale,
                "volume": int(_safe_float(row.get(col_vol) if col_vol else None)),
                "turnover": round(_safe_float(row.get(col_amt) if col_amt else None) / 1e8, 2),
                "fee": KNOWN_FEES.get(code, DEFAULT_FEE),
                "open": round(_safe_float(row.get(col_open) if col_open else None), 4),
                "high": round(_safe_float(row.get(col_high) if col_high else None), 4),
                "low": round(_safe_float(row.get(col_low) if col_low else None), 4),
                "prevClose": round(_safe_float(row.get(col_prev) if col_prev else None), 4),
            }
        except (ValueError, TypeError) as e:
            logger.warning(f"Spot parse error for {code}: {e}")
            continue
    logger.info(f"AKShare: spot data fetched for {len(result)} ETFs")
    return result


def fetch_indices_akshare():
    """Fetch major index quotes."""
    import akshare as ak
    target = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指"}
    indices = []
    try:
        # stock_zh_index_spot_em returns ALL indices, filter locally
        df = ak.stock_zh_index_spot_em()
        if df is not None and not df.empty:
            cols = list(df.columns)
            logger.info(f"Index spot columns: {cols}")
            col_code = "代码" if "代码" in cols else None
            col_price = next((c for c in ["最新价", "现价"] if c in cols), None)
            col_chg = next((c for c in ["涨跌幅", "涨幅"] if c in cols), None)
            if col_code:
                df[col_code] = df[col_code].astype(str)
                for code, name in target.items():
                    match = df[df[col_code] == code]
                    if not match.empty:
                        row = match.iloc[0]
                        indices.append({
                            "name": name,
                            "val": round(_safe_float(row.get(col_price) if col_price else None), 2),
                            "chg": round(_safe_float(row.get(col_chg) if col_chg else None), 2),
                        })
    except Exception as e:
        logger.warning(f"Index fetch failed: {e}")
    return indices if indices else _mock_indices()


def fetch_kline_akshare(code: str, days: int = 1200) -> list:
    """Fetch daily kline for one ETF via AKShare."""
    import akshare as ak
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    hist = ak.fund_etf_hist_em(
        symbol=code, period="daily",
        start_date=start_date, end_date=end_date, adjust="qfq"
    )
    if hist is None or hist.empty:
        return []
    cols = list(hist.columns)
    col_date = next((c for c in ["日期", "date"] if c in cols), cols[0])
    col_open = next((c for c in ["开盘", "开盘价", "open"] if c in cols), cols[1] if len(cols) > 1 else None)
    col_close = next((c for c in ["收盘", "收盘价", "close"] if c in cols), cols[2] if len(cols) > 2 else None)
    col_high = next((c for c in ["最高", "最高价", "high"] if c in cols), cols[3] if len(cols) > 3 else None)
    col_low = next((c for c in ["最低", "最低价", "low"] if c in cols), cols[4] if len(cols) > 4 else None)
    col_vol = next((c for c in ["成交量", "volume"] if c in cols), cols[5] if len(cols) > 5 else None)
    kline = []
    for _, r in hist.iterrows():
        try:
            kline.append({
                "date": str(r[col_date])[:10],
                "open": round(_safe_float(r.get(col_open)), 4),
                "close": round(_safe_float(r.get(col_close)), 4),
                "high": round(_safe_float(r.get(col_high)), 4),
                "low": round(_safe_float(r.get(col_low)), 4),
                "volume": int(_safe_float(r.get(col_vol))),
            })
        except (ValueError, TypeError):
            continue
    return kline


# ============================================================
# STATS COMPUTATION
# ============================================================
def _max_drawdown(arr):
    if not arr or len(arr) < 2:
        return 0
    peak = arr[0]
    mdd = 0
    for v in arr:
        if v > peak:
            peak = v
        dd = (v - peak) / peak
        if dd < mdd:
            mdd = dd
    return round(mdd * 100, 2)


def compute_stats(kline: list) -> dict:
    """Compute stats from kline data."""
    if not kline or len(kline) < 10:
        return {}
    closes = [k["close"] for k in kline]
    current = closes[-1]
    all_high = max(closes)
    all_low = min(closes)
    one_year = closes[-250:] if len(closes) > 250 else closes
    three_year = closes[-750:] if len(closes) > 750 else closes
    return {
        "dropFromHigh": round((current - all_high) / all_high * 100, 2),
        "riseFromLow": round((current - all_low) / all_low * 100, 2),
        "maxDD1Y": _max_drawdown(one_year),
        "maxDD3Y": _max_drawdown(three_year),
        "sparkline": [round(c, 4) for c in closes[-60:]],
    }


# ============================================================
# KLINE CACHE (disk-based, one file per ETF)
# ============================================================
def _kline_path(code: str) -> Path:
    return KLINE_DIR / f"{code}.json"


def save_kline(code: str, kline: list):
    try:
        _kline_path(code).write_text(json.dumps(kline, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.error(f"Save kline {code} failed: {e}")


def load_kline(code: str) -> list:
    p = _kline_path(code)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


# ============================================================
# SPOT CACHE (disk)
# ============================================================
def save_spot_cache():
    try:
        SPOT_CACHE.write_text(json.dumps({
            "spot": {c: s for c, s in etf_spot.items()},
            "stats": {c: s for c, s in etf_stats.items()},
            "indices": market_indices,
            "updated": last_updated,
            "source": data_source,
        }, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.error(f"Save spot cache failed: {e}")


def load_spot_cache() -> bool:
    global etf_spot, etf_stats, market_indices, last_updated, data_source
    if SPOT_CACHE.exists():
        try:
            d = json.loads(SPOT_CACHE.read_text(encoding="utf-8"))
            etf_spot = d.get("spot", {})
            etf_stats = d.get("stats", {})
            market_indices = d.get("indices", [])
            last_updated = d.get("updated")
            data_source = d.get("source", "cache")
            logger.info(f"Cache loaded: {len(etf_spot)} ETFs, source={data_source}")
            return bool(etf_spot)
        except Exception:
            pass
    return False


# ============================================================
# MOCK FALLBACK
# ============================================================
def _mock_indices():
    return [
        {"name": "上证指数", "val": 3287.45, "chg": 0.82},
        {"name": "深证成指", "val": 10456.78, "chg": -0.35},
        {"name": "创业板指", "val": 2089.12, "chg": 1.15},
    ]

_MOCK_ETFS = [
    ("510300","沪深300ETF",4.1,1282),("510500","中证500ETF",6.8,685),
    ("588000","科创50ETF",1.05,412),("159915","创业板ETF",2.6,356),
    ("510050","上证50ETF",2.9,789),("512100","中证1000ETF",1.45,198),
    ("513100","纳指ETF",1.78,320),("159934","黄金ETF",5.2,156),
    ("512010","医药ETF",0.52,245),("515030","新能源ETF",1.12,178),
    ("512660","军工ETF",1.08,267),("512880","证券ETF",0.95,312),
    ("515790","光伏ETF",0.68,89),("512690","酒ETF",1.32,145),
    ("159869","游戏ETF",0.88,56),("512480","半导体ETF",1.55,398),
    ("513050","中概互联ETF",0.72,210),("512200","房地产ETF",0.62,42),
    ("159766","旅游ETF",0.81,34),("562340","中证A50ETF",1.02,168),
    ("513130","恒生科技ETF",0.68,175),("518880","金ETF",6.1,220),
    ("511010","国债ETF",120.5,95),("159941","纳斯达克ETF",2.35,88),
    ("510330","华夏沪深300",4.85,456),
]

def generate_mock():
    global etf_spot, etf_stats, market_indices, last_updated, data_source
    logger.info("Generating mock data...")
    for code, name, base, scale in _MOCK_ETFS:
        rand = random.Random(int(code) + 42)
        price = base * (0.6 + rand.random() * 0.5)
        kline = []
        d = datetime(2023, 1, 3)
        for _ in range(1100):
            while d.weekday() >= 5:
                d += timedelta(days=1)
            vol = 0.015 + rand.random() * 0.02
            drift = (rand.random() - 0.48) * 0.003
            change = price * (drift + vol * (rand.random() - 0.5) * 2)
            op = price; cl = max(0.01, price + change)
            hi = max(op, cl) * (1 + rand.random() * 0.008)
            lo = min(op, cl) * (1 - rand.random() * 0.008)
            volume = int((50 + rand.random() * 200) * scale * 0.1)
            kline.append({"date": d.strftime("%Y-%m-%d"),
                "open": round(op, 4), "close": round(cl, 4),
                "high": round(hi, 4), "low": round(lo, 4), "volume": volume})
            price = cl; d += timedelta(days=1)
        save_kline(code, kline)
        stats = compute_stats(kline)
        etf_spot[code] = {
            "code": code, "name": name, "currentPrice": round(price, 4),
            "scale": scale, "fee": KNOWN_FEES.get(code, DEFAULT_FEE),
            "chgPct": round((rand.random() - 0.5) * 4, 2), "volume": 0, "turnover": 0,
        }
        etf_stats[code] = stats
    market_indices = _mock_indices()
    last_updated = datetime.now().isoformat()
    data_source = "mock"
    save_spot_cache()
    logger.info(f"Mock data: {len(etf_spot)} ETFs")


# ============================================================
# REFRESH JOBS
# ============================================================
def refresh_spot():
    """Fast refresh: spot data + indices for ALL ETFs. Runs every 1 min."""
    global etf_spot, market_indices, last_updated, data_source
    try:
        new_spot = fetch_spot_akshare()
        if not new_spot:
            logger.warning("Spot refresh returned empty, keeping cached data")
            return
        with _lock:
            # Merge: update existing, add new
            for code, info in new_spot.items():
                if code in etf_spot:
                    etf_spot[code].update(info)
                else:
                    etf_spot[code] = info
            # Remove delisted (not in new spot)
            for code in list(etf_spot.keys()):
                if code not in new_spot and data_source != "mock":
                    del etf_spot[code]
        try:
            indices = fetch_indices_akshare()
            if indices:
                market_indices = indices
        except Exception:
            pass
        last_updated = datetime.now().isoformat()
        data_source = "live"
        save_spot_cache()
        logger.info(f"Spot refreshed: {len(etf_spot)} ETFs")
    except Exception as e:
        logger.error(f"Spot refresh failed: {e}")


def refresh_kline_batch():
    """Slow background: fetch kline + compute stats in batches."""
    global etf_stats
    codes = list(etf_spot.keys())
    random.shuffle(codes)  # Randomize to spread load
    total = len(codes)
    done = 0
    for i in range(0, total, KLINE_BATCH_SIZE):
        batch = codes[i:i + KLINE_BATCH_SIZE]
        for code in batch:
            try:
                kline = fetch_kline_akshare(code)
                if kline and len(kline) >= 10:
                    save_kline(code, kline)
                    stats = compute_stats(kline)
                    with _lock:
                        etf_stats[code] = stats
                    done += 1
            except Exception as e:
                logger.error(f"Kline {code}: {e}")
        logger.info(f"Kline batch progress: {min(i + KLINE_BATCH_SIZE, total)}/{total}")
        time.sleep(0.5)  # Rate limit
    save_spot_cache()
    logger.info(f"Kline refresh complete: {done}/{total} updated")


# ============================================================
# FASTAPI APP
# ============================================================
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    loaded = load_spot_cache()
    # Always try a live refresh at startup, even if cache was loaded
    # This ensures we don't stay stuck on mock data forever
    try:
        logger.info("Startup: attempting live AKShare refresh...")
        refresh_spot()
        logger.info(f"Startup: live refresh succeeded, source={data_source}, {len(etf_spot)} ETFs")
    except Exception as e:
        logger.error(f"Startup: live refresh failed: {e}")
        if not etf_spot:
            generate_mock()
    # Spot refresh every N minutes
    scheduler.add_job(refresh_spot, "interval", minutes=REFRESH_MINUTES,
                      id="spot_refresh", max_instances=1, coalesce=True)
    # Kline refresh every 30 minutes (background, slow)
    scheduler.add_job(refresh_kline_batch, "interval", minutes=30,
                      id="kline_refresh", max_instances=1, coalesce=True)
    scheduler.start()
    logger.info(f"Scheduler started: spot every {REFRESH_MINUTES}min, kline every 30min")
    # Trigger initial kline fetch in background thread
    threading.Thread(target=refresh_kline_batch, daemon=True).start()
    yield
    scheduler.shutdown(wait=False)

app = FastAPI(title="ETF NEXUS", lifespan=lifespan)


@app.get("/api/etf-data")
async def get_etf_data():
    """Return all ETF data (spot + stats, no full kline)."""
    with _lock:
        etfs = []
        for code, spot in etf_spot.items():
            entry = {**spot}
            stats = etf_stats.get(code, {})
            entry["dropFromHigh"] = stats.get("dropFromHigh", None)
            entry["riseFromLow"] = stats.get("riseFromLow", None)
            entry["maxDD1Y"] = stats.get("maxDD1Y", None)
            entry["maxDD3Y"] = stats.get("maxDD3Y", None)
            entry["sparkline"] = stats.get("sparkline", [])
            etfs.append(entry)
    return JSONResponse({
        "etfs": etfs,
        "indices": market_indices,
        "updated": last_updated,
        "source": data_source,
    })


@app.get("/api/kline/{code}")
async def get_kline(code: str, range: str = "1Y"):
    """Return kline data. Tries cache first, then fetches live."""
    kline = load_kline(code)
    if not kline:
        # Try fetching live
        try:
            kline = fetch_kline_akshare(code)
            if kline:
                save_kline(code, kline)
                stats = compute_stats(kline)
                with _lock:
                    etf_stats[code] = stats
        except Exception as e:
            logger.error(f"On-demand kline {code}: {e}")
    if not kline:
        return JSONResponse({"error": "Kline data not available"}, status_code=404)
    range_map = {"1M": 22, "3M": 66, "6M": 132, "1Y": 250, "3Y": 750, "全部": 999999}
    n = range_map.get(range, 250)
    sliced = kline[-min(n, len(kline)):]
    spot = etf_spot.get(code, {})
    return JSONResponse({
        "code": code,
        "name": spot.get("name", code),
        "fee": spot.get("fee", DEFAULT_FEE),
        "scale": spot.get("scale", 0),
        "kline": sliced,
    })


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "etf_count": len(etf_spot),
        "stats_count": len(etf_stats),
        "source": data_source,
        "updated": last_updated,
        "refresh_minutes": REFRESH_MINUTES,
        "proxy": _PROXY or "none",
    }


@app.get("/api/diag")
async def diag():
    """Quick diagnostic: try one AKShare call and report result."""
    result = {
        "proxy": _PROXY or "none",
        "akshare_ok": False,
        "error": None,
        "etf_count": 0,
        "columns": [],
        "sample_row": {},
        "current_source": data_source,
        "current_etf_count": len(etf_spot),
        "current_stats_count": len(etf_stats),
    }
    try:
        import akshare as ak
        result["akshare_version"] = getattr(ak, "__version__", "unknown")
        df = ak.fund_etf_spot_em()
        result["akshare_ok"] = True
        result["etf_count"] = len(df)
        result["columns"] = list(df.columns)
        if not df.empty:
            result["sample_row"] = {str(k): str(v) for k, v in df.iloc[0].to_dict().items()}
    except Exception as e:
        result["error"] = str(e)[:500]
    return result

# Serve static files
static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
