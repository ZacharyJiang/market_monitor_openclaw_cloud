"""
ETF NEXUS — A股场内ETF实时数据终端 Backend
FastAPI + AKShare + APScheduler
"""
import os, json, time, logging, math, random
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etf-nexus")

# ============================================================
# CONFIG
# ============================================================
REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", "1"))
DATA_FILE = Path(__file__).parent / "etf_cache.json"

# Target ETFs (code -> metadata)
TARGET_ETFS = {
    "510300": {"name": "沪深300ETF", "fee": 0.20},
    "510500": {"name": "中证500ETF", "fee": 0.20},
    "588000": {"name": "科创50ETF", "fee": 0.20},
    "159915": {"name": "创业板ETF", "fee": 0.20},
    "510050": {"name": "上证50ETF", "fee": 0.20},
    "512100": {"name": "中证1000ETF", "fee": 0.20},
    "513100": {"name": "纳指ETF", "fee": 0.35},
    "159934": {"name": "黄金ETF", "fee": 0.15},
    "512010": {"name": "医药ETF", "fee": 0.20},
    "515030": {"name": "新能源ETF", "fee": 0.20},
    "512660": {"name": "军工ETF", "fee": 0.20},
    "512880": {"name": "证券ETF", "fee": 0.20},
    "515790": {"name": "光伏ETF", "fee": 0.20},
    "512690": {"name": "酒ETF", "fee": 0.20},
    "159869": {"name": "游戏ETF", "fee": 0.20},
    "512480": {"name": "半导体ETF", "fee": 0.20},
    "513050": {"name": "中概互联ETF", "fee": 0.35},
    "512200": {"name": "房地产ETF", "fee": 0.20},
    "159766": {"name": "旅游ETF", "fee": 0.20},
    "562340": {"name": "中证A50ETF", "fee": 0.15},
    "513130": {"name": "恒生科技ETF", "fee": 0.35},
    "518880": {"name": "金ETF", "fee": 0.15},
    "511010": {"name": "国债ETF", "fee": 0.10},
    "159941": {"name": "纳斯达克ETF", "fee": 0.35},
    "510330": {"name": "华夏沪深300", "fee": 0.20},
}

# ============================================================
# DATA STORE (in-memory, persisted to JSON)
# ============================================================
etf_store = {"etfs": [], "indices": [], "updated": None, "source": "none"}


# ============================================================
# AKSHARE DATA FETCHER
# ============================================================
def fetch_real_data():
    """Fetch real data via AKShare. Returns True on success."""
    import signal
    def _timeout_handler(signum, frame):
        raise TimeoutError("AKShare fetch timed out")
    try:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(30)  # 30s total timeout for AKShare
        import akshare as ak
        import pandas as pd
        logger.info("Fetching real ETF data via AKShare...")

        # 1. Get real-time spot data for all ETFs
        spot_df = ak.fund_etf_spot_em()
        spot_df["代码"] = spot_df["代码"].astype(str).str.zfill(6)
        spot_map = {}
        for _, row in spot_df.iterrows():
            spot_map[row["代码"]] = row

        etfs = []
        for code, meta in TARGET_ETFS.items():
            try:
                # Get historical K-line (3+ years)
                end_date = datetime.now().strftime("%Y%m%d")
                start_date = (datetime.now() - timedelta(days=1200)).strftime("%Y%m%d")
                hist = ak.fund_etf_hist_em(
                    symbol=code, period="daily",
                    start_date=start_date, end_date=end_date, adjust="qfq"
                )
                if hist.empty:
                    logger.warning(f"No hist data for {code}, skipping")
                    continue

                kline = []
                for _, r in hist.iterrows():
                    kline.append({
                        "date": str(r["日期"])[:10],
                        "open": round(float(r["开盘"]), 4),
                        "close": round(float(r["收盘"]), 4),
                        "high": round(float(r["最高"]), 4),
                        "low": round(float(r["最低"]), 4),
                        "volume": int(r["成交量"]),
                    })

                # Current price from spot or last kline
                spot = spot_map.get(code)
                current_price = float(spot["最新价"]) if spot is not None else kline[-1]["close"]
                scale_raw = float(spot["总市值"]) if spot is not None and "总市值" in spot.index else 0
                scale = round(scale_raw / 1e8, 2) if scale_raw > 0 else 0  # convert to 亿

                # Calc stats
                closes = [k["close"] for k in kline]
                all_high = max(closes)
                all_low = min(closes)
                drop_from_high = round((current_price - all_high) / all_high * 100, 2)
                rise_from_low = round((current_price - all_low) / all_low * 100, 2)

                def max_drawdown(arr):
                    peak = arr[0]
                    mdd = 0
                    for v in arr:
                        if v > peak:
                            peak = v
                        dd = (v - peak) / peak
                        if dd < mdd:
                            mdd = dd
                    return round(mdd * 100, 2)

                one_year = closes[-250:] if len(closes) > 250 else closes
                three_year = closes[-750:] if len(closes) > 750 else closes

                etfs.append({
                    "code": code,
                    "name": meta["name"],
                    "scale": scale,
                    "fee": meta["fee"],
                    "currentPrice": round(current_price, 4),
                    "dropFromHigh": drop_from_high,
                    "riseFromLow": rise_from_low,
                    "maxDD1Y": max_drawdown(one_year),
                    "maxDD3Y": max_drawdown(three_year),
                    "kline": kline,
                })
                logger.info(f"  ✓ {code} {meta['name']} - {len(kline)} days")
            except Exception as e:
                logger.error(f"  ✗ {code}: {e}")
                continue

        # Fetch index data
        indices = []
        for idx_code, idx_name in [("000001", "上证指数"), ("399001", "深证成指"), ("399006", "创业板指")]:
            try:
                idx_df = ak.stock_zh_index_spot_em(symbol=idx_code)
                if idx_df is not None and not idx_df.empty:
                    row = idx_df.iloc[0]
                    indices.append({
                        "name": idx_name,
                        "val": round(float(row.get("最新价", 0)), 2),
                        "chg": round(float(row.get("涨跌幅", 0)), 2),
                    })
            except Exception:
                pass

        if not indices:
            indices = _mock_indices()

        if etfs:
            global etf_store
            etf_store = {
                "etfs": etfs,
                "indices": indices,
                "updated": datetime.now().isoformat(),
                "source": "akshare",
            }
            _save_cache()
            logger.info(f"Real data fetched: {len(etfs)} ETFs")
            signal.alarm(0)
            return True
        signal.alarm(0)
        return False

    except (Exception, TimeoutError) as e:
        signal.alarm(0)
        logger.error(f"AKShare fetch failed: {e}")
        return False


# ============================================================
# MOCK DATA FALLBACK
# ============================================================
def _seeded_random(seed):
    r = random.Random(seed)
    return r.random

def _mock_indices():
    return [
        {"name": "上证指数", "val": 3287.45, "chg": 0.82},
        {"name": "深证成指", "val": 10456.78, "chg": -0.35},
        {"name": "创业板指", "val": 2089.12, "chg": 1.15},
    ]

BASE_PRICES = {
    "510300":4.1,"510500":6.8,"588000":1.05,"159915":2.6,"510050":2.9,
    "512100":1.45,"513100":1.78,"159934":5.2,"512010":0.52,"515030":1.12,
    "512660":1.08,"512880":0.95,"515790":0.68,"512690":1.32,"159869":0.88,
    "512480":1.55,"513050":0.72,"512200":0.62,"159766":0.81,"562340":1.02,
    "513130":0.68,"518880":6.1,"511010":120.5,"159941":2.35,"510330":4.85,
}
MOCK_SCALES = {
    "510300":1282,"510500":685,"588000":412,"159915":356,"510050":789,
    "512100":198,"513100":320,"159934":156,"512010":245,"515030":178,
    "512660":267,"512880":312,"515790":89,"512690":145,"159869":56,
    "512480":398,"513050":210,"512200":42,"159766":34,"562340":168,
    "513130":175,"518880":220,"511010":95,"159941":88,"510330":456,
}

def generate_mock_data():
    """Generate realistic mock data as fallback."""
    global etf_store
    logger.info("Generating mock data fallback...")
    etfs = []
    for code, meta in TARGET_ETFS.items():
        rand = random.Random(int(code) + 42)
        base = BASE_PRICES.get(code, 1.0)
        scale = MOCK_SCALES.get(code, 100)
        price = base * (0.6 + rand.random() * 0.5)
        kline = []
        d = datetime(2023, 1, 3)
        for _ in range(1100):
            while d.weekday() >= 5:
                d += timedelta(days=1)
            vol = 0.015 + rand.random() * 0.02
            drift = (rand.random() - 0.48) * 0.003
            change = price * (drift + vol * (rand.random() - 0.5) * 2)
            op = price
            cl = max(0.01, price + change)
            hi = max(op, cl) * (1 + rand.random() * 0.008)
            lo = min(op, cl) * (1 - rand.random() * 0.008)
            volume = int((50 + rand.random() * 200) * scale * 0.1)
            kline.append({
                "date": d.strftime("%Y-%m-%d"),
                "open": round(op, 4), "close": round(cl, 4),
                "high": round(hi, 4), "low": round(lo, 4),
                "volume": volume,
            })
            price = cl
            d += timedelta(days=1)

        closes = [k["close"] for k in kline]
        current = closes[-1]
        ah = max(closes); al = min(closes)
        def mdd(arr):
            pk = arr[0]; md = 0
            for v in arr:
                if v > pk: pk = v
                dd = (v - pk) / pk
                if dd < md: md = dd
            return round(md * 100, 2)

        etfs.append({
            "code": code, "name": meta["name"], "scale": scale, "fee": meta["fee"],
            "currentPrice": round(current, 4),
            "dropFromHigh": round((current - ah) / ah * 100, 2),
            "riseFromLow": round((current - al) / al * 100, 2),
            "maxDD1Y": mdd(closes[-250:] if len(closes) > 250 else closes),
            "maxDD3Y": mdd(closes[-750:] if len(closes) > 750 else closes),
            "kline": kline,
        })

    etf_store = {
        "etfs": etfs,
        "indices": _mock_indices(),
        "updated": datetime.now().isoformat(),
        "source": "mock",
    }
    _save_cache()
    logger.info(f"Mock data generated: {len(etfs)} ETFs")


# ============================================================
# CACHE PERSISTENCE
# ============================================================
def _save_cache():
    try:
        DATA_FILE.write_text(json.dumps(etf_store, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.error(f"Save cache failed: {e}")

def _load_cache():
    global etf_store
    if DATA_FILE.exists():
        try:
            etf_store = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            logger.info(f"Cache loaded: {len(etf_store.get('etfs',[]))} ETFs, source={etf_store.get('source')}")
            return True
        except Exception:
            pass
    return False


# ============================================================
# SCHEDULER JOB
# ============================================================
def refresh_job():
    """Called by scheduler every N minutes."""
    logger.info(f"Scheduled refresh triggered (interval={REFRESH_MINUTES}min)")
    if not fetch_real_data():
        # Only generate mock if we have NO data at all
        if not etf_store.get("etfs"):
            generate_mock_data()
        else:
            logger.info("AKShare failed but cached data still valid, skipping mock regeneration")


# ============================================================
# FASTAPI APP
# ============================================================
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    loaded = _load_cache()
    if not loaded:
        if not fetch_real_data():
            generate_mock_data()
    # Schedule periodic refresh
    scheduler.add_job(refresh_job, "interval", minutes=REFRESH_MINUTES, id="etf_refresh",
                      max_instances=1, coalesce=True)
    scheduler.start()
    logger.info(f"Scheduler started: refresh every {REFRESH_MINUTES} minute(s)")
    yield
    # Shutdown
    scheduler.shutdown(wait=False)

app = FastAPI(title="ETF NEXUS", lifespan=lifespan)

@app.get("/api/etf-data")
async def get_etf_data():
    """Return all ETF data (without kline to keep payload small)."""
    etfs_slim = []
    for e in etf_store.get("etfs", []):
        slim = {k: v for k, v in e.items() if k != "kline"}
        # Include last 60 closes for sparkline
        kline = e.get("kline", [])
        slim["sparkline"] = [k["close"] for k in kline[-60:]]
        etfs_slim.append(slim)
    return JSONResponse({
        "etfs": etfs_slim,
        "indices": etf_store.get("indices", []),
        "updated": etf_store.get("updated"),
        "source": etf_store.get("source"),
    })

@app.get("/api/kline/{code}")
async def get_kline(code: str, range: str = "1Y"):
    """Return full kline data for a specific ETF."""
    etf = next((e for e in etf_store.get("etfs", []) if e["code"] == code), None)
    if not etf:
        return JSONResponse({"error": "ETF not found"}, status_code=404)
    range_map = {"1M": 22, "3M": 66, "6M": 132, "1Y": 250, "3Y": 750, "全部": 999999}
    n = range_map.get(range, 250)
    kline = etf["kline"][-min(n, len(etf["kline"])):]
    return JSONResponse({
        "code": code,
        "name": etf["name"],
        "fee": etf["fee"],
        "scale": etf["scale"],
        "kline": kline,
    })

@app.get("/api/health")
async def health():
    return {"status": "ok", "etf_count": len(etf_store.get("etfs", [])),
            "source": etf_store.get("source"), "updated": etf_store.get("updated"),
            "refresh_minutes": REFRESH_MINUTES}

# Serve static files
static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
