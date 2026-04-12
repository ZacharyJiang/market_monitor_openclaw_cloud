#!/usr/bin/env python3
"""
独立监控采集进度，发现新完成的ETF K线和费率数据就发送通知
支持批量通知（每100只或每10%发送一次汇总通知）
不修改主程序，随时可以停止
"""

import json
import time
import os
import subprocess
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta

BEIJING_TZ = timezone(timedelta(hours=8))
DATA_DIR = Path(__file__).parent / "data"
KLINE_DIR = DATA_DIR / "kline"
STATE_FILE = Path(__file__).parent / "monitor-state.json"
LOG_FILE = Path(__file__).parent / "logs" / "monitor.log"

# 通知间隔配置
BATCH_NOTIFY_COUNT = 100  # 每100只发送一次通知
BATCH_NOTIFY_PERCENT = 10  # 每10%发送一次通知


def log_message(msg):
    """记录日志"""
    timestamp = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {msg}"
    print(log_line)
    
    # 写入日志文件
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")


def get_existing_kline_count():
    """获取当前已采集K线文件数量"""
    if not KLINE_DIR.exists():
        return 0
    return len(list(KLINE_DIR.glob("*.json")))


def get_existing_fee_count():
    """获取当前已采集费率数据数量"""
    fee_file = DATA_DIR / "fee_cache.json"
    if not fee_file.exists():
        return 0
    try:
        data = json.load(open(fee_file, encoding="utf-8"))
        return len(data)
    except Exception:
        return 0


def get_etf_name(code):
    """从spot_cache获取ETF名称"""
    spot_file = DATA_DIR / "spot_cache.json"
    if not spot_file.exists():
        return None
    try:
        data = json.load(open(spot_file, encoding="utf-8"))
        etf_spot = data.get("spot", {})
        if code in etf_spot:
            return etf_spot[code].get("name")
    except Exception:
        pass
    return None


def get_total_etf_count():
    """获取总ETF数量"""
    spot_file = DATA_DIR / "spot_cache.json"
    if spot_file.exists():
        try:
            data = json.load(open(spot_file, encoding="utf-8"))
            return len(data.get("spot", {}))
        except Exception:
            pass
    return 0


def load_last_state():
    """加载上次状态"""
    if not STATE_FILE.exists():
        return {
            "last_kline_count": 0,
            "last_fee_count": 0,
            "notified_kline_files": [],
            "notified_fee_codes": [],
            "last_batch_notify_kline": 0,
            "last_batch_notify_fee": 0,
            "start_time": datetime.now(BEIJING_TZ).isoformat(),
            "last_update": datetime.now(BEIJING_TZ).isoformat()
        }
    try:
        return json.load(open(STATE_FILE, encoding="utf-8"))
    except Exception:
        return {
            "last_kline_count": 0,
            "last_fee_count": 0,
            "notified_kline_files": [],
            "notified_fee_codes": [],
            "last_batch_notify_kline": 0,
            "last_batch_notify_fee": 0,
            "start_time": datetime.now(BEIJING_TZ).isoformat(),
            "last_update": datetime.now(BEIJING_TZ).isoformat()
        }


def save_state(state):
    """保存状态"""
    state["last_update"] = datetime.now(BEIJING_TZ).isoformat()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def calculate_eta(collected, total, start_time_iso):
    """计算预计完成时间"""
    if collected == 0 or total == 0:
        return "计算中..."
    
    try:
        start_time = datetime.fromisoformat(start_time_iso)
        elapsed = (datetime.now(BEIJING_TZ) - start_time).total_seconds()
        rate = collected / elapsed  # 每秒采集数量
        remaining = total - collected
        eta_seconds = remaining / rate if rate > 0 else 0
        
        if eta_seconds < 60:
            return f"{int(eta_seconds)}秒"
        elif eta_seconds < 3600:
            return f"{int(eta_seconds / 60)}分钟"
        else:
            return f"{eta_seconds / 3600:.1f}小时"
    except Exception:
        return "计算中..."


def send_batch_notification(kline_progress, fee_progress, eta_kline, eta_fee, is_complete=False):
    """发送批量进度通知"""
    script = Path(__file__).parent / "progress-notify.py"
    
    if is_complete:
        # 完成通知
        message = "🎉 ETF数据采集完成！\n\n"
        message += f"📈 K线数据: {kline_progress['collected']} / {kline_progress['total']} ({kline_progress['percent']}%)\n"
        message += f"💰 费率数据: {fee_progress['collected']} / {fee_progress['total']} ({fee_progress['percent']}%)\n"
        message += f"⏱️ 总耗时: {eta_kline}\n"
    else:
        # 进度通知
        message = "📊 ETF数据采集进度更新\n\n"
        message += f"📈 K线数据: {kline_progress['collected']} / {kline_progress['total']} ({kline_progress['percent']}%)\n"
        message += f"   ⏳ 剩余: {kline_progress['remaining']} 只\n"
        message += f"   ⌛ 预计: {eta_kline}\n\n"
        message += f"💰 费率数据: {fee_progress['collected']} / {fee_progress['total']} ({fee_progress['percent']}%)\n"
        message += f"   ⏳ 剩余: {fee_progress['remaining']} 只\n"
        message += f"   ⌛ 预计: {eta_fee}\n"
    
    cmd = ["python3", str(script), "--batch-message", message]
    try:
        subprocess.run(cmd, capture_output=False, check=False)
        log_message(f"Batch notification sent: Kline {kline_progress['percent']}%, Fee {fee_progress['percent']}%")
    except Exception as e:
        log_message(f"Failed to send batch notification: {e}")


def check_and_notify_batch(current_kline, current_fee, state, total):
    """检查是否需要发送批量通知"""
    last_batch_kline = state.get("last_batch_notify_kline", 0)
    last_batch_fee = state.get("last_batch_notify_fee", 0)
    start_time = state.get("start_time", datetime.now(BEIJING_TZ).isoformat())
    
    should_notify = False
    
    # 检查是否达到100只的倍数
    if current_kline >= last_batch_kline + BATCH_NOTIFY_COUNT:
        should_notify = True
        state["last_batch_notify_kline"] = (current_kline // BATCH_NOTIFY_COUNT) * BATCH_NOTIFY_COUNT
    
    # 检查是否达到10%的倍数
    if total > 0:
        current_percent = (current_kline / total) * 100
        last_percent = (last_batch_kline / total) * 100
        if int(current_percent / BATCH_NOTIFY_PERCENT) > int(last_percent / BATCH_NOTIFY_PERCENT):
            should_notify = True
            state["last_batch_notify_kline"] = current_kline
    
    # 检查费率数据批量通知
    if current_fee >= last_batch_fee + BATCH_NOTIFY_COUNT:
        should_notify = True
        state["last_batch_notify_fee"] = (current_fee // BATCH_NOTIFY_COUNT) * BATCH_NOTIFY_COUNT
    
    if should_notify and total > 0:
        kline_progress = {
            "collected": current_kline,
            "total": total,
            "remaining": total - current_kline,
            "percent": round((current_kline / total) * 100, 1)
        }
        fee_progress = {
            "collected": current_fee,
            "total": total,
            "remaining": total - current_fee,
            "percent": round((current_fee / total) * 100, 1)
        }
        
        eta_kline = calculate_eta(current_kline, total, start_time)
        eta_fee = calculate_eta(current_fee, total, start_time)
        
        send_batch_notification(kline_progress, fee_progress, eta_kline, eta_fee)
        save_state(state)
    
    return state


def main():
    log_message("Starting ETF collection progress monitor...")
    state = load_last_state()
    
    # 如果状态文件太旧（超过1天），重置状态
    try:
        last_update = datetime.fromisoformat(state.get("last_update", ""))
        if (datetime.now(BEIJING_TZ) - last_update).days >= 1:
            log_message("State file is older than 1 day, resetting...")
            state = {
                "last_kline_count": 0,
                "last_fee_count": 0,
                "notified_kline_files": [],
                "notified_fee_codes": [],
                "last_batch_notify_kline": 0,
                "last_batch_notify_fee": 0,
                "start_time": datetime.now(BEIJING_TZ).isoformat(),
                "last_update": datetime.now(BEIJING_TZ).isoformat()
            }
    except Exception:
        pass
    
    last_kline_count = state.get("last_kline_count", 0)
    last_fee_count = state.get("last_fee_count", 0)
    notified_kline_files = set(state.get("notified_kline_files", []))
    notified_fee_codes = set(state.get("notified_fee_codes", []))
    
    total = get_total_etf_count()
    log_message(f"Initial state: Kline={last_kline_count}, Fee={last_fee_count}, Total ETFs={total}")
    
    while True:
        try:
            # 获取当前数量
            current_kline = get_existing_kline_count()
            current_fee = get_existing_fee_count()
            total = get_total_etf_count() or max(current_kline, current_fee, 1500)
            
            # 检查是否有新增K线数据
            if current_kline > last_kline_count:
                all_kline_files = {p.stem for p in KLINE_DIR.glob("*.json")}
                new_kline_files = all_kline_files - notified_kline_files
                
                for code in new_kline_files:
                    if len(code) == 6:
                        name = get_etf_name(code)
                        log_message(f"New Kline collected: {code} {name or ''}")
                        notified_kline_files.add(code)
                
                last_kline_count = current_kline
                state["last_kline_count"] = last_kline_count
                state["notified_kline_files"] = list(notified_kline_files)
            
            # 检查是否有新增费率数据
            if current_fee > last_fee_count:
                fee_file = DATA_DIR / "fee_cache.json"
                if fee_file.exists():
                    try:
                        fee_data = json.load(open(fee_file, encoding="utf-8"))
                        new_fee_codes = set(fee_data.keys()) - notified_fee_codes
                        
                        for code in new_fee_codes:
                            if len(code) == 6:
                                name = get_etf_name(code)
                                log_message(f"New Fee collected: {code} {name or ''}")
                                notified_fee_codes.add(code)
                        
                        last_fee_count = current_fee
                        state["last_fee_count"] = last_fee_count
                        state["notified_fee_codes"] = list(notified_fee_codes)
                    except Exception as e:
                        log_message(f"Error reading fee cache: {e}")
            
            # 检查是否需要发送批量通知
            state = check_and_notify_batch(current_kline, current_fee, state, total)
            
            # 检查是否全部完成
            if current_kline >= total and current_fee >= total and total > 0:
                # 发送完成通知（只发送一次）
                if not state.get("completion_notified", False):
                    kline_progress = {
                        "collected": current_kline,
                        "total": total,
                        "remaining": 0,
                        "percent": 100.0
                    }
                    fee_progress = {
                        "collected": current_fee,
                        "total": total,
                        "remaining": 0,
                        "percent": 100.0
                    }
                    start_time = state.get("start_time", datetime.now(BEIJING_TZ).isoformat())
                    eta = calculate_eta(current_kline, total, start_time)
                    
                    send_batch_notification(kline_progress, fee_progress, eta, eta, is_complete=True)
                    state["completion_notified"] = True
                    save_state(state)
                    log_message("Collection complete! Completion notification sent.")
            
            save_state(state)
            
            # 每分钟检查一次
            time.sleep(60)
            
        except KeyboardInterrupt:
            log_message("Monitor stopped by user")
            break
        except Exception as e:
            log_message(f"Error in monitor loop: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
