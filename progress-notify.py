#!/usr/bin/env python3
"""
ETF 采集进度通知
支持批量进度通知和完成通知
"""

import json
import os
import sys
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta

BEIJING_TZ = timezone(timedelta(hours=8))
DATA_DIR = Path(__file__).parent / "data"
KLINE_DIR = DATA_DIR / "kline"


def get_progress():
    """获取当前采集进度"""
    # 已采集的K线文件数量
    kline_files = list(KLINE_DIR.glob("*.json")) if KLINE_DIR.exists() else []
    kline_collected = len(kline_files)
    
    # 已采集的费率数据数量
    fee_collected = 0
    fee_file = DATA_DIR / "fee_cache.json"
    if fee_file.exists():
        try:
            fee_data = json.load(open(fee_file, encoding="utf-8"))
            fee_collected = len(fee_data)
        except Exception as e:
            print(f"Warning: read fee_cache failed: {e}")
    
    # 读取spot_cache获取总ETF数量
    total = 0
    spot_file = DATA_DIR / "spot_cache.json"
    if spot_file.exists():
        try:
            data = json.load(open(spot_file, encoding="utf-8"))
            etf_spot = data.get("spot", {})
            total = len(etf_spot)
        except Exception as e:
            print(f"Warning: read spot_cache failed: {e}")
    
    if total == 0:
        total = max(kline_collected, fee_collected, 1500)  # 默认大约1500只
    
    return {
        "kline": {
            "collected": kline_collected,
            "total": total,
            "remaining": total - kline_collected,
            "percent": round((kline_collected / total * 100), 2) if total > 0 else 0
        },
        "fee": {
            "collected": fee_collected,
            "total": total,
            "remaining": total - fee_collected,
            "percent": round((fee_collected / total * 100), 2) if total > 0 else 0
        }
    }


def generate_batch_message(kline_progress, fee_progress, eta_kline="", eta_fee=""):
    """生成批量进度通知消息"""
    msg = "📊 ETF数据采集进度更新\n\n"
    
    # K线进度
    msg += f"📈 K线数据\n"
    msg += f"   进度: {kline_progress['collected']} / {kline_progress['total']} ({kline_progress['percent']}%)\n"
    msg += f"   剩余: {kline_progress['remaining']} 只\n"
    if eta_kline:
        msg += f"   预计: {eta_kline}\n"
    
    msg += "\n"
    
    # 费率进度
    msg += f"💰 费率数据\n"
    msg += f"   进度: {fee_progress['collected']} / {fee_progress['total']} ({fee_progress['percent']}%)\n"
    msg += f"   剩余: {fee_progress['remaining']} 只\n"
    if eta_fee:
        msg += f"   预计: {eta_fee}\n"
    
    return msg


def generate_completion_message(kline_progress, fee_progress, total_time=""):
    """生成完成通知消息"""
    msg = "🎉 ETF数据采集完成！\n\n"
    msg += f"📈 K线数据: {kline_progress['collected']} / {kline_progress['total']} ({kline_progress['percent']}%)\n"
    msg += f"💰 费率数据: {fee_progress['collected']} / {fee_progress['total']} ({fee_progress['percent']}%)\n"
    if total_time:
        msg += f"⏱️ 总耗时: {total_time}\n"
    msg += "\n✅ 所有数据已就绪！"
    return msg


def send_feishu_notification(message):
    """通过openclaw gateway API发送飞书通知"""
    import requests
    
    # 读取OPENCLAW_TOKEN
    token_paths = [
        "/root/.openclaw/workspace_coder/ai-newsletter/.env",
        "/opt/market_monitor_openclaw_cloud/.env",
        str(Path(__file__).parent / ".env")
    ]
    
    for token_path in token_paths:
        if os.path.exists(token_path):
            try:
                with open(token_path) as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, value = line.split("=", 1)
                            if key.strip() == "OPENCLAW_TOKEN":
                                os.environ[key.strip()] = value.strip()
                                break
            except Exception:
                continue
    
    token = os.environ.get("OPENCLAW_TOKEN")
    if not token:
        print("ERROR: OPENCLAW_TOKEN not found")
        return False
    
    url = "http://127.0.0.1:10845/api/v1/send-text"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "channel": "feishu",
        "target": "ou_67da0b7029564a121bf82791fb433864",
        "text": message
    }
    
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print("Notification sent successfully")
        return True
    except Exception as e:
        print(f"Failed to send notification: {e}")
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python progress-notify.py <code> [name]")
        print("   or: python progress-notify.py --batch-message '<message>'")
        sys.exit(1)
    
    # 批量消息模式
    if sys.argv[1] == "--batch-message":
        if len(sys.argv) >= 3:
            message = sys.argv[2]
        else:
            # 生成当前进度消息
            prog = get_progress()
            message = generate_batch_message(prog["kline"], prog["fee"])
        print(message)
        send_feishu_notification(message)
        return
    
    # 单只ETF通知模式（已弃用，保留兼容）
    code = sys.argv[1]
    name = sys.argv[2] if len(sys.argv) >= 3 else None
    
    prog = get_progress()
    name_str = f"({name})" if name else ""
    
    msg = f"✅ 已完成ETF采集: {code} {name_str}\n\n"
    msg += f"📈 K线进度: {prog['kline']['collected']} / {prog['kline']['total']} ({prog['kline']['percent']}%)\n"
    msg += f"💰 费率进度: {prog['fee']['collected']} / {prog['fee']['total']} ({prog['fee']['percent']}%)\n"
    
    print(msg)
    send_feishu_notification(msg)


if __name__ == "__main__":
    main()
