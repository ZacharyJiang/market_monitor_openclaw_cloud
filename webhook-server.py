#!/usr/bin/env python3
"""
独立 Webhook 服务器 - 运行在宿主机上
接收 GitHub webhook 并触发容器更新
"""

import os
import sys
import hmac
import hashlib
import subprocess
import logging
from flask import Flask, request, jsonify

# 配置
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
REPO_PATH = os.environ.get("REPO_PATH", "/opt/market_monitor_openclaw_cloud")
LOG_FILE = os.path.join(REPO_PATH, "logs", "webhook-server.log")

# 设置日志
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("webhook-server")

app = Flask(__name__)


def verify_signature(payload: bytes, signature: str, secret: str) -> bool:
    """验证 GitHub webhook 签名"""
    if not secret:
        return True
    if not signature:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.route("/webhook", methods=["POST"])
def github_webhook():
    """GitHub webhook endpoint"""
    body = request.get_data()
    signature = request.headers.get("X-Hub-Signature-256", "")
    
    if not verify_signature(body, signature, WEBHOOK_SECRET):
        logger.warning("Invalid webhook signature")
        return jsonify({"error": "Invalid signature"}), 401
    
    event = request.headers.get("X-GitHub-Event", "")
    if event != "push":
        logger.info(f"Ignored event: {event}")
        return jsonify({"message": f"Ignored event: {event}"}), 200
    
    # 立即返回成功响应
    logger.info("Received push event, triggering update...")
    
    # 在后台执行更新脚本
    try:
        subprocess.Popen(
            ["bash", f"{REPO_PATH}/auto-update.sh"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=REPO_PATH
        )
        logger.info("Update script started in background")
        return jsonify({
            "status": "success",
            "message": "Update triggered",
            "log_file": LOG_FILE
        }), 200
    except Exception as e:
        logger.error(f"Failed to start update: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    """健康检查"""
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    # 运行在 8082 端口，仅监听 localhost
    # Nginx 会将 /webhook 请求代理到这里
    port = int(os.environ.get("WEBHOOK_PORT", 8082))
    logger.info(f"Starting webhook server on port {port}")
    app.run(host="127.0.0.1", port=port, threaded=True)
