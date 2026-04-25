#!/bin/bash
# 自动拉取 GitHub 最新代码并重启服务（无 Docker 模式）

REPO_PATH="${REPO_PATH:-/root/.openclaw/workspace_coder/a_share_market_monitor}"
LOG_FILE="${REPO_PATH}/logs/auto-update.log"
APP_LOG="${REPO_PATH}/logs/app.log"
UVICORN_BIN="${UVICORN_BIN:-/usr/local/bin/uvicorn}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
APP_PORT="${APP_PORT:-8080}"

mkdir -p "$(dirname "$LOG_FILE")"

BACKGROUND_MODE="${1:-}"
if [ "$BACKGROUND_MODE" != "--background" ]; then
    nohup "$0" --background > /dev/null 2>&1 &
    echo "更新任务已在后台启动，日志: $LOG_FILE"
    exit 0
fi

cd "$REPO_PATH" || { echo "[ERROR] 无法进入目录: $REPO_PATH" >> "$LOG_FILE"; exit 1; }
exec >> "$LOG_FILE" 2>&1

echo ""
echo "=== $(date '+%Y-%m-%d %H:%M:%S') 开始检测更新 ==="

git fetch origin || { echo "[ERROR] git fetch 失败"; exit 1; }

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "[INFO] 已是最新版本 ($LOCAL)"
    echo "=== 检查完成 ==="
    exit 0
fi

echo "[INFO] 发现新版本: $LOCAL -> $REMOTE"
git pull origin main || { echo "[ERROR] git pull 失败"; exit 1; }
echo "[INFO] 代码更新完成"

# 找到正在运行的 uvicorn 进程并终止
OLD_PID=$(pgrep -f "uvicorn main_optimized:app" | head -1)
if [ -n "$OLD_PID" ]; then
    echo "[INFO] 终止旧进程 PID=$OLD_PID ..."
    kill "$OLD_PID"
    sleep 3
else
    echo "[WARN] 未找到运行中的 uvicorn 进程"
fi

# 重新启动
echo "[INFO] 启动新进程..."
source .env 2>/dev/null || true
nohup "$PYTHON_BIN" "$UVICORN_BIN" main_optimized:app \
    --host 0.0.0.0 --port "$APP_PORT" \
    >> "$APP_LOG" 2>&1 &

NEW_PID=$!
sleep 2

if kill -0 "$NEW_PID" 2>/dev/null; then
    echo "[SUCCESS] 新进程启动成功 PID=$NEW_PID"
else
    echo "[ERROR] 新进程启动失败，请检查 $APP_LOG"
    exit 1
fi

echo "=== 更新完成 $(date '+%Y-%m-%d %H:%M:%S') ==="

if [ -n "$FEISHU_WEBHOOK" ]; then
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"msg_type":"text","content":{"text":"ETF监控已自动更新到最新版本"}}' \
        "$FEISHU_WEBHOOK" || true
fi
