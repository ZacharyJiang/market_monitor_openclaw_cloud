#!/bin/bash
# 自动拉取 GitHub 最新代码并重启服务
# 作者: ZacharyJiang
# 日期: 2026-04-05
# 修改: 2026-04-11 - 修复路径，支持后台异步执行

# 使用环境变量或默认值设置仓库路径
REPO_PATH="${REPO_PATH:-/app}"
LOG_FILE="${REPO_PATH}/logs/auto-update.log"

# 创建日志目录
mkdir -p "$(dirname "$LOG_FILE")"

# 后台执行标志
BACKGROUND_MODE="${1:-}"

# 如果不是后台模式，则启动后台进程并立即返回
if [ "$BACKGROUND_MODE" != "--background" ]; then
    echo "启动后台更新任务..."
    nohup "$0" --background > /dev/null 2>&1 &
    echo "更新任务已在后台启动，请查看日志: $LOG_FILE"
    exit 0
fi

# 后台模式开始执行
cd "$REPO_PATH" || {
    echo "[ERROR] 无法进入目录: $REPO_PATH" >> "$LOG_FILE"
    exit 1
}

exec >> "$LOG_FILE" 2>&1

echo ""
echo "=== $(date '+%Y-%m-%d %H:%M:%S') 开始检测更新 ==="

# 拉取最新代码
echo "[INFO] 获取远程更新..."
git fetch origin || {
    echo "[ERROR] git fetch 失败"
    exit 1
}

# 检查是否有更新
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "[INFO] 当前已是最新版本，无需更新"
    echo "Local:  $LOCAL"
    echo "Remote: $REMOTE"
    echo "=== 检查完成 ==="
    exit 0
fi

echo "[INFO] 发现新版本"
echo "Local:  $LOCAL"
echo "Remote: $REMOTE"
echo "[INFO] 开始更新..."

# 拉取更新
git pull origin main || {
    echo "[ERROR] git pull 失败"
    exit 1
}

# 重新构建 Docker 镜像
echo "[INFO] 重新构建 Docker 镜像..."
docker build -t a-share-etf-monitor . || {
    echo "[ERROR] Docker 构建失败"
    exit 1
}

# 停止并删除旧容器
if [ "$(docker ps -aq -f name=a-share-etf-monitor)" ]; then
    echo "[INFO] 停止旧容器..."
    docker stop a-share-etf-monitor
    docker rm a-share-etf-monitor
fi

# 启动新容器
echo "[INFO] 启动新容器..."
docker run -d \
    --name a-share-etf-monitor \
    --restart unless-stopped \
    -p 127.0.0.1:8081:8080 \
    --env-file .env \
    -e USE_MOCK=false \
    a-share-etf-monitor || {
    echo "[ERROR] 容器启动失败"
    exit 1
}

echo "[SUCCESS] 更新完成，服务已重启"
echo "=== 更新完成 $(date '+%Y-%m-%d %H:%M:%S') ==="

# 发送通知（可选，可以配置飞书通知）
if [ -n "$FEISHU_WEBHOOK" ]; then
    curl -X POST -H "Content-Type: application/json" \
        -d '{"msg_type":"text","content":{"text":"ETF监控已自动更新到最新版本"}}' \
        "$FEISHU_WEBHOOK" || echo "[WARN] 飞书通知发送失败"
fi
