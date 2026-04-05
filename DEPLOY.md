# ETF NEXUS 部署指南（腾讯云 + Cloudflare）

## 1. 服务器准备

1. 在腾讯云服务器安装 Python 3.12+（或 Docker）。
2. 开放安全组端口：`80`、`443`（Nginx）以及内部应用端口（如 `8080`，仅本机监听）。
3. 拉取代码到服务器，例如：

```bash
git clone <your-repo-url> /opt/market_monitor_openclaw_cloud
cd /opt/market_monitor_openclaw_cloud
```

## 2. 应用启动（直接运行方式）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# 按需调整 .env
set -a && source .env && set +a
uvicorn main:app --host 127.0.0.1 --port 8080
```

建议使用 `systemd` 托管进程，避免会话断开后服务退出。

## 3. Nginx 反向代理

示例 `server` 配置（`/etc/nginx/conf.d/market-monitor.conf`）：

```nginx
server {
    listen 80;
    server_name market-monitor.uk;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

检查并重载：

```bash
nginx -t && systemctl reload nginx
```

## 4. Cloudflare 域名配置

1. `A` 记录：`market-monitor.uk` 指向腾讯云公网 IP。
2. 代理模式：可先 `DNS only` 验证可用，再切换为 `Proxied`。
3. SSL/TLS 建议使用 `Full` 或 `Full (strict)`（配合源站证书）。

## 5. 数据源与状态说明

- 当前后端只使用真实行情接口，不再自动生成 Demo/Mock 数据。
- 数据源策略：
  - ETF实时行情：`Eastmoney -> Sina` 回退
  - 指数：`Eastmoney -> Sina` 回退
  - K线：`Eastmoney -> Tencent` 回退
- `source` 字段说明：
  - `live`: 实时拉取成功
  - `cache`: 实时拉取失败，使用上次缓存
  - `degraded`: 实时和缓存均不可用

## 6. 常用检查

```bash
curl -s http://127.0.0.1:8080/api/health
curl -s http://127.0.0.1:8080/api/etf-data | head
```

关键限频参数（建议按默认值起步）：

- `API_BASE_INTERVAL`: Eastmoney 基础请求间隔
- `API_MAX_INTERVAL`: Eastmoney 失败后最大退避间隔
- `SECONDARY_API_INTERVAL`: 新浪/腾讯备用源请求最小间隔
