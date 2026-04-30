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
uvicorn main_optimized:app --host 127.0.0.1 --port 8080
```

建议使用 `systemd` 托管进程，避免会话断开后服务退出。

## 3. Nginx 反向代理

### 3.1 HTTP 配置（Flexible SSL 模式）

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

### 3.2 HTTPS 配置（Full SSL 模式 - 推荐）

当 Cloudflare SSL/TLS 设置为 `Full` 或 `Full (strict)` 时，需要源站支持 HTTPS。

**方式一：使用 Cloudflare Origin CA 证书（推荐）**

1. 在 Cloudflare Dashboard → SSL/TLS → Origin Server → 创建证书
2. 下载证书和私钥到服务器 `/etc/nginx/ssl/`
3. Nginx 配置：

```nginx
server {
    listen 80;
    server_name market-monitor.uk;
    # HTTP 重定向到 HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name market-monitor.uk;

    # Cloudflare Origin CA 证书路径
    ssl_certificate /etc/nginx/ssl/cloudflare-origin.pem;
    ssl_certificate_key /etc/nginx/ssl/cloudflare-origin.key;

    # SSL 安全配置
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;

    # 安全头部
    add_header Strict-Transport-Security "max-age=63072000" always;

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

**方式二：使用 Let's Encrypt 证书（Full strict 模式）**

```bash
# 安装 certbot
apt-get install certbot python3-certbot-nginx

# 申请证书
certbot --nginx -d market-monitor.uk

# 自动续期测试
certbot renew --dry-run
```

certbot 会自动修改 Nginx 配置添加 SSL 支持。

检查并重载：

```bash
nginx -t && systemctl reload nginx
```

## 4. Cloudflare 域名配置

1. `A` 记录：`market-monitor.uk` 指向腾讯云公网 IP。
2. 代理模式：可先 `DNS only` 验证可用，再切换为 `Proxied`。
3. **SSL/TLS 加密模式**：
   - `Flexible`：Cloudflare 到源站使用 HTTP（不推荐，安全性较低）
   - **`Full`**：Cloudflare 到源站使用 HTTPS，不验证源站证书（推荐，使用 Cloudflare Origin CA）
   - `Full (strict)`：Cloudflare 到源站使用 HTTPS 并验证证书（需要 Let's Encrypt 等受信任证书）

### Webhook 配置（用于自动部署）

当使用 `Full` SSL 模式时，GitHub Webhook 仍然可以正常工作，因为 Cloudflare 会处理 HTTPS 终止。确保：

1. GitHub Webhook URL 设置为 `https://market-monitor.uk/webhook`
2. 在 Cloudflare 中配置 Page Rule 或 Access Policy 允许 `/webhook` 路径
3. 服务器环境变量 `WEBHOOK_SECRET` 与 GitHub 配置一致

## 5. 数据源与状态说明

- 当前后端只使用真实行情接口，不再自动生成 Demo/Mock 数据。
- 数据源策略：
  - ETF 实时行情 (spot)：`push2 clist (88/48.push2.eastmoney.com)`
  - ETF 规模 (scale)：`push2 ulist f117/f20`
  - 溢价 (premium)：`push2 ulist f402/f441` → NAV+price 计算
  - 指数：`push2` → `新浪 hq.sinajs.cn` → `腾讯 qt.gtimg.cn`
  - K 线：`push2his.eastmoney.com` → `腾讯 web.ifzq.gtimg.cn`（备用约 641 行）
  - NAV：`api.fund.eastmoney.com/f10/lsjz`（每日 BJT 20:00 采集）
  - 费率：`fundf10.eastmoney.com/jjfl_{code}.html`（每日 03:00 采集）
- `source` 字段说明：
  - `live`: 实时拉取成功
  - `cache`: 实时拉取失败，使用上次缓存
  - `degraded`: 实时和缓存均不可用
- **push2 风控**：东方财富 push2 系列接口存在段级 IP 封禁风险（非单 IP）。封禁期间 spot/scale/premium/kline 降级为缓存，价格通过新浪/腾讯继续更新，但规模和 K 线全量历史受影响。

## 6. 常用检查

```bash
curl -s http://127.0.0.1:8080/api/health
curl -s http://127.0.0.1:8080/api/etf-data | head
```

关键限频参数（当前默认值，可 .env 覆盖）：

- `API_BASE_INTERVAL=5.0`: push2 基础请求间隔(s)
- `API_MAX_INTERVAL=30.0`: push2 失败后最大退避(s)
- `SECONDARY_API_INTERVAL=1.0`: 新浪/腾讯备用源请求间隔(s)
- `CIRCUIT_BREAKER_MAX_COOLDOWN=1800`: 渐进冷却上限(s)，风控持续时最长 30 分钟
