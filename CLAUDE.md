# market-monitor

A 股 ETF/LOF 实时行情监控网站，部署在 market-monitor.uk。单用户维护，AI 协作开发。

## 技术栈

- 语言 / 框架: Python 3.12+, FastAPI, APScheduler, uvicorn
- 关键依赖: requests, apscheduler, fastapi, uvicorn[standard]
- 部署: 腾讯云 VPS + Cloudflare CDN + Nginx 反向代理
- 主文件: `main_optimized.py`（单文件后端，~3200 行）

## 启动

```bash
pip install -r requirements.txt
# 环境变量见下方，可写入 .env 然后 set -a && source .env && set +a
uvicorn main_optimized:app --host 0.0.0.0 --port 8080
```

## 生产服务器

- 路径: `/root/.openclaw/workspace_coder/a_share_market_monitor`
- 自动更新: GitHub Webhook → `webhook-server.py` → `auto-update.sh`
- **注意**: `auto-update.sh` 在 LOCAL == REMOTE 时 early-exit，无新 commit 不重启 uvicorn

## API 路由

| 路由 | 方法 | 说明 |
|------|------|------|
| `/api/etf-data` | GET | 全量 ETF/LOF spot 数据 |
| `/api/kline/{code}` | GET | K 线（含历史统计） |
| `/api/health` | GET | 服务健康检查 |
| `/api/diag` | GET | 详细诊断（数据源/限频状态） |
| `/webhook` | POST | GitHub Webhook，触发 auto-update.sh |

## 数据源策略

| 数据类型 | 主源 | 备用 |
|----------|------|------|
| ETF 实时行情 (spot) | push2 clist (88/48.push2.eastmoney.com) | — |
| ETF 规模 (scale) | push2 ulist f117 → f20 | *(C-plan: fundf10 HTML，未实施)* |
| 溢价 (premium) | spot clist f402 / f441（已合并进 spot 刷新） | NAV+price 计算 → premium_cache |
| 指数 | 新浪 hq.sinajs（主） | → 腾讯 qt.gtimg → push2 INDEX_ENDPOINT（兜底） |
| K 线 | push2his.eastmoney.com（增量拉取） | → 腾讯 web.ifzq.gtimg（约 641 行） |
| NAV (净值) | api.fund.eastmoney.com/f10/lsjz | 内存缓存 |
| 费率 | fundf10.eastmoney.com/jjfl_{code}.html | fee_cache.json |

**LOF 规模注意**: fundf10 返回「基金合并资产规模」（含场外联接基金），≠ 场内流通份额。场内规模只能从 push2 ulist f20 取。

**K 线历史注意**: push2his 支持全量历史（KLINE_HISTORY_START = "20050101"）；腾讯备用约 641 行；Sina 备用 max 1000 行。早期 ETF（如 510050，2005 年成立）完整历史依赖 push2his。

**push2 防风控措施**（2026-04-30 改造）: premium 合并进 spot、指数改 Sina 主源、spot 降频 5min、K 线增量拉取、UA/Referer 轮换、请求抖动 2-5s。push2 请求量从 ~320 降至 ~35 req/hr。东方财富对高频 IP 执行段级封禁（非单 IP），换同段新 IP 无效。

## 关键常量（可 .env 覆盖）

```
API_BASE_INTERVAL=5.0         # push2 基础请求间隔(s)
API_MAX_INTERVAL=30.0         # push2 最大退避(s)
SECONDARY_API_INTERVAL=1.0    # 新浪/腾讯请求间隔(s)
CIRCUIT_BREAKER_THRESHOLD=4   # 连续失败触发熔断阈值
CIRCUIT_BREAKER_COOLDOWN=180  # 熔断基础冷却(s)
CIRCUIT_BREAKER_MAX_COOLDOWN=1800  # 渐进冷却上限(s)
REFRESH_MINUTES=5             # spot 刷新间隔（默认 5 分钟，降频防风控）
KLINE_REFRESH_MINUTES=180     # K 线批量刷新间隔
WEBHOOK_SECRET=               # GitHub Webhook 签名验证
FEISHU_WEBHOOK=               # 飞书通知（可选）
```

## 定时任务（APScheduler，北京时间）

| 任务 | 时间 |
|------|------|
| refresh_spot | 每 5 分钟（含溢价计算，已合并原 premium_refresh） |
| data_fill | 每 60 分钟 |
| kline_refresh | 每 180 分钟 |
| scale_refresh | 09:35 & 15:05 |
| nav_refresh | 20:00 (UTC 12:00) |
| etf_discovery | 02:00 |
| fee_refresh | 03:00 |
| save_morning_close_premium | 11:30 |
| save_afternoon_close_premium | 15:00 |

## 验证命令

```bash
curl -s http://127.0.0.1:8080/api/health
curl -s http://127.0.0.1:8080/api/diag | python3 -m json.tool | head -60
```

## 已知限制

1. **push2 段级封禁**: 见「数据源策略」中的防风控措施说明。2026-04-29 触发封禁后已做降量改造
2. **Cloudflare A 记录**: 腾讯云换 IP 后须在 Cloudflare Dashboard 手动更新，否则 522 错误
3. **规模无备用源**: fundf10 返回合并资产规模（含场外），不可替代 push2 ulist 的场内规模
