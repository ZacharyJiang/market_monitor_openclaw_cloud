# Market Monitor 项目长期记忆

## 项目架构
- 网站: market-monitor.uk (Cloudflare Access 保护)
- 服务器: 腾讯云 43.134.6.175:2222
- 技术栈: FastAPI + uvicorn, Docker, nginx, Cloudflare CDN
- 部署: GitHub push → webhook → auto-update.sh → docker build + restart
- 服务器项目路径: /root/.openclaw/workspace_coder/a_share_market_monitor/
- 数据源: 东方财富(主) + 新浪(备)

## CF Access 凭证
- CF-Access-Client-Id: 54d674ccaedaa55147c01ca6a8e1953a.access
- CF-Access-Client-Secret: eccc024810c1f8604e6d1080e7f557a6bc78f6936ceeedbe481cb4e3787f7dce

## 本地网络环境
- 本地DNS解析 market-monitor.uk → 198.18.0.44 (VPN/代理拦截)
- curl 访问网站超时，但 Python requests 正常
- SSH 连接服务器不可用（无SSH密钥配置）

## 关键代码约定
- **不能修改频控参数**: API_BASE_INTERVAL, API_MAX_INTERVAL, SECONDARY_API_INTERVAL 等
- 溢价数据使用 _premium_cache 字典缓存，持久化到 premium_cache.json
- 智能合并逻辑: refresh_spot 中新旧数据合并，新数据为0/null时保留旧数据有效值

## 踩坑记录
- 东方财富列表API f183(净值)/f184(溢价率) 之前请求了但未解析 (2026-04-19修复)
- 非交易时间 f184 返回0，不应当作有效溢价；需通过 f43/f183 手动计算
- f43 在东方财富单条接口中是放大1000倍的价格(如4739→4.739)
- refresh_spot 旧逻辑直接覆盖数据，导致非交易时间获取的空数据覆盖了有效数据
- **关键bug**: 东方财富单条股票API (`push2.eastmoney.com/api/qt/stock/get`) 缺少 `fltt=2` 参数时，f43/f183 返回放大1000倍的整数；代码只对f43做了/1000处理，遗漏了f183(nav) (2026-04-19修复)
- 自动部署(auto-update.sh)在OpenClaw平台环境下可能无法正常重启Docker容器，需要用户手动介入

## 部署注意
- webhook-server.py 运行在宿主机 8082 端口
- 应用容器映射到宿主机 8081 端口
- nginx 将 /webhook 代理到 8082，其他代理到 8081
- auto-update.sh 可能与 OpenClaw 平台的 Docker 管理机制不兼容，部署可能需要手动操作
