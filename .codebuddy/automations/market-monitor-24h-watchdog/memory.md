# Market Monitor 24h Watchdog - 执行记录

## 2026-04-19 01:59 执行

### 检查结果
- 网站前端正常可访问
- ETF总数1886只，价格/K线/费率基本正常
- **严重异常**: 所有1886只ETF的溢价率(premium)和净值(nav)100%为null

### 根因
东方财富单条股票API缺少 `fltt=2` 参数，导致 f183(nav) 返回放大1000倍的值，代码未做/1000处理，溢价率计算结果被 abs(premium)<30 过滤。

### 修复
- `_fetch_premium_batch_sync`: 添加 fltt=2，f183 安全处理
- `_fetch_premium_from_eastmoney`: 同上
- `check_and_fill_missing_data`: 价格补全添加 fltt=2
- Commit: 1461e0f

### 部署状态
- 代码已推送到 GitHub
- 自动部署未成功（auto-update.sh 与 OpenClaw 平台可能不兼容）
- **需要用户手动部署**
