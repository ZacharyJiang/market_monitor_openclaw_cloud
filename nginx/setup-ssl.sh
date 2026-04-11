#!/bin/bash
# Cloudflare Full SSL 模式设置脚本
# 在服务器上运行此脚本配置 SSL

set -e

echo "=== ETF NEXUS SSL 配置脚本 ==="
echo ""

# 检查是否以 root 运行
if [ "$EUID" -ne 0 ]; then
    echo "请使用 sudo 运行此脚本"
    exit 1
fi

# 创建 SSL 目录
SSL_DIR="/etc/nginx/ssl"
mkdir -p "$SSL_DIR"
echo "✓ 创建 SSL 目录: $SSL_DIR"

# 检查证书文件是否存在
if [ ! -f "$SSL_DIR/cloudflare-origin.pem" ] || [ ! -f "$SSL_DIR/cloudflare-origin.key" ]; then
    echo ""
    echo "⚠️  证书文件未找到！"
    echo ""
    echo "请按以下步骤获取 Cloudflare Origin CA 证书："
    echo "1. 登录 Cloudflare Dashboard"
    echo "2. 选择你的域名 (market-monitor.uk)"
    echo "3. 进入 SSL/TLS → Origin Server"
    echo "4. 点击 'Create Certificate'"
    echo "5. 选择 'Let Cloudflare generate a private key and a CSR'"
    echo "6. 复制证书内容到: $SSL_DIR/cloudflare-origin.pem"
    echo "7. 复制私钥内容到: $SSL_DIR/cloudflare-origin.key"
    echo ""
    echo "或者使用 Let's Encrypt:"
    echo "  certbot --nginx -d market-monitor.uk"
    exit 1
fi

# 设置证书权限
chmod 644 "$SSL_DIR/cloudflare-origin.pem"
chmod 600 "$SSL_DIR/cloudflare-origin.key"
echo "✓ 设置证书文件权限"

# 检查 Nginx 配置
if [ ! -f "/etc/nginx/nginx.conf" ]; then
    echo "✗ Nginx 未安装"
    echo "  安装命令: apt-get install nginx"
    exit 1
fi

echo "✓ Nginx 已安装"

# 备份现有配置
if [ -f "/etc/nginx/conf.d/market-monitor.conf" ]; then
    cp "/etc/nginx/conf.d/market-monitor.conf" "/etc/nginx/conf.d/market-monitor.conf.backup.$(date +%Y%m%d)"
    echo "✓ 备份现有配置"
fi

# 复制新配置
cp nginx/nginx-full-ssl.conf /etc/nginx/conf.d/market-monitor.conf
echo "✓ 复制 Nginx SSL 配置"

# 测试 Nginx 配置
echo ""
echo "测试 Nginx 配置..."
if nginx -t; then
    echo "✓ Nginx 配置测试通过"
else
    echo "✗ Nginx 配置测试失败"
    exit 1
fi

# 重载 Nginx
echo ""
echo "重载 Nginx..."
systemctl reload nginx
echo "✓ Nginx 重载完成"

echo ""
echo "=== SSL 配置完成 ==="
echo ""
echo "下一步："
echo "1. 登录 Cloudflare Dashboard"
echo "2. 进入 SSL/TLS → Overview"
echo "3. 将加密模式改为 'Full' 或 'Full (strict)'"
echo "4. 等待 1-2 分钟后测试访问 https://market-monitor.uk"
echo ""
echo "测试命令:"
echo "  curl -I https://market-monitor.uk/api/health"
