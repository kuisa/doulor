#!/bin/sh
# doulor 中转 一键管理脚本
#
#   ./d.sh install     首次安装（生成 env、装 systemd、交互设置账号）
#   ./d.sh start       启动
#   ./d.sh stop        停止
#   ./d.sh restart     重启
#   ./d.sh status      查看状态（默认）
#   ./d.sh key <key>   设置 API key
#   ./d.sh pass        重设账号密码
#   ./d.sh test        发一条测试消息
#   ./d.sh log         跟踪日志
#   ./d.sh uninstall   卸载（保留 env 和 cookie）

set -e
DIR=${DOULOR_DIR:-/opt/doulor-proxy}
ENV_FILE=$DIR/env
SVC=doulor-proxy
PORT=${DOULOR_LISTEN_PORT:-8003}
HERE=$(cd "$(dirname "$0")" && pwd)

mk_env() {
    mkdir -p "$DIR"
    [ -f "$ENV_FILE" ] || touch "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    grep -q '^DOULOR_USER=' "$ENV_FILE"  || echo 'DOULOR_USER=' >> "$ENV_FILE"
    grep -q '^DOULOR_PASS=' "$ENV_FILE"  || echo 'DOULOR_PASS=' >> "$ENV_FILE"
    grep -q '^DOULOR_API_KEY=' "$ENV_FILE" || echo 'DOULOR_API_KEY=' >> "$ENV_FILE"
    grep -q '^DOULOR_LISTEN_HOST=' "$ENV_FILE" || echo 'DOULOR_LISTEN_HOST=127.0.0.1' >> "$ENV_FILE"
    grep -q '^DOULOR_LISTEN_PORT=' "$ENV_FILE" || echo "DOULOR_LISTEN_PORT=$PORT" >> "$ENV_FILE"
    grep -q '^DOULOR_STATE_DIR=' "$ENV_FILE" || echo "DOULOR_STATE_DIR=$DIR" >> "$ENV_FILE"
    grep -q '^DOULOR_BASE=' "$ENV_FILE" || echo 'DOULOR_BASE=https://cloud.doulor.cn' >> "$ENV_FILE"
}

install_unit() {
    cat > /etc/systemd/system/$SVC.service <<EOF
[Unit]
Description=doulor.cloud AI 中转服务
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=/usr/bin/python3 $DIR/proxy.py
Restart=always
RestartSec=5
EnvironmentFile=-$ENV_FILE
StandardOutput=append:$DIR/proxy.log
StandardError=append:$DIR/proxy.log

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
}

# 安全写入 env：转义 sed 替换串里的特殊字符，避免密码含 | & / \ 时写坏文件
set_env() {
    _k=$1; _v=$2
    _esc=$(printf '%s' "$_v" | sed -e 's/[\\|&]/\\&/g')
    if grep -q "^$_k=" "$ENV_FILE"; then
        sed -i "s|^$_k=.*|$_k=$_esc|" "$ENV_FILE"
    else
        printf '%s=%s\n' "$_k" "$_v" >> "$ENV_FILE"
    fi
}

sync() { mk_env; install_unit; }

case "$1" in
  install)
      sync
      if ! grep -q '^DOULOR_USER=..' "$ENV_FILE"; then
          printf "账号 (identifier): "; read -r u
          printf "密码（输入不回显）: "; read -rs p; echo
          set_env DOULOR_USER "$u"
          set_env DOULOR_PASS "$p"
          unset u p
      fi
      cp -f "$HERE/proxy.py" "$DIR/proxy.py" 2>/dev/null || true
      cp -f "$HERE/checkin.py" "$DIR/checkin.py" 2>/dev/null || true
      cp -f "$HERE/autofix.py" "$DIR/autofix.py" 2>/dev/null || true
      cp -f "$HERE/d.sh" "$DIR/d.sh" 2>/dev/null || true
      chmod +x "$DIR/d.sh" 2>/dev/null || true
      # 每日自动签到 cron（日界按 UTC+8，主机若是 UTC 会自动换算）
      if [ -f "$HERE/doulor-checkin.cron" ]; then
          sed "s|__DIR__|$DIR|g" "$HERE/doulor-checkin.cron" > /etc/cron.d/doulor-checkin
          chmod 644 /etc/cron.d/doulor-checkin
      fi
      # 令牌分组自愈定时器（修 auto groups 502），路径按 $DIR 渲染
      if [ -f "$HERE/doulor-token-autofix.service" ] && [ -f "$HERE/doulor-token-autofix.timer" ]; then
          sed "s|__DIR__|$DIR|g" "$HERE/doulor-token-autofix.service" > /etc/systemd/system/doulor-token-autofix.service
          cp -f "$HERE/doulor-token-autofix.timer" /etc/systemd/system/doulor-token-autofix.timer
          systemctl daemon-reload
          systemctl enable --now doulor-token-autofix.timer >/dev/null 2>&1 || true
      fi
      systemctl enable $SVC >/dev/null 2>&1
      systemctl restart $SVC
      sleep 3
      echo "✅ 安装完成"
      "$HERE/d.sh" status
      ;;
  start)   sync; systemctl enable $SVC >/dev/null 2>&1; systemctl restart $SVC; echo "✅ 已启动" ;;
  stop)    systemctl stop $SVC; echo "✅ 已停止" ;;
  restart) sync; systemctl restart $SVC; echo "✅ 已重启" ;;
  status|"")
      echo -n "状态: "; systemctl is-active $SVC
      curl -sS --max-time 5 "http://127.0.0.1:$PORT/healthz" || echo "(服务没响应)"
      echo ;;
  key)
      sync
      if [ -n "$2" ]; then K=$2; else
          printf "API Key（输入不回显，留空则随机生成）: "; read -rs K; echo
          [ -n "$K" ] || K=$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | cut -c1-32)
      fi
      set_env DOULOR_API_KEY "$K"
      unset K
      systemctl restart $SVC
      echo "✅ API key 已设置并重启" ;;
  pass)
      sync
      printf "账号 (identifier): "; read -r u
      printf "密码（输入不回显）: "; read -rs p; echo
      set_env DOULOR_USER "$u"
      set_env DOULOR_PASS "$p"
      unset u p
      systemctl restart $SVC
      echo "✅ 账号密码已更新" ;;
  test)
      sync
      K=$(grep '^DOULOR_API_KEY=' "$ENV_FILE" | cut -d= -f2-)
      if [ -n "$K" ]; then H="Authorization: Bearer $K"; else H="X-Health: 1"; fi
      curl -sS -X POST "http://127.0.0.1:$PORT/v1/chat/completions" \
        -H "Content-Type: application/json" -H "$H" \
        -d '{"model":"workbuddy-deepseek-v4.1-flash","messages":[{"role":"user","content":"只回两个字：正常"}]}'
      echo ;;
  log)     tail -f "$DIR/proxy.log" ;;
  uninstall)
      systemctl stop $SVC 2>/dev/null || true
      systemctl disable $SVC 2>/dev/null || true
      rm -f /etc/systemd/system/$SVC.service
      rm -f /etc/systemd/system/doulor-token-autofix.service
      rm -f /etc/systemd/system/doulor-token-autofix.timer
      systemctl disable --now doulor-token-autofix.timer >/dev/null 2>&1 || true
      rm -f /etc/cron.d/doulor-checkin
      systemctl daemon-reload
      echo "✅ 已卸载（env 和 cookie 保留在 $DIR）" ;;
  *) echo "用法: $0 {install|start|stop|restart|status|key <k>|pass|test|log|uninstall}" ;;
esac
