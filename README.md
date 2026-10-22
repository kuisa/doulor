# doulor-proxy

doulor.cloud（cloud.doulor.cn）AI 实验室反代：把站点的 `/api/lab/chat`
包装成 **OpenAI 兼容 API**，自带登录态管理与签到积分。

```
OpenAI 客户端 ──► proxy.py :8003 ──► cloud.doulor.cn /api/lab/chat
                          （doulor_session cookie 自动续期）
```

## 文件

| 文件 | 作用 |
|---|---|
| `proxy.py` | 主服务：OpenAI 兼容端点（`/v1/chat/completions`、`/v1/models`、`/healthz`），SSE↔非流式自动转换，cookie 持久化 + 过期自动重登 |
| `checkin.py` | 每日自动签到（复用 proxy 的 cookie，失效自动重登写回），日志 `checkin.log` |
| `autofix.py` | 令牌分组自愈：站点托管 key 的 group 会被同步成 `auto` → 全部请求 502（`auto groups is not enabled`）。此脚本登录网关 API 把 group 改回空（继承 `donation` 分组），由 timer 每 5 分钟跑一次 |
| `d.sh` | 一键管理：`install / start / stop / restart / status / key <k> / pass / test / log / uninstall` |
| `doulor-proxy.service` | 主服务 systemd unit |
| `doulor-token-autofix.service` + `.timer` | 自愈脚本 unit + 每 5 分钟定时器 |
| `env`（不入库） | `DOULOR_USER / DOULOR_PASS / DOULOR_API_KEY / DOULOR_LISTEN_HOST / DOULOR_LISTEN_PORT / DOULOR_STATE_DIR / DOULOR_BASE` |

## 安装

```sh
sudo ./d.sh install     # 交互输入 doulor 账号密码
```

`install` 会：生成 `env`（chmod 600）→ 写入 `/etc/systemd/system/doulor-proxy.service` → enable + restart。

自愈定时器（可选但强烈建议）：

```sh
sudo cp doulor-token-autofix.service doulor-token-autofix.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now doulor-token-autofix.timer
```

## 使用

```sh
curl http://127.0.0.1:8003/v1/chat/completions \
  -H "Authorization: Bearer $DOULOR_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"workbuddy-deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}'
```

任意 OpenAI 兼容客户端填：

- **Base URL**：`http://你的IP:8003/v1`
- **API Key**：env 里 `DOULOR_API_KEY` 的值（自己随便设，proxy 只做本地校验）
- **模型**：`workbuddy-deepseek-v4.1-flash`（免费）

## 已踩过的坑（都有代码级注释）

- **上游永远 SSE 流式**：即使 `"stream": false` 也返回 `data: {...}` 事件流，proxy 做了聚合转换
- **响应 model 名归一化**：`workbuddy-deepseek-v4.1-flash` 上游返回 `deepseek-v4.1-flash`
- **`__cflb` cookie 不需要**：Cloudflare LB 的，不带也能过
- **chunked 大 payload**：上游只认 Content-Length 时，proxy 对 chunked 客户端做了兼容
- **签到日界按 UTC+8**：不是服务器时区
- **`auto groups is not enabled` 502**：站点会把你网关令牌的 group 同步成 `auto`，
  而该分组未启用 → 全部 502。表象是"我账号被ban了？"实际账号/额度都正常。
  **修复 = autofix.py 把 group 改回空**。网关对令牌信息有几分钟缓存，所以真实故障
  最长「5 分钟定时器 + 缓存过期」才恢复，不是秒级。

## 站点端备用

网页实验室里模型选 **Site models 的 `paid-cn:*`** 一定 502（站方系统的 auto key），
要用就选 **Free trial** 的 `deepseek-v4.1-flash` / `glm-5.3-flash`，或 **Free channels**。

## 已验证（2026-10-23）

- `install` → 服务 active，`/healthz` 200
- 反代 `/v1/chat/completions` → 200（本地 + cloudflared 隧道公网均通）
- 故意把令牌 group 设回 `auto` → 触发 autofix → 复核 `group=''` → 端到端 200
- 签到：cookie 失效自动重登写回，streak 正常累加
