# doulor 中转

把 [cloud.doulor.cn](https://cloud.doulor.cn) 的「实验室」AI（`/api/lab/chat`）包装成一个 **OpenAI 兼容**的本地端点，自己管登录态。

```
你的客户端  ──►  http://127.0.0.1:8003/v1/chat/completions  ──►  cloud.doulor.cn
```

## 特点

- **OpenAI 兼容** — 直接当 `base_url` 用，支持 `stream: true/false`
- **登录态自动管理** — cookie 持久化到 `session.json`，30 天有效
- **自愈** — 上游返回 401/403 时自动重新登录并重发请求，不丢消息
- **不撞风控** — 重登有 30s 冷却且并发单飞；额度用完等持续 403 不会变成登录风暴
- **提前换新** — cookie 剩不到 1 天就自动换
- **一键脚本** — 装/启/停/改密码/设 key/测速/看日志

## 快速开始

```sh
git clone https://github.com/kuisa/doulor.git
cd doulor
./d.sh install          # 会问你账号密码
./d.sh test             # 发一条测试消息
```

`install` 会：
1. 在 `/opt/doulor-proxy/` 放好程序
2. 生成 `/opt/doulor-proxy/env`（权限 600，存账号密码）
3. 写 `/etc/systemd/system/doulor-proxy.service` 并开机自启

## 命令

```sh
./d.sh status      # 状态 + cookie 剩余天数
./d.sh start       # 启动
./d.sh stop        # 停止
./d.sh restart     # 重启
./d.sh key <key>   # 设置 API key（设了之后要带 Authorization: Bearer <key>）
./d.sh pass        # 改账号密码
./d.sh test        # 发一条测试消息
./d.sh log         # 跟日志
./d.sh uninstall   # 卸载（保留 env 和 cookie）
```

## 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | 聊天（OpenAI 兼容） |
| GET  | `/v1/models` | 模型列表 |
| GET  | `/healthz` | 健康检查 |

## 用法示例

```sh
curl -X POST http://127.0.0.1:8003/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "workbuddy-deepseek-v4.1-flash",
    "messages": [{"role": "user", "content": "你好"}]
  }'
```

Python（OpenAI SDK）：

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8003/v1", api_key="anything")
r = client.chat.completions.create(
    model="workbuddy-deepseek-v4.1-flash",
    messages=[{"role": "user", "content": "你好"}],
)
print(r.choices[0].message.content)
```

## 配置（`/opt/doulor-proxy/env`）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOULOR_USER` | 空 | 账号（identifier） |
| `DOULOR_PASS` | 空 | 密码 |
| `DOULOR_API_KEY` | 空 | 设了则请求需带 `Authorization: Bearer <key>` |
| `DOULOR_LISTEN_HOST` | `127.0.0.1` | 监听地址，改 `0.0.0.0` 可对外 |
| `DOULOR_LISTEN_PORT` | `8003` | 端口 |
| `DOULOR_BASE` | `https://cloud.doulor.cn` | 上游地址 |
| `DOULOR_STATE_DIR` | `/opt/doulor-proxy` | cookie / 日志目录 |

## 实现要点

- 上游 `/api/lab/chat` **永远返回 SSE 流**（传 `stream: false` 也关不掉），本服务内部收完再按调用方要的格式输出
- 上游只认 `doulor_session` cookie（`Max-Age=2592000`，30 天）。Cloudflare 的 `__cflb` **不需要**
- 请求必须带浏览器 UA，否则 Cloudflare 返回 1010 拦截
- 上游返回的 `model` 字段会被归一化（`workbuddy-deepseek-v4.1-flash` → `deepseek-v4.1-flash`），本服务保持调用方传入的原名

## 注意

- `env` 里有明文密码，别提交到 git（仓库里只有 `env.example`）
- 对外暴露时**务必**设 `DOULOR_API_KEY`，并用防火墙限制来源 IP
