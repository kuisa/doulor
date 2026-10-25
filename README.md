# doulor-proxy

把 doulor.cloud（cloud.doulor.cn）的 AI 实验室接口 `/api/lab/chat`
包装成 **OpenAI 兼容 API**，带登录态自动续期、每日签到、多账号轮换和
**按账号绑定独立出口代理**。

```
OpenAI 客户端 ──► proxy.py :8003 ──► cloud.doulor.cn /api/lab/chat
                        │
                        ├─ 账号池：429 免费额度耗尽自动切下一个
                        └─ 出口池：每个账号一个独立代理 IP（防连坐封号）
```

## 快速开始

```sh
sudo ./d.sh install     # 交互输入账号密码，生成 env(chmod 600)、装 systemd、启动
curl http://127.0.0.1:8003/healthz
```

不想交互安装，也可以手填配置：`cp env.example env && chmod 600 env`，
把 `DOULOR_USER` / `DOULOR_PASS` / `DOULOR_API_KEY` 填上，再 `./d.sh start`。
**`env` 里有明文密码，已在 `.gitignore` 里，别提交。**

管理命令：

| 命令 | 作用 |
|---|---|
| `./d.sh install` | 交互式安装（写 env + systemd unit + 启动） |
| `./d.sh start` / `stop` / `restart` / `status` | 服务控制 |
| `./d.sh key <k>` | 设置本地 API Key |
| `./d.sh pass` | 重设上游账号密码（注意：不是面板密码） |
| `./d.sh test` | 打一次端到端请求 |
| `./d.sh log` | 跟踪日志 |
| `./d.sh uninstall` | 卸载 |

自愈定时器（修下文 `auto groups` 502 那个坑）由 `d.sh install` 自动装好并启用，
unit 里的目录会按实际 `$DOULOR_DIR` 渲染，不用手工改路径。想手动装也行：

```sh
sudo sed "s|__DIR__|/opt/doulor-proxy|g" doulor-token-autofix.service > /etc/systemd/system/doulor-token-autofix.service
sudo cp doulor-token-autofix.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now doulor-token-autofix.timer
```

每日自动签到由 `d.sh install` 装进 `/etc/cron.d/doulor-checkin`（按上游 UTC+8
日界，本地 00:10 触发）；`./d.sh uninstall` 会一并清理。不想用 cron 也行：
面板上有「立即签到」，而且**每次从面板添加账号时都会立刻签到一次**，不会漏掉。

## 客户端接入

任意 OpenAI 兼容客户端填：

- **Base URL**：`http://你的IP:8003/v1`
- **API Key**：env 里的 `DOULOR_API_KEY`（自己随便设，proxy 只做本地校验）
- **模型**：`workbuddy-deepseek-v4.1-flash`

```sh
curl http://127.0.0.1:8003/v1/chat/completions \
  -H "Authorization: Bearer $DOULOR_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"workbuddy-deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}'
```

端点：`/v1/chat/completions`、`/v1/models`、`/healthz`。上游永远 SSE，
`"stream": false` 时由 proxy 聚合成普通 JSON；响应里的 model 名会归一化回
`workbuddy-deepseek-v4.1-flash`。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DOULOR_USER` | — | 账号（邮箱或用户名） |
| `DOULOR_PASS` | — | 密码 |
| `DOULOR_API_KEY` | — | 本地 API Key，客户端用它做 Bearer |
| `DOULOR_BASE` | `https://cloud.doulor.cn` | lab 站地址（免费通道） |
| `DOULOR_V1_BASE` | — | v1 通道地址（lab 不可用时的回退） |
| `DOULOR_LISTEN_HOST` | `127.0.0.1` | 监听地址，要公网访问改 `0.0.0.0` |
| `DOULOR_LISTEN_PORT` | `8003` | 监听端口 |
| `DOULOR_STATE_DIR` | `/opt/doulor-proxy` | 状态目录（session/日志/池文件都在这） |
| `DOULOR_PANEL_PASS` | 首次启动随机生成 | 面板密码 |
| `DOULOR_MODEL_PREFIX` | — | 模型名前缀（多实例转发时区分） |
| `DOULOR_FAILOVER_MAX` | — | 通道失效切换的最大次数 |
| `DOULOR_MAX_TOOL_MSGS` | — | 单次请求 tool 消息上限，超出的折叠 |

多账号：`accounts.json` 里放 `[{"user":"..","pass":".."}]`，env 里的账号固定排第一。
**这个文件含密码，已在 .gitignore 里，不要提交。**

## 面板

浏览器打开 `http://127.0.0.1:8003/`，用面板密码登录。面板密码首次启动时
随机生成 16 位，在 `proxy.log` 里打印一次，同时以 `chmod 600` 写入
`$DOULOR_STATE_DIR/panel_pass.json`——之后要查就 `cat` 这个文件。

能做的事：看号池状态 / 今日额度 / 出口 IP、手动加号、加出口代理、
解绑、改面板密码、看签到记录。

### 免费额度的计算方式

上游免费额度是 **每账号每天 1000 次，按 UTC 日界重置**（= 北京时间每天早 8:00）。
注意日志时间戳是本地时间，别和 UTC 重置时间搞混。额度耗尽时 proxy 收到
`429 FREE_QUOTA_EXCEEDED`，自动切下一个账号立即重发；全部账号耗尽才把 429
回给客户端，并附上最早解禁时间。所以号池就是用来摊这个额度的。

## 出口代理池：为什么必须一个账号一个 IP

上游除了账号额度，还有 **IP 级风控**。多个账号共用同一个出口 IP 会互相连坐：
一个号被限，同 IP 的其他号一起遭殃。所以这里做了账号 ↔ 出口一对一绑定：

- 号池里每个账号绑一个独立代理，**登录 / 聊天 / 签到 / 模型列表全走它**；
- 没绑出口的账号自动直连，不需要额外配置；
- 空闲池空了不报错，新号安静回直连，不会炸服务。

代理地址支持 `http://`、`https://`、`socks5://`，带认证写
`socks5://user:pass@host:port`。socks5 需要 `pip install PySocks`
（或 `apt install python3-socks`），面板检测不到会提示。

### 解绑后的出口会「退役」，不会回到池子

这是刻意的：IP 一旦被烧过，悄悄分给下一个账号就是送它去死。

- 解绑 / 删号 → 该出口进 **retired（退役池）**，**永不自动复用**；
- 面板上单独一行显示 `🗑️ 退役（不自动复用·防连坐）`；
- 确实想再用这个 IP，必须手动把地址重新粘贴添加一次（从退役池摘出）。

面板标题会显示 `空闲 X · 已绑 X · 退役 X` 三个数。

### 添加账号时会自动签到

`checkin.py` 支持 `--user` 只签指定账号——面板加号只签新加的那一个，
不会顺带把整池账号去上游打一遍（每次请求都是风控暴露面）。签到走该账号
自己绑定的出口，和聊天流量一致。

## 已踩过的坑（代码里都有注释）

- **上游永远 SSE 流式**：即使 `"stream": false` 也返回 `data: {...}` 事件流，
  proxy 做了聚合转换。
- **响应 model 名归一化**：上游返回 `deepseek-v4.1-flash`，对外还原成全名。
- **`__cflb` cookie 不需要**：Cloudflare LB 的，不带也能过。
- **chunked 大 payload**：上游只认 Content-Length 时，对 chunked 客户端做了兼容。
- **签到日界按 UTC+8**：不是服务器时区。
- **`auto groups is not enabled` 502**：站点会把你网关令牌的 group 同步成 `auto`，
  而该分组未启用 → 全部 502。表象像"账号被封"，实际账号和额度都正常。
  修法是 `autofix.py` 把 group 改回空；网关对令牌信息有几分钟缓存，所以最长
  「5 分钟定时器 + 缓存过期」才恢复，不是秒级。

## 站点端备用

网页实验室里模型选 **Site models 的 `paid-cn:*`** 一定 502（站方系统的 auto key），
要用就选 **Free trial** 的 `deepseek-v4.1-flash` / `glm-5.3-flash`，或 **Free channels**。

## 安全

- `env`、`accounts.json`、`panel_pass.json`、`session.json`、`sessions/`、
  `proxies.json`、`*_token.json`、`*.log` 全部在 `.gitignore` 里；
- 面板密码没有硬编码默认值，首次启动随机生成 16 位并 `chmod 600` 落盘；
- 面板接口的代理地址一律只回 mask，`user:pass` 的凭据不会到浏览器；
- 面板默认只监听 `127.0.0.1`，要公网访问请自己套反代 + TLS。

## 许可

仅供个人学习研究，请遵守上游站点的服务条款。因使用产生的任何后果由使用者自负。
