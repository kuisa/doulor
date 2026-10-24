# doular

doulor.cloud AI 实验室反代：OpenAI 兼容 API + Web 管理面板。

免费（lab）通道不可用时自动切到 api.doulor.cn/v1 直连通道（需要自建令牌，扣账号额度），恢复后自动切回。

```
OpenAI 客户端 ──► :8003 /v1 ──► cloud.doulor.cn（免费 lab 通道）
                            └─► api.doulor.cn /v1（备用，扣额度）
```

## 一键部署

```sh
git clone https://github.com/kuisa/doular.git
cd doular
sudo ./d.sh install
```

按提示输入你的 doulor 账号、密码，装完即用。

## 使用

任意 OpenAI 兼容客户端填：

- **Base URL**: `http://你的IP:8003/v1`
- **API Key**: `d.sh key <你的key>` 设置的值（不设则不校验）
- **模型**: `workbuddy-deepseek-v4.1-flash` / `workbuddy-glm-5.3-flash` 等，`/v1/models` 可查

测试：

```sh
sudo ./d.sh test
```

## Web 面板

浏览器打开 `http://127.0.0.1:8003/`（或你的服务器 IP）：

- token 计数 / 请求统计 / 请求日志
- 每日签到（cron 每天 00:10 自动签，面板显示最近一次结果与积分）
- doulor 账号密码在线修改（保存后自动重新登录）
- 面板访问密码在线修改（初始 `admin123`，在 env 里设 `DOULOR_PANEL_PASS` 也可以）

## 环境变量

| 变量 | 必填 | 说明 |
|---|---|---|
| `DOULOR_USER` | ✅ | doulor 账号（邮箱/用户名） |
| `DOULOR_PASS` | ✅ | doulor 密码 |
| `DOULOR_API_KEY` |  | /v1 接口鉴权 key，留空不校验 |
| `DOULOR_PANEL_PASS` |  | 面板初始访问密码，默认 `admin123` |
| `DOULOR_LISTEN_HOST` |  | 监听地址，默认 `127.0.0.1`（对外用改 `0.0.0.0` + 防火墙） |
| `DOULOR_LISTEN_PORT` |  | 监听端口，默认 `8003` |

全部支持 `cp env.example env` 后填入（d.sh install 也会交互生成）。

## 管理命令

```sh
sudo ./d.sh {install|start|stop|restart|status|key <k>|pass|test|log|uninstall}
```

## 备用 v1 通道（可选）

lab 免费通道故障期间会自动切 api.doulor.cn/v1，需要你预先在 api.doulor.cn 建一个令牌，写入 `v1_token.json`：

```json
{"key": "sk-你的令牌"}
```

没有这个文件也能跑，只是切备用通道时会报「v1 通道未配置 key」。

## 需求

- Linux + systemd（Debian/Ubuntu 测试通过）
- Python 3.9+（仅标准库，无第三方依赖）
- cron（每日签到用）

自愈定时器（可选但建议）：

```sh
sudo cp doulor-token-autofix.service doulor-token-autofix.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now doulor-token-autofix.timer
```
