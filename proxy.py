#!/usr/bin/env python3
"""doulor.cloud 实验室 AI 中转服务

作用：把 cloud.doulor.cn 的 /api/lab/chat 包装成一个 OpenAI 兼容端点，
      自己管理登录态（cookie 持久化 + 过期自动重登）。

上游事实（已实测）：
  - 登录：POST /api/login  body {"identifier":..,"password":..}
           → 200，响应头 Set-Cookie: doulor_session=...; Max-Age=2592000 (30天)
  - 聊天：POST /api/lab/chat  body {model, messages, ...}
           → 200，**永远是 SSE 流式**（一行一个 "data: {json}"），
             即使传了 "stream": false 也关不掉
  - 响应里 model 被上游归一化（workbuddy-deepseek-v4.1-flash → deepseek-v4.1-flash）
  - 鉴权：只需要 doulor_session。__cflb 是 Cloudflare LB 的，**不需要**（实测不带也能过）
  - 403/401 → 需要重新登录拿新 cookie

对外：
  POST /v1/chat/completions   OpenAI 兼容，支持 stream=true/false
  GET  /v1/models             模型列表
  GET  /healthz               健康检查
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar

BASE = os.getenv("DOULOR_BASE", "https://cloud.doulor.cn")
LOGIN_URL = f"{BASE}/api/login"
CHAT_URL = f"{BASE}/api/lab/chat"

IDENTIFIER = os.getenv("DOULOR_USER", "")
PASSWORD = os.getenv("DOULOR_PASS", "")

STATE_DIR = os.getenv("DOULOR_STATE_DIR", "/opt/doulor-proxy")
COOKIE_FILE = os.path.join(STATE_DIR, "session.json")
LOG_FILE = os.path.join(STATE_DIR, "proxy.log")

LISTEN_HOST = os.getenv("DOULOR_LISTEN_HOST", "127.0.0.1")
LISTEN_PORT = int(os.getenv("DOULOR_LISTEN_PORT", "8003"))
API_KEY = os.getenv("DOULOR_API_KEY", "")   # 留空 = 不校验

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      " (KHTML, like Gecko) Chrome/125.0.0.0 Iron Safari/537.36")

# 401/403 触发重登的冷却秒数：防止上游持续拒绝时每个请求都打登录接口
AUTH_RELOGIN_COOLDOWN = 30

# 上游故障重试：上游的 400/502 是间歇性的（同一 body 重发即可成功），
# 实测 400 "至少需要一条用户消息" 会在合法请求上偶发出现，502
# "可用渠道不存在" 更是常态。这类不该让客户端一次失败就断掉。
RETRY_BACKOFF = (2, 5, 10)       # 每次重试前等待的秒数

# 渠道缺失时换模型，而不是重发同一个。
# 上游报「分组 auto 下模型 X 的可用渠道不存在」/「Failed to get available
# channel for model X」时，说明该模型此刻没有健康渠道：这条错误自己就要
# 等约 10 秒才返回，原样重发大概率再等 10 秒拿同样的 502。
# 换成同池另一个模型立刻重发，可以把 20 秒压到 3 秒左右。
# 只有这类「渠道缺失」才切换；普通 5xx/429 仍按 RETRY_BACKOFF 原样重试
# （那是上游整体抖动，换模型也一样）。
FAILOVER_MODELS = ("workbuddy-glm-5.3-flash", "workbuddy-hy3",
                   "workbuddy-deepseek-v4.1-flash")
FAILOVER_MAX = int(os.getenv("DOULOR_FAILOVER_MAX", "2"))

# 上游硬限制：单次请求 role=="tool" 的消息条数上限 29（30 条起返回
# 400 INVALID_MESSAGES，文案却写"至少需要一条用户消息"，极具误导性）。
# 实测 62 条消息的阈值本质就是 tool 条数，纯 user/assistant 对话可到 150+
# 无问题。超出时由 compact_tool_history 折叠较早的往返。
MAX_TOOL_MSGS = int(os.getenv("DOULOR_MAX_TOOL_MSGS", "29"))

# 模型列表：动态从上游 /api/lab/models 拉 freeModels（免费池）。
# 上游那个接口同时返回付费模型（paid-cn:* / workbuddy-auto 等），
# 调用会 502 "Failed to get available channel for model ..." 所以
# 只暴露 freeModels 里的。
#
# 2026-10-23 追加白名单：只要 workbuddy-* 开头的。
# Qoder-DeepSeek-Flash / Qoder-Qwen3.8-Flash 虽然曾出现在 freeModels 里，
# 但调用一律 401 USER_TOKEN_EXPIRED（"你的中转站登录已失效"）——那是
# 按渠道缺凭据，不是 session 过期，重登治不好，还会白送上游一个 30 天
# 会话（撞 autofix 的 AUTH_SESSION_LIMIT）。已从兜底列表移除并加过滤。
MODEL_ID = "workbuddy-deepseek-v4.1-flash"     # 默认模型 / 上游不可达时的兜底
MODELS_URL = f"{BASE}/api/lab/models"
MODELS_TTL = 1800                              # 免费池缓存 30 分钟
MODELS_RETRY_TTL = 120                         # 拉取失败后 2 分钟再试（别钉死 30 分钟）
MODEL_PREFIX = os.getenv("DOULOR_MODEL_PREFIX", "workbuddy-")  # 只放行此前缀

# ------------------------------------------------- 2026-10-23 双通道切换
# 上游把「实验室免费通道」(cloud.doulor.cn /api/lab/chat) 的 auto 分组关了
# （站点公告：中转站分组维护中），lab 路全部 502。备用通道是
# api.doulor.cn/v1 + 自建令牌（group=donation）：同一个模型池，但要扣
# 账号 quota（实测每次请求约 1 单位 = 500000 quota）。
# 代理启动时先探测 lab 是否恢复：恢复则走 lab（免费），否则走 v1（扣额度）。
V1_BASE = os.getenv("DOULOR_V1_BASE", "https://api.doulor.cn")
V1_CHAT_URL = f"{V1_BASE}/v1/chat/completions"
V1_MODELS_URL = f"{V1_BASE}/v1/models"
V1_KEY_FILE = os.path.join(STATE_DIR, "v1_token.json")   # {"key": "sk-..."}
# lab 恢复探测间隔：每 10 分钟静默试一次
LAB_PROBE_INTERVAL = 600

# 模型名映射：客户端/Hermes 仍用 workbuddy-* 名字；v1 通道用真名。
WORKBUDDY_TO_V1 = {
    "workbuddy-deepseek-v4.1-flash": "deepseek-v4.1-flash",
    "workbuddy-glm-5.3-flash": "glm-5.3-flash",
    "workbuddy-hy3": "hy3",
    "workbuddy-hy4-preview-f": "hy4-preview-f",
    "workbuddy-space-bunny": "space-bunny",
}


def load_v1_key():
    try:
        with open(V1_KEY_FILE, encoding="utf-8") as f:
            k = json.load(f).get("key")
        return k or None
    except Exception:
        return None


# 当前生效通道："lab"（免费）或 "v1"（扣额度）。启动时探测决定。
_channel = {"mode": None, "checked": 0.0}
_channel_lock = threading.Lock()


def _probe_lab():
    """静默探测 lab 通道是否恢复（一次小请求，失败不打扰日志）。"""
    try:
        sid = SESSION.get() if SESSION._sid else None
        if not sid:
            return False
        h = _upstream_headers()
        h["Cookie"] = f"doulor_session={sid}"
        body = {"model": MODEL_ID, "messages": [{"role": "user", "content": "ping"}],
                "stream": False, "max_tokens": 1}
        req = urllib.request.Request(CHAT_URL, data=json.dumps(body).encode(),
                                     method="POST", headers=h)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status == 200
    except Exception:
        return False


def current_channel():
    """返回当前通道。决定逻辑：
    - 未初始化：探测 lab，好则 lab，坏则 v1（有 key 才行）
    - v1 模式下每 10 分钟重探 lab，恢复则切回
    """
    with _channel_lock:
        now = time.time()
        if _channel["mode"] is None:
            _channel["mode"] = "lab" if _probe_lab() else "v1"
            _channel["checked"] = now
            log(f"🔀 通道初始化: {_channel['mode']}"
                + ("（免费实验室）" if _channel["mode"] == "lab" else "（v1 直连，扣额度）"))
        elif _channel["mode"] == "v1" and now - _channel["checked"] > LAB_PROBE_INTERVAL:
            _channel["checked"] = now
            if _probe_lab():
                _channel["mode"] = "lab"
                log("✅ 实验室通道已恢复，切回免费 lab 通道")
            else:
                log("🧪 lab 仍不可用，继续走 v1（每 10 分钟重探）")
        return _channel["mode"]


def upstream_chat(body, sid, timeout=180):
    """打上游 chat，返回 (status, resp_headers, 原始流迭代器)。

    双通道：lab 模式走 cloud.doulor.cn cookie；v1 模式走 api.doulor.cn/v1
    + Bearer key，模型名做 workbuddy-* → 真名映射。
    """
    mode = current_channel()
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    if mode == "v1":
        key = load_v1_key()
        if not key:
            raise RuntimeError("v1 通道未配置 key（缺失 v1_token.json）")
        vbody = dict(body)
        mapped = WORKBUDDY_TO_V1.get(vbody.get("model"))
        if mapped:
            vbody["model"] = mapped
        data = json.dumps(vbody, ensure_ascii=False).encode("utf-8")
        h = {"Content-Type": "application/json", "Accept": "application/json",
             "Authorization": f"Bearer {key}", "User-Agent": UA}
        req = urllib.request.Request(V1_CHAT_URL, data=data, method="POST", headers=h)
    else:
        h = _upstream_headers()
        h["Cookie"] = f"doulor_session={sid}"
        req = urllib.request.Request(CHAT_URL, data=data, method="POST", headers=h)
    r = urllib.request.urlopen(req, timeout=timeout)
    return r.status, r.headers, r
MODELS_FALLBACK = [
    # 2026-10-23 实测全部可用的 workbuddy 模型（上游拉取失败时用）
    "workbuddy-deepseek-v4.1-flash", "workbuddy-glm-5.3-flash",
    "workbuddy-hy3", "workbuddy-hy4-preview-f", "workbuddy-space-bunny",
]


def _model_allowed(mid):
    """只放行 workbuddy-* 前缀的模型。

    白名单放在本地入站处判，而不是等上游 401：既省一次往返，也避免
    任何非白名单模型把 401 风暴引回来。
    """
    return isinstance(mid, str) and mid.startswith(MODEL_PREFIX)


_models_cache = {"at": 0.0, "ids": list(MODELS_FALLBACK), "ttl": MODELS_TTL}
_models_lock = threading.Lock()

# ---------------------------------------------------------------- 用量统计
# 面板要显示 token 计数，但上游只在**最后一个 chunk** 里带 usage（实测
# 2026-10-23），而流式分支只转发 choices。所以这里在代理层自己记账：
# 拿到 usage 就记精确值，拿不到就按字符数粗估（标注 estimated）。

USAGE_FILE = os.path.join(STATE_DIR, "usage.json")
_usage_lock = threading.Lock()
_usage = {"total": 0, "days": {}, "models": {}, "requests": 0,
          "errors": 0, "prompt": 0, "completion": 0}


def _usage_load():
    try:
        with open(USAGE_FILE, encoding="utf-8") as f:
            d = json.load(f)
        for k in _usage:
            if k in d:
                _usage[k] = d[k]
    except Exception:
        pass


def _usage_save():
    try:
        tmp = USAGE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_usage, f, ensure_ascii=False)
        os.replace(tmp, USAGE_FILE)
    except Exception as e:
        log(f"⚠️ 用量保存失败: {e}")


def _usage_day_key(ts=None):
    """按 UTC+8 分天，与上游签到日界一致"""
    return time.strftime("%Y-%m-%d", time.gmtime((ts or time.time()) + 8 * 3600))


def record_usage(model, ok, usage=None, prompt_chars=0, completion_chars=0,
                 error=None, elapsed=None, detail=None):
    """记一条请求的用量。

    优先用上游返回的真实 usage；没有就按字符数估（中文约 1 token/字，
    英文约 4 字符/token，这里统一按 1.6 字符/token 折中，标记 estimated）。
    """
    p = c = t = 0
    estimated = False
    if isinstance(usage, dict):
        p = int(usage.get("prompt_tokens") or 0)
        c = int(usage.get("completion_tokens") or 0)
        t = int(usage.get("total_tokens") or (p + c))
    elif prompt_chars or completion_chars:
        p = int(prompt_chars / 1.6)
        c = int(completion_chars / 1.6)
        t = p + c
        estimated = True
    day = _usage_day_key()
    with _usage_lock:
        _usage["requests"] = int(_usage.get("requests") or 0) + 1
        if not ok:
            _usage["errors"] = int(_usage.get("errors") or 0) + 1
        _usage["prompt"] = int(_usage.get("prompt") or 0) + p
        _usage["completion"] = int(_usage.get("completion") or 0) + c
        _usage["total"] = int(_usage.get("total") or 0) + t
        days = _usage.setdefault("days", {})
        dd = days.setdefault(day, {"total": 0, "prompt": 0, "completion": 0,
                                   "requests": 0, "errors": 0})
        dd["total"] += t
        dd["prompt"] += p
        dd["completion"] += c
        dd["requests"] += 1
        if not ok:
            dd["errors"] += 1
        models = _usage.setdefault("models", {})
        mm = models.setdefault(model or "?", {"total": 0, "requests": 0,
                                              "errors": 0, "estimated": 0})
        mm["total"] += t
        mm["requests"] += 1
        if not ok:
            mm["errors"] += 1
        if estimated:
            mm["estimated"] += 1
        # 只留最近 60 天，防止文件无限长
        if len(days) > 60:
            for k in sorted(days)[:-60]:
                days.pop(k, None)
        _usage_save()


def usage_snapshot():
    with _usage_lock:
        return json.loads(json.dumps(_usage))


_usage_load()

# ---------------------------------------------------------------- 日志

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    # 只写文件，不 print：systemd 的 StandardOutput=append 已把 stdout
    # 指向同一个 proxy.log，再 print 会让每行出现两次。
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------- 登录态

class Session:
    """管理上游登录 cookie：持久化、过期检测、403/401 自动重登。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._sid = None
        self._expires = 0
        self._last_auth_relogin = 0.0
        self.load()

    # --- 持久化 ---
    def load(self):
        try:
            with open(COOKIE_FILE, encoding="utf-8") as f:
                d = json.load(f)
            self._sid = d.get("session")
            self._expires = float(d.get("expires", 0))
            if self._sid:
                log(f"🔄 载入已保存的 cookie，剩余 "
                    f"{max(0, self._expires - time.time()) / 86400:.1f} 天")
                return True
        except Exception:
            pass
        return False

    def save(self):
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            tmp = COOKIE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"session": self._sid, "expires": self._expires,
                           "saved_at": time.time()}, f)
            os.replace(tmp, COOKIE_FILE)
            os.chmod(COOKIE_FILE, 0o600)
        except Exception as e:
            log(f"⚠️ cookie 保存失败: {e}")

    # --- 登录 ---
    def _login_raw(self):
        """真正打登录接口，返回 (sid, expires_ts) 或 (None, None)"""
        body = json.dumps({"identifier": IDENTIFIER,
                           "password": PASSWORD}).encode()
        req = urllib.request.Request(
            LOGIN_URL, data=body, method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Origin": BASE,
                "Referer": f"{BASE}/dashboard/lab",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": UA,
            })
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                r.read()
                raw = r.headers.get_all("Set-Cookie") or []
        except urllib.error.HTTPError as e:
            log(f"❌ 登录失败 HTTP {e.code}")
            return None, None
        except Exception as e:
            log(f"❌ 登录异常: {type(e).__name__}: {e}")
            return None, None

        for sc in raw:
            m = re.match(r"\s*([^=]+)=([^;]*)", sc)
            if m and m.group(1).strip() == "doulor_session":
                sid = m.group(2).strip()
                mm = re.search(r"Max-Age=(\d+)", sc)
                ttl = int(mm.group(1)) if mm else 2592000
                return sid, time.time() + ttl
        log("❌ 登录成功但没找到 doulor_session")
        return None, None

    def login(self, force=False):
        """拿到有效 session（带锁，避免并发重复登录）"""
        if not IDENTIFIER or not PASSWORD:
            log("❌ 没配置账号密码，请在 env 文件里设置 DOULOR_USER / DOULOR_PASS")
            return None
        with self._lock:
            if not force and self._sid and self._expires - time.time() > 300:
                return self._sid
            sid, exp = self._login_raw()
            if not sid:
                return None
            self._sid = sid
            self._expires = exp
            self.save()
            log(f"✅ 登录成功，cookie 有效 {(exp - time.time()) / 86400:.1f} 天")
            return sid

    def get(self):
        """取当前 session，快过期就提前重登"""
        if not self._sid:
            return self.login()
        if self._expires - time.time() < 86400:      # 剩不到 1 天就换
            log("⏳ cookie 不到 1 天到期，提前换新")
            return self.login(force=True)
        return self._sid

    def invalidate(self):
        with self._lock:
            self._sid = None
            self._expires = 0

    def relogin_on_auth_error(self):
        """上游 401/403 时的重登，返回是否值得重试一次。

        两点约束，缺一不可：
        1. 冷却：持续 403（如额度用完）时不能每个请求都打一次登录接口，
           否则会触发上游风控。冷却只由这个函数自己计时，不受正常登录
           影响——否则启动时的首次登录就会把冷却占掉，导致真正的 403
           一次都重登不上。
        2. 单飞：多个并发请求同时 403 时，只让第一个去重登，其余直接
           返回错误，避免并发重复登录。
        """
        with self._lock:
            now = time.time()
            if now - self._last_auth_relogin < AUTH_RELOGIN_COOLDOWN:
                return False
            self._last_auth_relogin = now
            sid, exp = self._login_raw()
            if not sid:
                log("❌ 重登失败")
                return False
            self._sid = sid
            self._expires = exp
            self.save()
            log(f"✅ 重登成功，cookie 有效 {(exp - time.time()) / 86400:.1f} 天")
            return True


SESSION = Session()


# ---------------------------------------------------------------- 上游调用

def _upstream_headers():
    return {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": BASE,
        "Referer": f"{BASE}/dashboard/lab",
        "Sec-Ch-Ua": '"Chromium";v="125", "Not.A/Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": UA,
    }


def get_free_models(force=False):
    """从上游拉免费模型列表（freeModels），带缓存。

    上游 /api/lab/models 返回 {"models":[...全部...], "freeModels":[...免费池...]}，
    只有 freeModels 里的能正常调用。拉取结果再按 workbuddy-* 白名单过滤。

    自愈要点（2026-10-23 修）：
      - 拉取失败时**不再**把时间戳刷成 now。旧代码无论成败都写
        `_models_cache["at"] = now`，一次瞬时失败就等于把坏列表钉死
        30 分钟；现在失败只等 MODELS_RETRY_TTL（2 分钟）后再试。
      - 过滤后若为空（上游把 freeModels 清空/全是非白名单），保留上一次
        的好列表；完全没有历史时才退回 MODELS_FALLBACK。
      - 上游偶尔会返回 freeModels 含已下线的模型（如 Qoder-*），过滤
        在这里一次性挡掉，不会漏进 /v1/models，也不会漏进调用。
    """
    now = time.time()
    with _models_lock:
        if not force and now - _models_cache["at"] < _models_cache["ttl"]:
            return _models_cache["ids"]
    ids, ok = None, False
    try:
        sid = SESSION.get()
        if sid:
            h = _upstream_headers()
            h["Cookie"] = f"doulor_session={sid}"
            h["Accept"] = "application/json"
            req = urllib.request.Request(MODELS_URL, headers=h)
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.loads(r.read().decode("utf-8", "replace"))
            free = [str(m) for m in (d.get("freeModels") or [])]
            kept = [m for m in free if _model_allowed(m)]
            dropped = [m for m in free if not _model_allowed(m)]
            if dropped:
                log(f"🚫 模型白名单拦截 {len(dropped)} 个: {', '.join(dropped)}")
            if kept:
                ids, ok = kept, True
            else:
                log("⚠️ 上游 freeModels 过滤后为空，保留现有列表")
        else:
            log("⚠️ 无有效 session，模型列表本轮跳过刷新")
    except Exception as e:
        log(f"⚠️ 拉取上游模型列表失败，保留现有列表: {type(e).__name__}: {e}")
    with _models_lock:
        if ok:
            if ids != _models_cache["ids"]:
                log(f"🔄 模型列表更新: {ids}")
            _models_cache["ids"] = ids
            _models_cache["at"] = now
            _models_cache["ttl"] = MODELS_TTL
        else:
            # 失败：短退避，别把一次抖动当成 30 分钟的定局
            _models_cache["at"] = now
            _models_cache["ttl"] = MODELS_RETRY_TTL
        return _models_cache["ids"]


def models_payload():
    """OpenAI /v1/models 格式"""
    return [{"id": m, "object": "model", "owned_by": "doulor",
             "created": 0} for m in get_free_models()]


def parse_sse(raw_lines, state=None):
    """把上游 SSE 行流解析成 OpenAI 格式的 chunk 字典（逐个 yield）

    state 是可选 dict：见到 [DONE] 时置 state["done"]=True，供调用方
    判断这条流是真结束还是上游提前关闭连接（后者不抛异常，看不出来）。
    """
    for line in raw_lines:
        s = line.decode("utf-8", "replace").strip()
        if not s or not s.startswith("data:"):
            continue
        payload = s[5:].strip()
        if payload == "[DONE]":
            if state is not None:
                state["done"] = True
            break
        try:
            chunk = json.loads(payload)
        except Exception:
            continue
        yield chunk


def collect_text(chunks):
    out = []
    for c in chunks:
        try:
            d = c["choices"][0]["delta"]
        except Exception:
            continue
        if d.get("content"):
            out.append(d["content"])
    return "".join(out)


def _is_channel_credential_error(detail):
    """上游 401/403 是否属于「某个渠道/模型缺凭据」而非 session 过期。

    Qoder-* 一类模型的报错是：
      {"error":"你的中转站登录已失效，请重新输入密码绑定","code":"USER_TOKEN_EXPIRED"}
    这是按模型（渠道）绑定的上游账号没配，换自己的 cookie 完全没用。
    判据用错误码优先，文案兜底。
    """
    low = (detail or "").lower()
    if "user_token_expired" in low or "invalid_token" in low:
        return True
    return "中转站" in low and ("绑定" in low or "失效" in low)


def _normalize_error(detail):
    """把上游错误统一成 OpenAI 风格的 {"message","type","code"}。

    上游原始格式是 {"error":"文本","code":"XXX"}，直接塞进
    {"error":{"message": detail}} 会让客户端拿到嵌套的 JSON 字符串，
    既不好读也不好判断。这里拆平。
    """
    msg, code = detail, None
    try:
        obj = json.loads(detail)
        if isinstance(obj, dict):
            e = obj.get("error")
            if isinstance(e, str):
                msg = e
            elif isinstance(e, dict):
                msg = e.get("message") or json.dumps(e, ensure_ascii=False)
            code = obj.get("code") or (e.get("code") if isinstance(e, dict) else None)
    except Exception:
        pass
    out = {"message": msg, "type": "upstream_error"}
    if code:
        out["code"] = code
    return out


# ---------------------------------------------------------------- HTTP 服务

def read_body(handler):
    """读请求体，同时支持 Content-Length 和 Transfer-Encoding: chunked。

    只认 Content-Length 时，客户端用 chunked 发大 payload（openai SDK 超过
    一定体积会自动切换）会读到空 body，上游收到 {} 直接回 400
    INVALID_MESSAGES，看起来像是客户端发错了消息。
    """
    te = (handler.headers.get("Transfer-Encoding") or "").lower()
    if "chunked" in te:
        chunks = []
        while True:
            line = handler.rfile.readline(65536)
            if not line:
                break
            size_s = line.strip().split(b";")[0].strip()
            if not size_s:
                continue
            try:
                size = int(size_s, 16)
            except ValueError:
                break
            if size == 0:
                # 吞掉 trailer 直到空行
                while True:
                    t = handler.rfile.readline(65536)
                    if t in (b"\r\n", b"\n", b""):
                        break
                break
            chunks.append(handler.rfile.read(size))
            handler.rfile.read(2)  # 每块尾随的 CRLF
        return b"".join(chunks)

    n = int(handler.headers.get("Content-Length") or 0)
    return handler.rfile.read(n) if n else b""


def json_response(handler, code, obj, extra_headers=None):
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    for k, v in (extra_headers or {}).items():
        handler.send_header(k, v)
    handler.end_headers()
    handler.wfile.write(data)


def _is_channel_missing(detail):
    """上游 502 是否属于「该模型此刻没有可用渠道」。

    两种文案（中文站点 + 英文 new-api 兜底）：
      分组 auto 下模型 X 的可用渠道不存在（retry）
      Failed to get available channel for model X
    这类错误是**按模型**的：换个模型往往立刻可用，原样重发只是再等
    一个 10 秒超时。跟 5xx 整体抖动要分开处理。

    上游把中文写成 \\uXXXX 转义，所以先按 JSON 解一层再匹配，
    否则中文关键词永远匹配不到。
    """
    texts = [detail or ""]
    try:
        obj = json.loads(detail)
        if isinstance(obj, dict):
            e = obj.get("error")
            if isinstance(e, str):
                texts.append(e)
            elif isinstance(e, dict):
                texts.append(json.dumps(e, ensure_ascii=False))
    except Exception:
        pass
    for t in texts:
        low = t.lower()
        if ("可用渠道不存在" in low or "available channel" in low
                or "no channel" in low):
            return True
    return False


def _is_retryable_code(code):
    """上游哪些状态码值得重发。

    401/403 走重登分支，由调用方单独处理，不在这里判。
    400 也算：上游拿它兜底"至少需要一条用户消息"，实测对合法 body
    偶发返回，重发即可成功。真·非法请求被拒得很快，重试成本可忽略。
    """
    return code == 400 or code == 429 or 500 <= code <= 599


def compact_tool_history(msgs, max_tool_msgs=MAX_TOOL_MSGS):
    """把超量的历史 tool 往返折叠成文本摘要。

    上游硬限制：单次请求里 role=="tool" 的消息条数超过 29 直接 400
    INVALID_MESSAGES（报错文案是"至少需要一条用户消息"，与实情无关）。

    折叠以"轮"为单位：一条带 tool_calls 的 assistant 加上它名下全部
    tool 回应是不可分割的整体。按单条 tool 消息回溯配对会让并行
    tool_call（一轮多个 tool）产生孤儿消息，实测会这样。
    """
    tool_idx = [i for i, m in enumerate(msgs) if m.get("role") == "tool"]
    if len(tool_idx) <= max_tool_msgs:
        return msgs, 0

    # 划分"轮"：assistant(tool_calls) 起头，吞掉紧随其后、id 在它名下的
    # tool 消息；id 不匹配的 tool 消息单独成轮，交给下面按条数兜底。
    rounds, i, n = [], 0, len(msgs)
    while i < n:
        m = msgs[i]
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ids = {tc.get("id") for tc in m["tool_calls"]}
            idxs = [i]
            j = i + 1
            while j < n and msgs[j].get("role") == "tool" and msgs[j].get("tool_call_id") in ids:
                idxs.append(j)
                j += 1
            rounds.append(idxs)
            i = j
        elif m.get("role") == "tool":
            rounds.append([i])
            i += 1
        else:
            i += 1

    # 从最老的轮开始丢，直到剩余 tool 消息不超上限
    keep_from = 0
    total = sum(1 for x in msgs if x.get("role") == "tool")
    for k, idxs in enumerate(rounds):
        if total <= max_tool_msgs:
            keep_from = k
            break
        cnt = sum(1 for x in idxs if msgs[x].get("role") == "tool")
        total -= cnt
        keep_from = k + 1

    dropped = rounds[:keep_from]
    skip = {x for idxs in dropped for x in idxs}
    if not skip:
        return msgs, 0

    names = []
    for idxs in dropped:
        for x in idxs:
            for tc in (msgs[x].get("tool_calls") or []):
                names.append(((tc.get("function") or {}).get("name")) or "?")

    out, first_removed = [], None
    for i, m in enumerate(msgs):
        if i in skip:
            if first_removed is None:
                first_removed = len(out)
            continue
        out.append(m)

    uniq = sorted(set(names))
    note = (f"[历史压缩] 为满足上游单次请求的 tool 消息上限，"
            f"已折叠较早的 {len(dropped)} 轮工具调用"
            f"（涉及工具：{', '.join(uniq[:8]) or '无'}）。"
            f"这些调用的结果不再逐条可见；如需重新确认，请再次调用相应工具。")
    out.insert(min(first_removed, len(out)), {"role": "system", "content": note})

    return out, len(dropped)


def handle_chat(handler, body):
    """核心：转发到上游，401/403 重登重试，400/429/5xx 退避重试"""
    want_stream = bool(body.get("stream"))
    body.setdefault("model", MODEL_ID)
    body.pop("stream", None) if not want_stream else None

    # 模型白名单：非 workbuddy-* 一律本地拒掉。
    # 交给上游只会换来 401 USER_TOKEN_EXPIRED（Qoder 渠道缺凭据），
    # 而那类 401 会触发无谓的重新登录，白白消耗上游会话数配额。
    if not _model_allowed(body.get("model")):
        allowed = get_free_models()
        log(f"🚫 拒绝非白名单模型: {body.get('model')!r}")
        record_usage(model=str(body.get("model")), ok=False)
        json_response(handler, 400, {"error": {
            "message": f"model {body.get('model')!r} is not available; "
                       f"allowed: {', '.join(allowed)}",
            "type": "invalid_request_error",
            "code": "model_not_found",
        }})
        return

    _t0 = time.time()
    _model = str(body.get("model"))

    # 诊断：记录 messages 的角色序列和每条内容长度。
    # 上游要求至少一条非空 user 消息，否则 400 INVALID_MESSAGES。
    msgs = body.get("messages") or []
    msgs, folded = compact_tool_history(msgs)
    if folded:
        body["messages"] = msgs
        log(f"✂️ 折叠了 {folded} 轮旧工具调用（tool 消息上限 {MAX_TOOL_MSGS}）"
            f"，折叠后 msgs={len(msgs)}")
    roles = "/".join(str(m.get("role")) for m in msgs)
    lens = ",".join(str(len(str(m.get("content") or ""))) for m in msgs)
    log(f"📨 msgs={len(msgs)} roles=[{roles[:300]}] lens=[{lens[:300]}]"
        f" stream={want_stream} tools={len(body.get('tools') or [])}")
    # 上游 400 INVALID_MESSAGES 时最需要原始结构，落一份便于复盘。
    if os.environ.get("DOULOR_DUMP_BODY"):
        try:
            with open(os.path.join(STATE_DIR, "last_body.json"), "w", encoding="utf-8") as f:
                json.dump(body, f, ensure_ascii=False)
        except Exception:
            pass

    retries = 0          # 故障重试已用次数
    auth_retried = False  # 鉴权重试只给一次，和故障重试分开计
    failovers = 0         # 换模型重发已用次数
    tried_models = {_model}   # 已失败的模型，避免来回换同一个

    def backoff(reason):
        """按剩余额度退避等待；返回 True 表示调用方可以 continue 重试。
        额度用尽返回 False，由调用方把错误回给客户端。"""
        nonlocal retries
        if retries >= len(RETRY_BACKOFF):
            return False
        wait = RETRY_BACKOFF[retries]
        retries += 1
        log(f"🔁 {wait}s 后重试（{retries}/{len(RETRY_BACKOFF)}），原因 {reason}")
        time.sleep(wait)
        return True

    def failover():
        """换一个同池模型立刻重发（不 sleep）。返回 True 表示可以 continue。

        只用于「该模型无可用渠道」这类按模型的 502：那条错误自己就要等
        约 10 秒才返回，原样重发大概率再等 10 秒拿同样的 502，换模型常常
        3 秒内就通。响应里的 model 字段会如实反映实际生效的模型。
        """
        nonlocal failovers
        if failovers >= FAILOVER_MAX:
            return False
        for alt in FAILOVER_MODELS:
            if alt in tried_models:
                continue
            failovers += 1
            tried_models.add(alt)
            log(f"↪️ 渠道缺失，换模型重发: {body.get('model')} → {alt}"
                f"（{failovers}/{FAILOVER_MAX}）")
            body["model"] = alt
            return True
        return False

    while True:
        sid = SESSION.get()
        if not sid:
            json_response(handler, 502, {"error": {"message": "无法登录上游",
                                                 "type": "upstream_login_failed"}})
            return

        try:
            status, hdrs, resp = upstream_chat(body, sid)
        except urllib.error.HTTPError as e:
            code = e.code
            detail = e.read().decode("utf-8", "replace")[:500]
            try:
                e.close()
            except Exception:
                pass
            log(f"❌ 上游 HTTP {code}: {detail[:200]}")
            record_usage(model=_model, ok=False, error=code,
                         elapsed=time.time() - _t0, detail=detail[:200])

            # 401/403 里要分开看：USER_TOKEN_EXPIRED 是「某个渠道缺凭据」
            # （如 Qoder-*），不是 cookie 过期。重登治不好它，而且每次重登
            # 都会在上游新建一个 30 天会话，白耗 AUTH_SESSION_LIMIT 配额
            # ——autofix 要的就是同一份额度。这类直接回给客户端。
            if code in (401, 403) and _is_channel_credential_error(detail):
                log(f"🚫 上游 {code} 属于渠道凭据缺失（非 session 过期），不重登")
                json_response(handler, code, {"error": _normalize_error(detail)})
                return

            if code in (401, 403) and not auth_retried:
                auth_retried = True
                log(f"⚠️ 上游 {code}，尝试重新登录后重试")
                # 不要先 invalidate：清掉 sid 会让冷却期内的后续请求
                # 走 get() 重新登录，等于绕过冷却。重登成功会覆盖 sid，
                # 失败时留着旧 sid 让后续请求直接失败，不再打登录接口。
                if SESSION.relogin_on_auth_error():
                    continue
                log(f"⏸ 重登被冷却/失败拦住，直接返回 {code}")

            # 502 一律先试换模型（不限「渠道缺失」文案）：
            #   - 渠道缺失类：换模型常常 3s 内就通，原样重发是再等 10s；
            #   - 兜底文案「AI 调用失败（HTTP 502）」：分不清是渠道还是
            #     过载，但换模型重发不 sleep，失败后照旧落到下面的退避
            #     分支，最坏情况只是多试了一个模型，不会更差。
            # 非 502 的可重试错误（429/5xx 其他码）仍直接走退避原样重发。
            if code == 502 and failover():
                tag = "渠道缺失" if _is_channel_missing(detail) else "其他502"
                log(f"ℹ️ {tag}: {detail[:120]}")
                continue

            # 间歇性故障：退避后重发同一个 body。此时还没给客户端写过
            # 任何字节（流式也一样），所以重试是安全的。
            if _is_retryable_code(code) and backoff(code):
                continue
            json_response(handler, code, {"error": _normalize_error(detail)})
            return
        except Exception as e:
            log(f"❌ 上游异常: {type(e).__name__}: {e}")
            # 连接层的抖动（超时、连接重置）同样按故障重试
            if backoff(type(e).__name__):
                continue
            record_usage(model=_model, ok=False, error=type(e).__name__,
                         elapsed=time.time() - _t0)
            json_response(handler, 502, {"error": {"message": str(e),
                                                 "type": "upstream_error"}})
            return

        # 401/403 已在上面 except urllib.error.HTTPError 分支处理；
        # upstream_chat 用 urlopen，4xx/5xx 一律抛异常，不会走到这里的
        # 鉴权判断，故此处只处理正常 200。
        # --- 正常 200：开始回传 ---

        # v1 通道的非流式响应是纯 JSON（application/json），不是 SSE 流。
        # lab 通道即便非流式也走 SSE 帧返回，所以按 Content-Type 区分。
        ctype = (hdrs.get("Content-Type") or "")
        if "application/json" in ctype and not want_stream:
            raw = resp.read()
            resp.close()
            try:
                result = json.loads(raw.decode("utf-8", "replace"))
            except Exception as e:
                log(f"❌ v1 非流式响应解析失败: {e}")
                json_response(handler, 502, {"error": {
                    "message": f"bad upstream json: {e}", "type": "upstream_error"}})
                return
            # 模型名映射回 workbuddy-*（客户端看到的名字保持一致）
            if isinstance(result, dict) and result.get("model"):
                for wb, real in WORKBUDDY_TO_V1.items():
                    if result["model"] == real:
                        result["model"] = wb
                        break
            usage = (result or {}).get("usage") if isinstance(result, dict) else None
            record_usage(model=_model, ok=True, usage=usage,
                         prompt_chars=sum(len(str(m.get("content") or "")) for m in msgs),
                         completion_chars=len(str(((result or {}).get("choices") or [{}])[0]
                                                   .get("message", {}).get("content") or "")),
                         elapsed=time.time() - _t0)
            log(f"✅ v1 非流式完成（{current_channel()} 通道）")
            json_response(handler, 200, result)
            return

        if want_stream:
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
            handler.send_header("Cache-Control", "no-cache")
            handler.send_header("Connection", "close")
            handler.end_headers()
            # 尾块的标识：截断发生在任意位置，都要能补出一个形态合法的收尾块。
            chunk_id, created = "chatcmpl-proxy", int(time.time())

            # 读上游和写客户端要分开 try：两者的 IOError 类型相同（尤其
            # ConnectionResetError），合在一个 except 里就分不清是谁断的。
            # 旧代码把上游断连一起 pass 掉，收尾块压根没发出去——客户端于是
            # 把半截回答当成了完整回答。
            client_gone = False

            def emit(obj):
                # 客户端已断开就不必再写，让上层停下来
                nonlocal client_gone
                if client_gone:
                    return
                try:
                    handler.wfile.write(
                        f"data: {json.dumps(obj, ensure_ascii=False)}\n\n".encode())
                    handler.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    client_gone = True

            def finish_chunk(reason):
                return {"id": chunk_id, "object": "chat.completion.chunk",
                        "created": created, "model": _model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]}

            st = {}
            upstream_ok = False
            got_usage = None
            out_chars = 0
            try:
                try:
                    for chunk in parse_sse(resp, st):
                        chunk_id = chunk.get("id", chunk_id)
                        created = chunk.get("created", created)
                        # 上游把用量放在最后一个 chunk 的 usage 字段里；
                        # 顺手统计输出字符数，没有 usage 时用它估算。
                        if chunk.get("usage"):
                            got_usage = chunk["usage"]
                        choices = chunk.get("choices") or []
                        for c in choices:
                            d = c.get("delta") or {}
                            for k in ("content", "reasoning_content"):
                                if d.get(k):
                                    out_chars += len(str(d[k]))
                        if any(c.get("finish_reason") for c in choices):
                            st["finish"] = True
                        emit({"id": chunk_id, "object": "chat.completion.chunk",
                              "created": created, "model": _model,
                              "choices": choices})
                        if client_gone:
                            break
                    upstream_ok = True
                except OSError as e:
                    # 读上游时断连：urllib 可能抛 IncompleteRead / ConnectionResetError
                    # 等各种 OSError，一律当作上游截断，走下面的 length 收尾。
                    log(f"⚠️ 读上游中断: {type(e).__name__}: {e}")
            finally:
                resp.close()
                record_usage(model=_model, ok=upstream_ok, usage=got_usage,
                             prompt_chars=sum(len(str(m.get("content") or ""))
                                              for m in msgs),
                             completion_chars=out_chars,
                             elapsed=time.time() - _t0)

            if client_gone:
                return

            # 上游没给 [DONE] 就关了连接（也不抛异常）：这是截断，
            # 不能当成正常结束，否则客户端会把半截回答当完整的。
            # 例外：已经发过 finish_reason 说明上游自己认账了，只是少个 [DONE]，
            # 那是正常完成，别降级成截断（否则客户端会白续写一轮）。
            truncated = not st.get("done") and not st.get("finish")
            if not st.get("finish") and not truncated:
                # [DONE] 到了却没有 finish_reason：补一个 stop，
                # 别让客户端收到一个永远缺收尾块的流。
                emit(finish_chunk("stop"))

            if truncated:
                # 上游中途断开：用 finish_reason="length" 收尾，而不是 error 事件。
                # error 事件会被 openai SDK 解析成 APIError 抛出，能否保住已收到的
                # 正文取决于客户端；"length" 是纯协议内的信号，客户端按截断处理，
                # 既保留正文又能触发续写。参数被切断时客户端还能靠"不以 } 结尾"
                # 识别出工具调用不完整，从而拒绝执行。
                if not upstream_ok:
                    log("⚠️ 上游流异常中断，补 finish_reason=length")
                else:
                    log("⚠️ 上游流未以 [DONE] 结束，补 finish_reason=length")
                emit(finish_chunk("length"))

            if not client_gone and got_usage and \
                    (body.get("stream_options") or {}).get("include_usage"):
                # 客户端明确要 include_usage 才补这一块：OpenAI 规范里
                # usage 只在这种时候出现，无条件发会让严格客户端报错。
                emit({"id": chunk_id, "object": "chat.completion.chunk",
                      "created": created, "model": _model,
                      "choices": [], "usage": got_usage})

            if not client_gone:
                try:
                    handler.wfile.write(b"data: [DONE]\n\n")
                    handler.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            return

        # 非流式：收完再一次性返回
        try:
            st = {}
            chunks = list(parse_sse(resp, st))
        finally:
            resp.close()
        # 还没给客户端写过任何字节，截断可以直接退避重发
        if not st.get("done"):
            log("⚠️ 非流式上游流被截断（无 [DONE]）")
            if backoff("stream_truncated"):
                continue
            json_response(handler, 502, {"error": {
                "message": "上游流被截断", "type": "upstream_stream_truncated"}})
            return
        text = collect_text(chunks)
        usage = None
        finish = None
        for c in chunks:
            if c.get("usage"):
                usage = c["usage"]
            # 取最后一个非空的 finish_reason（上游可能多次给出）
            for ch in (c.get("choices") or []):
                if ch.get("finish_reason"):
                    finish = ch["finish_reason"]
        result = {
            "id": chunks[0].get("id", "chatcmpl-proxy") if chunks else "chatcmpl-proxy",
            "object": "chat.completion",
            "created": chunks[0].get("created", int(time.time())) if chunks else int(time.time()),
            "model": _model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                # 透传上游的 finish_reason（length/tool_calls 等），
                # 硬编码 stop 会让客户端把截断的回答当完整回答。
                "finish_reason": finish or "stop",
            }],
        }
        if usage:
            result["usage"] = usage
        record_usage(model=_model, ok=True, usage=usage,
                     prompt_chars=sum(len(str(m.get("content") or "")) for m in msgs),
                     completion_chars=len(text), elapsed=time.time() - _t0)
        json_response(handler, 200, result)
        return


# ---------------------------------------------------------------- 面板

PANEL_PASS_FILE = os.path.join(STATE_DIR, "panel_pass.json")


def panel_pass_load():
    """面板访问密码。优先 panel_pass.json，否则 env DOULOR_PANEL_PASS，
    否则初始密码 admin123。"""
    try:
        with open(PANEL_PASS_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if d.get("pass"):
            return d["pass"]
    except Exception:
        pass
    return os.getenv("DOULOR_PANEL_PASS", "admin123")


def panel_pass_set(newpass):
    with open(PANEL_PASS_FILE, "w", encoding="utf-8") as f:
        json.dump({"pass": newpass, "changed_at": time.time()}, f)
    os.chmod(PANEL_PASS_FILE, 0o600)


def _checkin_cron_status():
    """读 checkin.log 末尾，给面板显示 cron 自动签到的最近结果。"""
    path = os.path.join(STATE_DIR, "checkin.log")
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 16 * 1024))
            lines = [l for l in f.read().decode("utf-8", "replace").splitlines() if l.strip()]
    except Exception:
        return {"cron": "unknown", "last": "", "recent": []}
    if not lines:
        return {"cron": "unknown", "last": "", "recent": []}
    last = lines[-1]
    ok = last.startswith("[") and ("✅" in last or "🎉" in last)
    return {"cron": "ok" if ok else "fail", "last": last,
            "recent": lines[-5:]}


def _tail_file(path, n=200):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 64 * 1024))
            data = f.read().decode("utf-8", "replace")
            return "\n".join(data.splitlines()[-n:])
    except Exception:
        return ""


def _panel_points():
    """账户积分：只读 checkin.py 落盘的结果，不触碰上游（避免风控）。
    上游数据每次签到才刷新一次。"""
    path = os.path.join(STATE_DIR, "checkin_result.json")
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except FileNotFoundError:
        return False, {"error": "no_result", "hint": "尚未执行过签到（无 checkin_result.json）"}
    except Exception as e:
        return False, {"error": f"读取结果失败: {type(e).__name__}"}
    if not isinstance(d, dict):
        return False, {"error": "结果文件格式异常"}
    d["stale"] = (d.get("date") != time.strftime(
        "%Y-%m-%d", time.gmtime(time.time() + 8 * 3600)))
    return True, d


def _panel_checkin():
    """立即签到：直接跑 checkin.py，签到逻辑单一来源。
    脚本自己打上游，结果写进 checkin_result.json（面板随后只读它）。
    当天已签过就不再跑，避免反复打上游触发风控。"""
    today = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 8 * 3600))
    ok, cached = _panel_points()
    if ok and cached.get("date") == today and cached.get("ok"):
        return True, {"ok": True, "cached": True,
                      "note": "今天已签到，跳过重复请求上游"}
    script = os.path.join(STATE_DIR, "checkin.py")
    try:
        r = subprocess.run([sys.executable, script], capture_output=True,
                           text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return False, {"error": "签到脚本超时（120s）"}
    except Exception as e:
        return False, {"error": f"启动签到脚本失败: {type(e).__name__}: {e}"}
    tail = (r.stdout or r.stderr or "").strip().splitlines()[-3:]
    return r.returncode == 0, {"ok": r.returncode == 0, "exit": r.returncode,
                               "log": tail}


PANEL_HTML = r"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>doulor 中转面板</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;
background:#0f1117;color:#e1e4ec;padding:24px;max-width:1100px;margin:0 auto}
h1{font-size:20px;margin-bottom:4px}
.sub{color:#7a8194;font-size:12px;margin-bottom:20px}
.panel{background:#171a23;border:1px solid #262b38;border-radius:10px;
padding:18px;margin-bottom:16px}
h2{font-size:14px;color:#9aa3b8;margin-bottom:12px;font-weight:600}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.stat{background:#1d212d;border-radius:8px;padding:12px}
.stat .v{font-size:22px;font-weight:700;color:#4ec9b0}
.stat .l{font-size:11px;color:#7a8194;margin-top:2px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{color:#7a8194;text-align:left;padding:6px 8px;border-bottom:1px solid #262b38}
td{padding:6px 8px;border-bottom:1px solid #1f2430}
input,button{font-size:13px;border-radius:6px;border:1px solid #2d3242;
background:#1d212d;color:#e1e4ec;padding:8px 12px;outline:none}
input:focus{border-color:#4ec9b0}
button{cursor:pointer;background:#2a3f50;border-color:#2a3f50}
button:hover{background:#35526a}
button.ok{background:#1d5c4a;border-color:#1d5c4a}
.msg{font-size:12px;margin-top:8px;color:#7a8194;min-height:16px}
pre{background:#12141c;border:1px solid #262b38;border-radius:8px;padding:12px;
font-size:11px;max-height:340px;overflow:auto;color:#9fb0c8;line-height:1.5}
.tag{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;
background:#1d3a50;color:#58a6ff;margin-left:8px}
#login{max-width:340px;margin:80px auto}
#login input{width:100%;margin-bottom:10px}
#login button{width:100%}
.err{color:#f87171}.okc{color:#4ec9b0}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.row input{flex:1;min-width:160px}
</style></head><body>
<div id="login" class="panel" style="display:none">
<h2>访问密码</h2>
<input id="lp" type="password" placeholder="面板访问密码" onkeydown="if(event.key==='Enter')dologin()">
<button onclick="dologin()" style="width:100%;margin-top:8px">进入面板</button>
<div class="msg" id="lm"></div>
</div>
<div id="app" style="display:none">
<h1>doulor 中转面板 <span class="tag" id="sess"></span></h1>
<div class="sub">127.0.0.1:8003 · /v1 OpenAI 兼容</div>

<div class="panel"><h2>用量统计</h2>
<div class="grid" id="stats"></div>
<div style="margin-top:14px"><h2>按模型</h2>
<table><thead><tr><th>模型</th><th>请求数</th><th>tokens</th><th>错误</th></tr></thead>
<tbody id="models"></tbody></table></div>
<div style="margin-top:14px"><h2>最近 14 天</h2>
<table><thead><tr><th>日期</th><th>请求数</th><th>tokens</th><th>错误</th></tr></thead>
<tbody id="days"></tbody></table></div>
</div>

<div class="panel"><h2>每日签到 <span class="tag" id="ckstate"></span></h2>
<div class="msg" id="ckcron" style="margin-bottom:10px">自动签到状态加载中…</div>
<div class="row" id="ptrow" style="margin-bottom:10px"></div>
<div class="row">
<button id="ckbtn" class="ok" onclick="checkin(1)">立即签到</button>
<span class="msg" id="ckmsg"></span></div>
<div class="msg" style="color:#5b6478;margin-top:6px">此页只显示最近一次签到的结果（每天 cron 跑一次），不会自动向上游查询。</div></div>

<div class="panel"><h2>doulor 账号 <span class="tag">保存后自动重新登录</span></h2>
<div class="row"><input id="duser" placeholder="账号(邮箱/用户名)">
<input id="dpass" type="password" placeholder="密码"></div>
<div class="row" style="margin-top:10px">
<button onclick="savecreds()">保存并重登</button>
<span class="msg" id="cm"></span></div></div>

<div class="panel"><h2>修改面板访问密码</h2>
<div class="row"><input id="np1" type="password" placeholder="新密码">
<input id="np2" type="password" placeholder="再输一遍"></div>
<div class="row" style="margin-top:10px"><button onclick="chpass()">修改</button>
<span class="msg" id="pm"></span></div></div>

<div class="panel"><h2>请求日志（最近 200 行）</h2>
<pre id="lg">加载中…</pre></div>
</div>
<script>
let P=localStorage.getItem('dlpass')||'';
const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(path,opt={}){
  const r=await fetch(path,{...opt,headers:{'Content-Type':'application/json',
  'X-Panel-Pass':P,...(opt.headers||{})}});
  return r;}
function dologin(){P=$('lp').value;if(!P)return;
  api('/api/panel/stats').then(r=>{
    if(r.ok){localStorage.setItem('dlpass',P);boot(true);}
    else{$('lm').textContent='❌ 密码错误';$('lm').className='msg err';}});}
let booted=false;
async function boot(force){
  const r=await api('/api/panel/stats');
  if(!r.ok){$('login').style.display='block';$('app').style.display='none';booted=false;return;}
  $('login').style.display='none';$('app').style.display='block';
  const st=await r.json();
  $('sess').textContent=st.session==='valid'?'会话正常':'无会话';
  const s=st.usage;
  const fmt=n=>{n=+n||0;return n>1e6?(n/1e6).toFixed(1)+'M':n>1e3?(n/1e3).toFixed(1)+'k':n};
  $('stats').innerHTML=[
    ['总 tokens',fmt(s.total)],['请求',s.requests],['错误',s.errors],
    ['今日 tokens',fmt((s.days&&Object.values(s.days).slice(-1)[0]||{}).total||0)],
    ['模型数',st.models]
  ].map(x=>`<div class="stat"><div class="v">${x[1]}</div><div class="l">${x[0]}</div></div>`).join('');
  $('models').innerHTML=(Object.entries(s.models||{}).sort((a,b)=>b[1].total-a[1].total)
    .map(([m,d])=>`<tr><td class="mono">${esc(m)}</td><td>${d.requests}</td><td>${fmt(d.total)}</td><td>${d.errors||0}</td></tr>`).join(''))||'<tr><td colspan=4>暂无</td></tr>';
  const days=Object.entries(s.days||{}).sort().slice(-14);
  $('days').innerHTML=days.map(([d,v])=>`<tr><td>${d}</td><td>${v.requests}</td><td>${fmt(v.total)}</td><td>${v.errors||0}</td></tr>`).join('')||'<tr><td colspan=4>暂无</td></tr>';
  // cron 自动签到状态（纯本地读 checkin.log，不走上游）
  const ck=st.cron_checkin||{};
  if(ck.last){
    const okc=ck.cron==='ok';
    $('ckcron').innerHTML=(okc?'✅ 最近自动签到：':'⚠️ 最近自动签到：')+esc(ck.last)+
      `<div style="color:#5b6478;margin-top:4px">cron: 每天 00:10（UTC+8 日界）· /etc/cron.d/doulor-checkin</div>`;
    $('ckcron').className='msg '+(okc?'okc':'err');
  }else{
    $('ckcron').textContent='⚠️ 未读到 checkin.log';
  }
  // 输入框只在首次进入时回填，避免 5 秒刷新打断输入
  if(!booted||force){$('duser').value=st.cred_user||'';booted=true;loadResult();}
  api('/api/panel/logs').then(r=>r.text()).then(t=>{
    const el=$('lg');const atBottom=el.scrollTop+el.clientHeight>=el.scrollHeight-30;
    el.textContent=t||'(空)';if(atBottom)el.scrollTop=el.scrollHeight;});
}
async function loadResult(){
  // 只读本地 checkin_result.json，绝不查询上游
  const r=await api('/api/panel/points');
  const p=await r.json().catch(()=>({}));
  if(!r.ok||p.error){
    $('ptrow').innerHTML='<span class="msg">还没有签到记录（等 cron 或点“立即签到”）</span>';
    $('ckstate').textContent='未签';
    if(!p.stale)$('ckmsg').textContent='';
    return;
  }
  // 上游时间戳是 UTC，签到日按 UTC+8 算
  const bj=ts=>{if(!ts)return '';const d=new Date(ts);if(isNaN(d))return String(ts).slice(0,10);
    return new Date(d.getTime()+8*3600e3).toISOString().slice(0,10);};
  const at=p.at?new Date(p.at*1000).toLocaleString('zh-CN',{hour12:false}):'';
  $('ptrow').innerHTML=
   `<div class="stat" style="min-width:130px"><div class="v">${esc(p.balance??'—')}</div><div class="l">账户余额（1 分抵 ${esc(p.yuanPerPoint??'?')} 元）</div></div>
    <div class="stat" style="min-width:130px"><div class="v" style="color:#4ade80">+${esc(p.checkin_total??0)}</div><div class="l">签到累计获得（${esc(p.checkin_count??0)} 次）</div></div>
    <div class="stat" style="min-width:130px"><div class="v" style="color:#4ade80">${p.points!==undefined?'+'+esc(p.points):(p.todayPoints!==undefined?'+'+esc(p.todayPoints):'—')}</div><div class="l">${esc(p.date||'')} 本次${p.already?'（已签）':''}</div></div>`;
  $('ckstate').textContent=p.date||'—';
  const st=[];
  if(p.ok!==false){
    st.push(p.already?'✅ 已签':'🎉 签到成功');
    if(p.streak!==undefined)st.push(`连签 <b>${esc(p.streak)}</b> 天`);
    if(p.milestone)st.push(`里程碑 +${esc(p.milestone.points)}`);
    if(p.next)st.push(`距 ${esc(p.next.days)} 天里程碑还差 ${esc(p.next.daysLeft)} 天(+${esc(p.next.points)})`);
  }else{
    st.push('❌ '+(p.error||'失败'));
  }
  st.push(`<span style="color:#5b6478">结果时间 ${esc(at)}</span>`);
  $('ckmsg').innerHTML=st.join(' · ');
  if(p.stale)$('ckmsg').innerHTML+=` <span style="color:#d8a657">· 非今日结果，等下次签到</span>`;
}
async function checkin(doIt){
  $('ckmsg').textContent='签到中，请稍候…';
  $('ckbtn').disabled=true;
  try{
    const r=await api('/api/panel/checkin',{method:'POST',body:JSON.stringify({do:true})});
    const d=await r.json().catch(()=>({}));
    $('ckmsg').textContent=d.error?('❌ '+d.error):'完成，读取结果…';
  }catch(e){$('ckmsg').textContent='❌ 请求失败';}
  $('ckbtn').disabled=false;
  await loadResult();
}
async function savecreds(){
  const user=$('duser').value.trim(),pass=$('dpass').value;
  if(!user||!pass){$('cm').textContent='账号密码都要填';return}
  const r=await api('/api/panel/creds',{method:'POST',body:JSON.stringify({user,pass})});
  const d=await r.json();
  $('cm').textContent=d.message||'';$('cm').className='msg '+(d.ok?'okc':'err');
  if(d.ok)setTimeout(boot,1200);
}
async function chpass(){
  const a=$('np1').value,b=$('np2').value;
  if(!a){$('pm').textContent='新密码不能为空';return}
  if(a!==b){$('pm').textContent='两次输入不一致';return}
  const r=await api('/api/panel/pass',{method:'POST',body:JSON.stringify({old:P,np:a})});
  const d=await r.json();
  $('pm').textContent=d.message||'';$('pm').className='msg '+(d.ok?'okc':'err');
  if(d.ok){P=a;localStorage.setItem('dlpass',P);setTimeout(boot,800)}
}
boot();setInterval(boot,5000);
</script></body></html>"""


class Handler(__import__("http.server", fromlist=["BaseHTTPRequestHandler"]).BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        log("🌐 " + (fmt % args))

    def _auth_ok(self):
        if not API_KEY:
            return True
        h = self.headers.get("Authorization") or ""
        if h.startswith("Bearer ") and h[7:].strip() == API_KEY:
            return True
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        return q.get("key", [""])[0] == API_KEY

    def _panel_authed(self):
        return (self.headers.get("X-Panel-Pass") or "") == panel_pass_load()

    def do_GET(self):
        p = urllib.parse.urlparse(self.path).path
        if p == "/":
            body = PANEL_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if p == "/healthz":
            json_response(self, 200, {
                "ok": True,
                "session": "valid" if SESSION._sid else "none",
                "expires_in_days": round(max(0, SESSION._expires - time.time()) / 86400, 2),
            })
            return
        if p == "/api/panel/stats":
            if not self._panel_authed():
                json_response(self, 401, {"error": "unauthorized"})
                return
            json_response(self, 200, {
                "usage": usage_snapshot(),
                "models": len(models_payload()),
                "session": "valid" if SESSION._sid else "none",
                "session_days": round(max(0, SESSION._expires - time.time()) / 86400, 1),
                "cred_user": IDENTIFIER,
                "cron_checkin": _checkin_cron_status(),
            })
            return
        if p == "/api/panel/points":
            if not self._panel_authed():
                json_response(self, 401, {"error": "unauthorized"})
                return
            ok, d = _panel_points()
            json_response(self, 200 if ok else 502, d)
            return
        if p == "/api/panel/logs":
            if not self._panel_authed():
                json_response(self, 401, {"error": "unauthorized"})
                return
            body = _tail_file(LOG_FILE, 200).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if p == "/v1/models":
            json_response(self, 200, {"object": "list", "data": models_payload()})
            return
        self.send_error(404)

    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path

        # ---- 面板 API ----
        if p.startswith("/api/panel/"):
            if not self._panel_authed():
                json_response(self, 401, {"error": "unauthorized"})
                return
            try:
                body = json.loads(read_body(self) or b"{}")
            except Exception as e:
                json_response(self, 400, {"error": f"bad json: {e}"})
                return
            if p == "/api/panel/checkin":
                ok, d = _panel_checkin()
                log(f"📝 面板触发签到: exit={d.get('exit')} ok={d.get('ok')}")
                json_response(self, 200 if ok else 502, d)
                return
            if p == "/api/panel/points":
                ok, d = _panel_points()
                json_response(self, 200 if ok else 502, d)
                return
            if p == "/api/panel/creds":
                global IDENTIFIER, PASSWORD
                user = str(body.get("user") or "").strip()
                pass_ = str(body.get("pass") or "").strip()
                if not user or not pass_:
                    json_response(self, 400, {"ok": False, "message": "账号密码都要填"})
                    return
                # 写 env（保留其它行）
                env_path = os.path.join(STATE_DIR, "env")
                lines = []
                try:
                    with open(env_path, encoding="utf-8") as f:
                        lines = f.read().splitlines()
                except FileNotFoundError:
                    lines = []
                lines = [l for l in lines if not l.startswith("DOULOR_USER=")
                         and not l.startswith("DOULOR_PASS=")]
                lines.append(f"DOULOR_USER={user}")
                lines.append(f"DOULOR_PASS={pass_}")
                with open(env_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
                os.chmod(env_path, 0o600)
                IDENTIFIER, PASSWORD = user, pass_
                # 强制重登换新账号
                sid = SESSION.login(force=True)
                json_response(self, 200, {
                    "ok": bool(sid),
                    "message": "✅ 已保存并重新登录" if sid else "已保存，但登录失败（检查账号密码）"})
                log(f"🔧 面板更新账号: {user} → 登录{'成功' if sid else '失败'}")
                return
            if p == "/api/panel/pass":
                old = str(body.get("old") or "")
                np_ = str(body.get("np") or "").strip()
                if old != panel_pass_load():
                    json_response(self, 401, {"ok": False, "message": "旧密码不对"})
                    return
                if not np_ or len(np_) < 4:
                    json_response(self, 400, {"ok": False, "message": "新密码至少 4 位"})
                    return
                panel_pass_set(np_)
                log("🔑 面板访问密码已修改")
                json_response(self, 200, {"ok": True, "message": "✅ 面板密码已修改"})
                return
            json_response(self, 404, {"error": "not found"})
            return

        if p not in ("/v1/chat/completions", "/api/lab/chat", "/chat/completions"):
            self.send_error(404)
            return
        if not self._auth_ok():
            json_response(self, 401, {"error": {"message": "invalid api key"}})
            return
        try:
            body = json.loads(read_body(self) or b"{}")
        except Exception as e:
            json_response(self, 400, {"error": {"message": f"bad json: {e}"}})
            return
        try:
            handle_chat(self, body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            log(f"❌ 处理异常: {type(e).__name__}: {e}")
            try:
                json_response(self, 500, {"error": {"message": str(e)}})
            except Exception:
                pass


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    log("=" * 50)
    log("🚀 doulor 中转启动")
    log(f"   上游: {BASE}")
    log(f"   监听: {LISTEN_HOST}:{LISTEN_PORT}")
    log(f"   状态目录: {STATE_DIR}")
    log("=" * 50)

    if not SESSION.get():
        log("⚠️ 首次登录失败，稍后重试")

    from http.server import ThreadingHTTPServer
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    log(f"✅ 监听 http://{LISTEN_HOST}:{LISTEN_PORT}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log("👋 收到停止信号")
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()