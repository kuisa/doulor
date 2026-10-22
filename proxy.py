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

# 上游硬限制：单次请求 role=="tool" 的消息条数上限 29（30 条起返回
# 400 INVALID_MESSAGES，文案却写"至少需要一条用户消息"，极具误导性）。
# 实测 62 条消息的阈值本质就是 tool 条数，纯 user/assistant 对话可到 150+
# 无问题。超出时由 compact_tool_history 折叠较早的往返。
MAX_TOOL_MSGS = int(os.getenv("DOULOR_MAX_TOOL_MSGS", "29"))

# 只暴露这一个模型（已实测免费可用）。
# 上游 /api/lab/models 里还列着一堆收费模型，调用会返回
# 502 "Failed to get available channel for model ..."，所以不暴露。
MODEL_ID = "workbuddy-deepseek-v4.1-flash"
MODELS = [{"id": MODEL_ID, "object": "model", "owned_by": "doulor"}]

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


def upstream_chat(body, sid, timeout=180):
    """打上游 chat，返回 (status, resp_headers, 原始流迭代器)"""
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    h = _upstream_headers()
    h["Cookie"] = f"doulor_session={sid}"
    req = urllib.request.Request(CHAT_URL, data=data, method="POST", headers=h)
    r = urllib.request.urlopen(req, timeout=timeout)
    return r.status, r.headers, r


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
    body.setdefault("model", MODELS[0]["id"])
    body.pop("stream", None) if not want_stream else None

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
            if code in (401, 403) and not auth_retried:
                auth_retried = True
                log(f"⚠️ 上游 {code}，尝试重新登录后重试")
                # 不要先 invalidate：清掉 sid 会让冷却期内的后续请求
                # 走 get() 重新登录，等于绕过冷却。重登成功会覆盖 sid，
                # 失败时留着旧 sid 让后续请求直接失败，不再打登录接口。
                if SESSION.relogin_on_auth_error():
                    try:
                        e.close()
                    except Exception:
                        pass
                    continue
                log(f"⏸ 重登被冷却/失败拦住，直接返回 {code}")
            detail = e.read().decode("utf-8", "replace")[:500]
            try:
                e.close()
            except Exception:
                pass
            log(f"❌ 上游 HTTP {code}: {detail[:200]}")

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
            json_response(handler, 502, {"error": {"message": str(e),
                                                 "type": "upstream_error"}})
            return

        # 401/403 已在上面 except urllib.error.HTTPError 分支处理；
        # upstream_chat 用 urlopen，4xx/5xx 一律抛异常，不会走到这里的
        # 鉴权判断，故此处只处理正常 200。
        # --- 正常 200：开始回传 ---
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
                        "created": created, "model": body.get("model"),
                        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]}

            st = {}
            upstream_ok = False
            try:
                try:
                    for chunk in parse_sse(resp, st):
                        chunk_id = chunk.get("id", chunk_id)
                        created = chunk.get("created", created)
                        choices = chunk.get("choices") or []
                        if any(c.get("finish_reason") for c in choices):
                            st["finish"] = True
                        emit({"id": chunk_id, "object": "chat.completion.chunk",
                              "created": created, "model": body.get("model"),
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
            "model": body.get("model"),
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
        json_response(handler, 200, result)
        return


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

    def do_GET(self):
        p = urllib.parse.urlparse(self.path).path
        if p in ("/healthz", "/"):
            json_response(self, 200, {
                "ok": True,
                "session": "valid" if SESSION._sid else "none",
                "expires_in_days": round(max(0, SESSION._expires - time.time()) / 86400, 2),
            })
            return
        if p == "/v1/models":
            json_response(self, 200, {"object": "list", "data": MODELS})
            return
        self.send_error(404)

    def do_POST(self):
        p = urllib.parse.urlparse(self.path).path
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