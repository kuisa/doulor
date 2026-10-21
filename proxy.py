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

# 只暴露这一个模型（已实测免费可用）。
# 上游 /api/lab/models 里还列着一堆收费模型，调用会返回
# 502 "Failed to get available channel for model ..."，所以不暴露。
MODEL_ID = "workbuddy-deepseek-v4.1-flash"
MODELS = [{"id": MODEL_ID, "object": "model", "owned_by": "doulor"}]

# ---------------------------------------------------------------- 日志

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
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


def parse_sse(raw_lines):
    """把上游 SSE 行流解析成 OpenAI 格式的 chunk 字典（逐个 yield）"""
    for line in raw_lines:
        s = line.decode("utf-8", "replace").strip()
        if not s or not s.startswith("data:"):
            continue
        payload = s[5:].strip()
        if payload == "[DONE]":
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


def handle_chat(handler, body):
    """核心：转发到上游，遇 401/403 自动重登重试一次"""
    want_stream = bool(body.get("stream"))
    body.setdefault("model", MODELS[0]["id"])
    body.pop("stream", None) if not want_stream else None

    for attempt in (1, 2):
        sid = SESSION.get()
        if not sid:
            json_response(handler, 502, {"error": {"message": "无法登录上游",
                                                 "type": "upstream_login_failed"}})
            return

        try:
            status, hdrs, resp = upstream_chat(body, sid)
        except urllib.error.HTTPError as e:
            code = e.code
            if code in (401, 403) and attempt == 1:
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
            log(f"❌ 上游 HTTP {code}: {detail[:200]}")
            json_response(handler, code, {"error": _normalize_error(detail)})
            return
        except Exception as e:
            log(f"❌ 上游异常: {type(e).__name__}: {e}")
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
            try:
                for chunk in parse_sse(resp):
                    out = {
                        "id": chunk.get("id", "chatcmpl-proxy"),
                        "object": "chat.completion.chunk",
                        "created": chunk.get("created", int(time.time())),
                        "model": body.get("model"),
                        "choices": chunk.get("choices", []),
                    }
                    handler.wfile.write(
                        f"data: {json.dumps(out, ensure_ascii=False)}\n\n".encode())
                    handler.wfile.flush()
                handler.wfile.write(b"data: [DONE]\n\n")
                handler.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                log(f"⚠️ 流式回传中断: {type(e).__name__}: {e}")
                # 上游中途断开：补一个 error 事件 + [DONE]，
                # 否则客户端会以为拿到的是完整回答。
                try:
                    err = {"error": {"message": f"上游流中断: {type(e).__name__}",
                                     "type": "upstream_stream_error"}}
                    handler.wfile.write(
                        f"data: {json.dumps(err, ensure_ascii=False)}\n\n".encode())
                    handler.wfile.write(b"data: [DONE]\n\n")
                    handler.wfile.flush()
                except Exception:
                    pass
            finally:
                resp.close()
            return

        # 非流式：收完再一次性返回
        try:
            chunks = list(parse_sse(resp))
        finally:
            resp.close()
        text = collect_text(chunks)
        usage = None
        for c in chunks:
            if c.get("usage"):
                usage = c["usage"]
        result = {
            "id": chunks[0].get("id", "chatcmpl-proxy") if chunks else "chatcmpl-proxy",
            "object": "chat.completion",
            "created": chunks[0].get("created", int(time.time())) if chunks else int(time.time()),
            "model": body.get("model"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }],
        }
        if usage:
            result["usage"] = usage
        json_response(handler, 200, result)


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