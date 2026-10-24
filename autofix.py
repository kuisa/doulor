#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""doulor 网关令牌分组自愈（auto → 空/继承分组）

故障特征：
  cloud.doulor.cn 实验室 /api/lab/chat（以及本地 proxy.py 反代）返回 502：
  "Failed to get available channel ... under group auto(auto): auto groups is not enabled"

原因：
  站点托管的网关令牌（api.doulor.cn，名字「AI实验室」）group 被同步成 "auto"，
  而本账号的 auto 分组未启用（auto_groups=[]），走该令牌的请求全部 502。

修复：
  把 group 改回 ""（空 = 继承账号分组 donation），即刻恢复。

运行方式：
  由 doulor-token-autofix.timer 每 5 分钟调用；也可手动
  python3 /opt/doulor-proxy/autofix.py
日志：/opt/doulor-proxy/autofix.log
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

STATE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(STATE_DIR, "env")
GATEWAY = "https://api.doulor.cn"
LOG_PATH = os.path.join(STATE_DIR, "autofix.log")
# access_token 缓存：上游 AccessTokenTTL 是 15 分钟，定时器 5 分钟一跳，
# 缓存能把登录次数压到最低。每次登录都会新建一条 30 天会话，
# 而账号的 active session 上限只有 50 —— 登录本身就是稀缺操作。
TOKEN_CACHE = os.path.join(STATE_DIR, "autofix_token.json")
TOKEN_TTL_GUESS = 600          # 保守值，留出时钟偏差余量
# 409 AUTH_SESSION_LIMIT 期间的重试间隔。配额满了只有人工撤销会话
# 才会腾出来，盲目每 5 分钟重试只会刷日志、并无收益。
QUOTA_RETRY_SEC = 6 * 3600
BLOCK_MARKER = os.path.join(STATE_DIR, "autofix_quota_blocked.json")


def log(msg):
    line = "[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def load_env():
    env = {}
    with open(ENV_PATH, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def http_json(url, method="GET", body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json",
         "User-Agent": "doulor-autofix/1.0", "Origin": GATEWAY}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status = r.read().decode(), r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read().decode(), e.code
    except Exception as e:
        return None, {"__exc__": repr(e)}
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, {"__raw__": raw[:120]}


def read_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, obj):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)
    except OSError:
        pass


def quota_blocked():
    """配额占满时返回 blocker 记录（含 blocked_at），否则 None。"""
    st = read_json(BLOCK_MARKER)
    return st if isinstance(st, dict) and st.get("blocked_at") else None


def mark_quota_blocked(detail):
    write_json(BLOCK_MARKER, {"blocked_at": time.time(), "detail": str(detail)[:200]})
    log("⛔ 记录会话配额占满，%.0f 小时内不再尝试登录" % (QUOTA_RETRY_SEC / 3600))


def clear_quota_block():
    if os.path.exists(BLOCK_MARKER):
        try:
            os.remove(BLOCK_MARKER)
        except OSError:
            pass
        log("🔓 会话配额占满标记已清除")


def load_token():
    """取缓存 token；过期或缺失返回 None。"""
    d = read_json(TOKEN_CACHE)
    if not isinstance(d, dict):
        return None
    tok, saved = d.get("access_token"), d.get("saved_at") or 0
    if not tok:
        return None
    if time.time() - saved > TOKEN_TTL_GUESS:
        return None
    return tok


def save_token(tok):
    write_json(TOKEN_CACHE, {"access_token": tok, "saved_at": time.time()})


def login(user, password):
    """登录并缓存 token。返回 (token, 状态)。

    状态取值：
      ok          拿到 token
      quota_full  409 AUTH_SESSION_LIMIT，会话槽位已满，人工干预前无解
      failed      其它失败
    """
    st, d = http_json(GATEWAY + "/api/user/login", "POST",
                      {"username": user, "password": password})
    tok = (d.get("data") or {}).get("access_token") if isinstance(d, dict) else None
    if st == 200 and tok:
        save_token(tok)
        return tok, "ok"
    code = d.get("code") if isinstance(d, dict) else None
    if st == 409 and code == "AUTH_SESSION_LIMIT":
        log("❌ 网关登录被拒: HTTP 409 AUTH_SESSION_LIMIT（会话槽位已满）")
        return None, "quota_full"
    log("❌ 网关登录失败: HTTP %s %s" % (st, str(d)[:120]))
    return None, "failed"


def main():
    try:
        env = load_env()
    except OSError as e:
        log("❌ 读不到 %s: %s" % (ENV_PATH, e))
        return 1
    user = env.get("DOULOR_USER")
    password = env.get("DOULOR_PASS")
    if not user or not password:
        log("❌ env 里缺 DOULOR_USER / DOULOR_PASS")
        return 1

    # 配额占满期内直接跳过，不去打登录接口 —— 打也只会是 409。
    blocked = quota_blocked()
    if blocked:
        left = QUOTA_RETRY_SEC - (time.time() - (blocked.get("blocked_at") or 0))
        if left > 0:
            log("⏸ 跳过本轮：会话配额占满中，%.1f 小时后或人工清除后重试"
                % (left / 3600))
            return 0
        log("⌛ 配额阻塞期已过，重新尝试")

    # 优先用缓存 token：一次登录 = 一条 30 天会话，能省则省。
    tok = load_token()

    def authed(t):
        return {"Authorization": "Bearer " + t}

    if tok:
        st, d = http_json(GATEWAY + "/api/token/?p=0&size=100", headers=authed(tok))
        if not (st == 200 and isinstance(d, dict)):
            log("🔁 缓存 token 已失效（HTTP %s），重新登录" % st)
            tok = None
    if tok is None:
        tok, state = login(user, password)
        if state == "quota_full":
            mark_quota_blocked("login 409 AUTH_SESSION_LIMIT")
            log("⛔ 需人工处理：到 api.doulor.cn 设置页「登录会话」撤销其他会话")
            return 0   # 不是「修复失败」，避免 systemd 把它当故障反复重跑
        if not tok:
            return 1
        st, d = http_json(GATEWAY + "/api/token/?p=0&size=100", headers=authed(tok))
    auth = authed(tok)

    # 走到这里说明鉴权可用，配额标记（若有）已经过时了。
    clear_quota_block()

    items = ((d.get("data") or {}).get("items") or []) if isinstance(d, dict) else []
    if st != 200 or not isinstance(items, list):
        log("❌ 拉取令牌列表失败: HTTP %s" % st)
        return 1

    bad = [t for t in items if (t.get("group") or "") == "auto"]
    if not bad:
        log("✅ 无需修复（共 %d 个令牌，无 auto 分组）" % len(items))
        return 0

    failed = 0
    for t in bad:
        payload = {k: t.get(k) for k in (
            "id", "name", "remain_quota", "unlimited_quota", "expired_time",
            "model_limits_enabled", "model_limits", "allow_ips", "status")}
        payload["group"] = ""  # 空 = 继承账号分组(donation)
        st2, d2 = http_json(GATEWAY + "/api/token/", "PUT", payload, headers=auth)
        if st2 == 200 and isinstance(d2, dict) and d2.get("success"):
            log("🔧 修复令牌 id=%s name=%s：group auto → ''" % (t.get("id"), t.get("name")))
        else:
            failed += 1
            log("❌ 修复令牌 id=%s 失败: HTTP %s %s" % (t.get("id"), st2, str(d2)[:120]))

    st3, d3 = http_json(GATEWAY + "/api/token/?p=0&size=100", headers=auth)
    readback = {}
    if st3 == 200 and isinstance(d3, dict):
        readback = {t.get("id"): t.get("group")
                    for t in ((d3.get("data") or {}).get("items") or [])}
    log("复核: %s" % readback)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
