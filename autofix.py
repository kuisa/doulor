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

    st, d = http_json(GATEWAY + "/api/user/login", "POST",
                      {"username": user, "password": password})
    tok = (d.get("data") or {}).get("access_token") if isinstance(d, dict) else None
    if st != 200 or not tok:
        log("❌ 网关登录失败: HTTP %s %s" % (st, str(d)[:120]))
        return 1
    auth = {"Authorization": "Bearer " + tok}

    st, d = http_json(GATEWAY + "/api/token/?p=0&size=100", headers=auth)
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
