#!/usr/bin/env python3
"""doulor.cloud 每日自动签到

复用 /opt/doulor-proxy/session.json 里反代服务持有的 cookie，不重复登录。
cookie 失效时才回退到 proxy.py 的 Session 重新登录并写回同一文件，
保证与反代服务共用同一份登录态。

签到接口（从 /assets/api-*.js 逆出）：
  GET  /api/checkin   → {checkedIn, today, streak, ...}
  POST /api/checkin   → 成功返回积分；今天已签则 409 ALREADY_CHECKED_IN

上游签到日界按 UTC+8（实测 today=2026-10-22 而服务器 GMT 仍是 21 日 16:05）。

退出码：0 成功/今天已签  1 失败
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

STATE_DIR = os.getenv("DOULOR_STATE_DIR", "/opt/doulor-proxy")
COOKIE_FILE = os.path.join(STATE_DIR, "session.json")
LOG_FILE = os.path.join(STATE_DIR, "checkin.log")
ENV_FILE = os.path.join(STATE_DIR, "env")


def load_env_file():
    """cron 不继承 shell 环境，自己补上 env 文件（回退登录需要账号密码）"""
    try:
        with open(ENV_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass
    except Exception:
        pass


load_env_file()

BASE = os.getenv("DOULOR_BASE", "https://cloud.doulor.cn").rstrip("/")
CHECKIN_URL = f"{BASE}/api/checkin"
POINTS_PAGE = f"{BASE}/dashboard/points"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      " (KHTML, like Gecko) Chrome/125.0.0.0 Iron Safari/537.36")


def log(msg):
    line = f"[{time.strftime('%F %T')}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def local_date():
    """上游按 UTC+8 计算签到日"""
    t = time.gmtime(time.time() + 8 * 3600)
    return time.strftime("%Y-%m-%d", t)


def load_sid():
    try:
        with open(COOKIE_FILE, encoding="utf-8") as f:
            return json.load(f).get("session")
    except Exception:
        return None


def fallback_login():
    """cookie 失效时，复用反代服务的登录逻辑重新拿 session 并写回同一文件"""
    log("⚠️ cookie 失效，回退到 proxy.py 重新登录")
    try:
        sys.path.insert(0, STATE_DIR)
        import proxy  # noqa: E402  （同目录的反代模块）
        sid = proxy.Session().login(force=True)
        if sid:
            log("✅ 重新登录成功，已写回 session.json")
            return sid
        log("❌ 重新登录失败（检查 env 里的 DOULOR_USER / DOULOR_PASS）")
    except Exception as e:
        log(f"❌ 回退登录异常: {type(e).__name__}: {e}")
    return None


def request(sid, method="GET"):
    req = urllib.request.Request(
        CHECKIN_URL, method=method,
        data=b"" if method == "POST" else None,
        headers={
            "Cookie": f"doulor_session={sid}",
            "User-Agent": UA,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": BASE,
            "Referer": POINTS_PAGE,
        })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode() or "null")
        except Exception:
            body = None
        return e.code, body
    except Exception as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}


def main():
    sid = load_sid()
    if not sid:
        sid = fallback_login()
        if not sid:
            log("❌ 没有可用 cookie，且无法登录，放弃")
            return 1

    # 1) 先查状态，今天已签就不必再 POST
    code, st = request(sid, "GET")
    if code in (401, 403):
        sid = fallback_login()
        if not sid:
            return 1
        code, st = request(sid, "GET")
    if code != 200 or not isinstance(st, dict):
        log(f"❌ 查询签到状态失败 HTTP {code}: {st}")
        return 1

    today = st.get("today")
    exp = local_date()
    if today and today != exp:
        # 不阻断，只提示：以服务端 today 为准，便于发现时区漂移
        log(f"⚠️ 日期不一致：服务端 today={today}，本地 UTC+8={exp}（以服务端为准）")

    if st.get("checkedIn"):
        log(f"✅ 今天已签到，无需操作（today={today} streak={st.get('streak')} "
            f"本次得 {st.get('todayPoints')} 分）")
        return 0

    if st.get("enabled") is False:
        log("ℹ️ 上游签到功能当前未开放，跳过")
        return 0

    # 2) 执行签到
    code, res = request(sid, "POST")
    if code in (401, 403):
        sid = fallback_login()
        if not sid:
            return 1
        code, res = request(sid, "POST")

    if code == 200:
        pts = (res or {}).get("total") if isinstance(res, dict) else None
        bonus = (res or {}).get("milestoneHit") if isinstance(res, dict) else None
        extra = f"（里程碑额外 +{bonus.get('points')}）" if isinstance(bonus, dict) else ""
        log(f"🎉 签到成功，当前积分 {pts}{extra}")
        return 0

    if code == 409 or (isinstance(res, dict) and res.get("code") == "ALREADY_CHECKED_IN"):
        log("✅ 今天已经签过到了（并发/已签），视为成功")
        return 0

    log(f"❌ 签到失败 HTTP {code}: {res}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
