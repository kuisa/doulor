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
# 本次签到结果缓存：面板只读它，绝不再向上游查询，避免触发风控
RESULT_FILE = os.path.join(STATE_DIR, "checkin_result.json")


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


def fetch_points(sid):
    """读上游 /api/points，返回余额与签到流水汇总。失败返回 {}。"""
    try:
        req = urllib.request.Request(
            BASE + "/api/points",
            headers={"Cookie": f"doulor_session={sid}", "User-Agent": UA,
                     "Accept": "application/json", "Referer": POINTS_PAGE})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return {}
    tx = d.get("transactions") or []
    ck = [t for t in tx if t.get("reason") == "checkin"]
    return {
        "balance": d.get("balance"),
        "yuanPerPoint": (d.get("config") or {}).get("yuanPerPoint"),
        "checkin_total": sum(int(t.get("delta") or 0) for t in ck),
        "checkin_count": len(ck),
        "checkin_recent": [{"at": t.get("createdAt"), "delta": t.get("delta"),
                            "detail": t.get("detail") or ""} for t in ck[:10]],
    }


def balance_text(p):
    if not p or p.get("balance") is None:
        return "（未能取到余额）"
    return f"{p['balance']} 分" + (f"（1 分抵 {p['yuanPerPoint']} 元）"
                                  if p.get("yuanPerPoint") else "")


def save_result(d):
    """把本次签到结果落盘，供面板读取（面板不再打上游，避免风控）。"""
    d = dict(d, at=time.time(), date=local_date())
    try:
        tmp = RESULT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, RESULT_FILE)
    except Exception as e:
        log(f"⚠️ 写入签到结果缓存失败: {type(e).__name__}: {e}")


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
        pts = fetch_points(sid)          # 唯一一次 /api/points
        log(f"✅ 今天已签到，无需操作（today={today} streak={st.get('streak')} "
            f"本次得 +{st.get('todayPoints')} 分）")
        log(f"💰 账户余额 {balance_text(pts)}")
        save_result({"ok": True, "already": True, "today": today,
                     "streak": st.get("streak"),
                     "todayPoints": st.get("todayPoints"),
                     "next": st.get("next"), **pts})
        return 0

    if st.get("enabled") is False:
        log("ℹ️ 上游签到功能当前未开放，跳过")
        save_result({"ok": True, "skipped": True, "reason": "upstream_disabled",
                     "today": today})
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
        got = fetch_points(sid)          # 唯一一次 /api/points
        log(f"🎉 签到成功，本次获得 +{pts} 分{extra}")
        log(f"💰 账户余额 {balance_text(got)}")
        save_result({"ok": True, "already": False, "today": today,
                     "points": pts, "milestone": bonus, **got})
        return 0

    if code == 409 or (isinstance(res, dict) and res.get("code") == "ALREADY_CHECKED_IN"):
        log("✅ 今天已经签过到了（并发/已签），视为成功")
        got = fetch_points(sid)
        save_result({"ok": True, "already": True, "today": today, **got})
        return 0

    log(f"❌ 签到失败 HTTP {code}: {res}")
    save_result({"ok": False, "today": today, "error": f"HTTP {code}: {res}"})
    return 1


if __name__ == "__main__":
    sys.exit(main())
