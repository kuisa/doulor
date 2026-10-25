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

# 复用反代服务的代理池（账号↔出口绑定），让签到也走该账号的专属出口 IP。
# 直接 import 反代的 open_url（已经过验证的 opener 逻辑），不重复造轮子。
import importlib.util as _ilu

_PROXIES_FILE = os.path.join(os.getenv("DOULOR_STATE_DIR", "/opt/doulor-proxy"),
                             "proxies.json")
_PROXY_MOD = None


def _proxy_open_url():
    global _PROXY_MOD
    if _PROXY_MOD is None:
        spec = _ilu.spec_from_file_location("doulor_proxy_share",
                                            os.path.join(os.getenv("DOULOR_STATE_DIR",
                                            "/opt/doulor-proxy"), "proxy.py"))
        _PROXY_MOD = _ilu.module_from_spec(spec)
        spec.loader.exec_module(_PROXY_MOD)
    return _PROXY_MOD


def _proxy_for(user):
    try:
        with open(_PROXIES_FILE, encoding="utf-8") as f:
            return json.load(f).get("assigned", {}).get(user)
    except Exception:
        return None


def _open_url(req, proxy_url=None, timeout=30):
    """与反代服务一致的出口逻辑：绑了出口就走它，否则直连。绝不降级。"""
    if proxy_url is None:
        return _proxy_open_url().open_url(req, None, timeout=timeout)
    return _proxy_open_url().open_url(req, proxy_url, timeout=timeout)

STATE_DIR = os.getenv("DOULOR_STATE_DIR", "/opt/doulor-proxy")
COOKIE_FILE = os.path.join(STATE_DIR, "session.json")
_CUR_USER = None          # checkin_one 设置的当前账号（供 request() 选出口）
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


def save_result(d, result_file=None):
    """把本次签到结果落盘，供面板读取（面板不再打上游，避免风控）。"""
    d = dict(d, at=time.time(), date=local_date())
    rf = result_file or RESULT_FILE
    try:
        os.makedirs(os.path.dirname(rf) or ".", exist_ok=True)
        tmp = rf + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, rf)
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
        with _open_url(req, _proxy_for(_CUR_USER), timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode() or "null")
        except Exception:
            body = None
        return e.code, body
    except Exception as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}


def load_sid_for(user, cookie_file):
    try:
        with open(cookie_file, encoding="utf-8") as f:
            return json.load(f).get("session")
    except Exception:
        return None


def pool_sessions():
    """遍历账号池（accounts.json + env 账号），返回 [(user, cookie_file, pass)]。"""
    STATE = os.getenv("DOULOR_STATE_DIR", "/opt/doulor-proxy")
    items = []
    env_u = os.getenv("DOULOR_USER", "")
    env_p = os.getenv("DOULOR_PASS", "")
    if env_u and env_p:
        items.append((env_u, os.path.join(STATE, "session.json"), env_p))
    acc_file = os.path.join(STATE, "accounts.json")
    if os.path.isfile(acc_file):
        try:
            with open(acc_file, encoding="utf-8") as f:
                for a in json.load(f):
                    u = str((a or {}).get("user") or "").strip()
                    p = str((a or {}).get("pass") or "").strip()
                    if u and p and u != env_u:
                        items.append((u, os.path.join(STATE, "sessions", u + ".json"), p))
        except Exception as e:
            log(f"⚠️ accounts.json 读取失败: {e}")
    return items


def raw_login(user, password):
    """直接打登录接口拿新 sid（不 import proxy，避免副作用）。"""
    body = json.dumps({"identifier": user, "password": password}).encode()
    req = urllib.request.Request(
        f"{BASE}/api/login", data=body, method="POST",
        headers={"Content-Type": "application/json", "Accept": "*/*",
                 "Origin": BASE, "Referer": f"{BASE}/dashboard/points",
                 "User-Agent": UA})
    try:
        with _open_url(req, _proxy_for(user), timeout=30) as r:
            r.read()
            raw = r.headers.get_all("Set-Cookie") or []
        for sc in raw:
            m = __import__("re").match(r"\s*([^=]+)=([^;]*)", sc)
            if m and m.group(1).strip() == "doulor_session":
                return m.group(2).strip()
    except Exception as e:
        log(f"⚠️ 账号 {user} 登录失败: {e}")
    return None


def main():
    """多账号签到：遍历号池每个账号各签一次。

    单账号时行为与旧版一致（读 session.json → 失败回退登录）。
    结果写 checkin_result.json 时按账号分目录：主账号照旧写根目录
    （面板向下兼容），其余账号写 sessions/<user>.checkin.json。
    """
    items = pool_sessions()
    # --user X：只签这一个账号（面板"添加即签到"用，别顺带把全池重查一遍，
    # 每多查一次就多一分风控风险）
    only = ""
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a == "--user" and i + 1 < len(argv):
            only = argv[i + 1]
        elif a.startswith("--user="):
            only = a.split("=", 1)[1]
    if only:
        items = [it for it in items if it[0] == only]
        if not items:
            log(f"❌ 号池里没有账号 {only}")
            return 1
    if not items:
        log("❌ 没有配置任何账号（env 或 accounts.json）")
        return 1
    rc = 0
    for i, (user, cf, pw) in enumerate(items):
        log(f"👤 [{i+1}/{len(items)}] 账号 {user}")
        r = checkin_one(user, cf, pw,
                        result_file=(RESULT_FILE if i == 0
                                     else cf + ".checkin.json"))
        rc = rc or r
    return rc


def checkin_one(user, cookie_file, password, result_file):
    """单个账号的完整签到流程（原 main 的逻辑）。返回退出码。"""
    global _CUR_USER
    _CUR_USER = user          # request() 据此挑该账号绑定的出口
    _pw = _proxy_for(user)
    if _pw:
        log(f"🌐 账号 {user} 走出口 {_pw.split('@')[-1]}")
    sid = load_sid_for(user, cookie_file)
    if not sid:
        sid = raw_login(user, password)
        if sid:
            try:
                os.makedirs(os.path.dirname(cookie_file) or ".", exist_ok=True)
                with open(cookie_file, "w", encoding="utf-8") as f:
                    json.dump({"session": sid, "user": user,
                               "saved_at": time.time()}, f)
                os.chmod(cookie_file, 0o600)
                log(f"✅ 已写回 {os.path.basename(cookie_file)}")
            except OSError:
                pass
        else:
            log(f"❌ 账号 {user} 无 cookie 且登录失败，跳过")
            return 1

    # 1) 先查状态，今天已签就不必再 POST
    code, st = request(sid, "GET")
    if code in (401, 403):
        sid = raw_login(user, password)
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
        save_result({"ok": True, "already": True, "today": today, "user": user,
                     "streak": st.get("streak"),
                     "todayPoints": st.get("todayPoints"),
                     "next": st.get("next"), **pts}, result_file)
        return 0

    if st.get("enabled") is False:
        log("ℹ️ 上游签到功能当前未开放，跳过")
        save_result({"ok": True, "skipped": True, "reason": "upstream_disabled",
                     "today": today, "user": user}, result_file)
        return 0

    # 2) 执行签到
    code, res = request(sid, "POST")
    if code in (401, 403):
        sid = raw_login(user, password)
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
        save_result({"ok": True, "already": False, "today": today, "user": user,
                     "points": pts, "milestone": bonus, **got}, result_file)
        return 0

    if code == 409 or (isinstance(res, dict) and res.get("code") == "ALREADY_CHECKED_IN"):
        log("✅ 今天已经签过到了（并发/已签），视为成功")
        got = fetch_points(sid)
        save_result({"ok": True, "already": True, "today": today, "user": user,
                     **got}, result_file)
        return 0

    log(f"❌ 签到失败 HTTP {code}: {res}")
    save_result({"ok": False, "today": today, "user": user,
                 "error": f"HTTP {code}: {res}"}, result_file)
    return 1


if __name__ == "__main__":
    sys.exit(main())
