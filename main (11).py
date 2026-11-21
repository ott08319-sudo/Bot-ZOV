import os
import time
import uuid
import asyncio
import logging
import json
import random
import aiohttp
import asyncpg
from aiohttp import web
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties

try:
    from flyerapi import Flyer, APIError as FlyerAPIError
    FLYER_AVAILABLE = True
except ImportError:
    FLYER_AVAILABLE = False
    FlyerAPIError = Exception

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0") or "0")
PORT            = int(os.getenv("PORT", "10000"))
DATABASE_URL    = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY    = os.getenv("FLYER_API_KEY", "")
FLOWLY_API_KEY   = os.getenv("FLOWLY_API_KEY", "")
BOTOHUB_API_KEY  = os.getenv("BOTOHUB_API_KEY", "")
MGRAM_API_KEY    = os.getenv("MGRAM_API_KEY", "")
PULSAR_API_KEY   = os.getenv("PULSAR_API_KEY", "")
PUSHER_API_KEY   = os.getenv("PUSHER_API_KEY", "")
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")

PULSAR_PLATFORM_ID = "plt_2f01cf25adf0b578b6a6ce68be7d4599"

flyer = Flyer(FLYER_API_KEY) if (FLYER_AVAILABLE and FLYER_API_KEY) else None

# ========== КОНСТАНТЫ ==========
CHECK_COOLDOWN       = 5
DEFAULT_REWARD       = 0.005
DEFAULT_REF_BONUS    = 0.01
DEFAULT_MAX_SPONSORS = 100
MAX_MAX_SPONSORS     = 100
DEFAULT_MIN_WITHDRAW = 0.5
AUTOCHECK_INTERVAL   = 3600
LAZY_THRESHOLD       = 5
LAZY_INTERVAL        = 300
FLOWLY_TIMEOUT       = 4
INCOMPLETE_STATUSES  = ('incomplete', 'abort')

# ========== СЕРВИСЫ ==========
SERVICES_ALL = ["piarflow", "flowly", "flyer", "mgram", "pulsar", "pusher", "trafsly", "botohub"]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http = None
_pool = None
_last = {}
_states = {}


async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    return _http


def escape_html(text):
    if not text: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_money(value):
    s = f"{value:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def clean_db_url(url):
    if not url: return url
    url = url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("sslmode", None)
    qs.pop("channel_binding", None)
    if parsed.hostname and "neon.tech" in parsed.hostname:
        qs["ssl"] = ["true"]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=10)
    return _pool


def user_display(user_id, username=None):
    if username: return f"@{escape_html(username)} (<code>{user_id}</code>)"
    return f"<code>{user_id}</code>"


async def get_user_display(user_id):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        if row and row["username"]:
            return f"@{escape_html(row['username'])} (<code>{user_id}</code>)"
    except Exception: pass
    return f"<code>{user_id}</code>"


async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1", service, error_type, time.time() - 300)
            if recent: return
            await db.execute("INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)", service, error_type, message[:500], time.time())
    except Exception as e: logging.error(f"log_error failed: {e}")


async def notify_admin(text):
    try: await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception: pass


async def notify_admin_new_subs(user_id, subs_list):
    if not ADMIN_ID or not subs_list: return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        username = row["username"] if row else None
        user_str = user_display(user_id, username)
        total = sum(item[2] for item in subs_list)
        by_service = {}
        for service, aid, reward, link, task_type in subs_list:
            by_service.setdefault(service, [])
            by_service[service].append((aid, reward, link, task_type))
        blocks = []
        for service, items in by_service.items():
            cnt = len(items)
            sm = sum(r for _, r, _, _ in items)
            lines = [f"💵 <b>{service}</b> ({cnt} шт, +{fmt_money(sm)}$)"]
            for aid, reward, link, tt in items:
                short_link = str(link) if link else str(aid)
                if len(short_link) > 60: short_link = short_link[:57] + "…"
                lines.append(f"  • <a href=\"{short_link}\">{short_link}</a>")
            blocks.append("\n".join(lines))
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        text = f"🆕 <b>Новые подписки!</b>\n\n👤 <b>{user_str}</b>\n\n📦 <b>{len(subs_list)}</b> заданий выполнено:\n\n" + "\n\n".join(blocks) + f"\n\n💵 Итого: <b>+{fmt_money(total)}$</b>\n🕐 {ts}"
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e: logging.error(f"notify_admin_new_subs: {e}")


async def notify_admin_new_user(user_id, username, full_name, sponsors_count, services_map, ref_id=None):
    if not ADMIN_ID: return
    try:
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        svc_lines = "\n".join(f"  • <b>{s}</b> — {c}" for s, c in services_map.items()) if services_map else "  • нет доступных сервисов"
        ref_line = ""
        if ref_id: ref_line = f"\n🔗 Реферер: {await get_user_display(ref_id)}"
        user_str = user_display(user_id, username.replace("@", "") if username else None)
        text = f"👤 <b>Новый пользователь!</b>\n\n🆔 {user_str}\n📛 <b>{escape_html(full_name) or 'без имени'}</b>{ref_line}\n\n📦 Выдано спонсоров: <b>{sponsors_count}</b>\n🎯 По сервисам:\n{svc_lines}\n\n🕐 {ts}"
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e: logging.error(f"notify_admin_new_user: {e}")


async def notify_referrer(referrer_id, ref_username, subs_count, earned):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", referrer_id)
        bal = row["balance"] if row else 0
        text = f"🎉 <b>Твой реферал подписался!</b>\n\n👤 Реферал: @{escape_html(ref_username or 'без ника')}\n✅ Подписок: <b>{subs_count}</b>\n💎 Ты получаешь: <b>+{fmt_money(earned)}$</b>\n💰 Твой баланс: <b>{fmt_money(bal)}$</b>"
        await bot.send_message(referrer_id, text, disable_web_page_preview=True)
    except Exception: pass


LAZY_USER_MSGS = [
    "😡 <b>Эй, ты чего?!</b>\n\nТы пришёл заработать 💵, а сам ничего не сделал?\n\n🔥 Жми «💵 Получить», подпишись на всех и нажми «✅ Я подписался — проверить».\n\n⚠️ <i>Пока не подпишешься на 5+ каналов — ты НЕ получишь деньги.</i>",
    "😤 <b>Слушай, так не работает!</b>\n\nТы нажал /start и думаешь, что 💵 упадут сами?\n\n💎 Зайди в «💵 Получить», подпишись на спонсоров, проверь — и получишь первые деньги.\n\n🚀 <i>Хватит лениться!</i>",
    "😠 <b>Ты серьёзно?</b>\n\nДругие уже зарабатывают 💵, а ты не подписался ни на одного.\n\n⚠️ Жми «💵 Получить» → подпишись → «✅ Проверить».\n\n💰 <i>Минимум 5 подписок.</i>",
    "🔥 <b>Проснись!</b>\n\nТы зашёл в бота и ничего не делаешь.\n\n💎 Жми «💵 Получить», подписывайся, проверяй — и деньги твои.\n\n🚀 <i>Ты можешь больше!</i>",
]


async def notify_lazy_user(user_id):
    try:
        await bot.send_message(user_id, random.choice(LAZY_USER_MSGS), disable_web_page_preview=True)
        return True
    except Exception: return False


async def notify_admin_lazy(user_id, subs, mins):
    try: await bot.send_message(ADMIN_ID, f"😴 <b>Ленивый юзер</b>\n\n👤 {await get_user_display(user_id)}\n📊 Подписок: <b>{subs}</b>/{LAZY_THRESHOLD}\n⏱ Прошло: {mins} мин", disable_web_page_preview=True)
    except Exception: pass


# ========== БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, balance DOUBLE PRECISION DEFAULT 0,
            held DOUBLE PRECISION DEFAULT 0, referrer_id BIGINT, referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE, created_at DOUBLE PRECISION)""")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_earned DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS held DOUBLE PRECISION DEFAULT 0")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY, user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, created_at DOUBLE PRECISION, UNIQUE(user_id, service, assignment_id))""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS session_id TEXT")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS provider_pool TEXT")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, wallet TEXT,
            status TEXT DEFAULT 'pending', created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, amount DOUBLE PRECISION, max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0, active BOOLEAN DEFAULT TRUE, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT, code TEXT, used_at DOUBLE PRECISION, PRIMARY KEY (user_id, code))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY, service TEXT, error_type TEXT, message TEXT, created_at DOUBLE PRECISION)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY, referrer_id BIGINT, created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0, admin_notified BOOLEAN DEFAULT FALSE)""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_user_status ON sponsor_tasks (user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_service ON sponsor_tasks (service)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users (balance DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wd_status ON withdrawals (status)")
        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
        await db.execute("INSERT INTO settings (key,value) VALUES ('ref_bonus',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REF_BONUS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('min_withdraw',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MIN_WITHDRAW))
        await db.execute("INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MAX_SPONSORS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('autocheck','true') ON CONFLICT (key) DO NOTHING")


async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value))


async def get_reward(): return float(await get_setting("reward", DEFAULT_REWARD))
async def get_ref_bonus(): return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))
async def get_min_withdraw(): return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))
async def get_max_sponsors(): return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def is_service_enabled(service: str) -> bool:
    return (await get_setting(f"service_{service}", "true")) == "true"


async def set_service_enabled(service: str, enabled: bool):
    await set_setting(f"service_{service}", "true" if enabled else "false")


async def get_enabled_services() -> set:
    result = set()
    for s in SERVICES_ALL:
        if await is_service_enabled(s):
            result.add(s)
    return result


async def register_user(user_id, referrer_id=None, username=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username: await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            return False
        if referrer_id == user_id: referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r: referrer_id = None
        await db.execute("INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1,$2,$3,$4)", user_id, username, referrer_id, time.time())
        return True


async def update_username(user_id, username):
    if not username: return
    try:
        pool = await get_pool()
        async with pool.acquire() as db: await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
    except Exception: pass


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT balance, referrer_id, referred_earned, blocked, username FROM users WHERE user_id=$1", user_id)


async def find_user(query):
    query = query.strip()
    if query.isdigit():
        pool = await get_pool()
        async with pool.acquire() as db: row = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", int(query))
        return row["user_id"] if row else None
    clean = query.lstrip("@").lower()
    if not clean: return None
    pool = await get_pool()
    async with pool.acquire() as db: row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1 LIMIT 1", clean)
    return row["user_id"] if row else None


async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)


async def add_referred_earned(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", amount, user_id)


async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None, session_id=None, provider_pool=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, session_id, provider_pool, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'unsubscribed',$10)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type,
            str(session_id) if session_id is not None else None,
            str(provider_pool) if provider_pool is not None else None,
            time.time())


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)


async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""SELECT service, assignment_id, link, reward, signature, task_type, session_id, provider_pool
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'""", user_id)
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"], r["session_id"], r["provider_pool"]) for r in rows]


async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return row["c"]


async def pending_links(user_id, limit=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        q = "SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' ORDER BY id ASC"
        if limit: q += f" LIMIT {int(limit)}"
        rows = await db.fetch(q, user_id)
        return [r["link"] for r in rows]


async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' GROUP BY service", user_id)
        return {r["service"]: r["c"] for r in rows}


async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id)
        return row["c"]


async def add_lazy_watch(user_id, referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO lazy_watch (user_id, referrer_id, created_at, last_notified, admin_notified) VALUES ($1,$2,$3,0,FALSE) ON CONFLICT (user_id) DO NOTHING", user_id, referrer_id, time.time())


async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)
# ========== API: PIARFLOW ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("piarflow", "fetch", str(e))
    return []


async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("piarflow", "check", str(e))
    return []


# ========== API: FLOWLY ==========
async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY: return None
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers",
                headers={"Auth": FLOWLY_API_KEY},
                json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)}) as r:
                if r.status != 200: return None
                return await r.json()
    except Exception: return None


async def get_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return []
    if d.get("status") == "ok": return []
    out = []
    for item in d.get("offers", []):
        if item.get("subscribed"): continue
        link = item.get("link")
        if link: out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out


async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d: return None, []
    return d.get("status"), d.get("offers", [])


# ========== API: FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks: return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if not links: continue
            out.append({"id": t.get('signature'), "link": links[0], "signature": t.get('signature'), "task": t.get('task', '')})
        return out
    except Exception as e: await log_error("flyer", "get_tasks", str(e))
    return []


async def check_flyer(signature):
    if not flyer or not signature: return None
    try: return await flyer.check_task(signature=signature)
    except Exception as e: await log_error("flyer", "check", str(e))
    return None


# ========== API: MGRAM ==========
async def get_mgram(user_id):
    if not MGRAM_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://api.mgram.biz/tasks",
            json={"user_id": int(user_id), "limit": min(await get_max_sponsors(), 10)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            data = d.get("data") or {}
            if data.get("skip"): return []
            out = []
            for t in data.get("tasks") or []:
                if t.get("status") == "completed": continue
                link = t.get("link"); aid = t.get("id")
                if link and aid is not None: out.append({"id": aid, "link": link})
            return out
    except Exception as e: await log_error("mgram", "fetch", str(e))
    return []


async def check_mgram(user_id):
    if not MGRAM_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://api.mgram.biz/tasks/check",
            json={"user_id": int(user_id)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return False
            d = await r.json()
            return bool((d.get("data") or {}).get("skip"))
    except Exception as e: await log_error("mgram", "check", str(e))
    return False


# ========== API: PULSAR ==========
async def get_pulsar(user_id):
    if not PULSAR_API_KEY: return []
    s = await http()
    try:
        async with s.get("https://pulsarx.sale/api/v1/op/sponsors",
            params={"platform_id": PULSAR_PLATFORM_ID, "telegram_user_id": int(user_id), "language_code": "ru"},
            headers={"X-Api-Key": PULSAR_API_KEY}) as r:
            if r.status != 200: return []
            d = await r.json()
            if d.get("no_sponsors"): return []
            out = []
            for x in d.get("sponsors") or []:
                link = x.get("link"); aid = x.get("id") or x.get("channel_id") or link
                if link: out.append({"id": aid, "link": link})
            return out
    except Exception as e: await log_error("pulsar", "fetch", str(e))
    return []


async def check_pulsar(user_id):
    if not PULSAR_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://pulsarx.sale/api/v1/op/check-subscriptions",
            json={"platform_id": PULSAR_PLATFORM_ID, "api_key": PULSAR_API_KEY, "telegram_user_id": int(user_id), "language_code": "ru"}) as r:
            if r.status != 200: return False
            d = await r.json()
            if d.get("no_sponsors"): return True
            return bool(d.get("all_subscribed"))
    except Exception as e: await log_error("pulsar", "check", str(e))
    return False


# ========== API: PUSHER ==========
async def get_pusher(user):
    if not PUSHER_API_KEY: return []
    s = await http()
    try:
        payload = {
            "user_id": user.id, "chat_id": user.id,
            "username": user.username or "",
            "first_name": getattr(user, "first_name", "") or "",
            "last_name": getattr(user, "last_name", "") or "",
            "language_code": user.language_code or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
            "max_sponsors": min(await get_max_sponsors(), 10),
        }
        async with s.post("https://pusher.lol/api/v1/sponsors",
            json=payload, headers={"Auth": PUSHER_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            if d.get("status") != "ok": return []
            session_id = d.get("session_id")
            out = []
            for x in d.get("sponsors") or []:
                link = x.get("link"); aid = x.get("id")
                if link and aid: out.append({"id": aid, "link": link, "session_id": session_id})
            return out
    except Exception as e: await log_error("pusher", "fetch", str(e))
    return []


async def check_pusher(user_id, session_id):
    if not PUSHER_API_KEY or not session_id: return False
    s = await http()
    try:
        async with s.post("https://pusher.lol/api/v1/check",
            json={"user_id": int(user_id), "chat_id": int(user_id), "session_id": int(session_id)},
            headers={"Auth": PUSHER_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return False
            d = await r.json()
            return d.get("status") == "ok"
    except Exception as e: await log_error("pusher", "check", str(e))
    return False


# ========== API: TRAFSLY ==========
async def get_trafsly(user):
    if not TRAFSLY_API_KEY: return []
    s = await http()
    try:
        payload = {
            "user_id": user.id,
            "first_name": getattr(user, "first_name", "") or "",
            "username": user.username or "",
            "language_code": user.language_code or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
        }
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors",
            json=payload, headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            out = []
            for x in d.get("sponsors") or []:
                if x.get("status") in ("subscribed", "notgetted"): continue
                link = x.get("link"); aid = x.get("ads_id")
                if link and aid is not None:
                    out.append({"id": str(aid), "link": link, "source": x.get("source"), "provider_pool": x.get("provider_pool")})
            return out
    except Exception as e: await log_error("trafsly", "fetch", str(e))
    return []


async def check_trafsly(user_id, ads_id, source=None, provider_pool=None):
    if not TRAFSLY_API_KEY or not ads_id: return False
    s = await http()
    try:
        payload = {"user_id": int(user_id), "ads_id": ads_id}
        if source: payload["source"] = source
        if provider_pool: payload["provider_pool"] = provider_pool
        async with s.post("https://api.trafsly.com/api/v1/confirm-subscription",
            json=payload, headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return False
            d = await r.json()
            return d.get("subscribed") is True
    except Exception as e: await log_error("trafsly", "check", str(e))
    return False


# ========== API: BOTOHUB ==========
async def get_botohub(user_id):
    if not BOTOHUB_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id}, headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            if not d.get("skip") and not d.get("completed"):
                for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
                    v = d.get(field)
                    if isinstance(v, list): return v
    except Exception as e: await log_error("botohub", "fetch", str(e))
    return []


async def check_botohub(user_id):
    if not BOTOHUB_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id}, headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            return bool(d.get("completed"))
    except Exception as e: await log_error("botohub", "check", str(e))
    return False


# ========== СБОР (с учётом выключенных) ==========
async def collect_sponsors(user):
    uid = user.id
    lang = user.language_code or "ru"
    reward = await get_reward()
    enabled = await get_enabled_services()

    async def _call(name, coro):
        if name in enabled:
            return await coro
        return []

    results = await asyncio.gather(
        _call("piarflow", get_piarflow(uid)),
        _call("flowly", get_flowly(uid, lang)),
        _call("flyer", fetch_flyer(uid, lang)),
        _call("mgram", get_mgram(uid)),
        _call("pulsar", get_pulsar(uid)),
        _call("pusher", get_pusher(user)),
        _call("trafsly", get_trafsly(user)),
        _call("botohub", get_botohub(uid)),
        return_exceptions=True,
    )

    piarflow, flowly, flyer_, mgram, pulsar, pusher, trafsly, botohub = results

    if isinstance(piarflow, list):
        for x in piarflow:
            if x.get("link"): await save_sponsor(uid, "piarflow", x["link"], x["link"], reward)
    if isinstance(flowly, list):
        for x in flowly:
            if x.get("link"): await save_sponsor(uid, "flowly", x["link"], x["link"], reward, task_type=x.get("name"))
    if isinstance(flyer_, list):
        for x in flyer_:
            if x.get("link") and x.get("id"):
                await save_sponsor(uid, "flyer", str(x["id"]), x["link"], reward, signature=x.get("signature"), task_type=x.get("task"))
    if isinstance(mgram, list):
        for x in mgram:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "mgram", str(x["id"]), x["link"], reward)
    if isinstance(pulsar, list):
        for x in pulsar:
            if x.get("link"):
                await save_sponsor(uid, "pulsar", str(x.get("id")) if x.get("id") else x["link"], x["link"], reward)
    if isinstance(pusher, list):
        for x in pusher:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "pusher", str(x["id"]), x["link"], reward, session_id=x.get("session_id"))
    if isinstance(trafsly, list):
        for x in trafsly:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "trafsly", str(x["id"]), x["link"], reward, signature=x.get("source"), provider_pool=x.get("provider_pool"))
    if isinstance(botohub, list):
        for t in botohub:
            link = None; aid = None
            if isinstance(t, str): link, aid = t, t
            elif isinstance(t, dict):
                link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
                aid = t.get("id") or t.get("task_id") or t.get("assignment_id") or link
            if link: await save_sponsor(uid, "botohub", str(aid) if aid else link, link, reward)


# ========== ПРОВЕРКА (с учётом выключенных) ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, aid)
        return row["reward"] if row else 0.0


async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0, 0

    enabled = await get_enabled_services()

    pf, fl, mg, pu, ps, tr, bh = [], [], [], [], [], [], []
    flowly_pending = []
    link_map, task_type_map = {}, {}
    for service, aid, link, reward, sig, task_type, sess, pool_ in tasks:
        if service not in enabled: continue
        link_map[aid] = link
        task_type_map[aid] = task_type
        if service == "piarflow" and link: pf.append(link)
        elif service == "flyer" and sig: fl.append((aid, sig))
        elif service == "mgram" and aid: mg.append(aid)
        elif service == "pulsar" and aid: pu.append(aid)
        elif service == "pusher" and sess: ps.append((aid, sess))
        elif service == "trafsly" and aid: tr.append((aid, sig, pool_))
        elif service == "botohub" and aid: bh.append(aid)
        elif service == "flowly": flowly_pending.append((service, aid, link, reward, sig, task_type))

    done_count, done_sum, skipped_count, new_subs = 0, 0.0, 0, []

    if pf:
        for r in await check_piarflow(user_id, pf):
            if r.get("status") == "subscribed":
                await mark_subscribed(user_id, "piarflow", r.get("link"))
                rw = await _task_reward(user_id, "piarflow", r.get("link"))
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("piarflow", r.get("link"), rw, r.get("link"), None))
            elif r.get("status") == "not_counted": skipped_count += 1

    if mg:
        if await check_mgram(user_id):
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='mgram' AND status!='subscribed'", user_id)
                total_rw = sum(r["reward"] for r in rows)
                await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='mgram'", user_id)
            for r in rows:
                done_count += 1; done_sum += r["reward"]
                new_subs.append(("mgram", r["assignment_id"], r["reward"], link_map.get(r["assignment_id"]), None))
            if total_rw > 0: await add_balance(user_id, total_rw)

    if pu:
        if await check_pulsar(user_id):
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='pulsar' AND status!='subscribed'", user_id)
                total_rw = sum(r["reward"] for r in rows)
                await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='pulsar'", user_id)
            for r in rows:
                done_count += 1; done_sum += r["reward"]
                new_subs.append(("pulsar", r["assignment_id"], r["reward"], link_map.get(r["assignment_id"]), None))
            if total_rw > 0: await add_balance(user_id, total_rw)

    if ps:
        sessions = {}
        for aid, sid in ps: sessions.setdefault(sid, []).append(aid)
        for sid, aids in sessions.items():
            if await check_pusher(user_id, sid):
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='pusher' AND status!='subscribed'", user_id)
                    total_rw = sum(r["reward"] for r in rows)
                    await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='pusher'", user_id)
                for r in rows:
                    done_count += 1; done_sum += r["reward"]
                    new_subs.append(("pusher", r["assignment_id"], r["reward"], link_map.get(r["assignment_id"]), None))
                if total_rw > 0: await add_balance(user_id, total_rw)
                break

    if tr:
        for aid, src, pp in tr:
            if await check_trafsly(user_id, aid, src, pp):
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("trafsly", aid, rw, link_map.get(aid), None))

    if bh:
        if await check_botohub(user_id):
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT assignment_id, reward FROM sponsor_tasks WHERE user_id=$1 AND service='botohub' AND status!='subscribed'", user_id)
                total_rw = sum(r["reward"] for r in rows)
                await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='botohub'", user_id)
            for r in rows:
                done_count += 1; done_sum += r["reward"]
                new_subs.append(("botohub", r["assignment_id"], r["reward"], link_map.get(r["assignment_id"]), None))
            if total_rw > 0: await add_balance(user_id, total_rw)

    for aid, sig in fl:
        if await check_flyer(sig) == "complete":
            await mark_subscribed(user_id, "flyer", aid)
            rw = await _task_reward(user_id, "flyer", aid)
            await add_balance(user_id, rw); done_count += 1; done_sum += rw
            new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    if flowly_pending:
        status, _ = await check_flowly(user_id)
        if status == "ok":
            total_rw = 0.0
            for service, aid, link, reward, sig, tt in flowly_pending:
                await mark_subscribed(user_id, "flowly", aid)
                rw = await _task_reward(user_id, "flowly", aid)
                total_rw += rw; done_count += 1; done_sum += rw
                new_subs.append(("flowly", aid, rw, link, tt))
            if total_rw > 0: await add_balance(user_id, total_rw)

    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_total = (await get_ref_bonus()) * done_count
            if ref_total > 0:
                await add_balance(row["referrer_id"], ref_total)
                await add_referred_earned(row["referrer_id"], ref_total)
                await notify_referrer(row["referrer_id"], row["username"] or "", done_count, ref_total)

    if new_subs: await notify_admin_new_subs(user_id, new_subs)
    return done_count, done_sum, skipped_count


# ========== ВОРКЕРЫ ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            if await get_setting("autocheck", "true") == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch("SELECT user_id, username FROM users WHERE blocked = FALSE AND created_at > $1 LIMIT 500", time.time() - 86400)
                for r in rows:
                    uid = r["user_id"]
                    try:
                        try:
                            tg = await bot.get_chat(uid)
                            if tg.username: await update_username(uid, tg.username)
                        except Exception: pass
                        before = await pending_count(uid)
                        user = types.User(id=uid, is_bot=False, first_name="", language_code="ru", username=r["username"] or "")
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            try: await bot.send_message(uid, f"🔔 <b>Новые задания ждут тебя!</b>\n\n💵 Доступно: <b>{after - before}</b>\nНажми «💵 Получить», чтобы забрать деньги.")
                            except Exception: pass
                    except Exception as e: logging.error(f"autocheck {uid}: {e}")
                    await asyncio.sleep(0.5)
            await asyncio.sleep(AUTOCHECK_INTERVAL)
        except Exception as e:
            logging.error(f"autocheck_worker: {e}")
            await asyncio.sleep(600)


async def lazy_worker():
    await asyncio.sleep(60)
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT user_id, referrer_id, created_at, last_notified, admin_notified FROM lazy_watch")
            for r in rows:
                uid = r["user_id"]
                try:
                    total = await count_total_subs(uid)
                    if total >= LAZY_THRESHOLD:
                        await remove_lazy_watch(uid); continue
                    if await pending_count(uid) == 0: continue
                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL: continue
                    if await notify_lazy_user(uid):
                        pool2 = await get_pool()
                        async with pool2.acquire() as db:
                            await db.execute("UPDATE lazy_watch SET last_notified=$1 WHERE user_id=$2", time.time(), uid)
                        if not r["admin_notified"]:
                            mins = int((time.time() - (r["created_at"] or time.time())) / 60)
                            await notify_admin_lazy(uid, total, mins)
                            pool3 = await get_pool()
                            async with pool3.acquire() as db:
                                await db.execute("UPDATE lazy_watch SET admin_notified=TRUE WHERE user_id=$1", uid)
                except Exception as e: logging.error(f"lazy check {uid}: {e}")
                await asyncio.sleep(0.3)
            await asyncio.sleep(LAZY_INTERVAL)
        except Exception as e:
            logging.error(f"lazy_worker: {e}")
            await asyncio.sleep(120)


# ========== ДИАГНОСТИКА ==========
async def diagnose_service(name, key, checker):
    if not key: return f"❌ <b>{name}</b> — ключ не задан"
    t0 = time.time()
    try:
        res = await checker()
        dt = int((time.time() - t0) * 1000)
        if res.get("error"): return f"❌ <b>{name}</b> — {res['error']} ({dt}мс)"
        cnt, extra = res.get("count", 0), res.get("extra", "")
        if cnt > 0: return f"✅ <b>{name}</b> — {cnt} заданий ({dt}мс){extra}"
        return f"🟡 <b>{name}</b> — 0 заданий ({dt}мс){extra}"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {escape_html(str(e)[:100])} ({dt}мс)"


async def _diag_piarflow():
    s = await http()
    async with s.post("https://piarflow.com/v1/sponsors", json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "max_sponsors": 5}, headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
        body = await r.text()
        if r.status == 404: return {"count": 0, "extra": " (нет офферов)"}
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") == "ok": return {"count": len(d.get("sponsors", []))}
        return {"error": d.get("message", "unknown"), "count": 0}


async def _diag_mgram():
    if not MGRAM_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post("https://api.mgram.biz/tasks", json={"user_id": ADMIN_ID, "limit": 3}, headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        data = d.get("data") or {}
        if data.get("skip"): return {"count": 0, "extra": " (всё выполнено)"}
        return {"count": len([t for t in data.get("tasks") or [] if t.get("status") != "completed"])}


async def _diag_flowly():
    if not FLOWLY_API_KEY: return {"error": "ключ не задан", "count": 0}
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers", headers={"Auth": FLOWLY_API_KEY}, json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False}) as r:
                if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
                d = await r.json()
                if d.get("status") == "ok": return {"count": 0, "extra": " (всё выполнено)"}
                return {"count": sum(1 for o in d.get("offers", []) if not o.get("subscribed"))}
    except asyncio.TimeoutError: return {"error": "timeout", "count": 0}


async def _diag_flyer():
    if not flyer: return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks: return {"count": 0}
    return {"count": len([t for t in tasks if t.get('status') in INCOMPLETE_STATUSES])}


async def _diag_pulsar():
    if not PULSAR_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.get("https://pulsarx.sale/api/v1/op/sponsors", params={"platform_id": PULSAR_PLATFORM_ID, "telegram_user_id": ADMIN_ID, "language_code": "ru"}, headers={"X-Api-Key": PULSAR_API_KEY}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("no_sponsors"): return {"count": 0, "extra": " (нет)"}
        return {"count": len(d.get("sponsors", []))}


async def _diag_pusher():
    if not PUSHER_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post("https://pusher.lol/api/v1/sponsors", json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "language_code": "ru", "max_sponsors": 3}, headers={"Auth": PUSHER_API_KEY, "Content-Type": "application/json"}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") != "ok": return {"error": f"status={d.get('status')}", "count": 0}
        return {"count": len(d.get("sponsors", []))}


async def _diag_trafsly():
    if not TRAFSLY_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post("https://api.trafsly.com/api/v1/get-sponsors", json={"user_id": ADMIN_ID, "language_code": "ru"}, headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        return {"count": len(d.get("sponsors") or [])}


async def _diag_botohub():
    if not BOTOHUB_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post("https://botohub.me/get-tasks", json={"chat_id": ADMIN_ID}, headers={"Auth": BOTOHUB_API_KEY}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("skip") or d.get("completed"): return {"count": 0, "extra": " (всё выполнено)"}
        for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
            v = d.get(field)
            if isinstance(v, list): return {"count": len(v)}
        return {"error": "массив не найден", "count": 0}


async def run_diagnostics():
    enabled = await get_enabled_services()
    checks = []
    mapping = [
        ("Piarflow", "piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        ("Flowly", "flowly", FLOWLY_API_KEY, _diag_flowly),
        ("Flyer", "flyer", FLYER_API_KEY, _diag_flyer),
        ("MGRAM", "mgram", MGRAM_API_KEY, _diag_mgram),
        ("Pulsar", "pulsar", PULSAR_API_KEY, _diag_pulsar),
        ("Pusher", "pusher", PUSHER_API_KEY, _diag_pusher),
        ("Trafsly", "trafsly", TRAFSLY_API_KEY, _diag_trafsly),
        ("Botohub", "botohub", BOTOHUB_API_KEY, _diag_botohub),
    ]
    for name, key, api, fn in mapping:
        if key not in enabled:
            checks.append(f"⏸ <b>{name}</b> — выключен")
        else:
            checks.append(await diagnose_service(name, api, fn))
    return f"🔎 <b>Диагностика сервисов</b>\n🆔 user_id: <code>{ADMIN_ID}</code>\n\n" + "\n".join(checks)


async def get_recent_errors(hours=3):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT service, error_type, message, created_at FROM error_log WHERE created_at > $1 ORDER BY created_at DESC LIMIT 30", time.time() - hours * 3600)


async def get_top_by_balance(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT user_id, username, balance FROM users WHERE blocked = FALSE ORDER BY balance DESC LIMIT $1", limit)


async def get_top_by_refs(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch("SELECT u.user_id, u.username, (SELECT COUNT(*) FROM users r WHERE r.referrer_id = u.user_id) AS refs_count FROM users u WHERE u.blocked = FALSE ORDER BY refs_count DESC, u.user_id ASC LIMIT $1", limit)


# ========== КЛАВИАТУРЫ ==========
def user_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💵 Получить"), KeyboardButton(text="💰 Мой баланс")],
        [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="🎁 Промокод")],
    ], resize_keyboard=True, is_persistent=True)


def admin_reply_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💵 Получить"), KeyboardButton(text="💰 Мой баланс")],
        [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="🛠 Админка")],
    ], resize_keyboard=True, is_persistent=True)


def get_menu(user_id): return admin_reply_menu() if user_id == ADMIN_ID else user_menu()


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1): kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2: rows.append(1)
    rows.append(2); kb.adjust(*rows)
    return kb.as_markup()


def promo_kb():
    kb = InlineKeyboardBuilder(); kb.button(text="⬅️ Назад", callback_data="promo_cancel"); return kb.as_markup()


def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Награда", callback_data="adm_reward")
    kb.button(text="👥 Макс спонсоров", callback_data="adm_max")
    kb.button(text="💸 Заявки", callback_data="adm_withdraws")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🏆 Топ игроков", callback_data="adm_top")
    kb.button(text="🔎 Найти юзера", callback_data="adm_find")
    kb.button(text="💎 Начислить", callback_data="adm_give")
    kb.button(text="🎁 Создать промокод", callback_data="adm_promo_create")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Мин. вывод", callback_data="adm_minw")
    kb.button(text="👥 Реф-бонус", callback_data="adm_refb")
    kb.button(text="📊 Диагностика", callback_data="adm_diag")
    kb.button(text="🔌 Сервисы", callback_data="adm_services")
    kb.adjust(3, 3, 3, 3, 1)
    return kb.as_markup()


def services_menu_kb(enabled_map):
    kb = InlineKeyboardBuilder()
    for s in SERVICES_ALL:
        emoji = "✅" if enabled_map.get(s) else "❌"
        kb.button(text=f"{emoji} {s}", callback_data=f"svc_toggle_{s}")
    kb.button(text="⬅️ Назад", callback_data="svc_back")
    kb.adjust(2, 2, 2, 2, 1)
    return kb.as_markup()


def diag_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверить сервисы", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки за 3ч", callback_data="diag_errors")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1); return kb.as_markup()


def top_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 По балансу", callback_data="top_balance")
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1); return kb.as_markup()


def wd_kb(wid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"wd_done_{wid}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
    kb.adjust(2); return kb.as_markup()


# ========== ВЫДАЧА ==========
async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    if clean: await clear_pending_tasks(uid)
    await collect_sponsors(user)
    all_links = await pending_links(uid, limit=await get_max_sponsors())
    if not all_links:
        return False, "😕 <b>Заданий пока нет.</b>\n\nЗагляни через 15-20 минут — мы добавляем новых спонсоров 💵"
    reward = await get_reward(); total_reward = reward * len(all_links); min_wd = await get_min_withdraw()
    text = (
        f"💵 <b>Тебе доступно {len(all_links)} заданий!</b>\n\n"
        f"💎 Шанс получить <b>${fmt_money(total_reward)}</b> за минуту!\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💵 За спонсора: <b>${fmt_money(reward)}</b>\n"
        f"⚡ Награда сразу\n"
        f"📤 Минимум: <b>${fmt_money(min_wd)}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ Жми на кнопки → подпишись\n2️⃣ Нажми «✅ Проверить»\n3️⃣ Получи 💵 на баланс 🎉"
    )
    if delete_msg_id:
        try: await bot.delete_message(chat_id, delete_msg_id)
        except Exception: pass
    try:
        await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
    except Exception as e:
        await log_error("bot", "send_sponsors", str(e)); return False, "❌ Ошибка отправки."
    return True, None


# ========== ХЭНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split(); ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try: ref_id = int(args[1].replace("ref", ""))
        except Exception: pass
    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un)
    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Ты заблокирован.</b>"); return
    if is_new and ref_id: await add_lazy_watch(msg.from_user.id, ref_id)
    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if is_new:
        try:
            services_map = await pending_by_service(msg.from_user.id)
            await notify_admin_new_user(msg.from_user.id, f"@{un}" if un else "", msg.from_user.full_name or "", sum(services_map.values()), services_map, ref_id)
        except Exception as e: logging.error(f"notify: {e}")
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(F.text == "🛠 Админка")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(F.text == "💵 Получить")
async def msg_earn(msg: types.Message):
    await update_username(msg.from_user.id, msg.from_user.username)
    try: loading = await msg.answer("⏳ <b>Собираю спонсоров…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💰 Мой баланс")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    await msg.answer(f"💰 <b>Мой баланс</b>\n\n💵 Доступно: <b>${fmt_money(bal)}</b>\n📤 Минимум: <b>${fmt_money(min_wd)}</b>", reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd:
        await msg.answer(f"❌ <b>Недостаточно</b>\n\n📤 Минимум: <b>${fmt_money(min_wd)}</b>\n💵 У тебя: <b>${fmt_money(bal)}</b>", reply_markup=get_menu(msg.from_user.id)); return
    _states[msg.from_user.id] = "await_withdraw_data"
    await msg.answer(f"💸 <b>Вывод</b>\n\n💵 Доступно: <b>${fmt_money(bal)}</b>\n\nОтправь <b>сумму и ссылку на @CryptoBot</b>.\n<code>0.5 https://t.me/CryptoBot?start=IVxxxxx</code>", reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    me = await bot.get_me(); ref_link = f"https://t.me/{me.username}?start=ref{msg.from_user.id}"
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", msg.from_user.id))["c"]
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", msg.from_user.id))["s"]
    ref_bonus = await get_ref_bonus()
    await msg.answer(f"👥 <b>Рефералы</b>\n\n💎 Бонус: <b>${fmt_money(ref_bonus)}</b> за спонсора\n\n🔗 <code>{ref_link}</code>\n\n👤 Приглашено: <b>{refs_count}</b>\n💵 Заработано: <b>${fmt_money(earned)}</b>", reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "🎁 Промокод")
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID: return
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎁 <b>Активация промокода</b>\n\nОтправь код:", reply_markup=promo_kb())


@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id); bal = u["balance"] if u else 0
    await cq.answer("Отменено")
    try: await cq.message.edit_text("Отменено.")
    except Exception: pass
    await cq.message.answer(f"💵 Баланс: <b>${fmt_money(bal)}</b>", reply_markup=get_menu(cq.from_user.id))


@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди"); return
    _last[uid] = time.time(); await cq.answer("Проверяю…")
    cnt, sm, skipped = await check_all(uid); left = await pending_count(uid)
    pool = await get_pool()
    async with pool.acquire() as db:
        total_done = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", uid))["c"]
        total_all = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1", uid))["c"]
    u = await get_user(uid); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw(); reward = await get_reward()
    if left == 0 and total_all > 0:
        text = f"🎉 <b>Всё выполнено!</b>\n\nВыполнено: <b>{total_done}</b>\n💵 Баланс: <b>${fmt_money(bal)}</b>"
        try: await cq.message.edit_text(text)
        except Exception: await cq.message.answer(text)
        await cq.message.answer("Меню 👇", reply_markup=get_menu(uid)); return
    if cnt > 0:
        text = f"✅ <b>Засчитано: {cnt}</b>\n\n💵 +<b>${fmt_money(sm)}</b>\n💰 Баланс: <b>${fmt_money(bal)}</b>\n📊 {total_done}/{total_all}\n⚠️ Осталось: <b>{left}</b>"
    elif skipped > 0:
        text = f"⚠️ <b>Не засчитано: {skipped}</b>\n\n📊 {total_done}/{total_all}\n⚠️ Осталось: <b>{left}</b>"
    else:
        text = f"❌ <b>Новых нет</b>\n\n📊 {total_done}/{total_all}\n⚠️ Осталось: <b>{left}</b>"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="rebuild_subs")
    kb.button(text="💰 Баланс", callback_data="balance_inline")
    kb.adjust(1)
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception: await cq.message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "rebuild_subs")
async def cb_rebuild(cq: types.CallbackQuery):
    uid = cq.from_user.id; await cq.answer("Обновляю…")
    await check_all(uid)
    try: await cq.message.delete()
    except Exception: pass
    ok, err = await issue_sponsors_message(cq.from_user, cq.message.chat.id, clean=True)
    if not ok: await cq.message.answer(err, reply_markup=get_menu(uid))


@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id); bal = u["balance"] if u else 0
    await cq.answer()
    await cq.message.answer(f"💵 Баланс: <b>${fmt_money(bal)}</b>\n📤 Минимум: <b>${fmt_money(await get_min_withdraw())}</b>", reply_markup=get_menu(cq.from_user.id))


# ========== КНОПКА «СЕРВИСЫ» ==========
@dp.callback_query(F.data == "adm_services")
async def adm_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try: await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception: await cq.message.answer(text, reply_markup=services_menu_kb(enabled_map))


@dp.callback_query(F.data.startswith("svc_toggle_"))
async def svc_toggle(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    service = cq.data.replace("svc_toggle_", "")
    cur = await is_service_enabled(service)
    await set_service_enabled(service, not cur)
    await cq.answer(f"{service}: {'выкл' if cur else 'вкл'}")
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try: await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception: pass


@dp.callback_query(F.data == "svc_back")
async def svc_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception: pass


# ========== ТЕКСТОВЫЙ ХЭНДЛЕР ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id; state = _states.get(uid)

    if state == "await_promo":
        code = msg.text.strip(); pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE", code)
            if not promo: await msg.answer("❌ Неверный", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            if promo["used_count"] >= promo["max_uses"]: await msg.answer("❌ Закончился", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            if await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code): await msg.answer("❌ Уже активировал", reply_markup=get_menu(uid)); _states.pop(uid, None); return
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
            await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
            await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ +<b>${fmt_money(promo['amount'])}</b>", reply_markup=get_menu(uid)); return

    if state == "await_withdraw_data":
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2: await msg.answer("❌ Формат: <code>сумма ссылка</code>"); return
        try: amount = float(parts[0].replace(",", "."))
        except Exception: await msg.answer("❌ Сумма?"); return
        if amount <= 0: await msg.answer("❌ > 0"); return
        wallet = parts[1].strip()
        u = await get_user(uid); bal = u["balance"] if u else 0; min_wd = await get_min_withdraw()
        if amount < min_wd: await msg.answer(f"❌ Минимум ${fmt_money(min_wd)}"); return
        if amount > bal: await msg.answer(f"❌ У тебя ${fmt_money(bal)}"); return
        _states.pop(uid, None); pool = await get_pool()
        async with pool.acquire() as db:
            if await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1", amount, uid) == "UPDATE 0":
                await msg.answer("❌ Недостаточно"); return
            wid = (await db.fetchrow("INSERT INTO withdrawals (user_id, amount, wallet, status, created_at) VALUES ($1,$2,$3,'pending',$4) RETURNING id", uid, amount, wallet, time.time()))["id"]
        await msg.answer(f"✅ <b>Заявка #{wid}</b>\n💵 ${fmt_money(amount)}\n🔗 {wallet}", reply_markup=get_menu(uid))
        try:
            await bot.send_message(ADMIN_ID, f"💸 <b>Заявка #{wid}</b>\n👤 {await get_user_display(uid)}\n💵 ${fmt_money(amount)}\n🔗 {wallet}", reply_markup=wd_kb(wid))
        except Exception: pass
        return

    if state == "adm_reward":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("reward", v); _states.pop(uid, None)
        await msg.answer(f"✅ ${fmt_money(v)}", reply_markup=admin_menu()); return

    if state == "adm_max":
        try: v = int(msg.text)
        except Exception: await msg.answer("❌ Целое"); return
        if v > MAX_MAX_SPONSORS or v < 1: await msg.answer(f"❌ 1-{MAX_MAX_SPONSORS}"); return
        await set_setting("max_sponsors", v); _states.pop(uid, None)
        await msg.answer(f"✅ {v}", reply_markup=admin_menu()); return

    if state == "adm_minw":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0: await msg.answer("❌ > 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("min_withdraw", v); _states.pop(uid, None)
        await msg.answer(f"✅ ${fmt_money(v)}", reply_markup=admin_menu()); return

    if state == "adm_refb":
        try:
            v = float(msg.text.replace(",", "."))
            if v < 0: await msg.answer("❌ ≥ 0"); return
        except Exception: await msg.answer("❌ Число"); return
        await set_setting("ref_bonus", v); _states.pop(uid, None)
        await msg.answer(f"✅ ${fmt_money(v)}", reply_markup=admin_menu()); return

    if state == "adm_broadcast":
        _states.pop(uid, None); await msg.answer("📢 Рассылка…")
        pool = await get_pool()
        async with pool.acquire() as db: rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
        ok = 0; fail = 0
        for r in rows:
            try: await msg.copy_to(r["user_id"]); ok += 1
            except Exception: fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu()); return

    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3: await msg.answer("❌ <code>КОД | СУММА | АКТ</code>"); return
        code, a_s, u_s = parts
        try: a = float(a_s); u = int(u_s)
        except Exception: await msg.answer("❌ Числа"); return
        pool = await get_pool()
        async with pool.acquire() as db:
            try: await db.execute("INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)", code, a, u, time.time())
            except Exception: await msg.answer("❌ Уже есть"); return
        _states.pop(uid, None)
        await msg.answer(f"✅ <code>{code}</code>: ${fmt_money(a)} × {u}", reply_markup=admin_menu()); return

    if state == "adm_find":
        query = msg.text.strip(); _states.pop(uid, None)
        target = await find_user(query)
        if not target: await msg.answer(f"❌ Не найден", reply_markup=admin_menu()); return
        u = await get_user(target)
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target))["c"]
            subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", target))["c"]
        await msg.answer(f"🔎 {user_display(target, u['username'])}\n💰 ${fmt_money(u['balance'])}\n📊 {subs}\n👥 {refs}", reply_markup=admin_menu()); return

    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2: await msg.answer("❌ <code>ID СУММА</code>"); return
        target = await find_user(parts[0])
        if not target: await msg.answer("❌ Не найден"); return
        try: a = float(parts[1].replace(",", "."))
        except Exception: await msg.answer("❌ Сумма"); return
        await add_balance(target, a); _states.pop(uid, None)
        await msg.answer(f"✅ ${fmt_money(a)} → {await get_user_display(target)}", reply_markup=admin_menu())
        try: await bot.send_message(target, f"🎁 +<b>${fmt_money(a)}</b>")
        except Exception: pass
        return


# ========== КОМАНДЫ ==========
@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: q = msg.text.split()[1]
    except Exception: await msg.answer("Формат: /ban ID"); return
    t = await find_user(q)
    if not t: await msg.answer("❌"); return
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", t)
    await msg.answer(f"🚫 {await get_user_display(t)}", reply_markup=admin_menu())


@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: q = msg.text.split()[1]
    except Exception: await msg.answer("Формат: /unban ID"); return
    t = await find_user(q)
    if not t: await msg.answer("❌"); return
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", t)
    await msg.answer(f"✅ {await get_user_display(t)}", reply_markup=admin_menu())


@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try:
        p = msg.text.split(); q = p[1]; a = float(p[2].replace(",", "."))
    except Exception: await msg.answer("Формат: /give ID СУММА"); return
    t = await find_user(q)
    if not t: await msg.answer("❌"); return
    await add_balance(t, a)
    await msg.answer(f"✅ ${fmt_money(a)} → {await get_user_display(t)}", reply_markup=admin_menu())
    try: await bot.send_message(t, f"🎁 +<b>${fmt_money(a)}</b>")
    except Exception: pass


@dp.message(Command("errors"))
async def cmd_errors(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    try: rows = await get_recent_errors(hours=3)
    except Exception as e: await msg.answer(f"❌ {e}"); return
    if not rows: await msg.answer("✅ Ошибок нет"); return
    text = f"⚠️ <b>Ошибки:</b>\n\n"
    for r in rows[:15]:
        ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    await msg.answer(text[:4000])


# ========== МЕДИА ==========
@dp.message((F.photo | F.video | F.document | F.animation | F.sticker | F.voice) & F.from_user.id == ADMIN_ID)
async def media_broadcast_handler(msg: types.Message):
    uid = msg.from_user.id
    if _states.get(uid) != "adm_broadcast": return
    _states.pop(uid, None)
    await msg.answer("📢 Рассылка…")
    pool = await get_pool()
    async with pool.acquire() as db: rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
    ok = 0; fail = 0
    for r in rows:
        try: await msg.copy_to(r["user_id"]); ok += 1
        except Exception: fail += 1
        await asyncio.sleep(0.05)
    await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"; await cq.answer()
    try: await cq.message.edit_text(f"💵 Текущая: <b>${fmt_money(await get_reward())}</b>\n\nНовое:")
    except Exception: pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"; await cq.answer()
    try: await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nЧисло 1-{MAX_MAX_SPONSORS}:")
    except Exception: pass


@dp.callback_query(F.data == "adm_minw")
async def adm_minw(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_minw"; await cq.answer()
    try: await cq.message.edit_text(f"💸 Мин: <b>${fmt_money(await get_min_withdraw())}</b>\n\nНовое:")
    except Exception: pass


@dp.callback_query(F.data == "adm_refb")
async def adm_refb(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_refb"; await cq.answer()
    try: await cq.message.edit_text(f"👥 Реф: <b>${fmt_money(await get_ref_bonus())}</b>\n\nНовое:")
    except Exception: pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        blocked = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE blocked=TRUE"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE status='subscribed'"))["c"]
        paid = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'"))["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
        errs = (await db.fetchrow("SELECT COUNT(*) as c FROM error_log WHERE created_at > $1", time.time() - 86400))["c"]
    await cq.answer()
    try: await cq.message.edit_text(f"📊 <b>Статистика</b>\n\n👥 {total} (🚫 {blocked})\n💵 ${fmt_money(balances)}\n✅ {subs}\n💸 ${fmt_money(paid)}\n📤 {pend}\n⚠️ {errs}", reply_markup=admin_menu())
    except Exception: pass


@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_find"; await cq.answer()
    try: await cq.message.edit_text("🔎 <b>Найти юзера</b>\n\nID или @username:")
    except Exception: pass


@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_give"; await cq.answer()
    try: await cq.message.edit_text("💎 <b>Начислить</b>\n\n<code>ID СУММА</code>")
    except Exception: pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"; await cq.answer()
    try: await cq.message.edit_text("📢 Отправь сообщение:")
    except Exception: pass


@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_promo_create"; await cq.answer()
    try: await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТ</code>")
    except Exception: pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT w.id, w.user_id, w.amount, w.wallet, w.status, u.username FROM withdrawals w LEFT JOIN users u ON u.user_id = w.user_id WHERE w.status='pending' ORDER BY w.id DESC LIMIT 20")
    if not rows: await cq.answer("Нет заявок", show_alert=True); return
    text = "💸 <b>Заявки:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} · {user_display(r['user_id'], r['username'])} · <b>${fmt_money(r['amount'])}</b> → <code>{escape_html(r['wallet'])}</code>\n"
    await cq.answer()
    try: await cq.message.edit_text(text[:4000], reply_markup=admin_menu(), disable_web_page_preview=True)
    except Exception: pass


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    try: wid = int(cq.data.replace("wd_done_", ""))
    except Exception: await cq.answer("Ошибка"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE withdrawals SET status='done' WHERE id=$1", wid)
        row = await db.fetchrow("SELECT user_id, amount FROM withdrawals WHERE id=$1", wid)
    if row:
        try: await bot.send_message(row["user_id"], f"✅ <b>Заявка #{wid} оплачена</b>\n💵 ${fmt_money(row['amount'])}")
        except Exception: pass
    await cq.answer("✅")
    try: await cq.message.edit_text(f"✅ #{wid}")
    except Exception: pass


@dp.callback_query(F.data.startswith("wd_decl_"))
async def wd_decl(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    try: wid = int(cq.data.replace("wd_decl_", ""))
    except Exception: await cq.answer("Ошибка"); return
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id, amount, status FROM withdrawals WHERE id=$1", wid)
        if row and row["status"] == "pending":
            await db.execute("UPDATE withdrawals SET status='declined' WHERE id=$1", wid)
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", row["amount"], row["user_id"])
            try: await bot.send_message(row["user_id"], f"❌ <b>Заявка #{wid} отклонена</b>\n💵 ${fmt_money(row['amount'])} возвращены")
            except Exception: pass
    await cq.answer("❌")
    try: await cq.message.edit_text(f"❌ #{wid}")
    except Exception: pass


# ========== ДИАГНОСТИКА UI ==========
@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("📊 <b>Диагностика</b>", reply_markup=diag_kb())
    except Exception: await cq.message.answer("📊 <b>Диагностика</b>", reply_markup=diag_kb())


@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception: pass


@dp.callback_query(F.data == "diag_services")
async def cb_diag_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer("Проверяю…")
    try: await cq.message.edit_text("🔎 <b>Проверяю…</b>")
    except Exception: pass
    try: report = await run_diagnostics()
    except Exception as e: report = f"❌ {e}"
    if len(report) > 4000: report = report[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_services")
    kb.button(text="⚠️", callback_data="diag_errors")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(report, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(report, reply_markup=kb.as_markup())
        except Exception: pass


@dp.callback_query(F.data == "diag_errors")
async def cb_diag_errors(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: rows = await get_recent_errors(hours=3)
    except Exception as e: await cq.answer(f"Ошибка БД: {e}", show_alert=True); return
    if not rows: text = "✅ <b>Ошибок нет</b>"
    else:
        text = f"⚠️ <b>Ошибки:</b>\n\n"
        for r in rows[:15]:
            ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
            text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    if len(text) > 4000: text = text[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_errors")
    kb.button(text="🔍", callback_data="diag_services")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try: await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception: pass


# ========== ТОП ==========
@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🏆 <b>Топ</b>", reply_markup=top_kb())
    except Exception: await cq.message.answer("🏆 <b>Топ</b>", reply_markup=top_kb())


@dp.callback_query(F.data == "top_back")
async def cb_top_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    try: await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception: pass


@dp.callback_query(F.data == "top_balance")
async def cb_top_balance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_balance(15)
    text = "🏆 <b>Топ по балансу</b>\n\n" + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>${fmt_money(r['balance'])}</b>\n" for i, r in enumerate(rows, 1)) if rows else "📭 Пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="👥", callback_data="top_refs")
    kb.button(text="🔄", callback_data="top_balance")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception: pass


@dp.callback_query(F.data == "top_refs")
async def cb_top_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    await cq.answer()
    rows = await get_top_by_refs(15)
    text = "🏆 <b>Топ по рефералам</b>\n\n" + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{r['refs_count'] or 0}</b>\n" for i, r in enumerate(rows, 1)) if rows else "📭 Пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="💰", callback_data="top_balance")
    kb.button(text="🔄", callback_data="top_refs")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try: await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception: pass


# ========== WEB + MAIN ==========
async def health(_): return web.Response(text="ok")


async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")


async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed: await _http.close()


async def main():
    await init_db()
    await start_web()
    asyncio.create_task(autocheck_worker())
    asyncio.create_task(lazy_worker())
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
