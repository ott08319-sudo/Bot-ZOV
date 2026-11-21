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
    from flyerapi import Flyer
    FLYER_AVAILABLE = True
except ImportError:
    FLYER_AVAILABLE = False
    Flyer = None

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
ADMIN_ID        = int(os.getenv("ADMIN_ID", "0") or "0")
PORT            = int(os.getenv("PORT", "10000"))
DATABASE_URL    = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY    = os.getenv("FLYER_API_KEY", "")
FLOWLY_API_KEY   = os.getenv("FLOWLY_API_KEY", "")
BOTOHUB_API_KEY  = os.getenv("BOTOHUB_API_KEY", "")
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")
BRIZAHUB_API_KEY = os.getenv("BRIZAHUB_API_KEY", os.getenv("BRIZA_API_KEY", ""))

flyer = Flyer(FLYER_API_KEY) if (FLYER_AVAILABLE and FLYER_API_KEY) else None

# ========== КОНСТАНТЫ ==========
CHECK_COOLDOWN       = 3
DEFAULT_REWARD       = 0.005
DEFAULT_REF_BONUS    = 0.01
DEFAULT_MAX_SPONSORS = 10
MAX_MAX_SPONSORS     = 30
DEFAULT_MIN_WITHDRAW = 0.5
AUTOCHECK_INTERVAL   = 3600
LAZY_THRESHOLD       = 3
LAZY_INTERVAL        = 1800
FLOWLY_TIMEOUT       = 8.0
INCOMPLETE_STATUSES  = ('incomplete', 'pending', 'unsubscribed', 'new', 'abort', None)

REF_ACTIVE_MIN_SUBS = 3
REF_MILESTONES = {
    3: 0.02,
    5: 0.04,
    10: 0.10,
    25: 0.30,
    50: 0.75,
}

DAILY_REWARDS = [0.005, 0.007, 0.01, 0.012, 0.015, 0.02, 0.03]

# Убраны pulsar, pusher, mgram
SERVICES_ALL = ["piarflow", "flowly", "flyer", "trafsly", "botohub", "brizahub"]

NICKNAMES = [
    "солнышко", "котик", "зайка", "звёздочка", "чемпион",
    "красавчик", "умничка", "дружок", "герой", "счастливчик"
]

LAZY_USER_MSGS = [
    "😡 <b>Эй, ты чего?!</b>\n\nТы пришёл заработать 💵, а сам ничего не сделал?\n\n🔥 Жми «💵 Получить», подпишись на всех и нажми «✅ Я подписался — проверить».",
    "😤 <b>Слушай, так не работает!</b>\n\nТы нажал /start и думаешь, что 💵 упадут сами?\n\n💎 Зайди в «💵 Получить», подпишись на спонсоров, проверь — и получишь первые деньги.",
    "😠 <b>Ты серьёзно?</b>\n\nДругие уже зарабатывают 💵, а ты не подписался ни на одного.\n\n⚠️ Жми «💵 Получить» → подпишись → «✅ Проверить».",
    "🔥 <b>Проснись!</b>\n\nТы зашёл в бота и ничего не делаешь.\n\n💎 Жми «💵 Получить», подписывайся, проверяй — и деньги твои."
]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http = None
_pool = None
_last = {}
_states = {}
_user_locks = {}

async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
    return _http

def pick_nickname():
    return random.choice(NICKNAMES)

def escape_html(text):
    if not text: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_money(value):
    try:
        v = float(value or 0)
    except Exception:
        return "0"
    s = f"{v:.4f}".rstrip("0").rstrip(".")
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
        _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=15)
    return _pool

def user_display(user_id, username=None):
    if username: return f"@{escape_html(username.lstrip('@'))} (<code>{user_id}</code>)"
    return f"<code>{user_id}</code>"

async def get_user_display(user_id):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT username FROM users WHERE user_id=$1", user_id)
        if row and row["username"]:
            return f"@{escape_html(row['username'].lstrip('@'))} (<code>{user_id}</code>)"
    except Exception: pass
    return f"<code>{user_id}</code>"

async def log_error(service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("SELECT id FROM error_log WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1", service, error_type, time.time() - 300)
            if recent: return
            await db.execute("INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)", service, error_type, str(message)[:500], time.time())
    except Exception as e: logging.error(f"log_error failed: {e}")

async def notify_admin(text):
    if not ADMIN_ID: return
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
            lines = [f"💵 <b>{service}</b> ({cnt} шт, +${fmt_money(sm)})"]
            for aid, reward, link, tt in items[:10]:
                short_link = str(link) if link else str(aid)
                lines.append(f"  • {escape_html(short_link)}")
            blocks.append("\n".join(lines))
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        text = f"🆕 <b>Новые подписки!</b>\n\n👤 <b>{user_str}</b>\n\n📦 <b>{len(subs_list)}</b> заданий выполнено:\n\n" + "\n\n".join(blocks) + f"\n\n💵 Итого: <b>+${fmt_money(total)}</b>\n🕐 {ts}"
        await bot.send_message(ADMIN_ID, text[:4000], disable_web_page_preview=True)
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
        await bot.send_message(ADMIN_ID, text[:4000], disable_web_page_preview=True)
    except Exception as e: logging.error(f"notify_admin_new_user: {e}")

async def notify_referrer(referrer_id, ref_username, subs_count, earned):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", referrer_id)
        bal = row["balance"] if row else 0
        text = f"🎉 <b>Твой реферал подписался!</b>\n\n👤 Реферал: @{escape_html(ref_username or 'без ника')}\n✅ Подписок: <b>{subs_count}</b>\n💎 Ты получаешь: <b>+${fmt_money(earned)}</b>\n💰 Твой баланс: <b>${fmt_money(bal)}</b>"
        await bot.send_message(referrer_id, text, disable_web_page_preview=True)
    except Exception: pass

async def notify_lazy_user(user_id):
    try:
        await bot.send_message(user_id, random.choice(LAZY_USER_MSGS), disable_web_page_preview=True)
        return True
    except Exception: return False

async def notify_admin_lazy(user_id, subs, mins):
    if not ADMIN_ID: return
    try: await bot.send_message(ADMIN_ID, f"😴 <b>Ленивый юзер</b>\n\n👤 {await get_user_display(user_id)}\n📊 Подписок: <b>{subs}</b>/{LAZY_THRESHOLD}\n⏱ Прошло: {mins} мин", disable_web_page_preview=True)
    except Exception: pass

# ========== БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, balance DOUBLE PRECISION DEFAULT 0,
            held DOUBLE PRECISION DEFAULT 0, referrer_id BIGINT, referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE, created_at DOUBLE PRECISION, daily_streak INTEGER DEFAULT 0, last_daily_at DOUBLE PRECISION DEFAULT 0, sponsors_msg_id BIGINT)""")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_earned DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS held DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_streak INTEGER DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_at DOUBLE PRECISION DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sponsors_msg_id BIGINT")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users (LOWER(username))")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY, user_id BIGINT, service TEXT, assignment_id TEXT, link TEXT,
            reward DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'unsubscribed',
            signature TEXT, task_type TEXT, session_id TEXT, provider_pool TEXT, created_at DOUBLE PRECISION, UNIQUE(user_id, service, assignment_id))""")
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
        await db.execute("""CREATE TABLE IF NOT EXISTS ref_milestones_claimed (
            user_id BIGINT NOT NULL, milestone INTEGER NOT NULL, claimed_at DOUBLE PRECISION DEFAULT 0, PRIMARY KEY (user_id, milestone))""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_user_status ON sponsor_tasks (user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_service ON sponsor_tasks (service)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users (balance DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wd_status ON withdrawals (status)")

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

async def set_sponsors_msg_id(user_id, msg_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=$1 WHERE user_id=$2", msg_id, user_id)

async def clear_sponsors_msg_id(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=NULL WHERE user_id=$1", user_id)

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
        return await db.fetchrow("SELECT balance, referrer_id, referred_earned, blocked, username, daily_streak, last_daily_at FROM users WHERE user_id=$1", user_id)

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
            ON CONFLICT (user_id, service, assignment_id) DO UPDATE
            SET link = EXCLUDED.link,
                reward = EXCLUDED.reward,
                signature = COALESCE(EXCLUDED.signature, sponsor_tasks.signature),
                task_type = COALESCE(EXCLUDED.task_type, sponsor_tasks.task_type),
                session_id = COALESCE(EXCLUDED.session_id, sponsor_tasks.session_id),
                provider_pool = COALESCE(EXCLUDED.provider_pool, sponsor_tasks.provider_pool),
                status = CASE
                    WHEN sponsor_tasks.service = 'brizahub' THEN 'unsubscribed'
                    ELSE sponsor_tasks.status
                END""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type,
            str(session_id) if session_id is not None else None,
            str(provider_pool) if provider_pool is not None else None,
            time.time())

async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'", user_id, service, str(aid))
        return res != "UPDATE 0"

async def delete_stale_task(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db: await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'", user_id, service, str(aid))

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

async def check_ref_milestones(referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("""SELECT COUNT(*) as c FROM users u
               WHERE u.referrer_id = $1
               AND (SELECT COUNT(*) FROM sponsor_tasks st WHERE st.user_id = u.user_id AND st.status = 'subscribed') >= $2""", referrer_id, REF_ACTIVE_MIN_SUBS)
        active_refs = row["c"] if row else 0
        claimed_rows = await db.fetch("SELECT milestone FROM ref_milestones_claimed WHERE user_id=$1", referrer_id)
        claimed = {r["milestone"] for r in claimed_rows}
        for m_count, bonus in sorted(REF_MILESTONES.items()):
            if active_refs >= m_count and m_count not in claimed:
                await db.execute("INSERT INTO ref_milestones_claimed (user_id, milestone, claimed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", referrer_id, m_count, time.time())
                await add_balance(referrer_id, bonus)
                await add_referred_earned(referrer_id, bonus)
                try:
                    await bot.send_message(referrer_id, f"🏆 <b>Достижение разблокировано!</b>\n\nТы пригласил <b>{m_count} активных друзей</b>!\n🎁 Твой бонус: <b>+${fmt_money(bonus)}</b> зачислен на баланс!")
                except Exception: pass

# ========== API: PIARFLOW ==========
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}) as r:
            if r.status != 200: return []
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
            if r.status != 200: return []
            d = await r.json()
            if d.get("status") == "ok": return d.get("sponsors", [])
    except Exception as e: await log_error("piarflow", "check", str(e))
    return []

# ========== API: FLOWLY ==========
async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY: return None
    try:
        s = await http()
        async with s.post("https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY},
            json={"tg_user_id": user_id, "lang": lang or "ru", "is_premium": bool(is_premium)},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)) as r:
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
                if x.get("status") in ("subscribed", "notgetted", "completed"): continue
                link = x.get("link"); aid = x.get("ads_id") or x.get("id")
                if link and aid is not None:
                    out.append({"id": str(aid), "link": link, "source": x.get("source"), "provider_pool": x.get("provider_pool")})
            return out
    except Exception as e: await log_error("trafsly", "fetch", str(e))
    return []

async def check_trafsly(user_id, ads_id, source=None, provider_pool=None):
    if not TRAFSLY_API_KEY or not ads_id: return False
    s = await http()
    try:
        payload = {"user_id": int(user_id), "ads_id": str(ads_id)}
        if source: payload["source"] = source
        if provider_pool: payload["provider_pool"] = provider_pool
        async with s.post("https://api.trafsly.com/api/v1/confirm-subscription",
            json=payload, headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return False
            d = await r.json()
            return d.get("subscribed") is True or d.get("status") == "subscribed"
    except Exception as e: await log_error("trafsly", "check", str(e))
    return False

# ========== API: BOTOHUB ==========
def _extract_botohub_items(d):
    if not isinstance(d, dict): return []
    if d.get("skip") or d.get("completed"): return []
    for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
        v = d.get(field)
        if isinstance(v, list): return v
    return []

async def get_botohub(user_id):
    if not BOTOHUB_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": int(user_id)}, headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            return _extract_botohub_items(d)
    except Exception as e: await log_error("botohub", "fetch", str(e))
    return []

async def check_botohub(user_id):
    if not BOTOHUB_API_KEY: return None
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": int(user_id)}, headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200: return None
            d = await r.json()
            if d.get("skip") or d.get("completed"): return set()
            items = _extract_botohub_items(d)
            remaining_ids = set()
            for t in items:
                if isinstance(t, str): remaining_ids.add(t)
                elif isinstance(t, dict):
                    link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
                    aid = t.get("id") or t.get("task_id") or t.get("assignment_id") or link
                    if aid is not None: remaining_ids.add(str(aid))
                    if link is not None: remaining_ids.add(str(link))
            return remaining_ids
    except Exception as e: await log_error("botohub", "check", str(e))
    return None

# ========== API: BRIZAHUB ==========
async def get_brizahub(user_id):
    if not BRIZAHUB_API_KEY: return []
    s = await http()
    try:
        limit = max(1, min(await get_max_sponsors(), 10))
        async with s.post("https://api.brizahub.com/v1/tasks",
            json={"user_id": int(user_id), "limit": limit},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return []
            d = await r.json()
            out = []
            for t in d.get("tasks") or []:
                tid = t.get("task_id") if t.get("task_id") is not None else t.get("id")
                link = t.get("link") or t.get("url")
                if tid is not None and link:
                    out.append({"id": str(tid), "link": str(link).strip(), "type": t.get("type")})
            return out
    except Exception as e: await log_error("brizahub", "fetch", str(e))
    return []

async def check_brizahub(user_id, task_ids):
    if not BRIZAHUB_API_KEY or not task_ids: return set()
    s = await http()
    try:
        norm_ids = [int(x) if str(x).isdigit() else str(x) for x in task_ids]
        async with s.post("https://api.brizahub.com/v1/tasks/check",
            json={"user_id": int(user_id), "task_ids": norm_ids},
            headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200: return set()
            d = await r.json()
            completed = set()
            for item in d.get("results") or []:
                tid = item.get("task_id") if item.get("task_id") is not None else item.get("id")
                status = item.get("status")
                if tid is not None and status and status not in INCOMPLETE_STATUSES:
                    completed.add(str(tid))
            return completed
    except Exception as e: await log_error("brizahub", "check", str(e))
    return set()

# ========== СБОР ==========
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
        _call("trafsly", get_trafsly(user)),
        _call("botohub", get_botohub(uid)),
        _call("brizahub", get_brizahub(uid)),
        return_exceptions=True,
    )

    piarflow, flowly, flyer_, trafsly, botohub, brizahub = results

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
    if isinstance(brizahub, list):
        for x in brizahub:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "brizahub", str(x["id"]), x["link"], reward, task_type=x.get("type"))

# ========== ПРОВЕРКА ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3", user_id, service, str(aid))
        return row["reward"] if row else 0.0

async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0, 0

    enabled = await get_enabled_services()

    pf, fl, tr, bh, br = [], [], [], [], []
    flowly_pending = []
    link_map, task_type_map = {}, {}

    for service, aid, link, reward, sig, task_type, sess, pool_ in tasks:
        if service not in enabled: continue
        link_map[aid] = link
        task_type_map[aid] = task_type
        if service == "piarflow" and link: pf.append(link)
        elif service == "flyer" and sig: fl.append((aid, sig))
        elif service == "trafsly" and aid: tr.append((aid, sig, pool_))
        elif service == "botohub" and aid: bh.append((aid, link, reward))
        elif service == "brizahub" and aid: br.append(aid)
        elif service == "flowly": flowly_pending.append((service, aid, link, reward, sig, task_type))

    done_count, done_sum, skipped_count, new_subs = 0, 0.0, 0, []

    if pf:
        for r in await check_piarflow(user_id, pf):
            st = r.get("status")
            lnk = r.get("link")
            if st == "subscribed" and lnk:
                if await mark_subscribed(user_id, "piarflow", lnk):
                    rw = await _task_reward(user_id, "piarflow", lnk)
                    await add_balance(user_id, rw); done_count += 1; done_sum += rw
                    new_subs.append(("piarflow", lnk, rw, lnk, None))
            elif st == "not_counted" and lnk:
                await delete_stale_task(user_id, "piarflow", lnk)
                skipped_count += 1

    if tr:
        for aid, src, pp in tr:
            if await check_trafsly(user_id, aid, src, pp):
                if await mark_subscribed(user_id, "trafsly", aid):
                    rw = await _task_reward(user_id, "trafsly", aid)
                    await add_balance(user_id, rw); done_count += 1; done_sum += rw
                    new_subs.append(("trafsly", aid, rw, link_map.get(aid), None))

    if bh:
        remaining_bh = await check_botohub(user_id)
        if remaining_bh is not None:
            if len(remaining_bh) == 0:
                for aid, link, rw in bh:
                    if await mark_subscribed(user_id, "botohub", aid):
                        await add_balance(user_id, rw); done_count += 1; done_sum += rw
                        new_subs.append(("botohub", aid, rw, link, None))
            else:
                pass

    for aid, sig in fl:
        res_fl = await check_flyer(sig)
        if res_fl in ("complete", "subscribed"):
            if await mark_subscribed(user_id, "flyer", aid):
                rw = await _task_reward(user_id, "flyer", aid)
                await add_balance(user_id, rw); done_count += 1; done_sum += rw
                new_subs.append(("flyer", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    if flowly_pending:
        status, offers_map = await check_flowly(user_id)
        if status is not None:
            total_rw = 0.0
            for service, aid, link, reward, sig, tt in flowly_pending:
                is_sub = (status == "ok") or (offers_map.get(link) is True) or (link not in offers_map)
                if is_sub:
                    if await mark_subscribed(user_id, "flowly", aid):
                        rw = await _task_reward(user_id, "flowly", aid)
                        total_rw += rw; done_count += 1; done_sum += rw
                        new_subs.append(("flowly", aid, rw, link, tt))
            if total_rw > 0: await add_balance(user_id, total_rw)

    if br:
        completed_br = await check_brizahub(user_id, br)
        for aid in br:
            if str(aid) in completed_br:
                if await mark_subscribed(user_id, "brizahub", aid):
                    rw = await _task_reward(user_id, "brizahub", aid)
                    await add_balance(user_id, rw); done_count += 1; done_sum += rw
                    new_subs.append(("brizahub", aid, rw, link_map.get(aid), task_type_map.get(aid)))

    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_id = row["referrer_id"]
            ref_total = (await get_ref_bonus()) * done_count
            if ref_total > 0:
                await add_balance(ref_id, ref_total)
                await add_referred_earned(ref_id, ref_total)
                await notify_referrer(ref_id, row["username"] or "", done_count, ref_total)
            await check_ref_milestones(ref_id)

    if new_subs: await notify_admin_new_subs(user_id, new_subs)
    return done_count, done_sum, skipped_count

async def check_all_safe(uid):
    lock = _user_locks.setdefault(uid, asyncio.Lock())
    if lock.locked(): return 0, 0.0, 0
    async with lock: return await check_all(uid)

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

async def _diag_flowly():
    if not FLOWLY_API_KEY: return {"error": "ключ не задан", "count": 0}
    try:
        timeout = aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post("https://flowly.guru/api/v1/offers", headers={"Auth": FLOWLY_API_KEY}, json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False}) as r:
                if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
                d = await r.json()
                if d.get("status") == "ok": return {"count": 0, "extra": " (всё выполнено)"}
                offers = d.get("offers", [])
                if isinstance(offers, list):
                    return {"count": sum(1 for o in offers if not o.get("subscribed"))}
                return {"count": 0}
    except asyncio.TimeoutError: return {"error": "timeout", "count": 0}

async def _diag_flyer():
    if not flyer: return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks: return {"count": 0}
    return {"count": len([t for t in tasks if t.get('status') in INCOMPLETE_STATUSES])}

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
        items = _extract_botohub_items(d)
        return {"count": len(items)}

async def _diag_brizahub():
    if not BRIZAHUB_API_KEY: return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post("https://api.brizahub.com/v1/tasks", json={"user_id": ADMIN_ID, "limit": 3}, headers={"Authorization": f"Bearer {BRIZAHUB_API_KEY}", "Content-Type": "application/json"}) as r:
        body = await r.text()
        if r.status != 200: return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        tasks = [t for t in (d.get("tasks") or []) if (t.get("task_id") is not None or t.get("id") is not None) and (t.get("link") or t.get("url"))]
        return {"count": len(tasks)}

async def run_diagnostics():
    enabled = await get_enabled_services()
    checks = []
    mapping = [
        ("Piarflow", "piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        ("Flowly", "flowly", FLOWLY_API_KEY, _diag_flowly),
        ("Flyer", "flyer", FLYER_API_KEY, _diag_flyer),
        ("Trafsly", "trafsly", TRAFSLY_API_KEY, _diag_trafsly),
        ("Botohub", "botohub", BOTOHUB_API_KEY, _diag_botohub),
        ("Brizahub", "brizahub", BRIZAHUB_API_KEY, _diag_brizahub),
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
        [KeyboardButton(text="🎁 Ежедневный бонус"), KeyboardButton(text="🎟 Промокод")],
    ], resize_keyboard=True, is_persistent=True)

def admin_reply_menu():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💵 Получить"), KeyboardButton(text="💰 Мой баланс")],
        [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
        [KeyboardButton(text="🎁 Ежедневный бонус"), KeyboardButton(text="🛠 Админка")],
    ], resize_keyboard=True, is_persistent=True)

def get_menu(user_id): return admin_reply_menu() if user_id == ADMIN_ID else user_menu()

def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1): kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2: rows.append(1)
    rows.append(1); kb.adjust(*rows)
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
    kb.adjust(2, 2, 2, 1)
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
async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True, header_note=None):
    uid = user.id
    if clean: await clear_pending_tasks(uid); await collect_sponsors(user)
    all_links = await pending_links(uid, limit=await get_max_sponsors())
    if not all_links:
        if delete_msg_id:
            try: await bot.delete_message(chat_id, delete_msg_id)
            except Exception: pass
        return False, "😕 <b>Заданий пока нет.</b>\n\nЗагляни через 15-20 минут — мы добавляем новых спонсоров 💵"
    reward = await get_reward(); total_reward = reward * len(all_links); min_wd = await get_min_withdraw()
    nick = pick_nickname()
    prefix = f"{header_note}\n\n" if header_note else ""
    text = (
        f"{prefix}"
        f"💵 <b>{nick.capitalize()}, тебе доступно {len(all_links)} заданий!</b>\n\n"
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
        sent = await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
        await set_sponsors_msg_id(uid, sent.message_id)
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
    nick = pick_nickname()
    try: loading = await msg.answer(f"⏳ <b>Собираю спонсоров, {nick}…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None, clean=True)
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
    nick = pick_nickname()
    try: loading = await msg.answer(f"⏳ <b>Собираю спонсоров, {nick}…</b>")
    except Exception: loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None, clean=True)
    if not ok: await msg.answer(err, reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "💰 Мой баланс")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw(); nick = pick_nickname()
    await msg.answer(f"💰 <b>Мой баланс, {nick}</b>\n\n💵 Доступно: <b>${fmt_money(bal)}</b>\n📤 Минимум: <b>${fmt_money(min_wd)}</b>", reply_markup=get_menu(msg.from_user.id))

@dp.message(F.text == "🎁 Ежедневный бонус")
async def msg_daily_bonus(msg: types.Message):
    uid = msg.from_user.id
    u = await get_user(uid)
    if not u: return
    subs = await count_total_subs(uid)
    if subs < 1:
        await msg.answer("🎁 <b>Ежедневный бонус</b>\n\n⚠️ Чтобы забирать ежедневный бонус, выполни хотя бы <b>1 задание</b> в разделе «💵 Получить»!", reply_markup=get_menu(uid))
        return
    now = time.time(); last_at = u["last_daily_at"] or 0; streak = u["daily_streak"] or 0
    elapsed = now - last_at
    if elapsed < 86400:
        left_sec = int(86400 - elapsed)
        hrs, rem = divmod(left_sec, 3600); mins = rem // 60
        await msg.answer(f"⏳ <b>Бонус уже получен!</b>\n\n🔥 Твоя серия: <b>{streak} дн.</b>\n🕒 Следующий бонус через: <b>{hrs}ч {mins}мин</b>", reply_markup=get_menu(uid))
        return
    new_streak = (streak + 1) if (last_at > 0 and elapsed < 172800) else 1
    reward_idx = min(new_streak - 1, len(DAILY_REWARDS) - 1)
    bonus = DAILY_REWARDS[reward_idx]
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1, daily_streak = $2, last_daily_at = $3 WHERE user_id = $4", bonus, new_streak, now, uid)
    nick = pick_nickname()
    next_bonus = DAILY_REWARDS[min(new_streak, len(DAILY_REWARDS) - 1)]
    await msg.answer(f"🎁 <b>Ежедневный бонус получен, {nick}!</b>\n\n💵 Начислено: <b>+${fmt_money(bonus)}</b>\n🔥 Серия дней подряд: <b>{new_streak}</b>\n📅 Завтра тебя ждёт: <b>+${fmt_money(next_bonus)}</b>", reply_markup=get_menu(uid))

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
    uid = msg.from_user.id
    me = await bot.get_me(); ref_link = f"https://t.me/{me.username}?start=ref{uid}"
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", uid))["c"]
        active_refs = (await db.fetchrow("""SELECT COUNT(*) as c FROM users u WHERE u.referrer_id=$1
               AND (SELECT COUNT(*) FROM sponsor_tasks st WHERE st.user_id=u.user_id AND st.status='subscribed') >= $2""", uid, REF_ACTIVE_MIN_SUBS))["c"]
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", uid))["s"]
    ref_bonus = await get_ref_bonus(); nick = pick_nickname()
    milestones_lines = []
    for m_cnt, m_rw in sorted(REF_MILESTONES.items()):
        mark = "✅" if active_refs >= m_cnt else "▫️"
        milestones_lines.append(f"{mark} <b>{m_cnt}</b> активных друзей → <b>+${fmt_money(m_rw)}</b>")
    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться ссылкой", switch_inline_query=f"\n💵 Зарабатывай доллары в Telegram бесплатно! Забирай бонус: {ref_link}")
    kb.button(text="🏆 Топ рефоводов", callback_data="public_top_refs")
    kb.adjust(1)
    await msg.answer(f"👥 <b>Реферальная программа, {nick}</b>\n\n💎 Бонус: <b>${fmt_money(ref_bonus)}</b> за каждое задание друга!\n\n🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n👤 Всего приглашено: <b>{refs_count}</b> (активных: <b>{active_refs}</b>)\n💵 Заработано с друзей: <b>${fmt_money(earned)}</b>\n\n🏆 <b>Бонусы за активных друзей</b> (от {REF_ACTIVE_MIN_SUBS} подписок):\n" + "\n".join(milestones_lines), reply_markup=kb.as_markup())

@dp.callback_query(F.data == "public_top_refs")
async def cb_public_top_refs(cq: types.CallbackQuery):
    await cq.answer()
    rows = await get_top_by_refs(10)
    if not rows:
        await cq.message.answer("📭 Топ пока пуст."); return
    lines = ["🏆 <b>Топ-10 рефоводов бота:</b>\n"]
    medals = ["🥇", "🥈", "🥉"]
    for i, r in enumerate(rows, 1):
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        name = f"@{escape_html(r['username'])}" if r['username'] else f"Игрок #{str(r['user_id'])[-4:]}"
        lines.append(f"{prefix} {name} — <b>{r['refs_count'] or 0}</b> друзей")
    await cq.message.answer("\n".join(lines))

@dp.message(F.text.in_({"🎁 Промокод", "🎟 Промокод"}))
async def msg_promo(msg: types.Message):
    if msg.from_user.id == ADMIN_ID:
        pass # Allow admin
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎟 <b>Активация промокода</b>\n\nОтправь код:", reply_markup=promo_kb())

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
    cnt, sm, skipped = await check_all_safe(uid)
    left_links = await pending_links(uid, limit=await get_max_sponsors())
    u = await get_user(uid); bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw(); nick = pick_nickname()

    try: await cq.message.delete()
    except Exception: pass
    await clear_sponsors_msg_id(uid)

    if not left_links:
        if cnt > 0:
            text = (
                f"🎉 <b>Умница, {nick}! Все задания выполнены!</b>\n\n"
                f"✅ Засчитано: <b>{cnt}</b>\n"
                f"💵 Начислено: <b>+${fmt_money(sm)}</b>\n"
                f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
                f"📤 Минимум: <b>${fmt_money(min_wd)}</b>\n\n"
                f"<i>Загляни позже или нажми «💵 Получить» для новых заданий!</i>"
            )
        else:
            text = (
                f"😕 <b>Активных заданий больше нет, {nick}.</b>\n\n"
                f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
                f"Нажми «💵 Получить», чтобы проверить новых спонсоров."
            )
        await cq.message.answer(text, reply_markup=get_menu(uid)); return

    if cnt > 0:
        header = f"✅ <b>Умница, {nick}! Засчитано: {cnt} (+${fmt_money(sm)})</b>\n💰 Баланс: <b>${fmt_money(bal)}</b>\n⚠️ Осталось подписаться на <b>{len(left_links)}</b>:"
    elif skipped > 0:
        header = f"⚠️ <b>Не засчитано: {skipped}, {nick}</b>\nПодпишись на оставшихся спонсоров ({len(left_links)} шт.):"
    else:
        header = f"❌ <b>Новых подписок пока не найдено, {nick}.</b>\nПодпишись на каналы ниже и снова нажми «✅ Я подписался — проверить»:"

    await issue_sponsors_message(cq.from_user, cq.message.chat.id, delete_msg_id=None, clean=False, header_note=header)

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
        await msg.answer(f"✅ <b>Заявка #{wid}</b>\n💵 ${fmt_money(amount)}\n🔗 {escape_html(wallet)}", reply_markup=get_menu(uid))
        try:
            await bot.send_message(ADMIN_ID, f"💸 <b>Заявка #{wid}</b>\n👤 {await get_user_display(uid)}\n💵 ${fmt_money(amount)}\n🔗 {escape_html(wallet)}", reply_markup=wd_kb(wid))
        except Exception: pass
        return

    if uid != ADMIN_ID: return

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
        try: a = float(a_s.replace(",", ".")); u = int(u_s)
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
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    pass
