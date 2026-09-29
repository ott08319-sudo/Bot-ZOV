import os
import json
import time
import random
import asyncio
import logging
import aiohttp
import asyncpg
from aiohttp import web
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

logging.basicConfig(level=logging.INFO)


# ============================================================
# ENV
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")

CHECK_COOLDOWN = 3
DEFAULT_REWARD = 0.005
DEFAULT_MAX_SPONSORS = 50
MIN_WITHDRAW = 0.1
REF_PERCENT = 50
MAX_ATTEMPTS = 3

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http: aiohttp.ClientSession | None = None
_pool: asyncpg.Pool | None = None


async def http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    return _http


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


def fmt_money(value: float) -> str:
    s = f"{value:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def service_emoji(service: str) -> str:
    emojis = {
        "piarflow": "🟢",
        "tgrass": "🌿",
        "botohub": "🤖",
        "trafsly": "📡",
    }
    return emojis.get(service, "🔗")


# ============================================================
# БАЗА
# ============================================================
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        # Создаём таблицы (старая схема users — безопасно)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance DOUBLE PRECISION DEFAULT 0,
                referrer_id BIGINT,
                referred_earned DOUBLE PRECISION DEFAULT 0,
                created_at DOUBLE PRECISION
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sponsor_tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                service TEXT,
                assignment_id TEXT,
                link TEXT,
                reward DOUBLE PRECISION DEFAULT 0,
                status TEXT DEFAULT 'unsubscribed',
                signature TEXT,
                created_at DOUBLE PRECISION,
                UNIQUE(user_id, service, assignment_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount DOUBLE PRECISION,
                wallet TEXT,
                status TEXT DEFAULT 'pending',
                created_at DOUBLE PRECISION
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS balance_log (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount DOUBLE PRECISION,
                reason TEXT,
                created_at DOUBLE PRECISION
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                amount DOUBLE PRECISION,
                max_uses INTEGER DEFAULT 100,
                used INTEGER DEFAULT 0,
                created_at DOUBLE PRECISION
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS promo_uses (
                user_id BIGINT,
                code TEXT,
                used_at DOUBLE PRECISION,
                PRIMARY KEY (user_id, code)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                user_id BIGINT,
                achievement TEXT,
                unlocked_at DOUBLE PRECISION,
                PRIMARY KEY (user_id, achievement)
            )
        """)

        # АВТО-МИГРАЦИЯ: добавляем недостающие колонки
        alters = [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS ban_reason TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_bonus_at DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS total_earned DOUBLE PRECISION DEFAULT 0",
            "ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0",
        ]
        for sql in alters:
            try:
                await db.execute(sql)
            except Exception as e:
                logging.warning(f"ALTER: {e}")

        # Дефолтные настройки
        for k, v in [
            ("reward", str(DEFAULT_REWARD)),
            ("max_sponsors", str(DEFAULT_MAX_SPONSORS)),
            ("service_priority", "piarflow,tgrass,botohub,trafsly"),
        ]:
            await db.execute(
                "INSERT INTO settings (key,value) VALUES ($1,$2) "
                "ON CONFLICT (key) DO NOTHING", k, v
            )

        logging.info("✅ БД инициализирована")


async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ($1,$2) "
            "ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value)
        )


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def get_service_priority():
    p = await get_setting("service_priority", "piarflow,tgrass,botohub,trafsly")
    return [s.strip() for s in p.split(",") if s.strip()]


async def set_service_priority(order):
    await set_setting("service_priority", ",".join(order))


async def log_balance(user_id, amount, reason):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO balance_log (user_id, amount, reason, created_at) VALUES ($1,$2,$3,$4)",
            user_id, amount, reason, time.time()
        )


async def is_banned(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT banned FROM users WHERE user_id=$1", user_id)
        return row and row["banned"] == 1


async def ban_user(user_id, reason=""):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET banned=1, ban_reason=$1 WHERE user_id=$2", reason, user_id)


async def unban_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET banned=0, ban_reason=NULL WHERE user_id=$1", user_id)


async def register_user(user_id, username=None, referrer_id=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            return
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            ref_exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not ref_exists:
                referrer_id = None
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1,$2,$3,$4)",
            user_id, username, referrer_id, time.time()
        )


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT balance, referrer_id, referred_earned, banned, ban_reason, "
            "daily_bonus_at, total_earned, created_at, username "
            "FROM users WHERE user_id=$1", user_id
        )


async def find_user_by_username(username):
    pool = await get_pool()
    async with pool.acquire() as db:
        un = username.lstrip("@").lower()
        row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1", un)
        return row["user_id"] if row else None


async def add_balance(user_id, amount, reason="Задание"):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE users SET balance = balance + $1, total_earned = total_earned + $2 WHERE user_id=$3",
            amount, max(0, amount), user_id
        )
    await log_balance(user_id, amount, reason)


async def save_sponsor(user_id, service, aid, link, reward):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO sponsor_tasks
                (user_id, service, assignment_id, link, reward, status, created_at)
            VALUES ($1,$2,$3,$4,$5,'unsubscribed',$6)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING
        """, user_id, service, aid, link, reward, time.time())


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )


async def mark_skipped(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE sponsor_tasks SET status='skipped' "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )


async def increment_attempt(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE sponsor_tasks SET attempts = attempts + 1 "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )
        row = await db.fetchrow(
            "SELECT attempts FROM sponsor_tasks "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )
        return row["attempts"] if row else 0


async def get_next_task(user_id):
    priority = await get_service_priority()
    pool = await get_pool()
    async with pool.acquire() as db:
        for service in priority:
            row = await db.fetchrow("""
                SELECT service, assignment_id, link, reward, attempts
                FROM sponsor_tasks
                WHERE user_id=$1 AND status='unsubscribed' AND service=$2
                ORDER BY RANDOM() LIMIT 1
            """, user_id, service)
            if row:
                return dict(row)
        row = await db.fetchrow("""
            SELECT service, assignment_id, link, reward, attempts
            FROM sponsor_tasks
            WHERE user_id=$1 AND status='unsubscribed'
            ORDER BY RANDOM() LIMIT 1
        """, user_id)
        return dict(row) if row else None


async def count_pending(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='unsubscribed'",
            user_id
        )
        return row["c"]


async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT reward FROM sponsor_tasks "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )
        return row["reward"] if row else 0.0


# ============================================================
# ДОСТИЖЕНИЯ
# ============================================================
ACHIEVEMENTS = {
    "first_task": {"name": "🥉 Первое задание", "check": lambda d: d["done"] >= 1},
    "ten_tasks": {"name": "🥈 10 заданий", "check": lambda d: d["done"] >= 10},
    "fifty_tasks": {"name": "🥇 50 заданий", "check": lambda d: d["done"] >= 50},
    "hundred_tasks": {"name": "💎 100 заданий", "check": lambda d: d["done"] >= 100},
    "five_hundred": {"name": "👑 500 заданий", "check": lambda d: d["done"] >= 500},
    "first_ref": {"name": "🤝 Первый реферал", "check": lambda d: d["refs"] >= 1},
    "five_refs": {"name": "👥 5 рефералов", "check": lambda d: d["refs"] >= 5},
    "twenty_refs": {"name": "🎉 20 рефералов", "check": lambda d: d["refs"] >= 20},
    "earned_1": {"name": "💰 Заработал $0.1", "check": lambda d: d["total_earned"] >= 0.1},
    "earned_5": {"name": "💵 Заработал $0.5", "check": lambda d: d["total_earned"] >= 0.5},
    "earned_10": {"name": "💎 Заработал $1", "check": lambda d: d["total_earned"] >= 1.0},
}


async def get_user_stats(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        done = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'",
            user_id
        ))["c"]
        refs = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", user_id
        ))["c"]
        u = await db.fetchrow("SELECT total_earned FROM users WHERE user_id=$1", user_id)
        total_earned = u["total_earned"] if u else 0
    return {"done": done, "refs": refs, "total_earned": total_earned}


async def check_achievements(user_id):
    stats = await get_user_stats(user_id)
    pool = await get_pool()
    async with pool.acquire() as db:
        existing = await db.fetch("SELECT achievement FROM achievements WHERE user_id=$1", user_id)
        have = {r["achievement"] for r in existing}
    new_ones = []
    for key, ach in ACHIEVEMENTS.items():
        if key in have:
            continue
        try:
            if ach["check"](stats):
                pool2 = await get_pool()
                async with pool2.acquire() as db:
                    await db.execute(
                        "INSERT INTO achievements (user_id, achievement, unlocked_at) "
                        "VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                        user_id, key, time.time()
                    )
                new_ones.append(ach["name"])
        except Exception as e:
            logging.error(f"achievement {key}: {e}")
    return new_ones


# ============================================================
# API: PIARFLOW
# ============================================================
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id, "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        logging.error(f"Piarflow get: {e}")
    return []


async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        logging.error(f"Piarflow check: {e}")
    return []


# ============================================================
# API: TGRASS
# ============================================================
async def get_tgrass(user_id, username):
    if not TGRASS_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://tgrass.space/offers",
            json={"tg_user_id": user_id, "tg_login": username or "", "lang": "ru", "is_premium": False},
            headers={"Auth": TGRASS_API_KEY}
        ) as r:
            d = await r.json()
            if d.get("status") == "not_ok":
                return d.get("offers", [])
    except Exception as e:
        logging.error(f"TGrass get: {e}")
    return []


async def check_tgrass(user_id):
    if not TGRASS_API_KEY:
        return False
    s = await http()
    try:
        async with s.post(
            "https://tgrass.space/offers",
            json={"tg_user_id": user_id},
            headers={"Auth": TGRASS_API_KEY}
        ) as r:
            d = await r.json()
            return d.get("status") == "ok"
    except Exception as e:
        logging.error(f"TGrass check: {e}")
    return False


# ============================================================
# API: BOTOHUB
# ============================================================
async def get_botohub(user_id):
    if not BOTOHUB_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY}
        ) as r:
            d = await r.json()
            if not d.get("skip") and not d.get("completed"):
                return d.get("tasks", [])
    except Exception as e:
        logging.error(f"Botohub get: {e}")
    return []


async def check_botohub(user_id):
    if not BOTOHUB_API_KEY:
        return False
    s = await http()
    try:
        async with s.post(
            "https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY}
        ) as r:
            d = await r.json()
            return bool(d.get("completed"))
    except Exception as e:
        logging.error(f"Botohub check: {e}")
    return False


# ============================================================
# API: TRAFSLY
# ============================================================
async def get_trafsly(user_id, username):
    if not TRAFSLY_API_KEY:
        return []
    payload = {
        "user_id": user_id,
        "max_sponsors": await get_max_sponsors(),
        "language_code": "ru",
        "is_premium": False
    }
    if username:
        payload["username"] = username
    s = await http()
    try:
        async with s.post(
            "https://api.trafsly.com/api/v1/get-sponsors",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY}
        ) as r:
            d = await r.json()
            if d.get("status") == "warning":
                return d.get("sponsors", [])
    except Exception as e:
        logging.error(f"Trafsly get: {e}")
    return []


async def check_trafsly(user_id, ads_ids):
    if not TRAFSLY_API_KEY or not ads_ids:
        return []
    s = await http()
    res = []
    for aid in ads_ids:
        try:
            async with s.post(
                "https://api.trafsly.com/api/v1/confirm-subscription",
                json={"user_id": user_id, "ads_id": int(aid)},
                headers={"Auth": TRAFSLY_API_KEY}
            ) as r:
                d = await r.json()
                if d.get("subscribed") is True:
                    res.append({"ads_id": aid, "status": "subscribed"})
                elif d.get("subscribed") is False:
                    if d.get("status") == "error" or "not verified" in str(d.get("message", "")).lower():
                        res.append({"ads_id": aid, "status": "subscribed"})
                    else:
                        res.append({"ads_id": aid, "status": "unsubscribed"})
        except Exception as e:
            logging.error(f"Trafsly check {aid}: {e}")
    return res


# ============================================================
# ДИАГНОСТИКА
# ============================================================
async def diagnose_service(name, key, url, payload, headers=None):
    if not key:
        return f"❌ <b>{name}</b> — ключ не задан"
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    s = await http()
    t0 = time.time()
    try:
        async with s.post(url, json=payload, headers=h, timeout=aiohttp.ClientTimeout(total=10)) as r:
            dt = int((time.time() - t0) * 1000)
            body = await r.text()
            try:
                data = json.loads(body)
            except Exception:
                data = None
            if r.status not in (200, 201):
                return f"⚠️ <b>{name}</b> — HTTP <b>{r.status}</b> ({dt}мс)\n<code>{body[:150]}</code>"
            count = None
            if isinstance(data, dict):
                for field in ("tasks", "sponsors", "offers", "result", "results", "data", "items"):
                    v = data.get(field)
                    if isinstance(v, list):
                        count = len(v); break
            if count is not None:
                if count > 0:
                    return f"✅ <b>{name}</b> — <b>{count}</b> заданий ({dt}мс)"
                return f"🟡 <b>{name}</b> — ключ ок, 0 заданий ({dt}мс)"
            return f"🟡 <b>{name}</b> — {r.status} OK, формат не распознан ({dt}мс)\n<code>{body[:150]}</code>"
    except asyncio.TimeoutError:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — timeout ({dt}мс)"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {str(e)[:100]} ({dt}мс)"


async def run_diagnostics():
    uid = ADMIN_ID if ADMIN_ID else 1
    mx = await get_max_sponsors()
    results = await asyncio.gather(
        diagnose_service("Piarflow", PIARFLOW_API_KEY,
            "https://piarflow.com/v1/sponsors",
            {"user_id": uid, "chat_id": uid, "max_sponsors": mx},
            {"Authorization": f"Bearer {PIARFLOW_API_KEY}"}),
        diagnose_service("TGrass", TGRASS_API_KEY,
            "https://tgrass.space/offers",
            {"tg_user_id": uid, "tg_login": "", "lang": "ru", "is_premium": False},
            {"Auth": TGRASS_API_KEY}),
        diagnose_service("Botohub", BOTOHUB_API_KEY,
            "https://botohub.me/get-tasks",
            {"chat_id": uid},
            {"Auth": BOTOHUB_API_KEY}),
        diagnose_service("Trafsly", TRAFSLY_API_KEY,
            "https://api.trafsly.com/api/v1/get-sponsors",
            {"user_id": uid, "max_sponsors": mx, "language_code": "ru", "is_premium": False},
            {"Auth": TRAFSLY_API_KEY}),
    )
    return f"🔎 <b>Диагностика API</b>\n🆔 user_id: <code>{uid}</code>\n\n" + "\n\n".join(results)


# ============================================================
# СБОР СПОНСОРОВ
# ============================================================
async def collect_sponsors(user):
    uid = user.id
    un = user.username or ""
    reward = await get_reward()
    links = []

    for x in await get_piarflow(uid):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "piarflow", link, link, reward)
            links.append(link)

    for x in await get_tgrass(uid, un):
        link = x.get("link")
        if link:
            await save_sponsor(uid, "tgrass", str(x.get("offer_id")), link, reward)
            links.append(link)

    for link in await get_botohub(uid):
        if link:
            await save_sponsor(uid, "botohub", link, link, reward)
            links.append(link)

    for x in await get_trafsly(uid, un):
        link = x.get("link")
        aid = x.get("ads_id")
        if link:
            await save_sponsor(uid, "trafsly", str(aid) if aid else link, link, reward)
            links.append(link)

    return links


# ============================================================
# ПРОВЕРКА ОДНОГО ЗАДАНИЯ
# ============================================================
async def check_single_task(user_id, service, aid, link):
    try:
        if service == "piarflow":
            res = await check_piarflow(user_id, [link])
            return any(r.get("status") in ("subscribed", "not_counted") for r in res)
        elif service == "tgrass":
            return await check_tgrass(user_id)
        elif service == "botohub":
            return await check_botohub(user_id)
        elif service == "trafsly":
            res = await check_trafsly(user_id, [aid])
            return any(r.get("status") == "subscribed" for r in res)
    except Exception as e:
        logging.error(f"check_single {service}: {e}")
    return False


# ============================================================
# КЛАВИАТУРЫ
# ============================================================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 Заработать", callback_data="earn")
    kb.button(text="💰 Баланс", callback_data="balance")
    kb.button(text="💸 Вывести", callback_data="withdraw")
    kb.button(text="👥 Рефералы", callback_data="refs")
    kb.button(text="👤 Профиль", callback_data="profile")
    kb.button(text="🏆 Топ юзеров", callback_data="top")
    kb.button(text="🎁 Бонус", callback_data="daily_bonus")
    kb.button(text="🎟 Промокод", callback_data="promo")
    kb.adjust(2, 2, 2, 2)
    return kb.as_markup()


def task_link_kb(link):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Подписаться", url=link)
    kb.button(text="✅ Готово", callback_data="task_done")
    kb.button(text="⏭ Пропустить", callback_data="task_skip")
    kb.button(text="⬅️ В меню", callback_data="menu")
    kb.adjust(1, 2, 1)
    return kb.as_markup()


def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Изменить награду", callback_data="adm_reward")
    kb.button(text="👥 Макс. спонсоров", callback_data="adm_max")
    kb.button(text="⚙️ Приоритет сервисов", callback_data="adm_priority")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🔎 Диагностика API", callback_data="adm_diag")
    kb.button(text="👤 Инфо о юзере", callback_data="adm_userinfo")
    kb.button(text="💵 Начислить / списать", callback_data="adm_addbal")
    kb.button(text="🚫 Бан / Разбан", callback_data="adm_ban")
    kb.button(text="🏆 Топ юзеров", callback_data="adm_top")
    kb.button(text="🎁 Промокоды", callback_data="adm_promo")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Заявки на вывод", callback_data="adm_withdraws")
    kb.adjust(1)
    return kb.as_markup()


# ============================================================
# ХЭНДЛЕРЫ ЮЗЕРА
# ============================================================
_states = {}


@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    if await is_banned(msg.from_user.id):
        await msg.answer("🚫 <b>Ты забанен в боте</b>")
        return
    args = msg.text.split()
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try:
            ref_id = int(args[1].replace("ref", ""))
        except Exception:
            pass
    await register_user(msg.from_user.id, msg.from_user.username, ref_id)
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    await msg.answer(
        f"👋 Привет, {msg.from_user.first_name}!\n\n"
        f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
        f"💵 За задание: <b>${fmt_money(await get_reward())}</b>\n"
        f"📤 Минималка: <b>${MIN_WITHDRAW:.2f}</b>\n"
        f"👥 Реферальный бонус: <b>{REF_PERCENT}%</b>",
        reply_markup=main_menu()
    )


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.callback_query(F.data == "menu")
async def cb_menu(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    try:
        await cq.message.edit_text(
            f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
            f"💵 За задание: <b>${fmt_money(await get_reward())}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "balance")
async def cb_balance(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💰 Твой баланс: <b>${fmt_money(bal)}</b>\n"
            f"📤 Минималка: <b>${MIN_WITHDRAW:.2f}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "profile")
async def cb_profile(cq: types.CallbackQuery):
    uid = cq.from_user.id
    u = await get_user(uid)
    if not u:
        await cq.answer("Сначала /start"); return
    stats = await get_user_stats(uid)
    pool = await get_pool()
    async with pool.acquire() as db:
        refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", uid))["c"]
        ach = await db.fetch("SELECT achievement FROM achievements WHERE user_id=$1", uid)
        ach_names = [ACHIEVEMENTS[r["achievement"]]["name"] for r in ach if r["achievement"] in ACHIEVEMENTS]
    dt = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y")
    ach_text = "\n".join(ach_names) if ach_names else "—"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"👤 <b>Профиль</b>\n\n"
            f"🆔 <code>{uid}</code>\n"
            f"📅 С {dt}\n\n"
            f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>\n"
            f"💵 Всего заработал: <b>${fmt_money(u['total_earned'])}</b>\n"
            f"✅ Заданий: <b>{stats['done']}</b>\n"
            f"👥 Рефералов: <b>{refs}</b>\n\n"
            f"🏅 <b>Достижения:</b>\n{ach_text}",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "top")
async def cb_top(cq: types.CallbackQuery):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT 10"
        )
    await cq.answer()
    text = "🏆 <b>Топ-10 по балансу</b>\n\n"
    for i, r in enumerate(rows, 1):
        name = f"@{r['username']}" if r['username'] else f"user_{r['user_id']}"
        medal = ["🥇","🥈","🥉"][i-1] if i <= 3 else f"{i}."
        text += f"{medal} {name} — <b>${fmt_money(r['balance'])}</b>\n"
    try:
        await cq.message.edit_text(text, reply_markup=main_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "daily_bonus")
async def cb_daily_bonus(cq: types.CallbackQuery):
    uid = cq.from_user.id
    u = await get_user(uid)
    if not u:
        await cq.answer("Сначала /start"); return
    now = time.time()
    last = u["daily_bonus_at"] or 0
    if now - last < 86400:
        left = int(86400 - (now - last))
        h = left // 3600
        m = (left % 3600) // 60
        await cq.answer(f"⏰ Следующий бонус через {h}ч {m}м", show_alert=True)
        return
    bonus = 0.001
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET daily_bonus_at=$1 WHERE user_id=$2", now, uid)
    await add_balance(uid, bonus, "Ежедневный бонус")
    await cq.answer(f"🎁 +${fmt_money(bonus)}!")
    u = await get_user(uid)
    try:
        await cq.message.edit_text(
            f"🎁 <b>Бонус получен!</b>\n\n"
            f"💵 +${fmt_money(bonus)}\n"
            f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "promo")
async def cb_promo(cq: types.CallbackQuery):
    _states[cq.from_user.id] = "await_promo"
    await cq.answer()
    try:
        await cq.message.edit_text("🎟 <b>Активация промокода</b>\n\nОтправь промокод сообщением:")
    except Exception:
        pass


@dp.callback_query(F.data == "earn")
async def cb_earn(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if await is_banned(uid):
        await cq.answer("🚫 Ты забанен", show_alert=True); return
    await cq.answer("Подбираю…")
    logging.info(f"EARN pressed by {uid}")

    await collect_sponsors(cq.from_user)
    task = await get_next_task(uid)
    if not task:
        try:
            await cq.message.edit_text(
                "😕 <b>Заданий нет</b>\n\nВозвращайся позже — появятся новые.",
                reply_markup=main_menu()
            )
        except Exception:
            pass
        return

    emoji = service_emoji(task["service"])
    try:
        await cq.message.edit_text(
            f"📋 <b>Новое задание</b>\n\n"
            f"{emoji} Подпишись на канал:\n\n"
            f"Нажми <b>«Подписаться»</b>, потом <b>«Готово»</b>.",
            reply_markup=task_link_kb(task["link"]),
            disable_web_page_preview=True
        )
    except Exception:
        pass


@dp.callback_query(F.data == "task_done")
async def cb_task_done(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if await is_banned(uid):
        await cq.answer("🚫 Ты забанен", show_alert=True); return
    task = await get_next_task(uid)
    if not task:
        try:
            await cq.message.edit_text("😕 Заданий нет.", reply_markup=main_menu())
        except Exception:
            pass
        return

    service = task["service"]
    aid = task["assignment_id"]
    link = task["link"]
    reward = task["reward"]

    ok = await check_single_task(uid, service, aid, link)

    if ok:
        await mark_subscribed(uid, service, aid)
        await add_balance(uid, reward, f"Задание {service}")
        u = await get_user(uid)
        if u and u["referrer_id"]:
            ref_bonus = reward * REF_PERCENT / 100
            await add_balance(u["referrer_id"], ref_bonus, "Реферальный бонус")
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute(
                    "UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2",
                    ref_bonus, u["referrer_id"]
                )
            try:
                await bot.send_message(u["referrer_id"], f"💸 +${fmt_money(ref_bonus)} с реферала")
            except Exception:
                pass

        new_ach = await check_achievements(uid)
        next_task = await get_next_task(uid)

        if next_task:
            text = f"✅ <b>+${fmt_money(reward)}</b>\n\n📋 <b>Следующее задание</b>\n\n"
            if new_ach:
                text += f"🏅 <b>Новое достижение:</b>\n" + "\n".join(new_ach) + "\n\n"
            text += "Подпишись и жми <b>«Готово»</b>."
            try:
                await cq.message.edit_text(
                    text,
                    reply_markup=task_link_kb(next_task["link"]),
                    disable_web_page_preview=True
                )
            except Exception:
                pass
        else:
            text = f"✅ <b>+${fmt_money(reward)}</b>\n\n🎉 <b>Все задания выполнены!</b>\n\n"
            if new_ach:
                text += f"🏅 {', '.join(new_ach)}\n\n"
            text += "💰 Продолжай завтра — появятся новые."
            try:
                await cq.message.edit_text(text, reply_markup=main_menu())
            except Exception:
                pass
    else:
        attempts = await increment_attempt(uid, service, aid)
        if attempts >= MAX_ATTEMPTS:
            await mark_skipped(uid, service, aid)
            next_task = await get_next_task(uid)
            if next_task:
                try:
                    await cq.message.edit_text(
                        "📋 <b>Новое задание</b>\n\nПодпишись и жми <b>«Готово»</b>.",
                        reply_markup=task_link_kb(next_task["link"]),
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass
            else:
                try:
                    await cq.message.edit_text(
                        "😕 <b>Заданий больше нет</b>\n\nВозвращайся позже.",
                        reply_markup=main_menu()
                    )
                except Exception:
                    pass
        else:
            await cq.answer("❌ Ты ещё не подписался", show_alert=True)


@dp.callback_query(F.data == "task_skip")
async def cb_task_skip(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if await is_banned(uid):
        await cq.answer("🚫 Ты забанен", show_alert=True); return
    task = await get_next_task(uid)
    if not task:
        try:
            await cq.message.edit_text("😕 Заданий нет.", reply_markup=main_menu())
        except Exception:
            pass
        return

    await mark_skipped(uid, task["service"], task["assignment_id"])
    next_task = await get_next_task(uid)
    if next_task:
        try:
            await cq.message.edit_text(
                "📋 <b>Новое задание</b>\n\nПодпишись и жми <b>«Готово»</b>.",
                reply_markup=task_link_kb(next_task["link"]),
                disable_web_page_preview=True
            )
        except Exception:
            pass
    else:
        try:
            await cq.message.edit_text(
                "🎉 <b>Все задания выполнены!</b>\n\n💰 Возвращайся позже.",
                reply_markup=main_menu()
            )
        except Exception:
            pass


@dp.callback_query(F.data == "refs")
async def cb_refs(cq: types.CallbackQuery):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{cq.from_user.id}"
    u = await get_user(cq.from_user.id)
    earned = u["referred_earned"] if u else 0
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", cq.from_user.id
        ))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"👥 <b>Реферальная система</b>\n\n"
            f"Ты получаешь <b>{REF_PERCENT}%</b> от заработка приглашённых.\n\n"
            f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
            f"👤 Приглашено: <b>{refs_count}</b>\n"
            f"💵 Заработано: <b>${fmt_money(earned)}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(cq: types.CallbackQuery):
    if await is_banned(cq.from_user.id):
        await cq.answer("🚫 Ты забанен", show_alert=True); return
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await cq.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}. У тебя ${fmt_money(bal)}", show_alert=True)
        return
    _states[cq.from_user.id] = "await_wallet"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💸 <b>Вывод средств</b>\n\n"
            f"💰 Баланс: <b>${fmt_money(bal)}</b>\n\n"
            f"Отправь одним сообщением: <code>сумма ссылка_на_счёт</code>\n\n"
            f"<i>Пример:</i>\n"
            f"<code>0.5 https://t.me/CryptoBot?start=IVxxxxx</code>"
        )
    except Exception:
        pass


# ============================================================
# ТЕКСТОВЫЙ ХЭНДЛЕР
# ============================================================
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_wallet":
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.answer("❌ Формат: <code>сумма ссылка_на_счёт</code>"); return
        try:
            amount = float(parts[0].replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму"); return
        if amount <= 0:
            await msg.answer("❌ Сумма должна быть положительной"); return
        wallet = parts[1].strip()
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}"); return
        if amount > bal:
            await msg.answer(f"❌ У тебя только ${fmt_money(bal)}"); return
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", amount, uid)
            row = await db.fetchrow(
                "INSERT INTO withdrawals (user_id, amount, wallet, created_at) "
                "VALUES ($1,$2,$3,$4) RETURNING id",
                uid, amount, wallet, time.time()
            )
            wid = row["id"]
        _states.pop(uid, None)
        await msg.answer(f"✅ Заявка №<b>{wid}</b> создана!\n💵 Сумма: <b>${fmt_money(amount)}</b>")
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Оплачено", callback_data=f"wd_done_{wid}")
        kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
        kb.adjust(2)
        try:
            await bot.send_message(
                ADMIN_ID,
                f"💸 <b>Заявка #{wid}</b>\n"
                f"👤 <a href='tg://user?id={uid}'>{msg.from_user.full_name}</a> (<code>{uid}</code>)\n"
                f"💰 <b>${fmt_money(amount)}</b>\n🔗 {wallet}",
                reply_markup=kb.as_markup(),
                disable_web_page_preview=True
            )
        except Exception as e:
            logging.error(f"send admin: {e}")
        return

    if state == "await_promo":
        _states.pop(uid, None)
        code = msg.text.strip().upper()
        pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promocodes WHERE code=$1", code)
            if not promo:
                await msg.answer("❌ Промокод не найден"); return
            if promo["used"] >= promo["max_uses"]:
                await msg.answer("❌ Промокод исчерпан"); return
            used = await db.fetchrow(
                "SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code
            )
            if used:
                await msg.answer("❌ Ты уже использовал этот промокод"); return
            await db.execute("UPDATE promocodes SET used = used + 1 WHERE code=$1", code)
            await db.execute(
                "INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)",
                uid, code, time.time()
            )
        await add_balance(uid, promo["amount"], f"Промокод {code}")
        u = await get_user(uid)
        await msg.answer(
            f"🎉 <b>Промокод активирован!</b>\n\n"
            f"💵 +${fmt_money(promo['amount'])}\n"
            f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>",
            reply_markup=main_menu()
        )
        return

    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Введи число"); return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>${fmt_money(val)}</b>", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            val = int(msg.text)
        except Exception:
            await msg.answer("❌ Введи число"); return
        await set_setting("max_sponsors", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Макс: <b>{val}</b>", reply_markup=admin_menu())
        return

    if state == "adm_userinfo":
        target = msg.text.strip()
        target_uid = None
        if target.startswith("@") or not target.isdigit():
            target_uid = await find_user_by_username(target)
        else:
            target_uid = int(target)
        _states.pop(uid, None)
        if not target_uid:
            await msg.answer("❌ Не найден"); return
        u = await get_user(target_uid)
        if not u:
            await msg.answer("❌ Не найден"); return
        stats = await get_user_stats(target_uid)
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target_uid))["c"]
            wd = await db.fetchrow(
                "SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as s FROM withdrawals "
                "WHERE user_id=$1 AND status='done'", target_uid
            )
        dt = datetime.fromtimestamp(u["created_at"]).strftime("%d.%m.%Y")
        ban = "🚫 ЗАБАНЕН" if u["banned"] else "✅ активен"
        await msg.answer(
            f"👤 <b>Юзер</b> <code>{target_uid}</code>\n"
            f"📌 {ban}\n"
            f"📅 С {dt}\n\n"
            f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>\n"
            f"💵 Всего заработал: <b>${fmt_money(u['total_earned'])}</b>\n"
            f"✅ Заданий: <b>{stats['done']}</b>\n"
            f"👥 Рефералов: <b>{refs}</b>\n"
            f"💸 Выведено: <b>${fmt_money(wd['s'])}</b>",
            reply_markup=admin_menu()
        )
        return

    if state == "adm_addbal":
        parts = msg.text.split()
        if len(parts) != 2:
            await msg.answer("❌ Формат: <code>@username 0.5</code>"); return
        target = parts[0].strip()
        try:
            amount = float(parts[1].replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму"); return
        target_uid = None
        if target.startswith("@") or not target.isdigit():
            target_uid = await find_user_by_username(target)
        else:
            target_uid = int(target)
        if not target_uid:
            await msg.answer("❌ Юзер не найден"); return
        await add_balance(target_uid, amount, "От админа")
        _states.pop(uid, None)
        u = await get_user(target_uid)
        sign = "+" if amount > 0 else ""
        await msg.answer(
            f"✅ <b>{sign}${fmt_money(amount)}</b> юзеру <code>{target_uid}</code>\n"
            f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>",
            reply_markup=admin_menu()
        )
        try:
            await bot.send_message(
                target_uid,
                f"💵 <b>Изменение баланса</b>\n\n{sign}${fmt_money(amount)}\n"
                f"💰 Баланс: <b>${fmt_money(u['balance'])}</b>"
            )
        except Exception:
            pass
        return

    if state == "adm_ban":
        parts = msg.text.split(maxsplit=1)
        target = parts[0].strip()
        reason = parts[1] if len(parts) > 1 else "без причины"
        target_uid = None
        if target.startswith("@") or not target.isdigit():
            target_uid = await find_user_by_username(target)
        else:
            target_uid = int(target)
        if not target_uid:
            await msg.answer("❌ Юзер не найден"); return
        if await is_banned(target_uid):
            await unban_user(target_uid)
            await msg.answer(f"✅ <code>{target_uid}</code> разбанен", reply_markup=admin_menu())
            try:
                await bot.send_message(target_uid, "✅ Ты разбанен")
            except Exception:
                pass
        else:
            await ban_user(target_uid, reason)
            await msg.answer(f"🚫 <code>{target_uid}</code> забанен\n📝 {reason}", reply_markup=admin_menu())
            try:
                await bot.send_message(target_uid, f"🚫 Ты забанен. Причина: {reason}")
            except Exception:
                pass
        _states.pop(uid, None)
        return

    if state == "adm_promo":
        parts = msg.text.split()
        if len(parts) < 1:
            await msg.answer("❌ Формат: <code>CODE 0.05 100</code>"); return
        code = parts[0].upper()
        try:
            amount = float(parts[1]) if len(parts) > 1 else 0.05
            max_uses = int(parts[2]) if len(parts) > 2 else 100
        except Exception:
            await msg.answer("❌ Не понял параметры"); return
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO promocodes (code, amount, max_uses, created_at) "
                "VALUES ($1,$2,$3,$4) ON CONFLICT (code) DO UPDATE "
                "SET amount=$2, max_uses=$3",
                code, amount, max_uses, time.time()
            )
        _states.pop(uid, None)
        await msg.answer(
            f"🎁 Промокод <code>{code}</code> создан\n💵 ${fmt_money(amount)}\n👥 Макс: {max_uses}",
            reply_markup=admin_menu()
        )
        return

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Начинаю…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users WHERE banned=0")
        ok = 0; fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"]); ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())
        return


# ============================================================
# АДМИН CALLBACKS
# ============================================================
def _check_admin(cq: types.CallbackQuery):
    return cq.from_user.id == ADMIN_ID


@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💰 Текущая награда: <b>${fmt_money(await get_reward())}</b>\n\nОтправь новое значение:"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"👥 Текущий лимит: <b>{await get_max_sponsors()}</b>\n\nОтправь новое число:"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_priority")
async def adm_priority(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    priority = await get_service_priority()
    kb = InlineKeyboardBuilder()
    for i, s in enumerate(priority, 1):
        kb.button(text=f"{i}. {service_emoji(s)} {s}", callback_data=f"prio_move_{s}")
    kb.button(text="✅ Готово", callback_data="adm_priority_done")
    kb.adjust(1)
    await cq.answer()
    text = "⚙️ <b>Приоритет сервисов</b>\n\nНажми на сервис — поднимется выше.\n\nСейчас:\n"
    for i, s in enumerate(priority, 1):
        text += f"{i}. {service_emoji(s)} {s}\n"
    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        pass


@dp.callback_query(F.data.startswith("prio_move_"))
async def adm_priority_move(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    service = cq.data.replace("prio_move_", "")
    priority = await get_service_priority()
    if service in priority:
        idx = priority.index(service)
        if idx > 0:
            priority[idx], priority[idx-1] = priority[idx-1], priority[idx]
        await set_service_priority(priority)
    await cq.answer("✅")
    await adm_priority(cq)


@dp.callback_query(F.data == "adm_priority_done")
async def adm_priority_done(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    await cq.answer("Сохранено ✅")
    try:
        await cq.message.edit_text("✅ Приоритет сохранён", reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        banned = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE banned=1"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        row = await db.fetchrow(
            "SELECT COUNT(*) as c, COALESCE(SUM(reward),0) as s FROM sponsor_tasks WHERE status='subscribed'"
        )
        subs_cnt, paid = row["c"], row["s"]
        pending = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"
        ))["c"]
        today = time.time() - 86400
        active = (await db.fetchrow(
            "SELECT COUNT(DISTINCT user_id) as c FROM sponsor_tasks WHERE created_at > $1", today
        ))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n"
            f"👥 Юзеров: <b>{total}</b> (🚫 {banned})\n"
            f"🔥 Активных 24ч: <b>{active}</b>\n"
            f"💰 Суммарный баланс: <b>${fmt_money(balances)}</b>\n"
            f"✅ Подписок: <b>{subs_cnt}</b>\n"
            f"💵 Начислено: <b>${fmt_money(paid)}</b>\n"
            f"💸 Заявок: <b>{pending}</b>",
            reply_markup=admin_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    await cq.answer("Проверяю…")
    try:
        await cq.message.edit_text("🔎 Диагностика…")
    except Exception:
        pass
    report = await run_diagnostics()
    if len(report) > 4000:
        report = report[:4000] + "…"
    try:
        await cq.message.edit_text(report, reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_userinfo")
async def adm_userinfo(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_userinfo"
    await cq.answer()
    try:
        await cq.message.edit_text("👤 Отправь @username или user_id:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_addbal")
async def adm_addbal(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_addbal"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "💵 <b>Начислить / списать</b>\n\n"
            "Отправь: <code>@username 0.5</code>\n"
            "Или: <code>user_id -0.1</code>"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_ban")
async def adm_ban(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_ban"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "🚫 <b>Бан / Разбан</b>\n\n"
            "Отправь: <code>@username причина</code>\n"
            "Или: <code>user_id</code>"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT 10"
        )
    await cq.answer()
    text = "🏆 <b>Топ-10</b>\n\n"
    for i, r in enumerate(rows, 1):
        name = f"@{r['username']}" if r['username'] else f"user_{r['user_id']}"
        medal = ["🥇","🥈","🥉"][i-1] if i <= 3 else f"{i}."
        text += f"{medal} {name} — <b>${fmt_money(r['balance'])}</b>\n"
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "adm_promo")
async def adm_promo(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_promo"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "🎁 <b>Промокоды</b>\n\n"
            "Отправь: <code>CODE 0.05 100</code>\n"
            "CODE — название, 0.05 — сумма, 100 — макс. использований"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправь сообщение для рассылки:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT id, user_id, amount, wallet FROM withdrawals "
            "WHERE status='pending' ORDER BY id DESC LIMIT 20"
        )
    if not rows:
        await cq.answer("Нет заявок", show_alert=True); return
    text = "💸 <b>Заявки:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} | <code>{r['user_id']}</code> | <b>${fmt_money(r['amount'])}</b>\n{r['wallet']}\n\n"
    if len(text) > 4000:
        text = text[:4000] + "…"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu(), disable_web_page_preview=True)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    wid = int(cq.data.split("_")[-1])
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE withdrawals SET status='done' WHERE id=$1", wid)
        row = await db.fetchrow("SELECT user_id, amount FROM withdrawals WHERE id=$1", wid)
    if row:
        try:
            await bot.send_message(row["user_id"], f"✅ Заявка №{wid} на <b>${fmt_money(row['amount'])}</b> оплачена!")
        except Exception:
            pass
    await cq.answer("Оплачено ✅")
    try:
        await cq.message.edit_text(cq.message.html_text + "\n\n✅ <b>ОПЛАЧЕНО</b>")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_decl_"))
async def wd_decl(cq: types.CallbackQuery):
    if not _check_admin(cq): return
    wid = int(cq.data.split("_")[-1])
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id, amount, status FROM withdrawals WHERE id=$1", wid)
        if row and row["status"] == "pending":
            await db.execute("UPDATE withdrawals SET status='declined' WHERE id=$1", wid)
            await db.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id=$2",
                row["amount"], row["user_id"]
            )
            try:
                await bot.send_message(row["user_id"], f"❌ Заявка №{wid} отклонена. Средства возвращены.")
            except Exception:
                pass
    await cq.answer("Отклонено ❌")
    try:
        await cq.message.edit_text(cq.message.html_text + "\n\n❌ <b>ОТКЛОНЕНО</b>")
    except Exception:
        pass


# ============================================================
# WEB + MAIN
# ============================================================
async def health(_):
    return web.Response(text="ok")


async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")


async def main():
    await init_db()
    await start_web()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
