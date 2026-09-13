import os
import time
import asyncio
import logging
import aiohttp
import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
PORT = int(os.getenv("PORT", "10000"))
DATABASE_URL = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")

CHECK_COOLDOWN = 5
DEFAULT_REWARD = 0.005
DEFAULT_MAX_SPONSORS = 15
MIN_WITHDRAW = 0.1
REF_PERCENT = 50

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http: aiohttp.ClientSession | None = None
_pool: asyncpg.Pool | None = None

async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
    return _http


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


# ========== БД ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                balance DOUBLE PRECISION DEFAULT 0,
                referrer_id BIGINT,
                referred_earned DOUBLE PRECISION DEFAULT 0,
                banned INTEGER DEFAULT 0,
                ban_reason TEXT,
                created_at DOUBLE PRECISION
            )""")
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
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount DOUBLE PRECISION,
                wallet TEXT,
                status TEXT DEFAULT 'pending',
                created_at DOUBLE PRECISION
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )""")
        # Таблица логов начислений
        await db.execute("""
            CREATE TABLE IF NOT EXISTS balance_log (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount DOUBLE PRECISION,
                reason TEXT,
                service TEXT,
                link TEXT,
                created_at DOUBLE PRECISION
            )""")

        await db.execute(
            "INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING",
            str(DEFAULT_REWARD))
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING",
            str(DEFAULT_MAX_SPONSORS))


async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2",
            key, str(value))


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def is_banned(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT banned FROM users WHERE user_id=$1", user_id)
        return row and row["banned"] == 1


async def ban_user(user_id, reason=""):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE users SET banned=1, ban_reason=$1 WHERE user_id=$2",
            reason, user_id)


async def unban_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE users SET banned=0, ban_reason=NULL WHERE user_id=$1",
            user_id)


async def log_balance(user_id, amount, reason, service=None, link=None):
    """Логирует начисление/списание баланса."""
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO balance_log (user_id, amount, reason, service, link, created_at)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, user_id, amount, reason, service, link, time.time())


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
            user_id, username, referrer_id, time.time())


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT balance, referrer_id, referred_earned, banned FROM users WHERE user_id=$1",
            user_id)


async def find_user_by_username(username):
    pool = await get_pool()
    async with pool.acquire() as db:
        un = username.lstrip("@").lower()
        row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1", un)
        return row["user_id"] if row else None


async def add_balance(user_id, amount, reason="", service=None, link=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)
    if reason:
        await log_balance(user_id, amount, reason, service, link)


async def save_sponsor(user_id, service, aid, link, reward, signature=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, reward, signature, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,'unsubscribed',$7)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING
        """, user_id, service, aid, link, reward, signature, time.time())


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid)


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT service, assignment_id, link, reward, signature FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'",
            user_id)
        return [(r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"]) for r in rows]


async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return row["c"]


async def pending_links(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        return [r["link"] for r in rows]


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
    except Exception as e:
        logging.error(f"Piarflow get: {e}")
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
    except Exception as e:
        logging.error(f"Piarflow check: {e}")
    return []


# ========== API: TGRASS ==========
async def get_tgrass(user_id, username):
    if not TGRASS_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id, "tg_login": username or "", "lang": "ru", "is_premium": False},
            headers={"Auth": TGRASS_API_KEY}) as r:
            d = await r.json()
            if d.get("status") == "not_ok": return d.get("offers", [])
    except Exception as e:
        logging.error(f"TGrass get: {e}")
    return []


async def check_tgrass(user_id):
    if not TGRASS_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id},
            headers={"Auth": TGRASS_API_KEY}) as r:
            d = await r.json()
            return d.get("status") == "ok"
    except Exception as e:
        logging.error(f"TGrass check: {e}")
    return False


# ========== API: TRAFFY ==========
async def get_traffy(user_id, first_name, username, lang="ru"):
    if not TRAFFY_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://traffy.ai/publisher/tasks",
            json={"telegram_id": user_id, "limit": await get_max_sponsors(),
                  "first_name": first_name, "username": username, "language_code": lang},
            headers={"x-publisher-api-key": TRAFFY_API_KEY}) as r:
            d = await r.json()
            if d.get("ok") and d.get("tasks"): return d["tasks"]
    except Exception as e:
        logging.error(f"Traffy get: {e}")
    return []


async def check_traffy(user_id, ids):
    if not TRAFFY_API_KEY or not ids: return []
    s = await http()
    try:
        async with s.post("https://traffy.ai/publisher/tasks/check",
            json={"telegram_id": user_id, "assignment_ids": ids},
            headers={"x-publisher-api-key": TRAFFY_API_KEY}) as r:
            d = await r.json()
            if d.get("ok"): return d.get("results", [])
    except Exception as e:
        logging.error(f"Traffy check: {e}")
    return []


# ========== API: BOTOHUB ==========
async def get_botohub(user_id):
    if not BOTOHUB_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            if not d.get("skip") and not d.get("completed"):
                return d.get("tasks", [])
    except Exception as e:
        logging.error(f"Botohub get: {e}")
    return []


async def check_botohub(user_id):
    if not BOTOHUB_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY}) as r:
            d = await r.json()
            return bool(d.get("completed"))
    except Exception as e:
        logging.error(f"Botohub check: {e}")
    return False


# ========== API: TRAFSLY ==========
async def get_trafsly(user_id, username):
    if not TRAFSLY_API_KEY: return []
    payload = {"user_id": user_id, "max_sponsors": await get_max_sponsors(),
               "language_code": "ru", "is_premium": False}
    if username: payload["username"] = username
    s = await http()
    try:
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors",
            json=payload, headers={"Auth": TRAFSLY_API_KEY}) as r:
            d = await r.json()
            if d.get("status") == "warning": return d.get("sponsors", [])
    except Exception as e:
        logging.error(f"Trafsly get: {e}")
    return []


async def check_trafsly(user_id, ads_ids):
    if not TRAFSLY_API_KEY or not ads_ids: return []
    s = await http()
    res = []
    for aid in ads_ids:
        try:
            async with s.post("https://api.trafsly.com/api/v1/confirm-subscription",
                json={"user_id": user_id, "ads_id": int(aid)},
                headers={"Auth": TRAFSLY_API_KEY}) as r:
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


# ========== СБОР СПОНСОРОВ ==========
async def collect_sponsors(user):
    uid = user.id
    un = user.username or ""
    fn = user.first_name or ""
    lang = user.language_code or "ru"
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

    for x in await get_traffy(uid, fn, un, lang):
        link = x.get("target_link")
        if link:
            await save_sponsor(uid, "traffy", str(x.get("assignment_id")), link, reward)
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


# ========== ПРОВЕРКА ==========
async def _task_reward(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT reward FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid)
        return row["reward"] if row else 0.0


async def check_all(user_id):
    tasks = await pending_tasks(user_id)
    if not tasks: return 0, 0.0

    pf, ts, tf = [], [], []
    for service, aid, link, reward, sig in tasks:
        if service == "piarflow" and link: pf.append(link)
        elif service == "trafsly" and aid: ts.append(aid)
        elif service == "traffy" and aid: tf.append(aid)

    done_count = 0
    done_sum = 0.0

    if pf:
        for r in await check_piarflow(user_id, pf):
            if r.get("status") in ("subscribed", "not_counted"):
                link = r.get("link")
                await mark_subscribed(user_id, "piarflow", link)
                rw = await _task_reward(user_id, "piarflow", link)
                await add_balance(user_id, rw, reason="За подписку Piarflow",
                                  service="piarflow", link=link)
                done_count += 1; done_sum += rw

    if ts:
        for r in await check_trafsly(user_id, ts):
            if r.get("status") == "subscribed":
                aid = str(r.get("ads_id"))
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                await add_balance(user_id, rw, reason="За подписку Trafsly",
                                  service="trafsly", link=aid)
                done_count += 1; done_sum += rw

    if tf:
        for r in await check_traffy(user_id, tf):
            if r.get("status") == "completed":
                aid = str(r.get("assignment_id"))
                await mark_subscribed(user_id, "traffy", aid)
                rw = await _task_reward(user_id, "traffy", aid)
                await add_balance(user_id, rw, reason="За подписку Traffy",
                                  service="traffy", link=aid)
                done_count += 1; done_sum += rw

    # TGrass пачкой
    if await check_tgrass(user_id):
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch(
                "SELECT assignment_id, link, reward FROM sponsor_tasks WHERE user_id=$1 AND service='tgrass' AND status!='subscribed'",
                user_id)
            for r in rows:
                await add_balance(user_id, r["reward"], reason="За подписку TGrass",
                                  service="tgrass", link=r["link"])
                done_count += 1; done_sum += r["reward"]
            await db.execute(
                "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='tgrass'",
                user_id)

    # Botohub пачкой
    if await check_botohub(user_id):
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch(
                "SELECT assignment_id, link, reward FROM sponsor_tasks WHERE user_id=$1 AND service='botohub' AND status!='subscribed'",
                user_id)
            for r in rows:
                await add_balance(user_id, r["reward"], reason="За подписку Botohub",
                                  service="botohub", link=r["link"])
                done_count += 1; done_sum += r["reward"]
            await db.execute(
                "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service='botohub'",
                user_id)

    # Реферальные
    if done_sum > 0:
        u = await get_user(user_id)
        if u and u["referrer_id"]:
            ref_bonus = done_sum * REF_PERCENT / 100
            await add_balance(u["referrer_id"], ref_bonus,
                              reason=f"Реферальный бонус от {user_id}")
            pool = await get_pool()
            async with pool.acquire() as db:
                await db.execute(
                    "UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2",
                    ref_bonus, u["referrer_id"])
            try:
                await bot.send_message(u["referrer_id"], f"💸 +${ref_bonus:.4f} с реферала")
            except Exception:
                pass

    return done_count, done_sum


# ========== КЛАВИАТУРЫ ==========
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 Заработать", callback_data="earn")
    kb.button(text="💰 Баланс", callback_data="balance")
    kb.button(text="💸 Вывести", callback_data="withdraw")
    kb.button(text="👥 Рефералы", callback_data="refs")
    kb.adjust(2, 2)
    return kb.as_markup()


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 Спонсор {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="⬅️ В меню", callback_data="menu")
    kb.adjust(1)
    return kb.as_markup()


def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Изменить награду", callback_data="adm_reward")
    kb.button(text="👥 Макс. спонсоров", callback_data="adm_max")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🔍 Инфо о юзере", callback_data="adm_userinfo")
    kb.button(text="📋 Логи начислений", callback_data="adm_logs")
    kb.button(text="🚫 Бан / Разбан", callback_data="adm_ban")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Заявки на вывод", callback_data="adm_withdraws")
    kb.adjust(1)
    return kb.as_markup()


# ========== ХЭНДЛЕРЫ ==========
_last = {}
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
        f"💰 Баланс: <b>${bal:.4f}</b>\n"
        f"💵 За подписку: <b>${await get_reward():.4f}</b>\n"
        f"📤 Минималка вывода: <b>${MIN_WITHDRAW:.2f}</b>\n"
        f"👥 Реферальный бонус: <b>{REF_PERCENT}%</b>\n\n"
        f"Выбери действие 👇",
        reply_markup=main_menu()
    )


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    args = msg.text.split(maxsplit=2)
    if len(args) < 2:
        await msg.answer("Формат: <code>/ban 123456789 причина</code>")
        return
    try:
        target = int(args[1])
    except Exception:
        await msg.answer("❌ ID должен быть числом")
        return
    reason = args[2] if len(args) > 2 else "без причины"
    await ban_user(target, reason)
    await msg.answer(f"🚫 Юзер <code>{target}</code> забанен\nПричина: {reason}")


@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    args = msg.text.split()
    if len(args) < 2:
        await msg.answer("Формат: <code>/unban 123456789</code>")
        return
    try:
        target = int(args[1])
    except Exception:
        await msg.answer("❌ ID должен быть числом")
        return
    await unban_user(target)
    await msg.answer(f"✅ Юзер <code>{target}</code> разбанен")


@dp.callback_query(F.data == "menu")
async def cb_menu(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    try:
        await cq.message.edit_text(
            f"💰 Баланс: <b>${bal:.4f}</b>\n"
            f"💵 За подписку: <b>${await get_reward():.4f}</b>",
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
            f"💰 Твой баланс: <b>${bal:.4f}</b>\n"
            f"📤 Минималка: <b>${MIN_WITHDRAW:.2f}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "earn")
async def cb_earn(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if await is_banned(uid):
        await cq.answer("🚫 Ты забанен", show_alert=True)
        return
    await cq.answer("Подбираю спонсоров…")
    logging.info(f"EARN pressed by {uid}")

    new_links = await collect_sponsors(cq.from_user)
    pending = await pending_links(uid)
    all_links = list(dict.fromkeys(pending + new_links))[:await get_max_sponsors()]

    if not all_links:
        try:
            await cq.message.edit_text("😕 Заданий нет, попробуй позже.", reply_markup=main_menu())
        except Exception:
            pass
        return

    try:
        await cq.message.edit_text(
            f"🎯 Подпишись на <b>{len(all_links)}</b> канал(ов).\n"
            f"💵 За каждого — <b>${await get_reward():.4f}</b>.\n\n"
            f"После подписки жми «Проверить».",
            reply_markup=sponsors_kb(all_links),
            disable_web_page_preview=True
        )
    except Exception:
        pass


@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if await is_banned(uid):
        await cq.answer("🚫 Ты забанен", show_alert=True)
        return
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяю…")

    cnt, sm = await check_all(uid)
    left = await pending_count(uid)

    if cnt > 0:
        u = await get_user(uid)
        text = (f"✅ Засчитано подписок: <b>{cnt}</b>\n"
                f"💵 Начислено: <b>${sm:.4f}</b>\n"
                f"💰 Баланс: <b>${u['balance']:.4f}</b>")
        if left > 0:
            text += f"\n\n⚠️ Осталось: <b>{left}</b>"
        try:
            await cq.message.edit_text(text, reply_markup=main_menu())
        except Exception:
            try:
                await cq.message.answer(text, reply_markup=main_menu())
            except Exception:
                pass
    else:
        await cq.answer(f"❌ Ничего не засчитано. Осталось: {left}", show_alert=True)


@dp.callback_query(F.data == "refs")
async def cb_refs(cq: types.CallbackQuery):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{cq.from_user.id}"
    u = await get_user(cq.from_user.id)
    earned = u["referred_earned"] if u else 0
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", cq.from_user.id)
        refs_count = row["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"👥 <b>Реферальная система</b>\n\n"
            f"Ты получаешь <b>{REF_PERCENT}%</b> от заработка приглашённых.\n\n"
            f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
            f"👤 Приглашено: <b>{refs_count}</b>\n"
            f"💵 Заработано: <b>${earned:.4f}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(cq: types.CallbackQuery):
    if await is_banned(cq.from_user.id):
        await cq.answer("🚫 Ты забанен", show_alert=True)
        return
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await cq.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}. У тебя ${bal:.4f}", show_alert=True)
        return
    _states[cq.from_user.id] = "await_wallet"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💸 <b>Вывод средств</b>\n\n"
            f"💰 Баланс: <b>${bal:.4f}</b>\n\n"
            f"Отправь одним сообщением <b>сумму и ссылку на счёт в @CryptoBot</b>.\n\n"
            f"<i>Пример:</i>\n"
            f"<code>0.5 https://t.me/CryptoBot?start=IVxxxxx</code>"
        )
    except Exception:
        pass


@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_wallet":
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.answer("❌ Формат: <code>сумма ссылка_на_счёт</code>")
            return
        try:
            amount = float(parts[0].replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму")
            return
        if amount <= 0:
            await msg.answer("❌ Сумма должна быть положительной")
            return
        wallet = parts[1].strip()

        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}")
            return
        if amount > bal:
            await msg.answer(f"❌ У тебя только ${bal:.4f}")
            return

        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", amount, uid)
            row = await db.fetchrow(
                "INSERT INTO withdrawals (user_id, amount, wallet, created_at) VALUES ($1,$2,$3,$4) RETURNING id",
                uid, amount, wallet, time.time())
            wid = row["id"]
        await log_balance(uid, -amount, reason=f"Заявка на вывод #{wid}")

        _states.pop(uid, None)
        await msg.answer(
            f"✅ Заявка №<b>{wid}</b> создана!\n"
            f"💵 Сумма: <b>${amount:.4f}</b>\n"
            f"⏳ Ожидай оплаты от админа (обычно до 24ч)."
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Оплачено", callback_data=f"wd_done_{wid}")
        kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
        kb.adjust(2)
        try:
            await bot.send_message(
                ADMIN_ID,
                f"💸 <b>Новая заявка на вывод #{wid}</b>\n\n"
                f"👤 Юзер: <a href='tg://user?id={uid}'>{msg.from_user.full_name}</a> (<code>{uid}</code>)\n"
                f"💰 Сумма: <b>${amount:.4f}</b>\n"
                f"🔗 Счёт: {wallet}",
                reply_markup=kb.as_markup(),
                disable_web_page_preview=True
            )
        except Exception as e:
            logging.error(f"Send to admin: {e}")
        return

    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Введи число")
            return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>${val:.4f}</b>", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            val = int(msg.text)
        except Exception:
            await msg.answer("❌ Введи целое число")
            return
        await set_setting("max_sponsors", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Макс. спонсоров: <b>{val}</b>", reply_markup=admin_menu())
        return

    if state == "adm_userinfo":
        target = msg.text.strip()
        target_uid = None
        if target.startswith("@") or not target.isdigit():
            target_uid = await find_user_by_username(target.lstrip("@"))
            if not target_uid:
                await msg.answer(f"❌ Юзер <code>{target}</code> не найден")
                return
        else:
            target_uid = int(target)
        _states.pop(uid, None)
        await show_user_info(msg, target_uid)
        return

    if state == "adm_logs":
        target = msg.text.strip()
        target_uid = None
        if target.startswith("@") or not target.isdigit():
            target_uid = await find_user_by_username(target.lstrip("@"))
            if not target_uid:
                await msg.answer(f"❌ Юзер <code>{target}</code> не найден")
                return
        else:
            target_uid = int(target)
        _states.pop(uid, None)
        await show_user_logs(msg, target_uid)
        return

    if state == "adm_ban":
        parts = msg.text.split()
        if len(parts) < 1:
            await msg.answer("❌ Формат: <code>123456789</code> или <code>123456789 причина</code>")
            return
        try:
            target = int(parts[0])
        except Exception:
            await msg.answer("❌ Первым должен быть ID (число)")
            return
        reason = " ".join(parts[1:]) if len(parts) > 1 else "без причины"
        await ban_user(target, reason)
        _states.pop(uid, None)
        await msg.answer(
            f"🚫 Юзер <code>{target}</code> забанен\n"
            f"📝 Причина: {reason}",
            reply_markup=admin_menu()
        )
        try:
            await bot.send_message(target, f"🚫 Ты забанен в боте\nПричина: {reason}")
        except Exception:
            pass
        return

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Начинаю рассылку…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users WHERE banned=0")
        ok = 0; fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"])
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ Отправлено: {ok}\n❌ Ошибок: {fail}", reply_markup=admin_menu())
        return


async def show_user_info(msg, target_uid):
    u = await get_user(target_uid)
    if not u:
        await msg.answer(f"❌ Юзер <code>{target_uid}</code> не найден в БД")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        # подписки по сетям
        rows = await db.fetch(
            "SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed' GROUP BY service",
            target_uid)
        subs_by_service = "\n".join([f"  • {r['service']}: <b>{r['c']}</b>" for r in rows]) or "  нет"
        # рефералы
        refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target_uid))["c"]
        # заявки на вывод
        wd = await db.fetchrow(
            "SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as s FROM withdrawals WHERE user_id=$1",
            target_uid)
        # лог начислений
        log_sum = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) as s FROM balance_log WHERE user_id=$1 AND amount>0",
            target_uid)
        # banned статус
        ban_info = await db.fetchrow(
            "SELECT banned, ban_reason FROM users WHERE user_id=$1", target_uid)

    banned_str = "🚫 ЗАБАНЕН" + (f" ({ban_info['ban_reason']})" if ban_info['ban_reason'] else "") if ban_info['banned'] else "✅ активен"

    await msg.answer(
        f"👤 <b>Юзер</b> <code>{target_uid}</code>\n"
        f"📌 Статус: {banned_str}\n\n"
        f"💰 Баланс: <b>${u['balance']:.4f}</b>\n"
        f"💵 Начислено (логи): <b>${log_sum['s']:.4f}</b>\n"
        f"👥 Рефералов: <b>{refs}</b>\n"
        f"💸 Заработал с рефов: <b>${u['referred_earned']:.4f}</b>\n"
        f"🔗 Реферер: <code>{u['referrer_id'] or '—'}</code>\n\n"
        f"📋 <b>Подписок по сетям:</b>\n{subs_by_service}\n\n"
        f"💸 Выводов: <b>{wd['c']}</b> на <b>${wd['s']:.4f}</b>",
        reply_markup=admin_menu()
    )


async def show_user_logs(msg, target_uid):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT amount, reason, service, link, created_at FROM balance_log WHERE user_id=$1 ORDER BY id DESC LIMIT 30",
            target_uid)
    if not rows:
        await msg.answer(f"📋 У юзера <code>{target_uid}</code> нет записей о начислениях")
        return

    text = f"📋 <b>Логи начислений</b> <code>{target_uid}</code>\n\n"
    import datetime
    for r in rows:
        dt = datetime.datetime.fromtimestamp(r["created_at"]).strftime("%d.%m %H:%M")
        sign = "+" if r["amount"] > 0 else ""
        link_str = f"\n   🔗 {r['link']}" if r.get("link") else ""
        text += f"[{dt}] <b>{sign}${r['amount']:.4f}</b>\n   {r['reason']}{link_str}\n\n"
    if len(text) > 4000:
        text = text[:4000] + "\n…"
    await msg.answer(text, reply_markup=admin_menu())


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💰 Текущая награда: <b>${await get_reward():.4f}</b>\n\n"
            f"Отправь новое значение (например: <code>0.005</code>)"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"👥 Текущий лимит: <b>{await get_max_sponsors()}</b>\n\n"
            f"Отправь новое число (например: <code>20</code>)"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        banned = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE banned=1"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        row = await db.fetchrow("SELECT COUNT(*) as c, COALESCE(SUM(reward),0) as s FROM sponsor_tasks WHERE status='subscribed'")
        subs_cnt, paid = row["c"], row["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n"
            f"👥 Юзеров: <b>{total}</b> (🚫 забанено: {banned})\n"
            f"💰 Суммарный баланс: <b>${balances:.4f}</b>\n"
            f"✅ Подписок: <b>{subs_cnt}</b>\n"
            f"💵 Начислено: <b>${paid:.4f}</b>\n"
            f"💸 Заявок: <b>{pend}</b>",
            reply_markup=admin_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_userinfo")
async def adm_userinfo(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_userinfo"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "🔍 <b>Инфо о юзере</b>\n\n"
            "Отправь <b>@username</b> или <b>user_id</b>:\n"
            "<code>@vasya</code> или <code>123456789</code>"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_logs")
async def adm_logs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_logs"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "📋 <b>Логи начислений</b>\n\n"
            "Отправь <b>@username</b> или <b>user_id</b> юзера:\n"
            "<code>@vasya</code> или <code>123456789</code>"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_ban")
async def adm_ban(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_ban"
    await cq.answer()
    try:
        await cq.message.edit_text(
            "🚫 <b>Бан юзера</b>\n\n"
            "Отправь <b>ID юзера</b> и опционально причину:\n"
            "<code>123456789</code>\n"
            "<code>123456789 обман системы</code>\n\n"
            "💡 Для разбана — команда <code>/unban 123456789</code>"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправь сообщение для рассылки (забаненные не получат)")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT id, user_id, amount, wallet FROM withdrawals WHERE status='pending' ORDER BY id DESC LIMIT 20")
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "💸 <b>Ожидают вывода:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} | <code>{r['user_id']}</code> | <b>${r['amount']:.4f}</b>\n{r['wallet']}\n\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu(), disable_web_page_preview=True)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    wid = int(cq.data.split("_")[-1])
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE withdrawals SET status='done' WHERE id=$1", wid)
        row = await db.fetchrow("SELECT user_id, amount FROM withdrawals WHERE id=$1", wid)
    if row:
        try:
            await bot.send_message(row["user_id"], f"✅ Заявка №{wid} на <b>${row['amount']:.4f}</b> оплачена!")
        except Exception:
            pass
    await cq.answer("Оплачено ✅")
    try:
        await cq.message.edit_text(cq.message.html_text + "\n\n✅ <b>ОПЛАЧЕНО</b>")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_decl_"))
async def wd_decl(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    wid = int(cq.data.split("_")[-1])
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id, amount, status FROM withdrawals WHERE id=$1", wid)
        if row and row["status"] == "pending":
            await db.execute("UPDATE withdrawals SET status='declined' WHERE id=$1", wid)
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2",
                             row["amount"], row["user_id"])
    if row:
        await log_balance(row["user_id"], row["amount"], reason=f"Возврат заявки №{wid}")
        try:
            await bot.send_message(row["user_id"], f"❌ Заявка №{wid} отклонена. Средства возвращены.")
        except Exception:
            pass
    await cq.answer("Отклонено ❌")
    try:
        await cq.message.edit_text(cq.message.html_text + "\n\n❌ <b>ОТКЛОНЕНО</b>")
    except Exception:
        pass


# ========== WEB + MAIN ==========
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
