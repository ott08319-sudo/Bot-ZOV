import os
import time
import asyncio
import logging
import aiosqlite
import aiohttp
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

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")

DB_PATH = "bot.db"
CHECK_COOLDOWN = 5

DEFAULT_REWARD = 0.005
DEFAULT_MAX_SPONSORS = 15
MIN_WITHDRAW = 0.1
REF_PERCENT = 50

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http: aiohttp.ClientSession | None = None

async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
    return _http


# ========== БАЗА ==========
async def init_db():
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0,
            referrer_id INTEGER,
            referred_earned REAL DEFAULT 0,
            created_at REAL)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            reward REAL DEFAULT 0,
            status TEXT DEFAULT 'unsubscribed',
            signature TEXT,
            created_at REAL,
            UNIQUE(user_id, service, assignment_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            wallet TEXT,
            status TEXT DEFAULT 'pending',
            created_at REAL)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT)""")
        await db.execute("INSERT OR IGNORE INTO settings (key,value) VALUES ('reward', ?)", (str(DEFAULT_REWARD),))
        await db.execute("INSERT OR IGNORE INTO settings (key,value) VALUES ('max_sponsors', ?)", (str(DEFAULT_MAX_SPONSORS),))
        await db.commit()


async def get_setting(key, default=None):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as c:
            row = await c.fetchone()
            return row[0] if row else default


async def set_setting(key, value):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, str(value)))
        await db.commit()


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def register_user(user_id, referrer_id=None):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,)) as c:
            if await c.fetchone():
                return
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            async with db.execute("SELECT user_id FROM users WHERE user_id=?", (referrer_id,)) as c:
                if not await c.fetchone():
                    referrer_id = None
        await db.execute("INSERT INTO users (user_id, referrer_id, created_at) VALUES (?,?,?)",
                         (user_id, referrer_id, time.time()))
        await db.commit()


async def get_user(user_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT balance, referrer_id, referred_earned FROM users WHERE user_id=?", (user_id,)) as c:
            return await c.fetchone()


async def add_balance(user_id, amount):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
        await db.commit()


async def save_sponsor(user_id, service, aid, link, reward, signature=None):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("""INSERT OR IGNORE INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, status, created_at)
            VALUES (?,?,?,?,?,?,'unsubscribed',?)""",
            (user_id, service, aid, link, reward, signature, time.time()))
        await db.commit()


async def mark_subscribed(user_id, service, aid):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=? AND service=? AND assignment_id=?",
                         (user_id, service, aid))
        await db.commit()


async def pending_tasks(user_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT service, assignment_id, link, reward, signature FROM sponsor_tasks WHERE user_id=? AND status!='subscribed'",
                              (user_id,)) as c:
            return await c.fetchall()


async def pending_count(user_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT COUNT(*) FROM sponsor_tasks WHERE user_id=? AND status!='subscribed'", (user_id,)) as c:
            return (await c.fetchone())[0]


async def pending_links(user_id):
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT link FROM sponsor_tasks WHERE user_id=? AND status!='subscribed'", (user_id,)) as c:
            return [row[0] for row in await c.fetchall()]


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
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute(
            "SELECT reward FROM sponsor_tasks WHERE user_id=? AND service=? AND assignment_id=?",
            (user_id, service, aid)) as c:
            row = await c.fetchone()
            return row[0] if row else 0.0


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
                await mark_subscribed(user_id, "piarflow", r.get("link"))
                rw = await _task_reward(user_id, "piarflow", r.get("link"))
                done_count += 1; done_sum += rw

    if ts:
        for r in await check_trafsly(user_id, ts):
            if r.get("status") == "subscribed":
                aid = str(r.get("ads_id"))
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                done_count += 1; done_sum += rw

    if tf:
        for r in await check_traffy(user_id, tf):
            if r.get("status") == "completed":
                aid = str(r.get("assignment_id"))
                await mark_subscribed(user_id, "traffy", aid)
                rw = await _task_reward(user_id, "traffy", aid)
                done_count += 1; done_sum += rw

    if await check_tgrass(user_id):
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await db.execute("PRAGMA busy_timeout=5000;")
            async with db.execute(
                "SELECT COUNT(*), COALESCE(SUM(reward),0) FROM sponsor_tasks WHERE user_id=? AND service='tgrass' AND status!='subscribed'",
                (user_id,)) as c:
                cnt, sm = await c.fetchone()
            await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=? AND service='tgrass'", (user_id,))
            await db.commit()
            done_count += cnt; done_sum += sm

    if await check_botohub(user_id):
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await db.execute("PRAGMA busy_timeout=5000;")
            async with db.execute(
                "SELECT COUNT(*), COALESCE(SUM(reward),0) FROM sponsor_tasks WHERE user_id=? AND service='botohub' AND status!='subscribed'",
                (user_id,)) as c:
                cnt, sm = await c.fetchone()
            await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=? AND service='botohub'", (user_id,))
            await db.commit()
            done_count += cnt; done_sum += sm

    if done_sum > 0:
        await add_balance(user_id, done_sum)
        u = await get_user(user_id)
        if u and u[1]:
            ref_bonus = done_sum * REF_PERCENT / 100
            await add_balance(u[1], ref_bonus)
            async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
                await db.execute("PRAGMA busy_timeout=5000;")
                await db.execute("UPDATE users SET referred_earned = referred_earned + ? WHERE user_id=?", (ref_bonus, u[1]))
                await db.commit()
            try:
                await bot.send_message(u[1], f"💸 +${ref_bonus:.4f} с реферала")
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
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Заявки на вывод", callback_data="adm_withdraws")
    kb.adjust(1)
    return kb.as_markup()


# ========== ХЭНДЛЕРЫ ==========
_last = {}
_states = {}


@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split()
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try:
            ref_id = int(args[1].replace("ref", ""))
        except Exception:
            pass

    await register_user(msg.from_user.id, ref_id)

    u = await get_user(msg.from_user.id)
    bal = u[0] if u else 0
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


@dp.callback_query(F.data == "menu")
async def cb_menu(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u[0] if u else 0
    await cq.message.edit_text(
        f"💰 Баланс: <b>${bal:.4f}</b>\n"
        f"💵 За подписку: <b>${await get_reward():.4f}</b>",
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "balance")
async def cb_balance(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u[0] if u else 0
    await cq.answer()
    await cq.message.edit_text(
        f"💰 Твой баланс: <b>${bal:.4f}</b>\n"
        f"📤 Минималка: <b>${MIN_WITHDRAW:.2f}</b>",
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "earn")
async def cb_earn(cq: types.CallbackQuery):
    uid = cq.from_user.id
    await cq.answer("Подбираю спонсоров…")
    logging.info(f"EARN pressed by {uid}")

    new_links = await collect_sponsors(cq.from_user)
    logging.info(f"New from API: {len(new_links)}")

    pending = await pending_links(uid)
    all_links = list(dict.fromkeys(pending + new_links))[:await get_max_sponsors()]
    logging.info(f"Total to show: {len(all_links)}")

    if not all_links:
        await cq.message.edit_text("😕 Заданий нет, попробуй позже.", reply_markup=main_menu())
        return

    await cq.message.edit_text(
        f"🎯 Подпишись на <b>{len(all_links)}</b> канал(ов).\n"
        f"💵 За каждого — <b>${await get_reward():.4f}</b>.\n\n"
        f"После подписки жми «Проверить».",
        reply_markup=sponsors_kb(all_links),
        disable_web_page_preview=True
    )


@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
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
                f"💰 Баланс: <b>${u[0]:.4f}</b>")
        if left > 0:
            text += f"\n\n⚠️ Осталось: <b>{left}</b>"
        try:
            await cq.message.edit_text(text, reply_markup=main_menu())
        except Exception:
            await cq.message.answer(text, reply_markup=main_menu())
    else:
        await cq.answer(f"❌ Ничего не засчитано. Осталось: {left}", show_alert=True)


@dp.callback_query(F.data == "refs")
async def cb_refs(cq: types.CallbackQuery):
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{cq.from_user.id}"
    u = await get_user(cq.from_user.id)
    earned = u[2] if u else 0
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE referrer_id=?", (cq.from_user.id,)) as c:
            refs_count = (await c.fetchone())[0]
    await cq.answer()
    await cq.message.edit_text(
        f"👥 <b>Реферальная система</b>\n\n"
        f"Ты получаешь <b>{REF_PERCENT}%</b> от заработка приглашённых.\n\n"
        f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"👤 Приглашено: <b>{refs_count}</b>\n"
        f"💵 Заработано: <b>${earned:.4f}</b>",
        reply_markup=main_menu()
    )


@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u[0] if u else 0
    if bal < MIN_WITHDRAW:
        await cq.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}. У тебя ${bal:.4f}", show_alert=True)
        return
    _states[cq.from_user.id] = "await_wallet"
    await cq.answer()
    await cq.message.edit_text(
        f"💸 <b>Вывод средств</b>\n\n"
        f"💰 Баланс: <b>${bal:.4f}</b>\n\n"
        f"Отправь одним сообщением <b>сумму и ссылку на счёт в @CryptoBot</b>.\n\n"
        f"<i>Пример:</i>\n"
        f"<code>0.5 https://t.me/CryptoBot?start=IVxxxxx</code>"
    )


# ========== ТЕКСТОВЫЙ ХЭНДЛЕР ==========
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
        wallet = parts[1].strip()

        u = await get_user(uid)
        bal = u[0] if u else 0
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}")
            return
        if amount > bal:
            await msg.answer(f"❌ У тебя только ${bal:.4f}")
            return

        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            await db.execute("PRAGMA busy_timeout=5000;")
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, uid))
            cur = await db.execute(
                "INSERT INTO withdrawals (user_id, amount, wallet, created_at) VALUES (?,?,?,?)",
                (uid, amount, wallet, time.time()))
            wid = cur.lastrowid
            await db.commit()

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
        await msg.answer(f"✅ Награда за подписку: <b>${val:.4f}</b>", reply_markup=admin_menu())
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

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢 Начинаю рассылку…")
        async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
            async with db.execute("SELECT user_id FROM users") as c:
                rows = await c.fetchall()
        ok = 0; fail = 0
        for (u_id,) in rows:
            try:
                await msg.copy_to(u_id)
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ Отправлено: {ok}\n❌ Ошибок: {fail}", reply_markup=admin_menu())
        return


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    await cq.message.edit_text(
        f"💰 Текущая награда: <b>${await get_reward():.4f}</b>\n\n"
        f"Отправь новое значение (например: <code>0.004</code>)"
    )


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    await cq.message.edit_text(
        f"👥 Текущий лимит: <b>{await get_max_sponsors()}</b>\n\n"
        f"Отправь новое число (например: <code>20</code>)"
    )


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total = (await c.fetchone())[0]
        async with db.execute("SELECT COALESCE(SUM(balance),0) FROM users") as c:
            balances = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(reward),0) FROM sponsor_tasks WHERE status='subscribed'") as c:
            subs_cnt, paid = await c.fetchone()
        async with db.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'") as c:
            pend = (await c.fetchone())[0]
    await cq.answer()
    await cq.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Юзеров: <b>{total}</b>\n"
        f"💰 Суммарный баланс: <b>${balances:.4f}</b>\n"
        f"✅ Подписок: <b>{subs_cnt}</b>\n"
        f"💵 Начислено: <b>${paid:.4f}</b>\n"
        f"💸 Заявок (pending): <b>{pend}</b>",
        reply_markup=admin_menu()
    )


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    await cq.message.edit_text("📢 Отправь сообщение для рассылки (можно с фото/видео)")


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        async with db.execute("SELECT id, user_id, amount, wallet FROM withdrawals WHERE status='pending' ORDER BY id DESC LIMIT 20") as c:
            rows = await c.fetchall()
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "💸 <b>Ожидают вывода:</b>\n\n"
    for wid, u_id, amount, wallet in rows:
        text += f"#{wid} | <code>{u_id}</code> | <b>${amount:.4f}</b>\n{wallet}\n\n"
    await cq.answer()
    await cq.message.edit_text(text, reply_markup=admin_menu(), disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID: return
    wid = int(cq.data.split("_")[-1])
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("UPDATE withdrawals SET status='done' WHERE id=?", (wid,))
        async with db.execute("SELECT user_id, amount FROM withdrawals WHERE id=?", (wid,)) as c:
            row = await c.fetchone()
        await db.commit()
    if row:
        try:
            await bot.send_message(row[0], f"✅ Заявка №{wid} на <b>${row[1]:.4f}</b> оплачена!")
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
    async with aiosqlite.connect(DB_PATH, timeout=30.0) as db:
        await db.execute("PRAGMA busy_timeout=5000;")
        async with db.execute("SELECT user_id, amount, status FROM withdrawals WHERE id=?", (wid,)) as c:
            row = await c.fetchone()
        if row and row[2] == "pending":
            await db.execute("UPDATE withdrawals SET status='declined' WHERE id=?", (wid,))
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (row[1], row[0]))
            await db.commit()
            try:
                await bot.send_message(row[0], f"❌ Заявка №{wid} отклонена. Средства возвращены на баланс.")
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
