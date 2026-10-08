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
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")

CHECK_COOLDOWN = 3
DEFAULT_REWARD = 0.005
DEFAULT_MAX_SPONSORS = 50
MIN_WITHDRAW = 0.5  # ⚡ ИЗМЕНЕНО: было 0.1

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_http: aiohttp.ClientSession | None = None
_pool: asyncpg.Pool | None = None


async def http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        _http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15, connect=5),
            connector=connector,
        )
    return _http


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=10,
        )
    return _pool


def fmt_money(value: float) -> str:
    s = f"{value:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


# ============================================================
# БАЗА
# ============================================================
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance DOUBLE PRECISION DEFAULT 0,
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
        # ⚡ Таблица показанных ссылок (для защиты от повторов Botohub)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS shown_links (
                user_id BIGINT,
                link TEXT,
                service TEXT,
                shown_at DOUBLE PRECISION,
                PRIMARY KEY (user_id, link)
            )
        """)

        # Индексы
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_user_status ON sponsor_tasks (user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_service ON sponsor_tasks (service)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users (balance DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wd_status ON withdrawals (status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_shown_user ON shown_links (user_id)")

        await db.execute(
            "INSERT INTO settings (key,value) VALUES ('reward',$1) "
            "ON CONFLICT (key) DO NOTHING",
            str(DEFAULT_REWARD)
        )
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) "
            "ON CONFLICT (key) DO NOTHING",
            str(DEFAULT_MAX_SPONSORS)
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
            "ON CONFLICT (key) DO UPDATE SET value=$2",
            key, str(value)
        )


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def register_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO users (user_id, created_at) VALUES ($1, $2) "
            "ON CONFLICT (user_id) DO NOTHING",
            user_id, time.time()
        )


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", user_id)


async def add_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id=$2",
            amount, user_id
        )


async def was_shown(user_id, link):
    """Проверяет — показывалась ли уже эта ссылка юзеру."""
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT 1 FROM shown_links WHERE user_id=$1 AND link=$2",
            user_id, link
        )
        return row is not None


async def mark_shown(user_id, link, service):
    """Помечает ссылку как показанную."""
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO shown_links (user_id, link, service, shown_at) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            user_id, link, service, time.time()
        )


async def save_sponsor(user_id, service, aid, link, reward):
    """Сохраняет задание. Возвращает True если новая, False если уже была."""
    pool = await get_pool()
    async with pool.acquire() as db:
        result = await db.execute("""
            INSERT INTO sponsor_tasks
                (user_id, service, assignment_id, link, reward, status, created_at)
            VALUES ($1,$2,$3,$4,$5,'unsubscribed',$6)
            ON CONFLICT (user_id, service, assignment_id) DO NOTHING
        """, user_id, service, aid, link, reward, time.time())
        return result == "INSERT 0 1"


async def mark_subscribed(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' "
            "WHERE user_id=$1 AND service=$2 AND assignment_id=$3",
            user_id, service, aid
        )


async def get_all_pending(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT service, assignment_id, link, reward
            FROM sponsor_tasks
            WHERE user_id=$1 AND status='unsubscribed'
            ORDER BY id ASC
        """, user_id)
        return [dict(r) for r in rows]


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
# API: PIARFLOW
# ============================================================
async def get_piarflow(user_id):
    if not PIARFLOW_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": user_id,
                  "max_sponsors": await get_max_sponsors()},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}"}
        ) as r:
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        logging.error(f"Piarflow: {e}")
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
            json={"tg_user_id": user_id, "tg_login": username or "",
                  "lang": "ru", "is_premium": False},
            headers={"Auth": TGRASS_API_KEY}
        ) as r:
            d = await r.json()
            if d.get("status") == "not_ok":
                return d.get("offers", [])
    except Exception as e:
        logging.error(f"TGrass: {e}")
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
                for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
                    v = d.get(field)
                    if isinstance(v, list):
                        return v
    except Exception as e:
        logging.error(f"Botohub: {e}")
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
        logging.error(f"Trafsly: {e}")
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
# СБОР СПОНСОРОВ (БЕЗ ПОВТОРОВ)
# ============================================================
async def collect_sponsors(user):
    """Параллельно опрашивает все 4 сети.
    Удаляет старых unsubscribed + проверяет через shown_links."""
    uid = user.id
    un = user.username or ""
    reward = await get_reward()

    # ⚡ 1. УДАЛЯЕМ старых unsubscribed
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "DELETE FROM sponsor_tasks WHERE user_id=$1 AND status='unsubscribed'",
            uid
        )

    # ⚡ 2. ПАРАЛЛЕЛЬНЫЙ опрос всех API
    results = await asyncio.gather(
        get_piarflow(uid),
        get_tgrass(uid, un),
        get_botohub(uid),
        get_trafsly(uid, un),
        return_exceptions=True,
    )

    piarflow = results[0] if not isinstance(results[0], Exception) else []
    tgrass = results[1] if not isinstance(results[1], Exception) else []
    botohub = results[2] if not isinstance(results[2], Exception) else []
    trafsly = results[3] if not isinstance(results[3], Exception) else []

    # ⚡ 3. Сохраняем БЕЗ дублей (проверка через shown_links)
    save_tasks = []

    # Piarflow
    for x in piarflow:
        link = x.get("link")
        if link and not await was_shown(uid, link):
            save_tasks.append(save_sponsor(uid, "piarflow", link, link, reward))
            await mark_shown(uid, link, "piarflow")

    # TGrass
    for x in tgrass:
        link = x.get("link")
        if link and not await was_shown(uid, link):
            save_tasks.append(save_sponsor(uid, "tgrass", str(x.get("offer_id")), link, reward))
            await mark_shown(uid, link, "tgrass")

    # Botohub — с защитой от повторов
    for t in botohub:
        link = None
        aid = None
        if isinstance(t, str):
            link, aid = t, t
        elif isinstance(t, dict):
            link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
            aid = t.get("id") or t.get("task_id") or link
        if link and not await was_shown(uid, link):
            save_tasks.append(save_sponsor(uid, "botohub", str(aid) if aid else link, link, reward))
            await mark_shown(uid, link, "botohub")

    # Trafsly
    for x in trafsly:
        link = x.get("link")
        aid = x.get("ads_id")
        if link and not await was_shown(uid, link):
            save_tasks.append(save_sponsor(uid, "trafsly", str(aid) if aid else link, link, reward))
            await mark_shown(uid, link, "trafsly")

    if save_tasks:
        await asyncio.gather(*save_tasks, return_exceptions=True)


# ============================================================
# ПРОВЕРКА
# ============================================================
async def check_all(user_id):
    tasks = await get_all_pending(user_id)
    if not tasks:
        return 0, 0.0

    pf, ts = [], []
    for t in tasks:
        if t["service"] == "piarflow" and t["link"]:
            pf.append(t["link"])
        elif t["service"] == "trafsly" and t["assignment_id"]:
            ts.append(t["assignment_id"])

    done_count = 0
    done_sum = 0.0

    tgrass_res, botohub_res = await asyncio.gather(
        check_tgrass(user_id),
        check_botohub(user_id),
        return_exceptions=True,
    )

    if pf:
        for r in await check_piarflow(user_id, pf):
            if r.get("status") in ("subscribed", "not_counted"):
                link = r.get("link")
                await mark_subscribed(user_id, "piarflow", link)
                rw = await _task_reward(user_id, "piarflow", link)
                done_count += 1
                done_sum += rw

    if ts:
        for r in await check_trafsly(user_id, ts):
            if r.get("status") == "subscribed":
                aid = str(r.get("ads_id"))
                await mark_subscribed(user_id, "trafsly", aid)
                rw = await _task_reward(user_id, "trafsly", aid)
                done_count += 1
                done_sum += rw

    if tgrass_res is True:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow(
                "SELECT COUNT(*) as c, COALESCE(SUM(reward),0) as s "
                "FROM sponsor_tasks WHERE user_id=$1 AND service='tgrass' AND status!='subscribed'",
                user_id
            )
            await db.execute(
                "UPDATE sponsor_tasks SET status='subscribed' "
                "WHERE user_id=$1 AND service='tgrass'",
                user_id
            )
            done_count += row["c"]
            done_sum += row["s"]

    if botohub_res is True:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow(
                "SELECT COUNT(*) as c, COALESCE(SUM(reward),0) as s "
                "FROM sponsor_tasks WHERE user_id=$1 AND service='botohub' AND status!='subscribed'",
                user_id
            )
            await db.execute(
                "UPDATE sponsor_tasks SET status='subscribed' "
                "WHERE user_id=$1 AND service='botohub'",
                user_id
            )
            done_count += row["c"]
            done_sum += row["s"]

    if done_sum > 0:
        await add_balance(user_id, done_sum)

    return done_count, done_sum


# ============================================================
# КЛАВИАТУРЫ
# ============================================================
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 Заработать", callback_data="earn")
    kb.button(text="💰 Баланс", callback_data="balance")
    kb.button(text="💸 Вывести", callback_data="withdraw")
    kb.adjust(2, 1)
    return kb.as_markup()


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 Спонсор {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.adjust(1)
    return kb.as_markup()


def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="💰 Изменить награду", callback_data="adm_reward")
    kb.button(text="💸 Заявки на вывод", callback_data="adm_withdraws")
    kb.adjust(1)
    return kb.as_markup()


# ============================================================
# ХЭНДЛЕРЫ
# ============================================================
_last = {}
_states = {}


@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    await register_user(msg.from_user.id)

    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0

    await msg.answer(
        f"👋 Привет!\n\n"
        f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
        f"💵 За подписку: <b>${fmt_money(await get_reward())}</b>\n\n"
        f"⏳ Подбираю спонсоров…"
    )

    await collect_sponsors(msg.from_user)
    tasks = await get_all_pending(msg.from_user.id)

    if not tasks:
        await msg.answer(
            "😕 <b>Заданий нет</b>\n\nВозвращайся позже.",
            reply_markup=main_menu()
        )
        return

    links = [t["link"] for t in tasks]
    await msg.answer(
        f"📌 <b>Подпишись на {len(links)} канал(ов)</b>\n\n"
        f"💵 За каждого — <b>${fmt_money(await get_reward())}</b>.\n\n"
        f"После подписки жми «✅ Я подписался».",
        reply_markup=sponsors_kb(links),
        disable_web_page_preview=True
    )


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.callback_query(F.data == "earn")
async def cb_earn(cq: types.CallbackQuery):
    uid = cq.from_user.id
    await cq.answer("Подбираю…")

    await collect_sponsors(cq.from_user)
    tasks = await get_all_pending(uid)

    if not tasks:
        try:
            await cq.message.edit_text(
                "😕 <b>Заданий нет</b>\n\nВозвращайся позже.",
                reply_markup=main_menu()
            )
        except Exception:
            pass
        return

    links = [t["link"] for t in tasks]
    try:
        await cq.message.edit_text(
            f"📌 <b>Подпишись на {len(links)} канал(ов)</b>\n\n"
            f"💵 За каждого — <b>${fmt_money(await get_reward())}</b>.\n\n"
            f"После подписки жми «✅ Я подписался».",
            reply_markup=sponsors_kb(links),
            disable_web_page_preview=True
        )
    except Exception:
        pass


@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд")
        return
    _last[uid] = time.time()
    await cq.answer("Проверяю…")

    cnt, sm = await check_all(uid)
    u = await get_user(uid)
    bal = u["balance"] if u else 0

    if cnt > 0:
        text = (f"✅ <b>Засчитано: {cnt}</b>\n"
                f"💵 +${fmt_money(sm)}\n"
                f"💰 Баланс: <b>${fmt_money(bal)}</b>")
        try:
            await cq.message.edit_text(text, reply_markup=main_menu())
        except Exception:
            try:
                await cq.message.answer(text, reply_markup=main_menu())
            except Exception:
                pass
    else:
        await cq.answer("❌ Ничего не засчитано.", show_alert=True)


@dp.callback_query(F.data == "balance")
async def cb_balance(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💰 Баланс: <b>${fmt_money(bal)}</b>\n"
            f"📤 Минимум: <b>${MIN_WITHDRAW:.2f}</b>",
            reply_markup=main_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    if bal < MIN_WITHDRAW:
        await cq.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}. У тебя ${fmt_money(bal)}", show_alert=True)
        return
    _states[cq.from_user.id] = "await_wallet"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💸 <b>Вывод</b>\n\n"
            f"💰 Баланс: <b>${fmt_money(bal)}</b>\n\n"
            f"Отправь: <code>сумма ссылка_на_CryptoBot</code>\n\n"
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
            await msg.answer("❌ Формат: <code>сумма ссылка</code>")
            return
        try:
            amount = float(parts[0].replace(",", "."))
        except Exception:
            await msg.answer("❌ Не понял сумму")
            return
        if amount <= 0:
            await msg.answer("❌ Сумма > 0")
            return
        wallet = parts[1].strip()

        u = await get_user(uid)
        bal = u["balance"] if u else 0
        if amount < MIN_WITHDRAW:
            await msg.answer(f"❌ Минимум ${MIN_WITHDRAW:.2f}")
            return
        if amount > bal:
            await msg.answer(f"❌ У тебя только ${fmt_money(bal)}")
            return

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
        await msg.answer(f"✅ Заявка №<b>{wid}</b> на <b>${fmt_money(amount)}</b> создана!")

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Оплачено", callback_data=f"wd_done_{wid}")
        kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
        kb.adjust(2)
        try:
            await bot.send_message(
                ADMIN_ID,
                f"💸 <b>Заявка #{wid}</b>\n"
                f"👤 <a href='tg://user?id={uid}'>{msg.from_user.full_name}</a>\n"
                f"💰 <b>${fmt_money(amount)}</b>\n🔗 {wallet}",
                reply_markup=kb.as_markup(),
                disable_web_page_preview=True
            )
        except Exception as e:
            logging.error(f"admin msg: {e}")
        return

    if state == "adm_reward":
        try:
            val = float(msg.text.replace(",", "."))
        except Exception:
            await msg.answer("❌ Введи число")
            return
        await set_setting("reward", val)
        _states.pop(uid, None)
        await msg.answer(f"✅ Награда: <b>${fmt_money(val)}</b>", reply_markup=admin_menu())
        return


# ============================================================
# АДМИН
# ============================================================
@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        row = await db.fetchrow(
            "SELECT COUNT(*) as c, COALESCE(SUM(reward),0) as s "
            "FROM sponsor_tasks WHERE status='subscribed'"
        )
        subs_cnt, paid = row["c"], row["s"]
        pend = (await db.fetchrow(
            "SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"
        ))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n"
            f"👥 Юзеров: <b>{total}</b>\n"
            f"💰 Баланс: <b>${fmt_money(balances)}</b>\n"
            f"✅ Подписок: <b>{subs_cnt}</b>\n"
            f"💵 Начислено: <b>${fmt_money(paid)}</b>\n"
            f"💸 Заявок: <b>{pend}</b>",
            reply_markup=admin_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"💰 Награда: <b>${fmt_money(await get_reward())}</b>\n\nОтправь новое:"
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT id, user_id, amount, wallet FROM withdrawals "
            "WHERE status='pending' ORDER BY id DESC LIMIT 20"
        )
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "💸 <b>Заявки:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} | <code>{r['user_id']}</code> | <b>${fmt_money(r['amount'])}</b>\n{r['wallet']}\n\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text, reply_markup=admin_menu(), disable_web_page_preview=True)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
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
    await cq.answer("✅")


@dp.callback_query(F.data.startswith("wd_decl_"))
async def wd_decl(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
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
    await cq.answer("❌")


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
    try:
        import uvloop
        uvloop.install()
        logging.info("✅ uvloop установлен")
    except ImportError:
        logging.info("⚠️ uvloop не установлен")

    await init_db()
    await start_web()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
