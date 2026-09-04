import os
import asyncio
import time
from datetime import datetime
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, TelegramObject
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
import aiosqlite

from database import init_db, get_user, register_user, update_balance, DB_PATH
from piarflow_service import get_sponsors_for_user, check_subscriptions_on_service

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# --- Состояния FSM ---
class UserStates(StatesGroup):
    enter_promo = State()
    enter_withdraw_amount = State()
    admin_give_stars_id = State()
    admin_give_stars_amount = State()

# --- Safe Middleware ---
class SponsorGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)
            
        if isinstance(event, CallbackQuery) and event.data in ("check_sub", "to_main"):
            return await handler(event, data)

        try:
            sponsors = await get_sponsors_for_user(user.id)
            if sponsors:
                is_subbed = await check_subscriptions_on_service("piarflow", user.id)
                if not is_subbed:
                    kb = []
                    text = "⚠️ **Для доступа к боту подпишитесь на спонсоров:**\n\n"
                    for idx, s in enumerate(sponsors, 1):
                        text += f"{idx}. Канал #{idx}\n"
                        kb.append([InlineKeyboardButton(text=f"📢 Подписаться #{idx}", url=s["link"])])
                    kb.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")])
                    
                    markup = InlineKeyboardMarkup(inline_keyboard=kb)
                    if isinstance(event, Message):
                        await event.answer(text, reply_markup=markup, parse_mode="Markdown")
                    elif isinstance(event, CallbackQuery) and event.message:
                        await event.message.answer(text, reply_markup=markup, parse_mode="Markdown")
                    return
        except Exception as e:
            print(f"SponsorGuard Error: {e}")

        return await handler(event, data)

# --- Клавиатуры ---
def main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Заработать звёзды")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="📋 Задания")],
            [KeyboardButton(text="🏆 Рейтинг")],
            [KeyboardButton(text="💸 Обменять звёзды")],
            [KeyboardButton(text="⚙️ Админ панель")]
        ],
        resize_keyboard=True
    )

def profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo"), InlineKeyboardButton(text="🎁 Ежедневный бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="📜 История баланса", callback_data="history")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="to_main")]
    ])

def tasks_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Выполнить клик-задание (+0.25 ⭐)", callback_data="do_simple_task")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def rating_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 По рефералам", callback_data="top_ref"), InlineKeyboardButton(text="💰 По балансу", callback_data="top_balance")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def withdraw_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="withdraw_stars")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать звезды пользователю", callback_data="adm_give")],
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="to_main")]
    ])

# --- Инициализация ---
bot = Bot(token=os.getenv("BOT_TOKEN", ""))
dp = Dispatcher(storage=MemoryStorage())

dp.message.middleware(SponsorGuardMiddleware())
dp.callback_query.middleware(SponsorGuardMiddleware())

# --- Обработчики Главного Меню ---
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    args = message.text.split() if message.text else []
    ref_id = int(args[1].replace("ref_", "")) if len(args) > 1 and args[1].startswith("ref_") and args[1].replace("ref_", "").isdigit() else None
    
    user_name = message.from_user.username or message.from_user.first_name or "User"
    await register_user(message.from_user.id, user_name, ref_id)
    
    u = await get_user(message.from_user.id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{message.from_user.id}"
    
    text = (
        f"❤️ **Добро пожаловать в DarkStar!**\n\n"
        f"🔗 Твоя ссылка: `{ref_link}`\n"
        f"💰 Награда за реферала: +3.0 ⭐\n"
        f"⭐ Ваш баланс: {u['balance'] if u else 0.0} ⭐\n"
        f"👥 Приглашено: {u['referrals_count'] if u else 0}\n"
    )
    await message.answer(text, reply_markup=main_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "💰 Заработать звёзды")
async def earn_stars(message: Message):
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{message.from_user.id}"
    text = (
        f"🚀 **Зарабатывайте звёзды, приглашая друзей!**\n\n"
        f"📢 За каждого приглашенного пользователя вы получаете **3.0 ⭐** на свой баланс.\n\n"
        f"🔗 Твоя реферальная ссылка:\n`{ref_link}`"
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    u = await get_user(message.from_user.id)
    if not u:
        await register_user(message.from_user.id, message.from_user.username or "User")
        u = await get_user(message.from_user.id)
        
    reg_date = datetime.fromtimestamp(u['created_at']).strftime('%d.%m.%Y')
    text = (
        f"👤 **Твой профиль**\n\n"
        f"ID: `{u['user_id']}`\n"
        f"📅 Зарегистрирован: {reg_date}\n\n"
        f"💬 Баланс: **{u['balance']:.2f} ⭐**\n"
        f"🏆 Всего заработано: **{u['total_earned']:.2f} ⭐**\n"
        f"👥 Приглашено друзей: **{u['referrals_count']}**\n"
        f"📋 Выполнено заданий: **{u['tasks_done']}**"
    )
    await message.answer(text, reply_markup=profile_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "📋 Задания")
async def show_tasks(message: Message):
    u = await get_user(message.from_user.id)
    text = (
        f"📋 **Доступные задания**\n\n"
        f"Выполнено: {u['tasks_done'] if u else 0}\n"
        f"💰 Награда за клик-задание: 0.25 ⭐"
    )
    await message.answer(text, reply_markup=tasks_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "do_simple_task")
async def process_task(call: CallbackQuery):
    await update_balance(call.from_user.id, 0.25)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET tasks_done = tasks_done + 1 WHERE user_id = ?", (call.from_user.id,))
        await db.commit()
    await call.answer("✅ Задание выполнено! Начислено +0.25 ⭐", show_alert=True)

@dp.message(F.text == "🏆 Рейтинг")
async def show_rating(message: Message):
    await message.answer("🏆 **Выберите категорию рейтинга:**", reply_markup=rating_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "top_ref")
async def top_ref(call: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT username, referrals_count FROM users ORDER BY referrals_count DESC LIMIT 10") as c:
            rows = await c.fetchall()
            
    text = "📊 **Топ 10 по рефералам:**\n\n"
    for idx, r in enumerate(rows, 1):
        name = r['username'] or "Аноним"
        text += f"{idx}. @{name} — {r['referrals_count']} реф.\n"
    await call.message.edit_text(text, reply_markup=rating_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "top_balance")
async def top_balance(call: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 10") as c:
            rows = await c.fetchall()
            
    text = "💰 **Топ 10 по балансу:**\n\n"
    for idx, r in enumerate(rows, 1):
        name = r['username'] or "Аноним"
        text += f"{idx}. @{name} — {r['balance']:.2f} ⭐\n"
    await call.message.edit_text(text, reply_markup=rating_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "💸 Обменять звёзды")
async def show_withdraw(message: Message):
    u = await get_user(message.from_user.id)
    text = (
        f"💰 **Вывод средств**\n\n"
        f"🪙 Ваш баланс: **{u['balance'] if u else 0.0:.2f} ⭐**\n"
        f"📌 Минимальная сумма для вывода: **15.0 ⭐**\n"
    )
    await message.answer(text, reply_markup=withdraw_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "withdraw_stars")
async def withdraw_start(call: CallbackQuery, state: FSMContext):
    u = await get_user(call.from_user.id)
    if not u or u['balance'] < 15.0:
        await call.answer("❌ Недостаточно средств. Минимум для вывода: 15 ⭐", show_alert=True)
        return
    
    await state.set_state(UserStates.enter_withdraw_amount)
    await call.message.answer("Введите количество ⭐ для вывода:")
    await call.answer()

@dp.message(UserStates.enter_withdraw_amount)
async def process_withdraw(message: Message, state: FSMContext):
    u = await get_user(message.from_user.id)
    try:
        amount = float(message.text)
        if amount < 15.0 or amount > u['balance']:
            await message.answer("❌ Некорректная сумма или превышает ваш баланс. Попробуйте еще раз:")
            return
        
        await update_balance(message.from_user.id, -amount)
        await message.answer(f"✅ Заявка на вывод **{amount} ⭐** создана! Ожидайте обработки администратором.", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Введите числовую сумму:")

# --- Промокоды и Бонусы ---
@dp.callback_query(F.data == "bonus")
async def get_daily_bonus(call: CallbackQuery):
    u = await get_user(call.from_user.id)
    now = time.time()
    if now - u.get('last_bonus', 0) < 86400:
        hours = int((86400 - (now - u['last_bonus'])) // 3600)
        await call.answer(f"⏳ Следующий бонус доступен через {hours} ч.", show_alert=True)
        return
        
    await update_balance(call.from_user.id, 1.0)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET last_bonus = ? WHERE user_id = ?", (now, call.from_user.id))
        await db.commit()
    await call.answer("🎁 Вы получили ежедневный бонус +1.0 ⭐!", show_alert=True)

@dp.callback_query(F.data == "promo")
async def promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.enter_promo)
    await call.message.answer("🎟 Введите промокод (например, `START2026`):", parse_mode="Markdown")
    await call.answer()

@dp.message(UserStates.enter_promo)
async def process_promo(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes WHERE code = ? AND uses_left > 0", (code,)) as c:
            promo = await c.fetchone()
            
        if promo:
            await db.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code = ?", (code,))
            await db.commit()
            await update_balance(message.from_user.id, float(promo['reward']))
            await message.answer(f"✅ Промокод активирован! Начислено **+{promo['reward']} ⭐**", parse_mode="Markdown")
        else:
            await message.answer("❌ Промокод не существует или закончился.")
            
    await state.clear()

@dp.callback_query(F.data == "support")
async def support_info(call: CallbackQuery):
    await call.message.answer("🆘 По всем вопросам обращайтесь к администратору бота.")
    await call.answer()

@dp.callback_query(F.data == "to_main")
async def to_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await call.message.delete()
    except Exception:
        pass
    await cmd_start(call.message, state)

# --- Админ панель ---
@dp.message(F.text == "⚙️ Админ панель")
async def show_admin_panel(message: Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return
    await message.answer("⚙️ **Админ панель**", reply_markup=admin_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_stats")
async def admin_stats(call: CallbackQuery):
    if ADMIN_ID != 0 and call.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*), SUM(balance) FROM users") as c:
            count, total_bal = await c.fetchone()
            
    await call.message.edit_text(
        f"📊 **Статистика бота:**\n\n"
        f"👥 Пользователей: {count}\n"
        f"💰 Сумма всех балансов: {total_bal or 0.0:.2f} ⭐",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "adm_give")
async def adm_give_start(call: CallbackQuery, state: FSMContext):
    if ADMIN_ID != 0 and call.from_user.id != ADMIN_ID:
        return
    await state.set_state(UserStates.admin_give_stars_id)
    await call.message.answer("Введите Telegram ID пользователя:")
    await call.answer()

@dp.message(UserStates.admin_give_stars_id)
async def adm_give_id(message: Message, state: FSMContext):
    if message.text.isdigit():
        await state.update_data(target_id=int(message.text))
        await state.set_state(UserStates.admin_give_stars_amount)
        await message.answer("Введите количество ⭐ для начисления:")
    else:
        await message.answer("ID должен состоять из цифр:")

@dp.message(UserStates.admin_give_stars_amount)
async def adm_give_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text)
        data = await state.get_data()
        target_id = data['target_id']
        
        await update_balance(target_id, amount)
        await message.answer(f"✅ Пользователю `{target_id}` успешно начислено **{amount} ⭐**", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Введите корректное число:")

# --- Веб Сервер Render ---
async def health_check(request):
    return web.Response(text="Bot Alive")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def main():
    await init_db()
    await start_web_server()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
