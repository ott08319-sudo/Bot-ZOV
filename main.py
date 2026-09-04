import os
import asyncio
from datetime import datetime
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, TelegramObject
)
from aiogram.filters import CommandStart, Command
from aiohttp import web

from database import init_db, get_user, register_user
from piarflow_service import get_sponsors_for_user, check_subscriptions_on_service

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# --- Middleware Автопроверки Спонсоров ---
class SponsorGuardMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if not user:
            return await handler(event, data)
            
        if isinstance(event, CallbackQuery) and event.data in ("check_sub", "to_main"):
            return await handler(event, data)

        sponsors = await get_sponsors_for_user(user.id)
        if sponsors:
            is_subbed = await check_subscriptions_on_service("piarflow", user.id)
            if not is_subbed:
                kb = []
                text = "⚠️ **Для использования бота подпишитесь на каналы:**\n\n"
                for idx, s in enumerate(sponsors, 1):
                    text += f"{idx}. Канал #{idx}\n"
                    kb.append([InlineKeyboardButton(text=f"📢 Подписаться #{idx}", url=s["link"])])
                kb.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")])
                
                markup = InlineKeyboardMarkup(inline_keyboard=kb)
                if isinstance(event, Message):
                    await event.answer(text, reply_markup=markup, parse_mode="Markdown")
                elif isinstance(event, CallbackQuery):
                    await event.message.answer(text, reply_markup=markup, parse_mode="Markdown")
                return

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
        [InlineKeyboardButton(text="⭐ Купить Premium", callback_data="buy_premium")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="to_main")]
    ])

def tasks_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать свое задание", callback_data="create_task")],
        [InlineKeyboardButton(text="📋 Мои задания", callback_data="my_tasks")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def rating_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 По рефералам (все время)", callback_data="top_ref_all"), InlineKeyboardButton(text="📊 По рефералам (за 24ч)", callback_data="top_ref_24")],
        [InlineKeyboardButton(text="💰 По балансу", callback_data="top_balance"), InlineKeyboardButton(text="💸 По выводам", callback_data="top_withdraws")],
        [InlineKeyboardButton(text="🏆 По Батл Пассу", callback_data="top_bp"), InlineKeyboardButton(text="📊 Полная статистика", callback_data="full_stats")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def withdraw_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Telegram Stars", callback_data="withdraw_stars"), InlineKeyboardButton(text="Обычные подарки", callback_data="withdraw_gifts")],
        [InlineKeyboardButton(text="🟢 Активные выплаты", callback_data="active_payouts")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

def admin_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_settings"), InlineKeyboardButton(text="🌐 Сервисы", callback_data="admin_services")],
        [InlineKeyboardButton(text="🏠 Локальные спонсоры", callback_data="admin_local_sponsors"), InlineKeyboardButton(text="💰 Выдать звезды", callback_data="admin_give_stars")]
    ])

def admin_settings_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Разбан", callback_data="adm_unban"), InlineKeyboardButton(text="📢 Рассылка", callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="⏰ Отложенные посты", callback_data="adm_delayed"), InlineKeyboardButton(text="📡 Каналы", callback_data="adm_channels")],
        [InlineKeyboardButton(text="🎟 Промокоды", callback_data="adm_promos"), InlineKeyboardButton(text="👋 Приветы", callback_data="adm_greetings")],
        [InlineKeyboardButton(text="📋 Задания", callback_data="adm_tasks"), InlineKeyboardButton(text="💰 Штраф", callback_data="adm_penalty")],
        [InlineKeyboardButton(text="⭐ Premium", callback_data="adm_premium"), InlineKeyboardButton(text="🧾 Чеки", callback_data="adm_checks")],
        [InlineKeyboardButton(text="📢 Реклама", callback_data="adm_ads"), InlineKeyboardButton(text="🔍 Поиск", callback_data="adm_search")],
        [InlineKeyboardButton(text="🛡 Вайболист", callback_data="adm_whitelist"), InlineKeyboardButton(text="🔗 Реф. ссылки", callback_data="adm_reflinks")],
        [InlineKeyboardButton(text="👁 Показы", callback_data="adm_views"), InlineKeyboardButton(text="⚡ Автовыплаты", callback_data="adm_autopayouts")],
        [InlineKeyboardButton(text="💸 Выплаты", callback_data="adm_payouts"), InlineKeyboardButton(text="📢 Операции", callback_data="adm_operations")],
        [InlineKeyboardButton(text="🤖 Юзерботы подарков", callback_data="adm_userbots"), InlineKeyboardButton(text="🏆 Батл Пасс", callback_data="adm_bp")],
        [InlineKeyboardButton(text="📊 Управление статистикой", callback_data="adm_stats_manage"), InlineKeyboardButton(text="🏷 Префиксы", callback_data="adm_prefixes")],
        [InlineKeyboardButton(text="👮 Админы", callback_data="adm_admins"), InlineKeyboardButton(text="🪞 Зеркала", callback_data="adm_mirrors")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_admin_main")]
    ])

# --- Роутеры и Хэндлеры ---
bot = Bot(token=os.getenv("BOT_TOKEN", ""))
dp = Dispatcher()
dp.message.middleware(SponsorGuardMiddleware())
dp.callback_query.middleware(SponsorGuardMiddleware())

@dp.message(CommandStart())
async def cmd_start(message: Message):
    args = message.text.split()
    ref_id = int(args[1].replace("ref_", "")) if len(args) > 1 and args[1].startswith("ref_") else None
    await register_user(message.from_user.id, message.from_user.username or "Пользователь", ref_id)
    
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{message.from_user.id}"
    
    text = (
        f"❤️ **Добро пожаловать в DarkStar!**\n\n"
        f"🔗 Твоя ссылка: {ref_link}\n"
        f"💰 Бонус за подписку: +3.0 ⭐\n"
        f"⭐ Баланс: 420.3\n"
        f"👥 Приглашено: 373\n\n"
        f"⚡ **Быстрые действия:**"
    )
    await message.answer(text, reply_markup=main_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    u = await get_user(message.from_user.id)
    reg_date = datetime.fromtimestamp(u['created_at']).strftime('%d.%m.%Y') if u else "21.08.2026"
    
    text = (
        f"👤 **Твой профиль**\n\n"
        f"👤 Легендарный @{message.from_user.username}\n"
        f"📅 Зарегистрирован: {reg_date}\n\n"
        f"💬 Баланс: {u['balance'] if u else 420.3} ⭐\n"
        f"🏆 Всего заработано: {u['total_earned'] if u else 1251.8} ⭐\n"
        f"👥 Приглашено друзей: 373 (за 24ч: 7)\n"
        f"✅ Прошли проверку: 242\n"
        f"🏆 Батл Пасс: 15/50 уровень (1286/1500 XP)"
    )
    await message.answer(text, reply_markup=profile_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "📋 Задания")
async def show_tasks(message: Message):
    text = (
        f"🎉 **Ты выполнил все доступные задания!**\n\n"
        f"Всего выполнено заданий: 1\n\n"
        f"💰 Награда за каждое задание: 0.25 ⭐\n\n"
        f"🌟 *Нужны подписчики для своего канала?*\n"
        f"Создай собственное задание!"
    )
    await message.answer(text, reply_markup=tasks_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "🏆 Рейтинг")
async def show_rating(message: Message):
    text = (
        f"🏆 **Выберите тип рейтинга:**\n\n"
        f"📌 *Топ за сутки получает всякие плюшки ;)*"
    )
    await message.answer(text, reply_markup=rating_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "💸 Обменять звёзды")
async def show_withdraw(message: Message):
    text = (
        f"💰 **Выберите способ вывода**\n\n"
        f"🪙 Ваш баланс: 420.3 ⭐\n"
        f"💵 Примерно: ~6.30 USD\n\n"
        f"⭐ **Telegram Stars** - вывод в звёздах\n"
        f"🧸 **Обычные подарки** - подарок можно обменять\n\n"
        f"📌 *Минимум рефералов за 2 дня: обычные подарки — 5, Telegram Stars — 10, TON — 15.*"
    )
    await message.answer(text, reply_markup=withdraw_keyboard(), parse_mode="Markdown")

@dp.message(F.text == "⚙️ Админ панель")
async def show_admin_panel(message: Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен.")
        return
        
    text = (
        f"⚙️ **Админ панель**\n\n"
        f"👥 Всего: 588\n"
        f"✅ Активных: 588\n"
        f"🚫 Забанено: 0\n"
        f"⭐ Premium: 6\n"
        f"📊 Новых сегодня: 0\n\n"
        f"📈 Рефералов всего: 513\n"
        f"✅ Прошли проверку: 332\n"
        f"💰 Выплачено реферерам: 325\n\n"
        f"📝 Заявок всего: 0\n"
        f"⏳ Ожидают: 6\n"
        f"💎 Выведено всего: 0 ⭐\n"
        f"🏆 Заработано всего: 4644.75 ⭐\n\n"
        f"🎟 Промокодов: 1\n"
        f"👋 Активных приветов: 0\n"
        f"🎁 Активных розыгрышей: 0"
    )
    await message.answer(text, reply_markup=admin_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_settings")
async def process_admin_settings(call: CallbackQuery):
    await call.message.edit_text("⚙️ **Настройки бота**", reply_markup=admin_settings_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "to_admin_main")
async def process_to_admin_main(call: CallbackQuery):
    text = (
        f"⚙️ **Админ панель**\n\n"
        f"👥 Всего: 588\n"
        f"✅ Активных: 588\n"
        f"🚫 Забанено: 0\n"
        f"⭐ Premium: 6"
    )
    await call.message.edit_text(text, reply_markup=admin_main_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "to_main")
async def process_to_main(call: CallbackQuery):
    await call.message.delete()
    await cmd_start(call.message)

@dp.callback_query(F.data == "check_sub")
async def check_subscription_callback(call: CallbackQuery):
    is_subbed = await check_subscriptions_on_service("piarflow", call.from_user.id)
    if is_subbed:
        await call.message.answer("✅ **Отлично!** Вы подписаны на всех спонсоров.", parse_mode="Markdown")
    else:
        await call.answer("❌ Вы подписаны не на все каналы!", show_alert=True)

# --- Веб Сервер для Render ---
async def health_check(request):
    return web.Response(text="Bot Alive")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
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
