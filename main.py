import asyncio
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, TelegramObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

from config import BOT_TOKEN, PORT
from database import init_db
from services.piarflow import get_sponsors_for_user, check_subscriptions_on_service

# Подключаем роутеры
from handlers.start import router as start_router
from handlers.profile import router as profile_router
from handlers.tasks import router as tasks_router
from handlers.rating import router as rating_router
from handlers.withdraw import router as withdraw_router
from handlers.admin import router as admin_router

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
                is_subbed = await check_subscriptions_on_service(user.id)
                if not is_subbed:
                    kb = []
                    text = "⚠️ **Для доступа к боту подпишитесь на каналы:**\n\n"
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

async def health_check(request):
    return web.Response(text="Bot Status: Online")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

async def main():
    await init_db()
    
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware
    dp.message.middleware(SponsorGuardMiddleware())
    dp.callback_query.middleware(SponsorGuardMiddleware())

    # Регистрация роутеров
    dp.include_routers(
        start_router,
        profile_router,
        tasks_router,
        rating_router,
        withdraw_router,
        admin_router
    )

    # Обработчик проверки подписки
    @dp.callback_query(F.data == "check_sub")
    async def check_sub_cb(call: CallbackQuery):
        is_subbed = await check_subscriptions_on_service(call.from_user.id)
        if is_subbed:
            await call.message.answer("✅ **Отлично!** Все подписки подтверждены.", parse_mode="Markdown")
        else:
            await call.answer("❌ Вы подписаны не на все каналы!", show_alert=True)

    @dp.callback_query(F.data == "to_main")
    async def to_main_cb(call: CallbackQuery, state):
        await state.clear()
        try:
            await call.message.delete()
        except Exception:
            pass

    await start_web_server()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
