from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from database import register_user, get_user

router = Router()

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

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    args = message.text.split() if message.text else []
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref_"):
        raw_ref = args[1].replace("ref_", "")
        if raw_ref.isdigit():
            ref_id = int(raw_ref)
            
    user_name = message.from_user.username or message.from_user.first_name or "Пользователь"
    await register_user(message.from_user.id, user_name, ref_id)
    
    u = await get_user(message.from_user.id)
    bot_info = await message.bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{message.from_user.id}"
    
    text = (
        f"❤️ **Добро пожаловать в DarkStar!**\n\n"
        f"🔗 Твоя ссылка: `{ref_link}`\n"
        f"💰 Бонус за реферала: **+3.0 ⭐**\n"
        f"⭐ Ваш баланс: **{u['balance'] if u else 0.0:.2f} ⭐**\n"
        f"👥 Приглашено: **{u['referrals_count'] if u else 0}**\n\n"
        f"⚡ **Выберите действие в меню ниже:**"
    )
    await message.answer(text, reply_markup=main_keyboard(), parse_mode="Markdown")

@router.message(F.text == "💰 Заработать звёзды")
async def earn_stars(message: Message):
    bot_info = await message.bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{message.from_user.id}"
    text = (
        f"🚀 **Зарабатывайте звёзды, приглашая друзей!**\n\n"
        f"📢 За каждого приглашенного пользователя вы получаете **3.0 ⭐** на свой баланс.\n\n"
        f"🔗 Твоя реферальная ссылка:\n`{ref_link}`"
    )
    await message.answer(text, parse_mode="Markdown")
