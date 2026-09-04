from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
import aiosqlite

from database import DB_PATH

router = Router()

def rating_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 По рефералам", callback_data="top_ref"), InlineKeyboardButton(text="💰 По балансу", callback_data="top_balance")],
        [InlineKeyboardButton(text="🏆 По Батл Пассу", callback_data="top_bp")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

@router.message(F.text == "🏆 Рейтинг")
async def show_rating(message: Message):
    await message.answer("🏆 **Выберите тип рейтинга:**", reply_markup=rating_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "top_ref")
async def top_ref(call: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT username, referrals_count FROM users ORDER BY referrals_count DESC LIMIT 10") as c:
            rows = await c.fetchall()
            
    text = "📊 **Топ 10 по рефералам:**\n\n"
    for idx, r in enumerate(rows, 1):
        name = r['username'] or "Аноним"
        text += f"{idx}. @{name} — {r['referrals_count']} чел.\n"
    await call.message.edit_text(text, reply_markup=rating_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "top_balance")
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

@router.callback_query(F.data == "top_bp")
async def top_bp(call: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT username, bp_level FROM users ORDER BY bp_level DESC LIMIT 10") as c:
            rows = await c.fetchall()
            
    text = "🏆 **Топ 10 по Батл Пассу:**\n\n"
    for idx, r in enumerate(rows, 1):
        name = r['username'] or "Аноним"
        text += f"{idx}. @{name} — {r['bp_level']} Уровень\n"
    await call.message.edit_text(text, reply_markup=rating_keyboard(), parse_mode="Markdown")
