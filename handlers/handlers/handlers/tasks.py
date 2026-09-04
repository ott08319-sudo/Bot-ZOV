from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
import aiosqlite

from database import get_user, update_balance, add_xp, DB_PATH
from config import TASK_REWARD

router = Router()

def tasks_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Выполнить клик-задание (+0.25 ⭐)", callback_data="do_task")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

@router.message(F.text == "📋 Задания")
async def show_tasks(message: Message):
    u = await get_user(message.from_user.id)
    text = (
        f"🎉 **Задания для заработка**\n\n"
        f"Выполнено вами: **{u['tasks_done'] if u else 0}**\n"
        f"💰 Награда за каждое задание: **{TASK_REWARD} ⭐**\n"
        f"🏆 XP для Батл Пасса: **+20 XP**"
    )
    await message.answer(text, reply_markup=tasks_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "do_task")
async def process_task(call: CallbackQuery):
    await update_balance(call.from_user.id, TASK_REWARD)
    await add_xp(call.from_user.id, 20)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET tasks_done = tasks_done + 1 WHERE user_id = ?", (call.from_user.id,))
        await db.commit()
        
    await call.answer(f"✅ Задание выполнено! Начислено +{TASK_REWARD} ⭐ и +20 XP!", show_alert=True)
