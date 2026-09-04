import time
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiosqlite

from database import get_user, update_balance, DB_PATH
from config import MIN_WITHDRAW

router = Router()

class WithdrawStates(StatesGroup):
    enter_amount = State()

def withdraw_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="withdraw_stars")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="to_main")]
    ])

@router.message(F.text == "💸 Обменять звёзды")
async def show_withdraw(message: Message):
    u = await get_user(message.from_user.id)
    text = (
        f"💰 **Выберите способ вывода**\n\n"
        f"🪙 Ваш баланс: **{u['balance'] if u else 0.0:.2f} ⭐**\n"
        f"📌 Минимум для вывода: **{MIN_WITHDRAW} ⭐**\n"
    )
    await message.answer(text, reply_markup=withdraw_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "withdraw_stars")
async def withdraw_start(call: CallbackQuery, state: FSMContext):
    u = await get_user(call.from_user.id)
    if not u or u['balance'] < MIN_WITHDRAW:
        await call.answer(f"❌ Недостаточно средств. Минимум для вывода: {MIN_WITHDRAW} ⭐", show_alert=True)
        return
    
    await state.set_state(WithdrawStates.enter_amount)
    await call.message.answer(f"Введите количество ⭐ для вывода (от {MIN_WITHDRAW} до {u['balance']:.2f}):")
    await call.answer()

@router.message(WithdrawStates.enter_amount)
async def process_withdraw(message: Message, state: FSMContext):
    u = await get_user(message.from_user.id)
    try:
        amount = float(message.text)
        if amount < MIN_WITHDRAW or amount > u['balance']:
            await message.answer("❌ Некорректная сумма или превышает доступный баланс.")
            return
        
        await update_balance(message.from_user.id, -amount)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO withdraw_requests (user_id, amount, system, created_at) VALUES (?, ?, ?, ?)",
                (message.from_user.id, amount, "Telegram Stars", time.time())
            )
            await db.commit()

        await message.answer(f"✅ Заявка на вывод **{amount} ⭐** оформлена! Ожидайте обработки администратором.", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Введите корректное число:")
