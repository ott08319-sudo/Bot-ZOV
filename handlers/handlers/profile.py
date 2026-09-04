import time
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiosqlite

from database import get_user, register_user, update_balance, DB_PATH
from config import DAILY_BONUS

router = Router()

class ProfileStates(StatesGroup):
    enter_promo = State()

def profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo"), InlineKeyboardButton(text="🎁 Ежедневный бонус", callback_data="bonus")],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data="support")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="to_main")]
    ])

@router.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    u = await get_user(message.from_user.id)
    if not u:
        await register_user(message.from_user.id, message.from_user.username or "User")
        u = await get_user(message.from_user.id)
        
    reg_date = datetime.fromtimestamp(u['created_at']).strftime('%d.%m.%Y')
    username = message.from_user.username or "Не указан"
    
    text = (
        f"👤 **Твой профиль**\n\n"
        f"👤 Легендарный @{username}\n"
        f"🆔 ID: `{u['user_id']}`\n"
        f"📅 Зарегистрирован: {reg_date}\n\n"
        f"💬 Баланс: **{u['balance']:.2f} ⭐**\n"
        f"🏆 Всего заработано: **{u['total_earned']:.2f} ⭐**\n"
        f"👥 Приглашено друзей: **{u['referrals_count']}**\n"
        f"✅ Прошли проверку: **{u['verified_refs']}**\n"
        f"🏆 Батл Пасс: **{u['bp_level']} уровень** ({u['bp_xp']}/100 XP)"
    )
    await message.answer(text, reply_markup=profile_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "bonus")
async def get_daily_bonus(call: CallbackQuery):
    u = await get_user(call.from_user.id)
    now = time.time()
    if now - u.get('last_bonus', 0) < 86400:
        hours = int((86400 - (now - u['last_bonus'])) // 3600)
        await call.answer(f"⏳ Следующий бонус доступен через {hours} ч.", show_alert=True)
        return
        
    await update_balance(call.from_user.id, DAILY_BONUS)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET last_bonus = ? WHERE user_id = ?", (now, call.from_user.id))
        await db.commit()
    await call.answer(f"🎁 Вы получили ежедневный бонус +{DAILY_BONUS} ⭐!", show_alert=True)

@router.callback_query(F.data == "promo")
async def promo_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(ProfileStates.enter_promo)
    await call.message.answer("🎟 Введите промокод:")
    await call.answer()

@router.message(ProfileStates.enter_promo)
async def process_promo(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    user_id = message.from_user.id
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        # Проверяем, юзал ли уже
        async with db.execute("SELECT * FROM used_promos WHERE user_id = ? AND code = ?", (user_id, code)) as c:
            if await c.fetchone():
                await message.answer("❌ Вы уже активировали этот промокод.")
                await state.clear()
                return

        async with db.execute("SELECT * FROM promo_codes WHERE code = ? AND uses_left > 0", (code,)) as c:
            promo = await c.fetchone()
            
        if promo:
            await db.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code = ?", (code,))
            await db.execute("INSERT INTO used_promos VALUES (?, ?)", (user_id, code))
            await db.commit()
            
            await update_balance(user_id, float(promo['reward']))
            await message.answer(f"✅ Промокод активирован! Начислено **+{promo['reward']} ⭐**", parse_mode="Markdown")
        else:
            await message.answer("❌ Промокод не существует или его лимит исчерпан.")
            
    await state.clear()

@router.callback_query(F.data == "support")
async def support_info(call: CallbackQuery):
    await call.message.answer("🆘 По всем вопросам и сотрудничеству обращайтесь в поддержку.")
    await call.answer()
