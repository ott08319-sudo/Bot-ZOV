from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiosqlite

from config import ADMIN_ID, DB_PATH
from database import update_balance

router = Router()

class AdminStates(StatesGroup):
    give_stars_id = State()
    give_stars_amount = State()
    create_promo_code = State()
    create_promo_reward = State()
    create_promo_uses = State()

def admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать звезды", callback_data="adm_give"), InlineKeyboardButton(text="🎟 Создать промокод", callback_data="adm_create_promo")],
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="adm_stats"), InlineKeyboardButton(text="💸 Заявки на вывод", callback_data="adm_payouts")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="to_main")]
    ])

@router.message(F.text == "⚙️ Админ панель")
async def show_admin_panel(message: Message):
    if ADMIN_ID != 0 and message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return
    await message.answer("⚙️ **Админ панель управления**", reply_markup=admin_keyboard(), parse_mode="Markdown")

@router.callback_query(F.data == "adm_stats")
async def admin_stats(call: CallbackQuery):
    if ADMIN_ID != 0 and call.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*), SUM(balance) FROM users") as c:
            count, total_bal = await c.fetchone()
        async with db.execute("SELECT COUNT(*) FROM withdraw_requests WHERE status = 'pending'") as c:
            pending_reqs = (await c.fetchone())[0]

    await call.message.edit_text(
        f"📊 **Статистика системы:**\n\n"
        f"👥 Пользователей: **{count}**\n"
        f"💰 Общий баланс у пользователей: **{total_bal or 0.0:.2f} ⭐**\n"
        f"⏳ Ожидает вывода: **{pending_reqs} заявок**",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "adm_give")
async def adm_give_start(call: CallbackQuery, state: FSMContext):
    if ADMIN_ID != 0 and call.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.give_stars_id)
    await call.message.answer("Введите Telegram ID пользователя:")
    await call.answer()

@router.message(AdminStates.give_stars_id)
async def adm_give_id(message: Message, state: FSMContext):
    if message.text.isdigit():
        await state.update_data(target_id=int(message.text))
        await state.set_state(AdminStates.give_stars_amount)
        await message.answer("Введите количество ⭐ для начисления:")
    else:
        await message.answer("ID должен состоять только из цифр:")

@router.message(AdminStates.give_stars_amount)
async def adm_give_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text)
        data = await state.get_data()
        await update_balance(data['target_id'], amount)
        await message.answer(f"✅ Пользователю `{data['target_id']}` начислено **{amount} ⭐**", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Введите числовую сумму:")

@router.callback_query(F.data == "adm_create_promo")
async def adm_promo_start(call: CallbackQuery, state: FSMContext):
    if ADMIN_ID != 0 and call.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.create_promo_code)
    await call.message.answer("Введите название нового промокода (например: BONUS2026):")
    await call.answer()

@router.message(AdminStates.create_promo_code)
async def adm_promo_code(message: Message, state: FSMContext):
    await state.update_data(promo_code=message.text.strip().upper())
    await state.set_state(AdminStates.create_promo_reward)
    await message.answer("Введите сумму награды в ⭐:")

@router.message(AdminStates.create_promo_reward)
async def adm_promo_reward(message: Message, state: FSMContext):
    try:
        reward = float(message.text)
        await state.update_data(promo_reward=reward)
        await state.set_state(AdminStates.create_promo_uses)
        await message.answer("Введите количество активаций:")
    except ValueError:
        await message.answer("Введите число:")

@router.message(AdminStates.create_promo_uses)
async def adm_promo_uses(message: Message, state: FSMContext):
    if message.text.isdigit():
        uses = int(message.text)
        data = await state.get_data()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO promo_codes VALUES (?, ?, ?)", (data['promo_code'], data['promo_reward'], uses))
            await db.commit()
        await message.answer(f"✅ Промокод `{data['promo_code']}` на {data['promo_reward']} ⭐ ({uses} исп.) создан!", parse_mode="Markdown")
        await state.clear()
    else:
        await message.answer("Введите число:")
