import os
import re
import uuid
import logging
import asyncio
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict

from dotenv import load_dotenv
load_dotenv()

# FastAPI & Web Admin
import uvicorn
from fastapi import FastAPI
from sqladmin import Admin, ModelView

# Aiogram
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters.callback_data import CallbackData
from aiogram.types import (
    Message, CallbackQuery, PreCheckoutQuery, LabeledPrice,
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
)

# SQLAlchemy (PostgreSQL)
from sqlalchemy import String, BigInteger, Text, DateTime, select, update, Integer, Boolean
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# =====================================================================
# 1. НАЛАШТУВАННЯ ТА КОНСТАНТИ
# =====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BOT_TOKEN       = os.getenv("BOT_TOKEN")
ADMIN_ID_ENV    = os.getenv("ADMIN_ID")
PROVIDER_TOKEN  = os.getenv("PROVIDER_TOKEN")   # Stripe/LiqPay/WayForPay через BotFather
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/eduexpert")
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")
MANAGER_LINK    = os.getenv("MANAGER_LINK", "https://t.me/Olenka_EduExpert")
REVIEWS_LINK    = os.getenv("REVIEWS_LINK", "https://t.me/EduExpert_Reviews")

if not all([BOT_TOKEN, ADMIN_ID_ENV]):
    raise ValueError("❌ Критична помилка: Перевірте BOT_TOKEN та ADMIN_ID у змінних оточення!")

ADMIN_ID = int(ADMIN_ID_ENV)

# Тарифи калькулятора (грн за 1 сторінку)
PRICING = {"Реферат": 40, "Контрольна": 50, "Курсова": 70, "Дипломна": 120}

# Ліміти валідації
MIN_SUBJECT_LEN = 2
MAX_SUBJECT_LEN = 200
MIN_DETAILS_LEN = 10
MAX_DETAILS_LEN = 2000

# =====================================================================
# 2. БАЗА ДАНИХ
# =====================================================================
engine        = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id:         Mapped[int]      = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id:   Mapped[str]      = mapped_column(String(50), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    username:   Mapped[str|None] = mapped_column(String(100))
    tg_id:      Mapped[int]      = mapped_column(BigInteger, index=True)
    subject:    Mapped[str]      = mapped_column(String(255))
    work_type:  Mapped[str]      = mapped_column(String(100))
    pages:      Mapped[int]      = mapped_column(Integer)
    deadline:   Mapped[str]      = mapped_column(String(100))
    phone:      Mapped[str]      = mapped_column(String(30))
    details:    Mapped[str]      = mapped_column(Text)
    status:     Mapped[str]      = mapped_column(String(50), default="Нове")
    price:      Mapped[int]      = mapped_column(Integer, default=0)
    is_paid:    Mapped[bool]     = mapped_column(Boolean, default=False)


# =====================================================================
# 3. STORAGE — перевірка Redis через реальний ping
#
# ВИПРАВЛЕНО: оригінальна версія просто ставила MemoryStorage без
# спроби підключитись до Redis. Тепер перевіряємо через ping()
# асинхронно в main() — до старту polling.
# =====================================================================
async def _build_storage():
    from aiogram.fsm.storage.memory import MemoryStorage
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(REDIS_URL, socket_connect_timeout=3)
        await client.ping()
        await client.aclose()
        from aiogram.fsm.storage.redis import RedisStorage
        logging.info("✅ Redis доступний — використовую RedisStorage.")
        return RedisStorage.from_url(REDIS_URL)
    except Exception as e:
        logging.warning(
            f"⚠️ Redis недоступний ({e}). "
            f"Перемикаюсь на MemoryStorage (стани скидаються при рестарті!)."
        )
        return MemoryStorage()


# =====================================================================
# 4. АНТИСПАМ (Throttling Middleware) — збережено з Gemini-версії
# =====================================================================
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.8):
        super().__init__()
        self.limit  = limit
        self.caches: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        if not event.from_user:
            return await handler(event, data)
        user_id = event.from_user.id
        now = asyncio.get_event_loop().time()
        if user_id in self.caches and now - self.caches[user_id] < self.limit:
            return   # ігноруємо флуд
        self.caches[user_id] = now
        return await handler(event, data)


# =====================================================================
# 5. CALLBACK DATA
#
# ВИПРАВЛЕНО: оригінальна версія використовувала action="accept"/
# "reject"/"invoice" — разом з іншими полями це перевищує 64 байти
# Telegram. Скорочено до "a"/"r"/"p" — вкладається з запасом (~46 б).
# =====================================================================
class OrderAction(CallbackData, prefix="ord"):
    action:    str   # "a"=accept | "r"=reject | "p"=price/invoice
    order_id:  str
    client_id: int


class OrderStates(StatesGroup):
    waiting_for_subject  = State()
    waiting_for_type     = State()
    waiting_for_pages    = State()
    waiting_for_deadline = State()
    waiting_for_phone    = State()
    waiting_for_details  = State()


class CalcStates(StatesGroup):
    waiting_for_type  = State()
    waiting_for_pages = State()


class AdminStates(StatesGroup):
    waiting_for_price = State()


# =====================================================================
# 6. КЛАВІАТУРИ
# =====================================================================
def get_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 Створити заявку"),    KeyboardButton(text="🧮 Калькулятор цін")],
        [KeyboardButton(text="📊 Мої замовлення"),     KeyboardButton(text="ℹ️ FAQ / Довідка")],
        [KeyboardButton(text="💬 Відгуки"),            KeyboardButton(text="👤 Зв'язок з менеджером")],
    ], resize_keyboard=True)


def _cancel_btn() -> list:
    return [KeyboardButton(text="❌ Скасувати процес")]


def get_type_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Реферат"),  KeyboardButton(text="Контрольна")],
        [KeyboardButton(text="Курсова"),  KeyboardButton(text="Дипломна")],
        _cancel_btn(),
    ], resize_keyboard=True, one_time_keyboard=True)


def get_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📱 Надіслати номер", request_contact=True)],
        _cancel_btn(),
    ], resize_keyboard=True, one_time_keyboard=True)


def get_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[_cancel_btn()], resize_keyboard=True)


def get_admin_action_kb(order_id: str, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Прийняти",
                callback_data=OrderAction(action="a", order_id=order_id, client_id=client_id).pack(),
            ),
            InlineKeyboardButton(
                text="❌ Відхилити",
                callback_data=OrderAction(action="r", order_id=order_id, client_id=client_id).pack(),
            ),
        ],
        [
            InlineKeyboardButton(
                text="💰 Виставити рахунок",
                callback_data=OrderAction(action="p", order_id=order_id, client_id=client_id).pack(),
            ),
        ],
    ])


# =====================================================================
# 7. БАЗОВІ КОМАНДИ
# =====================================================================
# Dispatcher створюється в main() після перевірки Redis.
# Хендлери реєструються через окремий Router нижче.
from aiogram import Router
router = Router()


@router.message(Command("cancel"))
@router.message(F.text == "❌ Скасувати процес")
async def process_global_cancel(message: Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer("Немає активних процесів для скасування.", reply_markup=get_main_kb())
        return
    await state.clear()
    await message.answer("❌ Процес повністю скасовано. Повертаємось у меню.", reply_markup=get_main_kb())


@router.message(Command("start"))
async def process_start_cmd(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Вітаємо в <b>EduExpert</b> — автоматизованій системі замовлення студентських робіт!\n\n"
        "Тут ви можете розрахувати вартість, оформити офіційну заявку, відстежувати статуси "
        "та здійснювати безпечну онлайн-оплату замовлень.",
        reply_markup=get_main_kb(),
    )


# =====================================================================
# 8. ІНФОРМАЦІЙНІ БЛОКИ (FAQ, ВІДГУКИ, МЕНЕДЖЕР)
# =====================================================================
@router.message(F.text == "ℹ️ FAQ / Довідка")
async def process_faq(message: Message):
    await message.answer(
        "ℹ️ <b>FAQ (Часті запитання):</b>\n\n"
        "⚡ <b>Які реальні терміни виконання?</b>\n"
        "Реферати — 2-3 дні, курсові — від 5 до 9 днів, дипломні — від 14 діб.\n\n"
        "⚡ <b>Чи безкоштовні виправлення?</b>\n"
        "Так. Всі правки наукового керівника в межах первинного ТЗ — безкоштовно.\n\n"
        "⚡ <b>Який рівень унікальності?</b>\n"
        "Перевірка через Unicheck/Антиплагіат: від 75-80% унікальності.",
        reply_markup=get_main_kb(),
    )


@router.message(F.text == "💬 Відгуки")
async def process_reviews(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Переглянути відгуки в Telegram", url=REVIEWS_LINK)]
    ])
    await message.answer(
        "💬 Ознайомтеся з реальними відгуками студентів за посиланням нижче:",
        reply_markup=kb,
    )


@router.message(F.text == "👤 Зв'язок з менеджером")
async def process_manager_contact(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👩‍💻 Написати Оленці", url=MANAGER_LINK)]
    ])
    await message.answer(
        "👤 Потрібно терміново передати методичні вказівки або обговорити нестандартне замовлення? "
        "Наш менеджер Оленка допоможе!",
        reply_markup=kb,
    )


# =====================================================================
# 9. КАЛЬКУЛЯТОР ЦІН — збережено повністю з Gemini-версії
# =====================================================================
@router.message(F.text == "🧮 Калькулятор цін")
async def process_calc_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Оберіть тип студентської роботи для калькуляції:", reply_markup=get_type_kb())
    await state.set_state(CalcStates.waiting_for_type)


@router.message(CalcStates.waiting_for_type)
async def process_calc_type(message: Message, state: FSMContext):
    w_type = (message.text or "").strip()
    if w_type not in PRICING:
        await message.answer("❌ Оберіть варіант із запропонованої клавіатури!")
        return
    await state.update_data(work_type=w_type)
    await message.answer("Введіть необхідну кількість сторінок (числом):", reply_markup=get_cancel_kb())
    await state.set_state(CalcStates.waiting_for_pages)


@router.message(CalcStates.waiting_for_pages)
async def process_calc_pages(message: Message, state: FSMContext):
    pages_input = (message.text or "").strip()
    if not pages_input.isdigit() or int(pages_input) <= 0:
        await message.answer("❌ Введіть ціле позитивне число сторінок:")
        return

    pages  = int(pages_input)
    data   = await state.get_data()
    w_type = data["work_type"]
    est_price = PRICING[w_type] * pages

    await state.update_data(pages=pages)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Створити заявку на основі розрахунку", callback_data="convert_calc")]
    ])
    await message.answer(
        f"🧮 <b>Попередній кошторис:</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🔹 <b>Тип:</b> {w_type}\n"
        f"📄 <b>Обсяг:</b> {pages} сторінок\n"
        f"💵 <b>Орієнтовна ціна:</b> від <b>{est_price} грн</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"<i>*Остаточна ціна фіксується менеджером після оцінки складності теми.</i>",
        reply_markup=get_main_kb(),
    )
    await message.answer("Бажаєте надіслати заявку менеджеру?", reply_markup=kb)


@router.callback_query(F.data == "convert_calc")
async def process_calc_redirect(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "work_type" not in data:
        await callback.answer("Сесія застаріла. Почніть заново через головне меню.", show_alert=True)
        return
    # ВИПРАВЛЕНО: генеруємо order_id одразу, щоб уникнути дублікатів при retry
    order_id = f"EDU-{datetime.now().strftime('%d%m%Y')}-{uuid.uuid4().hex[:6].upper()}"
    await state.update_data(order_id=order_id)
    await callback.message.answer(
        "Введіть предмет або конкретну тему роботи:",
        reply_markup=get_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_subject)
    await callback.answer()


# =====================================================================
# 10. АНКЕТА ЗАМОВЛЕННЯ
#
# ВИПРАВЛЕНО:
# — order_id генерується на ПОЧАТКУ анкети (тут або в calc_redirect),
#   а не в process_details_state. Повторна відправка деталей після
#   помилки не створює дублікат у БД.
# — Валідація subject та details (мін/макс довжина).
# — process_details_state: спочатку зберігаємо в БД, потім
#   надсилаємо адміну, потім state.clear().
# =====================================================================
@router.message(F.text == "📝 Створити заявку")
async def process_order_start(message: Message, state: FSMContext):
    await state.clear()
    # Генеруємо order_id одразу — зберігаємо в стані
    order_id = f"EDU-{datetime.now().strftime('%d%m%Y')}-{uuid.uuid4().hex[:6].upper()}"
    await state.update_data(order_id=order_id)
    await message.answer(
        "Введіть предмет або тему наукової роботи\n"
        "(наприклад: <i>Цивільне право, Маркетинг</i>):",
        reply_markup=get_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_subject)


@router.message(OrderStates.waiting_for_subject)
async def process_subject_state(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < MIN_SUBJECT_LEN:
        await message.answer(f"❌ Занадто коротко. Мінімум {MIN_SUBJECT_LEN} символи:")
        return
    if len(text) > MAX_SUBJECT_LEN:
        await message.answer(f"❌ Занадто довго. Максимум {MAX_SUBJECT_LEN} символів:")
        return
    await state.update_data(subject=text)
    data = await state.get_data()

    if "work_type" in data and "pages" in data:
        # Прийшли з калькулятора — пропускаємо тип і обсяг
        await message.answer(
            f"Тип роботи: <b>{data['work_type']}</b> ({data['pages']} ст.).\n"
            f"Вкажіть кінцевий дедлайн (наприклад: <i>до 15.06.2026</i>):",
            reply_markup=get_cancel_kb(),
        )
        await state.set_state(OrderStates.waiting_for_deadline)
    else:
        await message.answer("Оберіть тип роботи з клавіатури або напишіть свій варіант:", reply_markup=get_type_kb())
        await state.set_state(OrderStates.waiting_for_type)


@router.message(OrderStates.waiting_for_type)
async def process_type_state(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Оберіть або введіть тип роботи:")
        return
    await state.update_data(work_type=text)
    await message.answer(
        "Вкажіть необхідну кількість сторінок (тільки число):",
        reply_markup=get_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_pages)


@router.message(OrderStates.waiting_for_pages)
async def process_pages_state(message: Message, state: FSMContext):
    pages_text = (message.text or "").strip()
    if not pages_text.isdigit() or int(pages_text) <= 0:
        await message.answer("❌ Введіть кількість сторінок цілим числом (наприклад: 35):")
        return
    await state.update_data(pages=int(pages_text))
    await message.answer(
        "Вкажіть бажаний дедлайн (наприклад: <i>до 20.06.2026</i>):",
        reply_markup=get_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_deadline)


@router.message(OrderStates.waiting_for_deadline)
async def process_deadline_state(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Введіть дату дедлайну:")
        return
    await state.update_data(deadline=text)
    await message.answer(
        "Надішліть контактний телефон (кнопкою або у форматі <code>+380XXXXXXXXX</code>):",
        reply_markup=get_phone_kb(),
    )
    await state.set_state(OrderStates.waiting_for_phone)


@router.message(OrderStates.waiting_for_phone)
async def process_phone_state(message: Message, state: FSMContext):
    phone = (
        message.contact.phone_number
        if message.contact
        else (message.text or "").strip()
    )
    if not re.match(r"^\+?\d{9,16}$", phone):
        await message.answer(
            "❌ Некоректний формат. Натисніть кнопку або введіть у форматі\n"
            "<code>+380XXXXXXXXX</code>:"
        )
        return
    await state.update_data(phone=phone)
    await message.answer(
        "Введіть деталі замовлення:\nплан, вимоги викладача, посилання на джерела тощо.",
        reply_markup=get_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_details)


@router.message(OrderStates.waiting_for_details)
async def process_details_state(message: Message, state: FSMContext, bot: Bot):
    text = (message.text or "").strip()
    if len(text) < MIN_DETAILS_LEN:
        await message.answer(f"❌ Опишіть детальніше (мінімум {MIN_DETAILS_LEN} символів):")
        return
    if len(text) > MAX_DETAILS_LEN:
        await message.answer(f"❌ Занадто довго (максимум {MAX_DETAILS_LEN} символів):")
        return

    await state.update_data(details=text)
    user_data  = await state.get_data()
    order_id   = user_data["order_id"]   # Беремо зі стану — не генеруємо знову
    client_id  = message.from_user.id

    # ВИПРАВЛЕНО: спочатку зберігаємо в БД, потім надсилаємо адміну,
    # потім clear(). Якщо надсилання адміну впало — стан не очищаємо,
    # юзер пробує ще раз. Повторний insert не пройде через unique=True
    # на order_id — БД поверне помилку, яку ловимо нижче.
    db_saved = False
    try:
        async with async_session() as session:
            async with session.begin():
                # Перевіряємо чи вже є такий order_id (retry після помилки надсилання)
                existing = await session.execute(
                    select(Order).where(Order.order_id == order_id)
                )
                if not existing.scalar_one_or_none():
                    session.add(Order(
                        order_id  = order_id,
                        username  = message.from_user.username,
                        tg_id     = client_id,
                        subject   = user_data["subject"],
                        work_type = user_data["work_type"],
                        pages     = user_data.get("pages", 0),
                        deadline  = user_data["deadline"],
                        phone     = user_data["phone"],
                        details   = user_data["details"],
                    ))
        db_saved = True
    except Exception as e:
        logging.error(f"❌ PostgreSQL помилка збереження {order_id}: {e}")
        try:
            await bot.send_message(
                ADMIN_ID,
                f"⚠️ <b>УВАГА:</b> Замовлення <b>{order_id}</b> не збережено в БД!\n"
                f"Перевірте логи сервера.",
            )
        except Exception:
            pass

    admin_msg = (
        f"🔥 <b>НОВЕ ЗАМОВЛЕННЯ: {order_id}</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клієнт:</b> @{message.from_user.username or 'немає'} (ID: <code>{client_id}</code>)\n"
        f"📚 <b>Предмет:</b> {user_data['subject']}\n"
        f"📝 <b>Тип:</b> {user_data['work_type']}\n"
        f"📄 <b>Обсяг:</b> {user_data.get('pages', '—')} сторінок\n"
        f"⏳ <b>Дедлайн:</b> {user_data['deadline']}\n"
        f"📱 <b>Телефон:</b> {user_data['phone']}\n"
        f"📋 <b>Деталі:</b> {user_data['details']}"
    )
    if not db_saved:
        admin_msg += "\n\n⚠️ <b>[БД: не збережено — перевірте логи]</b>"

    try:
        await bot.send_message(
            ADMIN_ID,
            admin_msg,
            reply_markup=get_admin_action_kb(order_id, client_id),
        )
        await message.answer(
            "✨ <b>Ваша заявка успішно зареєстрована!</b>\n\n"
            "Менеджер Оленка розгляне її та зв'яжеться з вами найближчим часом.",
            reply_markup=get_main_kb(),
        )
        await state.clear()

    except Exception as e:
        logging.error(f"❌ Надсилання адміну: {e}")
        # Стан НЕ очищаємо — order_id збережено, повторна спроба не дасть дублікату
        await message.answer(
            "⚠️ Виникла помилка зв'язку з менеджером.\n"
            "Спробуйте надіслати деталі ще раз за кілька секунд."
        )


# =====================================================================
# 11. АДМІНСЬКІ ДІЇ
#
# ВИПРАВЛЕНО: оригінальний хендлер waiting_for_price робив
# `state.clear()` при зверненні від не-адміна, скидаючи стан адміна.
# Замінено на фільтр F.from_user.func(...) на рівні декоратора —
# повідомлення від інших юзерів просто падають у fallback.
# =====================================================================
@router.callback_query(OrderAction.filter(F.action == "a"))
async def admin_accept(callback: CallbackQuery, callback_data: OrderAction, bot: Bot):
    async with async_session() as session:
        async with session.begin():
            await session.execute(
                update(Order)
                .where(Order.order_id == callback_data.order_id)
                .values(status="В роботі")
            )
    try:
        await bot.send_message(
            callback_data.client_id,
            f"✅ Ваше замовлення <b>{callback_data.order_id}</b> схвалено та взято в роботу!",
        )
        await callback.message.edit_text(
            callback.message.text + "\n\n🟢 <b>Статус: Прийнято в роботу</b>"
        )
    except Exception as e:
        logging.error(f"❌ admin_accept: {e}")
        await callback.message.answer(f"❌ Помилка інформування клієнта: {e}")
    await callback.answer("✅ Прийнято")


@router.callback_query(OrderAction.filter(F.action == "r"))
async def admin_reject(callback: CallbackQuery, callback_data: OrderAction, bot: Bot):
    async with async_session() as session:
        async with session.begin():
            await session.execute(
                update(Order)
                .where(Order.order_id == callback_data.order_id)
                .values(status="Відхилено")
            )
    try:
        await bot.send_message(
            callback_data.client_id,
            f"❌ На жаль, замовлення <b>{callback_data.order_id}</b> відхилено менеджером.\n"
            f"Якщо є питання — зверніться до підтримки.",
        )
        await callback.message.edit_text(
            callback.message.text + "\n\n🔴 <b>Статус: Відхилено</b>"
        )
    except Exception as e:
        logging.error(f"❌ admin_reject: {e}")
        await callback.message.answer(f"❌ Помилка інформування клієнта: {e}")
    await callback.answer("❌ Відхилено")


@router.callback_query(OrderAction.filter(F.action == "p"))
async def admin_invoice_request(callback: CallbackQuery, callback_data: OrderAction, state: FSMContext):
    # Додаткова перевірка на рівні коду (хоча фільтр декоратора надійніший)
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Доступ заборонено!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_price)
    await state.update_data(
        target_client_id = callback_data.client_id,
        target_order_id  = callback_data.order_id,
    )
    await callback.message.answer(
        f"💵 Введіть фінальну ціну в грн (тільки ціле число)\n"
        f"для замовлення <b>{callback_data.order_id}</b>:"
    )
    await callback.answer()


# ВИПРАВЛЕНО: фільтр F.from_user.func(...) на рівні декоратора.
# Повідомлення від юзерів при активному стані адміна йдуть у fallback —
# стан адміна не зачіпається і не скидається.
@router.message(
    AdminStates.waiting_for_price,
    F.from_user.func(lambda u: u.id == ADMIN_ID),
)
async def admin_send_invoice(message: Message, state: FSMContext, bot: Bot):
    price_text = (message.text or "").strip()
    if not price_text.isdigit() or int(price_text) <= 0:
        await message.answer("❌ Введіть ціну позитивним цілим числом:")
        return

    price_val = int(price_text)
    adm_data  = await state.get_data()
    order_id  = adm_data["target_order_id"]
    client_id = int(adm_data["target_client_id"])

    async with async_session() as session:
        async with session.begin():
            await session.execute(
                update(Order)
                .where(Order.order_id == order_id)
                .values(price=price_val, status="Очікує оплати")
            )

    if PROVIDER_TOKEN:
        try:
            await bot.send_invoice(
                chat_id       = client_id,
                title         = f"Оплата замовлення {order_id}",
                description   = f"Рахунок на оплату студентської роботи по замовленню {order_id}.",
                payload       = order_id,
                provider_token= PROVIDER_TOKEN,
                currency      = "UAH",
                prices        = [LabeledPrice(label="Вартість послуг EduExpert", amount=price_val * 100)],
            )
            await message.answer(f"✅ Платіжний інвойс на суму {price_val} грн надіслано клієнту.")
        except Exception as e:
            logging.error(f"❌ Telegram Invoice: {e}")
            await message.answer(f"⚠️ Помилка генерації платежу: {e}. Надсилаю звичайне повідомлення.")
            await bot.send_message(
                client_id,
                f"💰 Замовлення <b>{order_id}</b> оцінено у <b>{price_val} грн</b>. "
                f"Зв'яжіться з менеджером для оплати.",
            )
    else:
        await bot.send_message(
            client_id,
            f"💰 Замовлення <b>{order_id}</b> оцінено у <b>{price_val} грн</b>.\n"
            f"Для оплати напишіть менеджеру Оленці.",
        )
        await message.answer(f"✅ Сповіщення про ціну {price_val} грн надіслано.")

    await state.clear()


# =====================================================================
# 12. ОБРОБКА ОНЛАЙН-ОПЛАТИ — збережено з Gemini-версії
# =====================================================================
@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def process_successful_payment(message: Message, bot: Bot):
    order_id = message.successful_payment.invoice_payload
    async with async_session() as session:
        async with session.begin():
            await session.execute(
                update(Order)
                .where(Order.order_id == order_id)
                .values(is_paid=True, status="Оплачено")
            )
    await message.answer(
        f"🎉 Дякуємо! Оплата замовлення <b>{order_id}</b> пройшла успішно. "
        f"Роботу передано у виконання."
    )
    await bot.send_message(
        ADMIN_ID,
        f"🔔 <b>ПЛАТІЖ ОТРИМАНО:</b> Замовлення <b>{order_id}</b> успішно оплачено клієнтом.",
    )


# =====================================================================
# 13. МОЇ ЗАМОВЛЕННЯ — зчитування з PostgreSQL
# =====================================================================
@router.message(F.text == "📊 Мої замовлення")
async def process_my_orders(message: Message):
    async with async_session() as session:
        result = await session.execute(
            select(Order)
            .where(Order.tg_id == message.from_user.id)
            .order_by(Order.created_at.desc())
        )
        orders = result.scalars().all()

    if not orders:
        await message.answer(
            "У вас немає зареєстрованих замовлень.\n"
            "Натисніть <b>📝 Створити заявку</b>, щоб оформити перше.",
            reply_markup=get_main_kb(),
        )
        return

    res_text = "📋 <b>Ваша історія замовлень:</b>\n\n"
    for o in orders:
        pay_status   = "💳 Оплачено" if o.is_paid else "⏳ Очікує оплати"
        price_status = f"{o.price} грн ({pay_status})" if o.price > 0 else "На оцінці менеджера"
        res_text += (
            f"🔹 <b>ID:</b> <code>{o.order_id}</code>\n"
            f"📚 <b>Предмет:</b> {o.subject} ({o.work_type})\n"
            f"📊 <b>Статус:</b> {o.status}\n"
            f"💵 <b>Вартість:</b> {price_status}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    await message.answer(res_text, reply_markup=get_main_kb())


# =====================================================================
# 14. FALLBACK
#
# ВИПРАВЛЕНО: додано F.text — медіа (фото, голос, стікери) не
# отримують текстову відповідь "не зрозумів".
# StateFilter(None) — тільки коли немає активного стану.
# =====================================================================
@router.message(StateFilter(None), F.text)
async def process_unknown(message: Message):
    await message.answer(
        "Я вас не зрозумів. 🤔\n"
        "Будь ласка, оберіть потрібний пункт у меню нижче:",
        reply_markup=get_main_kb(),
    )


# =====================================================================
# 15. ВЕБ-АДМІН ПАНЕЛЬ (FastAPI + SQLAdmin) — збережено з Gemini
# =====================================================================
fastapi_app      = FastAPI(title="EduExpert Admin Panel")
admin_dashboard  = Admin(fastapi_app, engine, title="EduExpert Management Console")


class OrderAdminView(ModelView, model=Order):
    name               = "Замовлення"
    name_plural        = "Замовлення"
    icon               = "fa-solid fa-graduation-cap"
    column_list        = [Order.id, Order.order_id, Order.subject, Order.work_type,
                          Order.status, Order.price, Order.is_paid, Order.created_at]
    column_searchable_list = [Order.order_id, Order.subject, Order.phone]
    column_filters         = [Order.status, Order.is_paid, Order.work_type]
    form_columns           = [Order.status, Order.price, Order.is_paid, Order.details]


admin_dashboard.add_view(OrderAdminView)


# =====================================================================
# 16. ЗАПУСК
# =====================================================================
async def run_bot():
    # Перевірка Redis через реальний ping — до старту polling
    storage = await _build_storage()

    dp = Dispatcher(storage=storage)
    dp.message.middleware(ThrottlingMiddleware(limit=0.8))
    dp.include_router(router)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    # Створюємо таблиці якщо їх ще немає
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logging.info("🚀 PostgreSQL синхронізовано. Бот запущено.")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        logging.info("🛑 Бот зупинено.")


async def main():
    config = uvicorn.Config(fastapi_app, host="127.0.0.1", port=8000, log_level="warning")
    server = uvicorn.Server(config)
    # Бот і веб-панель працюють паралельно
    await asyncio.gather(run_bot(), server.serve())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Систему EduExpert зупинено.")
