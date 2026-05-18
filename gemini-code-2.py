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
from aiogram import Bot, Dispatcher, html, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.storage.memory import MemoryStorage
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
# 1. НАЛАШТУВАННЯ ТА ІНІЦІАЛІЗАЦІЯ
# =====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_ENV = os.getenv("ADMIN_ID")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN") # Для Stripe/LiqPay/WayForPay через BotFather
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/eduexpert")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MANAGER_LINK = os.getenv("MANAGER_LINK", "https://t.me/Olenka_EduExpert")
REVIEWS_LINK = os.getenv("REVIEWS_LINK", "https://t.me/EduExpert_Reviews")

if not all([BOT_TOKEN, ADMIN_ID_ENV]):
    raise ValueError("❌ Критична помилка: Перевірте BOT_TOKEN та ADMIN_ID у змінних оточення!")

ADMIN_ID = int(ADMIN_ID_ENV)

# Підключення бази даних
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Налаштування FSM сховища
# Використовуємо локальну пам'ять, щоб бот не залежав від сервера Redis
storage = MemoryStorage()
logging.info("🧠 Тимчасово активовано MemoryStorage для станів.")
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)
fastapi_app = FastAPI(title="EduExpert Admin Panel")

# Тарифи для калькулятора (грн за 1 сторінку)
PRICING = {"Реферат": 40, "Контрольна": 50, "Курсова": 70, "Дипломна": 120}

# =====================================================================
# 2. МОДЕЛІ БАЗИ ДАНИХ (SQLAlchemy)
# =====================================================================
class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    username: Mapped[str | None] = mapped_column(String(100))
    tg_id: Mapped[int] = mapped_column(BigInteger, index=True)
    subject: Mapped[str] = mapped_column(String(255))
    work_type: Mapped[str] = mapped_column(String(100))
    pages: Mapped[int] = mapped_column(Integer)
    deadline: Mapped[str] = mapped_column(String(100))
    phone: Mapped[str] = mapped_column(String(30))
    details: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(50), default="Нове")
    price: Mapped[int] = mapped_column(Integer, default=0)
    is_paid: Mapped[bool] = mapped_column(Boolean, default=False)

# =====================================================================
# 3. АНТИСПАМ (Throttling Middleware)
# =====================================================================
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 1.0):
        super().__init__()
        self.limit = limit
        self.caches: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        if not event.from_user:
            return await handler(event, data)
            
        user_id = event.from_user.id
        now = asyncio.get_event_loop().time()
        
        if user_id in self.caches:
            if now - self.caches[user_id] < self.limit:
                # Ігноруємо флуд-повідомлення
                return
                
        self.caches[user_id] = now
        return await handler(event, data)

dp.message.middleware(ThrottlingMiddleware(limit=0.8))

# =====================================================================
# 4. ДАТАКЛАСИ ТА СТАНИ FSM
# =====================================================================
class OrderAction(CallbackData, prefix="ord"):
    action: str
    order_id: str
    client_id: int

class OrderStates(StatesGroup):
    waiting_for_subject = State()
    waiting_for_type = State()
    waiting_for_pages = State()
    waiting_for_deadline = State()
    waiting_for_phone = State()
    waiting_for_details = State()

class CalcStates(StatesGroup):
    waiting_for_type = State()
    waiting_for_pages = State()

class AdminStates(StatesGroup):
    waiting_for_price = State()

# =====================================================================
# 5. СТВОРЕННЯ КЛАВІАТУР
# =====================================================================
def get_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📝 Створити заявку"), KeyboardButton(text="🧮 Калькулятор цін")],
        [KeyboardButton(text="📊 Мої замовлення"), KeyboardButton(text="ℹ️ FAQ / Довідка")],
        [KeyboardButton(text="💬 Відгуки"), KeyboardButton(text="👤 Зв'язок з менеджером")]
    ], resize_keyboard=True)

def get_cancel_btn() -> list:
    return [KeyboardButton(text="❌ Скасувати процес")]

def get_type_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Реферат"), KeyboardButton(text="Контрольна")],
        [KeyboardButton(text="Курсова"), KeyboardButton(text="Дипломна")],
        get_cancel_btn()
    ], resize_keyboard=True, one_time_keyboard=True)

def get_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📱 Надіслати номер", request_contact=True)],
        get_cancel_btn()
    ], resize_keyboard=True, one_time_keyboard=True)

def get_simple_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[get_cancel_btn()], resize_keyboard=True)

def get_admin_action_kb(order_id: str, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Прийняти", callback_data=OrderAction(action="accept", order_id=order_id, client_id=client_id).pack()),
            InlineKeyboardButton(text="❌ Відхилити", callback_data=OrderAction(action="reject", order_id=order_id, client_id=client_id).pack())
        ],
        [
            InlineKeyboardButton(text="💰 Виставити рахунок", callback_data=OrderAction(action="invoice", order_id=order_id, client_id=client_id).pack())
        ]
    ])

# =====================================================================
# 6. БАЗОВІ КОМАНДИ ТА СКАСУВАННЯ
# =====================================================================
@dp.message(Command("cancel"))
@dp.message(F.text == "❌ Скасувати процес")
async def process_global_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Немає активних процесів для скасування.", reply_markup=get_main_kb())
        return
    await state.clear()
    await message.answer("❌ Процес повністю скасовано. Повертаємось у меню.", reply_markup=get_main_kb())

@dp.message(Command("start"))
async def process_start_cmd(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Вітаємо в <b>EduExpert</b> — автоматизованій системі замовлення студентських робіт!\n\n"
        "Тут ви можете розрахувати вартість, оформити офіційну заявку, відстежувати статуси "
        "та здійснювати безпечну онлайн-оплату ваших замовлень.",
        reply_markup=get_main_kb()
    )

# =====================================================================
# 7. ІНФОРМАЦІЙНІ БЛОКИ (FAQ, ВІДГУКИ, МЕНЕДЖЕР)
# =====================================================================
@dp.message(F.text == "ℹ️ FAQ / Довідка")
async def process_faq(message: Message):
    text = (
        "ℹ️ <b>FAQ (Часті запитання та відповіді):</b>\n\n"
        "⚡ <b>Які реальні терміни виконання?</b>\n"
        "Реферати виконуються за 2-3 дні, курсові проекти — від 5 до 9 днів, дипломні роботи — від 14 діб.\n\n"
        "⚡ <b>Чи безкоштовні виправлення?</b>\n"
        "Абсолютно. Всі доопрацювання та правки вашого наукового керівника в межах первинного ТЗ безкоштовні протягом усього гарантійного терміну.\n\n"
        "⚡ <b>Який рівень унікальності тексту?</b>\n"
        "Стандартна перевірка проходить через системи Unicheck/Антиплагіат із показником від 75-80% унікальності."
    )
    await message.answer(text, reply_markup=get_main_kb())

@dp.message(F.text == "💬 Відгуки")
async def process_reviews(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Переглянути відгуки в Telegram", url=REVIEWS_LINK)]
    ])
    await message.answer("💬 Ми відкриті перед нашими клієнтами. Ознайомтеся з реальними відгуками студентів за посиланням нижче:", reply_markup=kb)

@dp.message(F.text == "👤 Зв'язок з менеджером")
async def process_manager_contact(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👩‍💻 Написати Оленці", url=MANAGER_LINK)]
    ])
    await message.answer("👤 Потрібно терміново передати методичні вказівки або обговорити нестандартне замовлення? Наш старший менеджер Оленка допоможе у всьому!", reply_markup=kb)

# =====================================================================
# 8. МОДУЛЬ КАЛЬКУЛЯТОРА ЦІН
# =====================================================================
@dp.message(F.text == "🧮 Калькулятор цін")
async def process_calc_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Оберіть тип студентської роботи для калькуляції:", reply_markup=get_type_kb())
    await state.set_state(CalcStates.waiting_for_type)

@dp.message(CalcStates.waiting_for_type)
async def process_calc_type(message: Message, state: FSMContext):
    w_type = message.text.strip()
    if w_type not in PRICING:
        await message.answer("❌ Будь ласка, оберіть варіант із запропонованої клавіатури!")
        return
    await state.update_data(work_type=w_type)
    await message.answer("Введіть необхідну кількість сторінок (числом):", reply_markup=get_simple_cancel_kb())
    await state.set_state(CalcStates.waiting_for_pages)

@dp.message(CalcStates.waiting_for_pages)
async def process_calc_pages(message: Message, state: FSMContext):
    pages_input = message.text.strip()
    if not pages_input.isdigit():
        await message.answer("❌ Обсяг повинен бути цілим числом. Спробуйте ще раз:")
        return
    
    pages = int(pages_input)
    data = await state.get_data()
    w_type = data["work_type"]
    
    est_price = PRICING[w_type] * pages
    await state.update_data(pages=pages)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Створити заявку на основі розрахунку", callback_data="convert_calc_to_order")]
    ])
    
    await message.answer(
        f"🧮 <b>Попередній кошторис:</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"🔹 <b>Тип:</b> {w_type}\n"
        f"📄 <b>Обсяг:</b> {pages} сторінок\n"
        f"💵 <b>Орієнтовна ціна:</b> від <b>{est_price} грн</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"<i>*Остаточна ціна фіксується менеджером після оцінки складності теми.</i>",
        reply_markup=get_main_kb()
    )
    await message.answer("Бажаєте надіслати цю заявку менеджеру?", reply_markup=kb)

@dp.callback_query(F.data == "convert_calc_to_order")
async def process_calc_redirect(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "work_type" not in data:
        await callback.answer("Сесія застаріла. Почніть заново через головне меню.", show_alert=True)
        return
    
    await callback.message.answer("Введіть предмет або конкретну тему роботи:", reply_markup=get_simple_cancel_kb())
    await state.set_state(OrderStates.waiting_for_subject)
    await callback.answer()

# =====================================================================
# 9. МОДУЛЬ СТВОРЕННЯ ЗАЯВКИ ТА СТРОГОЇ ВАЛІДАЦІЇ
# =====================================================================
@dp.message(F.text == "📝 Створити заявку")
async def process_order_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Введіть предмет або тему наукової роботи (наприклад: <i>Цивільне право, Маркетинг</i>):", reply_markup=get_simple_cancel_kb())
    await state.set_state(OrderStates.waiting_for_subject)

@dp.message(OrderStates.waiting_for_subject)
async def process_subject_state(message: Message, state: FSMContext):
    await state.update_data(subject=message.text.strip())
    data = await state.get_data()
    if "work_type" in data:  # Перейшли з калькулятора
        await message.answer(f"Тип роботи: <b>{data['work_type']}</b> ({data['pages']} ст.).\nВкажіть кінцевий дедлайн (наприклад: <i>до 15.06.2026</i>):", reply_markup=get_simple_cancel_kb())
        await state.set_state(OrderStates.waiting_for_deadline)
    else:
        await message.answer("Оберіть тип роботи з клавіатури або напишіть свій варіант:", reply_markup=get_type_kb())
        await state.set_state(OrderStates.waiting_for_type)

@dp.message(OrderStates.waiting_for_type)
async def process_type_state(message: Message, state: FSMContext):
    await state.update_data(work_type=message.text.strip())
    await message.answer("Вкажіть необхідну кількість сторінок (введіть тільки чисте число):", reply_markup=get_simple_cancel_kb())
    await state.set_state(OrderStates.waiting_for_pages)

@dp.message(OrderStates.waiting_for_pages)
async def process_pages_state(message: Message, state: FSMContext):
    pages_text = message.text.strip()
    if not pages_text.isdigit():
        await message.answer("❌ Помилка валідації! Введіть кількість сторінок цілим числом (наприклад: 35):")
        return
    await state.update_data(pages=int(pages_text))
    await message.answer("Вкажіть бажаний дедлайн здачі роботи (наприклад: <i>до 20.06.2026</i>):", reply_markup=get_simple_cancel_kb())
    await state.set_state(OrderStates.waiting_for_deadline)

@dp.message(OrderStates.waiting_for_deadline)
async def process_deadline_state(message: Message, state: FSMContext):
    await state.update_data(deadline=message.text.strip())
    await message.answer("Надішліть ваш контактний телефон для оперативного зв'язку (через кнопку або у форматі +380XXXXXXXXX):", reply_markup=get_phone_kb())
    await state.set_state(OrderStates.waiting_for_phone)

@dp.message(OrderStates.waiting_for_phone)
async def process_phone_state(message: Message, state: FSMContext):
    phone_input = message.contact.phone_number if message.contact else message.text.strip()
    if not re.match(r"^\+?\d{9,16}$", phone_input):
        await message.answer("❌ Помилка валідації телефону! Перевірте формат (+380XXXXXXXXX) та введіть ще раз:")
        return
    await state.update_data(phone=phone_input)
    await message.answer("Введіть деталі вашого замовлення (план, специфічні вимоги викладача, посилання на джерела):", reply_markup=get_simple_cancel_kb())
    await state.set_state(OrderStates.waiting_for_details)

@dp.message(OrderStates.waiting_for_details)
async def process_details_state(message: Message, state: FSMContext):
    await state.update_data(details=message.text.strip())
    user_data = await state.get_data()
    
    order_uuid = f"EDU-{datetime.now().strftime('%d%m%Y')}-{uuid.uuid4().hex[:6].upper()}"
    client_id = message.from_user.id
    
    # Збереження в PostgreSQL за допомогою SQLAlchemy
    async with async_session() as session:
        async with session.begin():
            new_order = Order(
                order_id=order_uuid,
                username=message.from_user.username,
                tg_id=client_id,
                subject=user_data["subject"],
                work_type=user_data["work_type"],
                pages=user_data["pages"],
                deadline=user_data["deadline"],
                phone=user_data["phone"],
                details=user_data["details"]
            )
            session.add(new_order)
            
    # Формування сповіщення для адміністратора
    admin_msg = (
        f"🔥 <b>НОВЕ ЗАМОВЛЕННЯ: {order_uuid}</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клієнт:</b> @{message.from_user.username or 'немає'} (ID: <code>{client_id}</code>)\n"
        f"📚 <b>Предмет:</b> {user_data['subject']}\n"
        f"📝 <b>Тип:</b> {user_data['work_type']}\n"
        f"📄 <b>Обсяг:</b> {user_data['pages']} сторінок\n"
        f"⏳ <b>Дедлайн:</b> {user_data['deadline']}\n"
        f"📱 <b>Телефон:</b> {user_data['phone']}\n"
        f"📋 <b>Деталі:</b> {user_data['details']}"
    )
    
    try:
        await bot.send_message(chat_id=ADMIN_ID, text=admin_msg, reply_markup=get_admin_action_kb(order_uuid, client_id))
        await message.answer("✨ Ваша детальна заявка успішно зареєстрована та передана менеджеру Оленці на оцінку! Ви отримаєте сповіщення найближчим часом.", reply_markup=get_main_kb())
        await state.clear()
    except Exception as e:
        logging.error(f"Помилка відправки адміну: {e}")
        await message.answer("⚠️ Сталася технічна помилка зв'язку з адмін-панеллю. Спробуйте надіслати фінальний текст ще раз.")

# =====================================================================
# 10. АДМІНСЬКІ ДІЇ ТА ВИСТАВЛЕННЯ ІНВОЙСІВ НА ОПЛАТУ
# =====================================================================
@dp.callback_query(OrderAction.filter(F.action == "accept"))
async def admin_accept(callback: CallbackQuery, callback_data: OrderAction):
    async with async_session() as session:
        async with session.begin():
            await session.execute(update(Order).where(Order.order_id == callback_data.order_id).values(status="В роботі"))
            
    try:
        await callback.bot.send_message(chat_id=callback_data.client_id, text=f"✅ Ваше замовлення <b>{callback_data.order_id}</b> успішно схвалено та взято в роботу!")
        await callback.message.edit_text(callback.message.text + "\n\n🟢 <b>Статус: Прийнято в роботу</b>")
    except Exception as e:
        await callback.message.answer(f"Помилка інформування клієнта: {e}")
    await callback.answer()

@dp.callback_query(OrderAction.filter(F.action == "reject"))
async def admin_reject(callback: CallbackQuery, callback_data: OrderAction):
    async with async_session() as session:
        async with session.begin():
            await session.execute(update(Order).where(Order.order_id == callback_data.order_id).values(status="Відхилено"))
            
    try:
        await callback.bot.send_message(chat_id=callback_data.client_id, text=f"❌ На жаль, ваше замовлення <b>{callback_data.order_id}</b> було відхилено менеджером.")
        await callback.message.edit_text(callback.message.text + "\n\n🔴 <b>Статус: Відхилено</b>")
    except Exception as e:
        await callback.message.answer(f"Помилка інформування клієнта: {e}")
    await callback.answer()

@dp.callback_query(OrderAction.filter(F.action == "invoice"))
async def admin_invoice_request(callback: CallbackQuery, callback_data: OrderAction, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Доступ заборонено!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_price)
    await state.update_data(target_client_id=callback_data.client_id, target_order_id=callback_data.order_id, admin_msg_id=callback.message.message_id)
    await callback.message.answer(f"💵 Введіть фінальну ціну в грн (тільки число) для <b>{callback_data.order_id}</b>:")
    await callback.answer()

@dp.message(AdminStates.waiting_for_price)
async def admin_send_invoice(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return
        
    price_text = message.text.strip()
    if not price_text.isdigit():
        await message.answer("❌ Введіть ціну цілим числовим значенням:")
        return
        
    price_val = int(price_text)
    adm_data = await state.get_data()
    order_id = adm_data["target_order_id"]
    client_id = int(adm_data["target_client_id"])
    
    # Оновлюємо дані в базі
    async with async_session() as session:
        async with session.begin():
            await session.execute(update(Order).where(Order.order_id == order_id).values(price=price_val, status="Очікує оплати"))
            
    # Надсилаємо офіційний платіжний інвойс користувачу
    if PROVIDER_TOKEN:
        try:
            await message.bot.send_invoice(
                chat_id=client_id,
                title=f"Оплата замовлення {order_id}",
                description=f"Рахунок на оплату студентської роботи по замовленню {order_id}.",
                payload=order_id,
                provider_token=PROVIDER_TOKEN,
                currency="UAH",
                prices=[LabeledPrice(label="Вартість послуг EduExpert", amount=price_val * 100)] # Сума в копійках
            )
            await message.answer(f"✅ Офіційний платіжний інвойс на суму {price_val} грн надіслано клієнту.")
        except Exception as e:
            await message.answer(f"⚠️ Помилка генерації Telegram платежу: {e}. Надсилаю звичайне повідомлення.")
            await message.bot.send_message(chat_id=client_id, text=f"💰 Ваше замовлення <b>{order_id}</b> оцінено у <b>{price_val} грн</b>. Зв'яжіться з менеджером для оплати.")
    else:
        # Режим без реального токена оплати
        await message.bot.send_message(chat_id=client_id, text=f"💰 Ваше замовлення <b>{order_id}</b> оцінено у <b>{price_val} грн</b>. Для оплати напишіть менеджеру Оленці.")
        await message.answer(f"✅ Сповіщення про ціну {price_val} грн надіслано.")
        
    await state.clear()

# =====================================================================
# 11. ОБРОБКА РЕЗУЛЬТАТІВ ОНЛАЙН-ОПЛАТИ
# =====================================================================
@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    # Підтверджуємо платіжній системі, що товар в наявності та все ок
    await pre_checkout_query.answer(ok=True)

@dp.message(F.successful_payment)
async def process_successful_payment(message: Message):
    order_id = message.successful_payment.invoice_payload
    
    async with async_session() as session:
        async with session.begin():
            await session.execute(update(Order).where(Order.order_id == order_id).values(is_paid=True, status="Оплачено"))
            
    await message.answer(f"🎉 Дякуємо! Ваша онлайн-оплата замовлення <b>{order_id}</b> пройшла успішно. Робота передана у виконання.")
    await bot.send_message(chat_id=ADMIN_ID, text=f"🔔 <b>ПЛАТІЖ ОТРИМАНО:</b> Замовлення <b>{order_id}</b> успішно оплачено клієнтом через шлюз.")

# =====================================================================
# 12. ПЕРЕВІРКА СТАТУСУ (Зчитування з PostgreSQL)
# =====================================================================
@dp.message(F.text == "📊 Мої замовлення")
async def process_my_orders_status(message: Message):
    async with async_session() as session:
        stmt = select(Order).where(Order.tg_id == message.from_user.id).order_by(Order.created_at.desc())
        result = await session.execute(stmt)
        orders = result.scalars().all()
        
    if not orders:
        await message.answer("У вас немає зареєстрованих замовлень у нашій системі.", reply_markup=get_main_kb())
        return
        
    res_text = "📋 <b>Ваша історія замовлень та їх статуси:</b>\n\n"
    for o in orders:
        pay_status = "💳 Оплачено" if o.is_paid else "⏳ Очікує оплати"
        price_status = f"{o.price} грн ({pay_status})" if o.price > 0 else "На оцінці менеджера"
        
        res_text += (
            f"🔹 <b>ID замовлення:</b> <code>{o.order_id}</code>\n"
            f"📚 <b>Предмет:</b> {o.subject} ({o.work_type})\n"
            f"📊 <b>Статус:</b> {o.status}\n"
            f"💵 <b>Вартість:</b> {price_status}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    await message.answer(res_text, reply_markup=get_main_kb())

# =====================================================================
# 13. ВЕБ-АДМІН ПАНЕЛЬ (FastAPI + SQLAdmin)
# =====================================================================
admin_dashboard = Admin(fastapi_app, engine, title="EduExpert Management Console")

class OrderAdminView(ModelView, model=Order):
    name = "Замовлення"
    name_plural = "Замовлення"
    icon = "fa-solid fa-graduation-cap"
    column_list = [Order.id, Order.order_id, Order.subject, Order.work_type, Order.status, Order.price, Order.is_paid, Order.created_at]
    column_searchable_list = [Order.order_id, Order.subject, Order.phone]
    column_filters = [Order.status, Order.is_paid, Order.work_type]
    form_columns = [Order.status, Order.price, Order.is_paid, Order.details]

admin_dashboard.add_view(OrderAdminView)

# =====================================================================
# 14. ЗАПУСК ОБОХ СИСТЕМ (Асинхронний Event Loop)
# =====================================================================
@dp.message(StateFilter(None))
async def process_unknown_messages(message: Message):
    await message.answer("Я вас не зрозумів. Будь ласка, оберіть потрібний пункт у меню нижче:", reply_markup=get_main_kb())

async def run_bot():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logging.info("🚀 База даних PostgreSQL синхронізована.")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

async def main_entry():
    # Запуск бота паралельно з веб-сервером FastAPI на порті 8000
    config = uvicorn.Config(fastapi_app, host="127.0.0.1", port=8000, log_level="warning")
    server = uvicorn.Server(config)
    
    await asyncio.gather(
        run_bot(),
        server.serve()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main_entry())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Систему EduExpert зупинено.")
