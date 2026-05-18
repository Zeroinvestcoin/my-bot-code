import os
import json
import logging
import asyncio
import uuid
import re
import threading
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import sentry_sdk
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.filters.callback_data import CallbackData
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

# =====================================================================
# 1. КОНФІГУРАЦІЯ ТА КОНСТАНТИ
# =====================================================================
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID_ENV = os.getenv("ADMIN_ID")
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SENTRY_DSN = os.getenv("SENTRY_DSN")

if not all([TOKEN, ADMIN_ID_ENV, SPREADSHEET_KEY]):
    raise ValueError("❌ Відсутні обов'язкові змінні оточення (BOT_TOKEN, ADMIN_ID, SPREADSHEET_KEY)")

ADMIN_ID = int(ADMIN_ID_ENV)

# Константи колонок (1-індекс, як у gspread)
COL_ORDER_ID = 1
COL_STATUS = 11
COL_PRICE = 12

SHEET_HEADERS = [
    "ID Замовлення", "Дата створення", "Username", "Telegram ID",
    "Предмет", "Тип роботи", "Обсяг (ст.)", "Дедлайн",
    "Телефон", "Деталі", "Статус", "Ціна"
]

# Ліміти валідації
MIN_SUBJECT_LEN = 2
MAX_SUBJECT_LEN = 200
MIN_DETAILS_LEN = 5
MAX_DETAILS_LEN = 2000

if SENTRY_DSN:
    sentry_sdk.init(SENTRY_DSN)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =====================================================================
# 2. ІНІЦІАЛІЗАЦІЯ З REDIS FALLBACK
# =====================================================================
try:
    storage = RedisStorage.from_url(REDIS_URL)
    logging.info("✅ Успішно підключено RedisStorage.")
except Exception as e:
    from aiogram.fsm.storage.memory import MemoryStorage
    logging.warning(f"⚠️ Redis недоступний ({e}). Перемикаюсь на аварійний MemoryStorage!")
    storage = MemoryStorage()

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=storage)


# =====================================================================
# 3. CALLBACK DATA
# Увага: Telegram обмежує callback_data до 64 байт.
# order_id зберігаємо коротким (EDU-DDMMYYYY-XXXXXX = 20 символів).
# client_id — до 10 цифр. Разом з префіксом "order:" вкладається в ліміт.
# =====================================================================
class OrderAction(CallbackData, prefix="ord"):
    action: str      # "a" = accept, "r" = reject, "p" = price
    order_id: str
    client_id: int


class OrderStates(StatesGroup):
    waiting_for_subject = State()
    waiting_for_type = State()
    waiting_for_pages = State()
    waiting_for_deadline = State()
    waiting_for_phone = State()
    waiting_for_details = State()


class AdminStates(StatesGroup):
    waiting_for_price = State()


# =====================================================================
# 4. GOOGLE SHEETS: asyncio.Lock + автостворення + перевірка заголовків
#
# ВИПРАВЛЕНО: threading.Lock() блокував event loop. Замінено на
# asyncio.Lock(). Усі синхронні виклики Google API йдуть через
# asyncio.to_thread(), тому блокування event loop'а немає.
# =====================================================================
_client_cache = None
_sheet_cache = None
_cache_lock = asyncio.Lock()   # ← ВИПРАВЛЕНО: був threading.Lock()


def _sync_get_google_sheet(force_refresh: bool = False):
    """
    Синхронна частина — виконується в окремому thread через asyncio.to_thread.
    Використовує модуль-рівневі змінні кешу без власного lock (lock — зовні).
    """
    global _client_cache, _sheet_cache

    if force_refresh:
        _client_cache = None
        _sheet_cache = None
        logging.info("🔄 Примусове скидання кешу Google API.")

    if _sheet_cache is not None:
        return _sheet_cache

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if not _client_cache:
        creds_json_str = os.getenv("GOOGLE_CREDS_JSON")
        if creds_json_str:
            try:
                creds = Credentials.from_service_account_info(
                    json.loads(creds_json_str), scopes=scopes
                )
            except Exception:
                creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
        _client_cache = gspread.authorize(creds)

    wb = _client_cache.open_by_key(SPREADSHEET_KEY)
    try:
        sheet = wb.worksheet("Orders")
    except WorksheetNotFound:
        logging.info("📝 Лист 'Orders' не знайдено. Створюю новий...")
        sheet = wb.add_worksheet("Orders", rows=5000, cols=20)

    # ВИПРАВЛЕНО: перевіряємо чи заголовки вже є, щоб не дублювати їх
    # при force_refresh або після помилкового retry.
    existing = sheet.row_values(1)
    if existing != SHEET_HEADERS:
        if not existing:
            sheet.append_row(SHEET_HEADERS)
            logging.info("📝 Заголовки додано до нового листа.")
        else:
            logging.warning("⚠️ Перший рядок таблиці не збігається з очікуваними заголовками!")

    _sheet_cache = sheet
    return _sheet_cache


async def get_google_sheet(force_refresh: bool = False):
    """Асинхронна обгортка з asyncio.Lock для безпечного кешування."""
    async with _cache_lock:
        return await asyncio.to_thread(_sync_get_google_sheet, force_refresh)


async def save_to_google_sheets(data: dict) -> bool:
    """
    Зберігає рядок замовлення в таблицю.
    ВИПРАВЛЕНО: викликається ДО state.clear() у process_details,
    щоб дані не зникали при помилці.
    """
    row = [
        data.get("order_id"),
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        f"@{data.get('username')}" if data.get("username") else "Немає",
        str(data.get("tg_id")),
        data.get("subject"),
        data.get("work_type"),
        data.get("pages"),
        data.get("deadline"),
        data.get("phone"),
        data.get("details"),
        "Нове",
        "Не оцінено",
    ]

    async def _do_append(force: bool = False):
        sheet = await get_google_sheet(force_refresh=force)
        await asyncio.to_thread(sheet.append_row, row)

    try:
        await _do_append()
        return True
    except APIError as e:
        if e.response is not None and e.response.status_code in [401, 403]:
            try:
                await _do_append(force=True)
                return True
            except Exception as inner:
                logging.error(f"❌ Google Sheets (після refresh): {inner}")
        else:
            logging.error(f"❌ Google Sheets APIError: {e}")
    except Exception as e:
        logging.error(f"❌ Google Sheets невідома помилка: {e}")
    return False


async def update_status_or_price(order_id: str, col_idx: int, new_value: str) -> bool:
    async def _do_update(force: bool = False):
        sheet = await get_google_sheet(force_refresh=force)

        def _find_and_update():
            cell = sheet.find(str(order_id), in_column=COL_ORDER_ID)
            if cell:
                sheet.update_cell(cell.row, col_idx, new_value)
                return True
            return False

        return await asyncio.to_thread(_find_and_update)

    try:
        return await _do_update()
    except APIError as e:
        if e.response is not None and e.response.status_code in [401, 403]:
            try:
                return await _do_update(force=True)
            except Exception as inner:
                logging.error(f"❌ update після refresh: {inner}")
        else:
            logging.error(f"❌ update APIError: {e}")
    except Exception as e:
        logging.error(f"❌ update невідома помилка: {e}")
    return False


async def get_student_orders(client_tg_id: str) -> list:
    """
    ВИПРАВЛЕНО: порівняння через str().strip() з обох боків,
    щоб уникнути хибних промахів коли Google Sheets конвертує числа.
    """
    try:
        sheet = await get_google_sheet()
        all_records = await asyncio.to_thread(sheet.get_all_values)
        found = []
        needle = str(client_tg_id).strip()
        for row in all_records[1:]:
            if len(row) >= 12 and str(row[3]).strip() == needle:  # ← ВИПРАВЛЕНО
                found.append({
                    "id": row[0],
                    "subject": row[4],
                    "type": row[5],
                    "status": row[COL_STATUS - 1],
                    "price": row[COL_PRICE - 1],
                })
        return found
    except Exception as e:
        logging.error(f"❌ get_student_orders: {e}")
        return []


# =====================================================================
# 5. КЛАВІАТУРИ
# =====================================================================
def get_main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Створити заявку")],
            [KeyboardButton(text="📊 Мої замовлення"), KeyboardButton(text="ℹ️ Довідка")],
        ],
        resize_keyboard=True,
    )


def _cancel_row() -> list:
    return [KeyboardButton(text="❌ Скасувати замовлення")]


def get_type_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Курсова"), KeyboardButton(text="Дипломна")],
            [KeyboardButton(text="Реферат"), KeyboardButton(text="Контрольна")],
            _cancel_row(),
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитися номером", request_contact=True)],
            _cancel_row(),
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def get_simple_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[_cancel_row()], resize_keyboard=True)


def get_admin_order_kb(order_id: str, client_tg_id: int) -> InlineKeyboardMarkup:
    # Скорочені дії: "a"=accept, "r"=reject, "p"=price — економимо байти callback_data
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Взяти в роботу",
                    callback_data=OrderAction(action="a", order_id=order_id, client_id=client_tg_id).pack(),
                ),
                InlineKeyboardButton(
                    text="❌ Відхилити",
                    callback_data=OrderAction(action="r", order_id=order_id, client_id=client_tg_id).pack(),
                ),
            ],
            [
                InlineKeyboardButton(
                    text="💰 Надіслати ціну",
                    callback_data=OrderAction(action="p", order_id=order_id, client_id=client_tg_id).pack(),
                )
            ],
        ]
    )


# =====================================================================
# 6. ГЛОБАЛЬНЕ СКАСУВАННЯ
# =====================================================================
@dp.message(Command("cancel"))
@dp.message(F.text == "❌ Скасувати замовлення")
async def cmd_cancel(message: Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer(
            "У вас немає активного процесу оформлення.",
            reply_markup=get_main_menu_kb(),
        )
        return
    await state.clear()
    await message.answer("❌ Оформлення скасовано.", reply_markup=get_main_menu_kb())


# =====================================================================
# 7. СТАРТ І МЕНЮ
#
# ВИПРАВЛЕНО: /start більше не кидає одразу в анкету.
# Новий юзер бачить привітання та меню. Анкета стартує окремо.
# =====================================================================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 <b>Вітаємо в EduExpert!</b>\n\n"
        "Ми допомагаємо з курсовими, дипломними, рефератами та іншими роботами.\n\n"
        "Натисніть <b>📝 Створити заявку</b>, щоб розпочати оформлення, "
        "або <b>ℹ️ Довідка</b> для додаткової інформації.",
        reply_markup=get_main_menu_kb(),
    )


@dp.message(F.text == "📝 Створити заявку")
async def cmd_new_order(message: Message, state: FSMContext):
    """
    ВИПРАВЛЕНО: order_id генерується тут — один раз на початку анкети
    і зберігається в стані. Якщо юзер відповідає на process_details
    повторно (після помилки), новий order_id не генерується — дублікатів нема.
    """
    await state.clear()

    order_id = f"EDU-{datetime.now().strftime('%d%m%Y')}-{uuid.uuid4().hex[:6].upper()}"
    await state.update_data(order_id=order_id)

    await message.answer(
        "📝 <b>Нова заявка</b>\n\n"
        "Введіть предмет або тему роботи\n"
        "(наприклад: <i>Цивільне право, Менеджмент</i>):",
        reply_markup=get_simple_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_subject)


# =====================================================================
# 8. КРОКИ АНКЕТИ З ВАЛІДАЦІЄЮ
# =====================================================================
@dp.message(OrderStates.waiting_for_subject)
async def process_subject(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if len(text) < MIN_SUBJECT_LEN:
        await message.answer(f"❌ Предмет занадто короткий. Введіть щонайменше {MIN_SUBJECT_LEN} символи:")
        return
    if len(text) > MAX_SUBJECT_LEN:
        await message.answer(f"❌ Предмет занадто довгий (максимум {MAX_SUBJECT_LEN} символів). Скоротіть:")
        return
    await state.update_data(subject=text)
    await message.answer("Оберіть або напишіть тип роботи:", reply_markup=get_type_kb())
    await state.set_state(OrderStates.waiting_for_type)


@dp.message(OrderStates.waiting_for_type)
async def process_type(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text:
        await message.answer("❌ Будь ласка, оберіть або введіть тип роботи:")
        return
    await state.update_data(work_type=text)
    await message.answer(
        "Вкажіть обсяг у сторінках (наприклад: <i>30</i>):",
        reply_markup=get_simple_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_pages)


@dp.message(OrderStates.waiting_for_pages)
async def process_pages(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    # Дозволяємо "30", "30 сторінок", "близько 30" — але не порожньо
    if not text:
        await message.answer("❌ Введіть обсяг роботи:")
        return
    await state.update_data(pages=text)
    await message.answer(
        "Вкажіть дедлайн (наприклад: <i>25.06.2026</i>):",
        reply_markup=get_simple_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_deadline)


@dp.message(OrderStates.waiting_for_deadline)
async def process_deadline(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if not text:
        await message.answer("❌ Введіть дату дедлайну:")
        return
    await state.update_data(deadline=text)
    await message.answer(
        "Надішліть номер телефону кнопкою або введіть у форматі <code>+380XXXXXXXXX</code>:",
        reply_markup=get_phone_kb(),
    )
    await state.set_state(OrderStates.waiting_for_phone)


@dp.message(OrderStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone_val = (
        message.contact.phone_number if message.contact else (message.text or "").strip()
    )
    if not re.match(r"^\+?\d{9,15}$", phone_val):
        await message.answer(
            "❌ Некоректний формат. Натисніть кнопку або введіть номер цифрами\n"
            "(наприклад: <code>+380671234567</code>):"
        )
        return
    await state.update_data(phone=phone_val)
    await message.answer(
        "Введіть деталі замовлення:\nплан, вимоги до унікальності, особливі побажання тощо.",
        reply_markup=get_simple_cancel_kb(),
    )
    await state.set_state(OrderStates.waiting_for_details)


@dp.message(OrderStates.waiting_for_details)
async def process_details(message: Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    if len(text) < MIN_DETAILS_LEN:
        await message.answer(f"❌ Деталі занадто короткі. Опишіть вимоги докладніше (мінімум {MIN_DETAILS_LEN} символів):")
        return
    if len(text) > MAX_DETAILS_LEN:
        await message.answer(f"❌ Деталі занадто довгі (максимум {MAX_DETAILS_LEN} символів). Скоротіть:")
        return

    await state.update_data(details=text)
    user_data = await state.get_data()

    # order_id був згенерований на старті — беремо з стану, не генеруємо знову
    order_id = user_data["order_id"]
    client_tg_id = message.from_user.id

    full_order_data = {
        "order_id": order_id,
        "username": message.from_user.username,
        "tg_id": client_tg_id,
        **user_data,
    }

    report_text = (
        f"💎 <b>НОВЕ ЗАМОВЛЕННЯ №{order_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клієнт:</b> @{message.from_user.username or 'немає'} "
        f"(ID: <code>{client_tg_id}</code>)\n"
        f"📚 <b>Предмет:</b> {user_data.get('subject')}\n"
        f"📝 <b>Тип:</b> {user_data.get('work_type')}\n"
        f"📄 <b>Обсяг:</b> {user_data.get('pages')}\n"
        f"⏳ <b>Дедлайн:</b> {user_data.get('deadline')}\n"
        f"📱 <b>Телефон:</b> {user_data.get('phone')}\n"
        f"📋 <b>Деталі:</b> {user_data.get('details')}\n"
    )

    # ВИПРАВЛЕНО: спочатку зберігаємо в Google Sheets,
    # потім надсилаємо адміну, потім очищаємо стан.
    # Якщо Sheets впав — попереджаємо адміна, але не блокуємо процес.
    sheets_ok = await save_to_google_sheets(full_order_data)
    if not sheets_ok:
        logging.error(f"❌ Замовлення {order_id} НЕ збережено в таблицю! Дані: {full_order_data}")
        # Надсилаємо адміну попередження, але продовжуємо
        try:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=f"⚠️ <b>УВАГА:</b> Замовлення <b>{order_id}</b> не вдалося зберегти в Google Sheets!\n"
                     f"Перевірте логи сервера.",
            )
        except Exception:
            pass

    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=report_text,
            reply_markup=get_admin_order_kb(order_id, client_tg_id),
        )
        await message.answer(
            "✨ <b>Вашу заявку прийнято!</b>\n\n"
            "Менеджер розгляне її та зв'яжеться з вами найближчим часом.",
            reply_markup=get_main_menu_kb(),
        )
        await state.clear()

    except Exception as e:
        logging.error(f"❌ Помилка надсилання замовлення адміну: {e}")
        # Стан НЕ очищаємо — юзер може спробувати ще раз.
        # order_id вже збережено в стані, дублікатів не буде.
        await message.answer(
            "⚠️ Виникла помилка зв'язку з менеджером.\n"
            "Спробуйте надіслати деталі ще раз через кілька секунд.\n\n"
            "Якщо проблема не зникає — зверніться до підтримки."
        )


# =====================================================================
# 9. АДМІНІСТРУВАННЯ
#
# ВИПРАВЛЕНО: хендлер waiting_for_price отримує додатковий фільтр
# lambda — тільки ADMIN_ID може надсилати ціну. Якщо інший юзер
# напише щось у цей момент, його повідомлення падає у fallback,
# стан адміна НЕ скидається.
# =====================================================================
@dp.callback_query(OrderAction.filter(F.action == "a"))
async def admin_accept_order(callback: CallbackQuery, callback_data: OrderAction):
    try:
        await callback.bot.send_message(
            chat_id=callback_data.client_id,
            text=f"✅ Ваше замовлення <b>{callback_data.order_id}</b> взято в роботу! "
                 f"Менеджер зв'яжеться з вами.",
        )
        new_text = callback.message.text + "\n\n🟢 <b>Статус: В роботі</b>"
        await callback.message.edit_text(new_text)
        await update_status_or_price(callback_data.order_id, COL_STATUS, "В роботі")
    except Exception as e:
        logging.error(f"❌ admin_accept: {e}")
        await callback.message.answer(f"❌ Не вдалося сповістити студента: {e}")
    await callback.answer("✅ Прийнято")


@dp.callback_query(OrderAction.filter(F.action == "r"))
async def admin_reject_order(callback: CallbackQuery, callback_data: OrderAction):
    try:
        await callback.bot.send_message(
            chat_id=callback_data.client_id,
            text=f"❌ На жаль, замовлення <b>{callback_data.order_id}</b> відхилено.\n"
                 f"Якщо маєте питання — зверніться до менеджера.",
        )
        new_text = callback.message.text + "\n\n🔴 <b>Статус: Відхилено</b>"
        await callback.message.edit_text(new_text)
        await update_status_or_price(callback_data.order_id, COL_STATUS, "Відхилено")
    except Exception as e:
        logging.error(f"❌ admin_reject: {e}")
        await callback.message.answer(f"❌ Не вдалося сповістити студента: {e}")
    await callback.answer("❌ Відхилено")


@dp.callback_query(OrderAction.filter(F.action == "p"))
async def admin_request_price(callback: CallbackQuery, callback_data: OrderAction, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_price)
    await state.update_data(
        target_client_id=callback_data.client_id,
        target_order_id=callback_data.order_id,
    )
    await callback.message.answer(
        f"💵 Введіть вартість у гривнях (тільки число)\n"
        f"для замовлення <b>{callback_data.order_id}</b>:"
    )
    await callback.answer()


# ВИПРАВЛЕНО: фільтр lambda гарантує що тільки ADMIN_ID обробляється тут.
# Повідомлення від інших юзерів йдуть у fallback — стан адміна не зачіпається.
@dp.message(
    AdminStates.waiting_for_price,
    F.from_user.func(lambda u: u.id == ADMIN_ID),
)
async def admin_send_price_to_student(message: Message, state: FSMContext):
    price_val = (message.text or "").strip()
    if not price_val.isdigit() or int(price_val) <= 0:
        await message.answer("❌ Введіть тільки позитивне ціле число (наприклад: <code>500</code>):")
        return

    admin_data = await state.get_data()
    client_id = int(admin_data["target_client_id"])
    order_id = admin_data["target_order_id"]

    try:
        await message.bot.send_message(
            chat_id=client_id,
            text=f"💰 Вартість вашого замовлення <b>{order_id}</b>: <b>{price_val} грн</b>.\n"
                 f"Якщо погоджуєтесь — менеджер перейде до виконання.",
        )
        await message.answer(f"✅ Ціну {price_val} грн надіслано студенту.")
        await update_status_or_price(order_id, COL_PRICE, f"{price_val} грн")
    except Exception as e:
        logging.error(f"❌ admin_send_price: {e}")
        await message.answer(f"❌ Помилка доставки ціни студенту: {e}")

    await state.clear()


# =====================================================================
# 10. МОЇ ЗАМОВЛЕННЯ
# =====================================================================
@dp.message(Command("status"))
@dp.message(F.text == "📊 Мої замовлення")
async def cmd_status(message: Message):
    await message.answer("🔍 Шукаю ваші замовлення...")
    orders = await get_student_orders(str(message.from_user.id))

    if not orders:
        await message.answer(
            "У вас поки немає замовлень.\n"
            "Натисніть <b>📝 Створити заявку</b>, щоб оформити перше.",
            reply_markup=get_main_menu_kb(),
        )
        return

    response = "📋 <b>Ваші замовлення:</b>\n\n"
    for o in orders:
        response += (
            f"🔹 <b>Код:</b> <code>{o['id']}</code>\n"
            f"📚 <b>Дисципліна:</b> {o['subject']} ({o['type']})\n"
            f"📊 <b>Статус:</b> {o['status']}\n"
            f"💵 <b>Ціна:</b> {o['price']}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    await message.answer(response, reply_markup=get_main_menu_kb())


# =====================================================================
# 11. ДОВІДКА
# =====================================================================
@dp.message(Command("help"))
@dp.message(F.text == "ℹ️ Довідка")
async def cmd_help(message: Message):
    await message.answer(
        "<b>Як користуватися ботом:</b>\n\n"
        "📝 <b>Створити заявку</b> або /start — оформити нову роботу\n"
        "📊 <b>Мої замовлення</b> або /status — перевірити статус і ціну\n"
        "❌ <b>Скасувати замовлення</b> або /cancel — зупинити анкетування\n"
        "ℹ️ <b>Довідка</b> або /help — це меню\n\n"
        "Після оформлення заявки менеджер зв'яжеться з вами особисто.",
        reply_markup=get_main_menu_kb(),
    )


# =====================================================================
# 12. FALLBACK
#
# ВИПРАВЛЕНО: фільтр F.text — медіа (фото, голос, стікери) не
# потрапляють сюди і не отримують безглузду відповідь.
# StateFilter(None) — тільки якщо немає активного стану.
# =====================================================================
@dp.message(StateFilter(None), F.text)
async def fallback(message: Message):
    await message.answer(
        "Не зовсім зрозумів вас. 🤔\n"
        "Скористайтеся кнопками меню для швидкої навігації.",
        reply_markup=get_main_menu_kb(),
    )


# =====================================================================
# 13. ЗАПУСК
# =====================================================================
async def main():
    logging.info("🚀 EduExpert Bot запущено.")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        logging.info("🛑 Бот зупинено, сесія закрита.")


if __name__ == "__main__":
    asyncio.run(main())