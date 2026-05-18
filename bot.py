import os
import json
import logging
import asyncio
import uuid
import re
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import sentry_sdk
from aiogram import Bot, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters.callback_data import CallbackData
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

# =====================================================================
# 1. КОНФІГУРАЦІЯ ТА КОНСТАНТИ
# =====================================================================
TOKEN           = os.getenv("BOT_TOKEN")
ADMIN_ID_ENV    = os.getenv("ADMIN_ID")
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY")
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")
SENTRY_DSN      = os.getenv("SENTRY_DSN")

if not all([TOKEN, ADMIN_ID_ENV, SPREADSHEET_KEY]):
    raise ValueError("❌ Відсутні обов'язкові змінні: BOT_TOKEN, ADMIN_ID, SPREADSHEET_KEY")

ADMIN_ID = int(ADMIN_ID_ENV)

# Індекси колонок таблиці (1-based, як у gspread)
COL_ORDER_ID = 1
COL_STATUS   = 11
COL_PRICE    = 12

SHEET_HEADERS = [
    "ID Замовлення", "Дата створення", "Username", "Telegram ID",
    "Предмет", "Тип роботи", "Обсяг (ст.)", "Дедлайн",
    "Телефон", "Деталі", "Статус", "Ціна",
]

# Ліміти для валідації вводу
MIN_SUBJECT_LEN = 2
MAX_SUBJECT_LEN = 200
MIN_DETAILS_LEN = 5
MAX_DETAILS_LEN = 2000

if SENTRY_DSN:
    sentry_sdk.init(SENTRY_DSN)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# =====================================================================
# 2. STORAGE: перевірка Redis через реальний ping
#
# ВИПРАВЛЕНО: RedisStorage.from_url() НЕ перевіряє з'єднання одразу —
# він падає лише при першому зверненні під час polling. Тому
# try/except при ініціалізації на рівні модуля — марний.
#
# Рішення: _build_storage() викликається асинхронно в main(),
# робить справжній ping і тільки після успіху повертає RedisStorage.
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
# 3. CALLBACK DATA
#
# Telegram обмежує callback_data до 64 байт.
# Префікс "ord" + дії "a"/"r"/"p" + order_id (20 симв) + client_id
# (10 цифр) + роздільники = ~45 байт. Вкладається з запасом.
# =====================================================================
class OrderAction(CallbackData, prefix="ord"):
    action: str    # "a" = accept | "r" = reject | "p" = price
    order_id: str
    client_id: int


class OrderStates(StatesGroup):
    waiting_for_subject  = State()
    waiting_for_type     = State()
    waiting_for_pages    = State()
    waiting_for_deadline = State()
    waiting_for_phone    = State()
    waiting_for_details  = State()


class AdminStates(StatesGroup):
    waiting_for_price = State()


# =====================================================================
# 4. GOOGLE SHEETS
#
# ВИПРАВЛЕНО: замість threading.Lock() використовується asyncio.Lock().
# threading.Lock() блокує весь event loop при очікуванні.
# Синхронні виклики gspread йдуть через asyncio.to_thread().
# =====================================================================
_client_cache = None
_sheet_cache  = None
_cache_lock   = asyncio.Lock()


def _sync_build_sheet(force_refresh: bool) -> object:
    """Виконується в окремому thread. Lock тримається зовні (asyncio)."""
    global _client_cache, _sheet_cache

    if force_refresh:
        _client_cache = None
        _sheet_cache  = None
        logging.info("🔄 Кеш Google API скинуто.")

    if _sheet_cache is not None:
        return _sheet_cache

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if not _client_cache:
        raw = os.getenv("GOOGLE_CREDS_JSON")
        if raw:
            try:
                creds = Credentials.from_service_account_info(json.loads(raw), scopes=scopes)
            except Exception:
                creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
        else:
            creds = Credentials.from_service_account_file("creds.json", scopes=scopes)
        _client_cache = gspread.authorize(creds)

    wb = _client_cache.open_by_key(SPREADSHEET_KEY)
    try:
        sheet = wb.worksheet("Orders")
    except WorksheetNotFound:
        logging.info("📝 Лист 'Orders' не знайдено — створюю новий.")
        sheet = wb.add_worksheet("Orders", rows=5000, cols=20)

    # ВИПРАВЛЕНО: перевіряємо заголовки перед додаванням,
    # щоб не дублювати їх після force_refresh або retry.
    existing = sheet.row_values(1)
    if not existing:
        sheet.append_row(SHEET_HEADERS)
        logging.info("📝 Заголовки додано.")
    elif existing != SHEET_HEADERS:
        logging.warning("⚠️ Перший рядок таблиці відрізняється від очікуваного!")

    _sheet_cache = sheet
    return _sheet_cache


async def _get_sheet(force_refresh: bool = False):
    async with _cache_lock:
        return await asyncio.to_thread(_sync_build_sheet, force_refresh)


async def save_to_google_sheets(data: dict) -> bool:
    row = [
        data.get("order_id"),
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        f"@{data['username']}" if data.get("username") else "Немає",
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

    async def _append(force: bool = False):
        sheet = await _get_sheet(force_refresh=force)
        await asyncio.to_thread(sheet.append_row, row)

    try:
        await _append()
        return True
    except APIError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            try:
                await _append(force=True)
                return True
            except Exception as ie:
                logging.error(f"❌ Sheets після refresh: {ie}")
        else:
            logging.error(f"❌ Sheets APIError: {e}")
    except Exception as e:
        logging.error(f"❌ Sheets невідома помилка: {e}")
    return False


async def update_sheet_cell(order_id: str, col_idx: int, value: str) -> bool:
    async def _update(force: bool = False):
        sheet = await _get_sheet(force_refresh=force)

        def _find_update():
            cell = sheet.find(str(order_id), in_column=COL_ORDER_ID)
            if cell:
                sheet.update_cell(cell.row, col_idx, value)
                return True
            return False

        return await asyncio.to_thread(_find_update)

    try:
        return await _update()
    except APIError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            try:
                return await _update(force=True)
            except Exception as ie:
                logging.error(f"❌ update після refresh: {ie}")
        else:
            logging.error(f"❌ update APIError: {e}")
    except Exception as e:
        logging.error(f"❌ update невідома помилка: {e}")
    return False


async def get_student_orders(tg_id: str) -> list:
    """
    ВИПРАВЛЕНО: str().strip() з обох боків — Google Sheets іноді
    конвертує числові ID в число, і пряме порівняння "123" == 123 → False.
    """
    try:
        sheet = await _get_sheet()
        rows = await asyncio.to_thread(sheet.get_all_values)
        needle = str(tg_id).strip()
        result = []
        for row in rows[1:]:
            if len(row) >= 12 and str(row[3]).strip() == needle:
                result.append({
                    "id":      row[0],
                    "subject": row[4],
                    "type":    row[5],
                    "status":  row[COL_STATUS - 1],
                    "price":   row[COL_PRICE  - 1],
                })
        return result
    except Exception as e:
        logging.error(f"❌ get_student_orders: {e}")
        return []


# =====================================================================
# 5. КЛАВІАТУРИ
# =====================================================================
def _cancel_row() -> list:
    return [KeyboardButton(text="❌ Скасувати замовлення")]


def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Створити заявку")],
            [KeyboardButton(text="📊 Мої замовлення"), KeyboardButton(text="ℹ️ Довідка")],
        ],
        resize_keyboard=True,
    )


def kb_type() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Курсова"),    KeyboardButton(text="Дипломна")],
            [KeyboardButton(text="Реферат"),    KeyboardButton(text="Контрольна")],
            _cancel_row(),
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_phone() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитися номером", request_contact=True)],
            _cancel_row(),
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[_cancel_row()], resize_keyboard=True)


def kb_admin_order(order_id: str, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Взяти в роботу",
                callback_data=OrderAction(action="a", order_id=order_id, client_id=client_id).pack(),
            ),
            InlineKeyboardButton(
                text="❌ Відхилити",
                callback_data=OrderAction(action="r", order_id=order_id, client_id=client_id).pack(),
            ),
        ],
        [
            InlineKeyboardButton(
                text="💰 Надіслати ціну",
                callback_data=OrderAction(action="p", order_id=order_id, client_id=client_id).pack(),
            ),
        ],
    ])


# =====================================================================
# 6. ROUTER ТА ХЕНДЛЕРИ
#
# Використовуємо Router замість глобального Dispatcher — це стандартний
# aiogram 3.x патерн. Router підключається до dp в main().
# =====================================================================
router = Router()


# --- Скасування -------------------------------------------------------
@router.message(Command("cancel"))
@router.message(F.text == "❌ Скасувати замовлення")
async def cmd_cancel(message: Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer("У вас немає активного оформлення.", reply_markup=kb_main())
        return
    await state.clear()
    await message.answer("❌ Оформлення скасовано.", reply_markup=kb_main())


# --- Старт ------------------------------------------------------------
# ВИПРАВЛЕНО: /start показує привітання і меню — не кидає одразу в анкету.
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 <b>Вітаємо в EduExpert!</b>\n\n"
        "Ми допомагаємо з курсовими, дипломними, рефератами та іншими роботами.\n\n"
        "Натисніть <b>📝 Створити заявку</b>, щоб розпочати,\n"
        "або <b>ℹ️ Довідка</b> для детальнішої інформації.",
        reply_markup=kb_main(),
    )


# --- Нова заявка -------------------------------------------------------
# ВИПРАВЛЕНО: order_id генерується тут — один раз на початку анкети.
# Якщо юзер повторно надсилає деталі після помилки,
# новий order_id не генерується, дублікатів немає.
@router.message(F.text == "📝 Створити заявку")
async def cmd_new_order(message: Message, state: FSMContext):
    await state.clear()
    order_id = f"EDU-{datetime.now().strftime('%d%m%Y')}-{uuid.uuid4().hex[:6].upper()}"
    await state.update_data(order_id=order_id)
    await message.answer(
        "📝 <b>Нова заявка</b>\n\n"
        "Введіть предмет або тему роботи\n"
        "(наприклад: <i>Цивільне право, Менеджмент</i>):",
        reply_markup=kb_cancel(),
    )
    await state.set_state(OrderStates.waiting_for_subject)


# --- Кроки анкети -----------------------------------------------------
@router.message(OrderStates.waiting_for_subject)
async def process_subject(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < MIN_SUBJECT_LEN:
        await message.answer(f"❌ Занадто коротко. Мінімум {MIN_SUBJECT_LEN} символи:")
        return
    if len(text) > MAX_SUBJECT_LEN:
        await message.answer(f"❌ Занадто довго. Максимум {MAX_SUBJECT_LEN} символів:")
        return
    await state.update_data(subject=text)
    await message.answer("Оберіть або введіть тип роботи:", reply_markup=kb_type())
    await state.set_state(OrderStates.waiting_for_type)


@router.message(OrderStates.waiting_for_type)
async def process_type(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Будь ласка, оберіть або введіть тип роботи:")
        return
    await state.update_data(work_type=text)
    await message.answer(
        "Вкажіть обсяг у сторінках (наприклад: <i>30</i>):",
        reply_markup=kb_cancel(),
    )
    await state.set_state(OrderStates.waiting_for_pages)


@router.message(OrderStates.waiting_for_pages)
async def process_pages(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Введіть кількість сторінок:")
        return
    await state.update_data(pages=text)
    await message.answer(
        "Вкажіть дедлайн (наприклад: <i>25.06.2026</i>):",
        reply_markup=kb_cancel(),
    )
    await state.set_state(OrderStates.waiting_for_deadline)


@router.message(OrderStates.waiting_for_deadline)
async def process_deadline(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Введіть дату дедлайну:")
        return
    await state.update_data(deadline=text)
    await message.answer(
        "Надішліть номер телефону кнопкою або введіть у форматі <code>+380XXXXXXXXX</code>:",
        reply_markup=kb_phone(),
    )
    await state.set_state(OrderStates.waiting_for_phone)


@router.message(OrderStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = (
        message.contact.phone_number
        if message.contact
        else (message.text or "").strip()
    )
    if not re.match(r"^\+?\d{9,15}$", phone):
        await message.answer(
            "❌ Некоректний формат. Натисніть кнопку або введіть цифрами\n"
            "(наприклад: <code>+380671234567</code>):"
        )
        return
    await state.update_data(phone=phone)
    await message.answer(
        "Введіть деталі замовлення:\nплан, вимоги до унікальності, особливі побажання тощо.",
        reply_markup=kb_cancel(),
    )
    await state.set_state(OrderStates.waiting_for_details)


@router.message(OrderStates.waiting_for_details)
async def process_details(message: Message, state: FSMContext, bot: Bot):
    text = (message.text or "").strip()
    if len(text) < MIN_DETAILS_LEN:
        await message.answer(f"❌ Опишіть детальніше (мінімум {MIN_DETAILS_LEN} символів):")
        return
    if len(text) > MAX_DETAILS_LEN:
        await message.answer(f"❌ Занадто довго (максимум {MAX_DETAILS_LEN} символів):")
        return

    await state.update_data(details=text)
    data = await state.get_data()

    # order_id взятий зі стану — генерується один раз, дублікатів немає
    order_id     = data["order_id"]
    client_tg_id = message.from_user.id

    order_data = {
        "order_id": order_id,
        "username": message.from_user.username,
        "tg_id":    client_tg_id,
        **data,
    }

    report = (
        f"💎 <b>НОВЕ ЗАМОВЛЕННЯ №{order_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Клієнт:</b> @{message.from_user.username or 'немає'} "
        f"(ID: <code>{client_tg_id}</code>)\n"
        f"📚 <b>Предмет:</b> {data.get('subject')}\n"
        f"📝 <b>Тип:</b> {data.get('work_type')}\n"
        f"📄 <b>Обсяг:</b> {data.get('pages')}\n"
        f"⏳ <b>Дедлайн:</b> {data.get('deadline')}\n"
        f"📱 <b>Телефон:</b> {data.get('phone')}\n"
        f"📋 <b>Деталі:</b> {data.get('details')}\n"
    )

    # ВИПРАВЛЕНО: спочатку зберігаємо в Sheets, потім надсилаємо адміну,
    # потім clear(). Якщо надсилання адміну впало — стан не очищаємо,
    # юзер пробує ще раз з тим самим order_id (без дублікатів у таблиці).
    sheets_ok = await save_to_google_sheets(order_data)
    if not sheets_ok:
        logging.error(f"❌ Замовлення {order_id} НЕ записано в таблицю! {order_data}")
        try:
            await bot.send_message(
                ADMIN_ID,
                f"⚠️ <b>УВАГА:</b> Замовлення <b>{order_id}</b> не збережено в Google Sheets!\n"
                f"Перевірте логи.",
            )
        except Exception:
            pass

    try:
        await bot.send_message(
            ADMIN_ID,
            report,
            reply_markup=kb_admin_order(order_id, client_tg_id),
        )
        await message.answer(
            "✨ <b>Вашу заявку прийнято!</b>\n\n"
            "Менеджер розгляне її та зв'яжеться з вами найближчим часом.",
            reply_markup=kb_main(),
        )
        await state.clear()

    except Exception as e:
        logging.error(f"❌ Надсилання адміну: {e}")
        await message.answer(
            "⚠️ Виникла помилка зв'язку з менеджером.\n"
            "Спробуйте надіслати деталі ще раз за кілька секунд."
        )
        # Стан не очищаємо — order_id збережено, дублікатів не буде


# =====================================================================
# 7. АДМІНІСТРУВАННЯ
#
# ВИПРАВЛЕНО: хендлер waiting_for_price фільтрується на рівні
# декоратора через F.from_user.func(...). Якщо звичайний юзер
# надсилає повідомлення поки адмін вводить ціну — воно йде у
# fallback і стан адміна не зачіпається.
# =====================================================================
@router.callback_query(OrderAction.filter(F.action == "a"))
async def cb_accept(callback: CallbackQuery, callback_data: OrderAction, bot: Bot):
    try:
        await bot.send_message(
            callback_data.client_id,
            f"✅ Замовлення <b>{callback_data.order_id}</b> взято в роботу!\n"
            f"Менеджер зв'яжеться з вами.",
        )
        await callback.message.edit_text(
            callback.message.text + "\n\n🟢 <b>Статус: В роботі</b>"
        )
        await update_sheet_cell(callback_data.order_id, COL_STATUS, "В роботі")
    except Exception as e:
        logging.error(f"❌ cb_accept: {e}")
        await callback.message.answer(f"❌ Не вдалося сповістити студента: {e}")
    await callback.answer("✅ Прийнято")


@router.callback_query(OrderAction.filter(F.action == "r"))
async def cb_reject(callback: CallbackQuery, callback_data: OrderAction, bot: Bot):
    try:
        await bot.send_message(
            callback_data.client_id,
            f"❌ На жаль, замовлення <b>{callback_data.order_id}</b> відхилено.\n"
            f"Якщо є питання — зверніться до менеджера.",
        )
        await callback.message.edit_text(
            callback.message.text + "\n\n🔴 <b>Статус: Відхилено</b>"
        )
        await update_sheet_cell(callback_data.order_id, COL_STATUS, "Відхилено")
    except Exception as e:
        logging.error(f"❌ cb_reject: {e}")
        await callback.message.answer(f"❌ Не вдалося сповістити студента: {e}")
    await callback.answer("❌ Відхилено")


@router.callback_query(OrderAction.filter(F.action == "p"))
async def cb_request_price(callback: CallbackQuery, callback_data: OrderAction, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_price)
    await state.update_data(
        target_client_id=callback_data.client_id,
        target_order_id=callback_data.order_id,
    )
    await callback.message.answer(
        f"💵 Введіть вартість у гривнях (тільки ціле число)\n"
        f"для замовлення <b>{callback_data.order_id}</b>:"
    )
    await callback.answer()


@router.message(
    AdminStates.waiting_for_price,
    F.from_user.func(lambda u: u.id == ADMIN_ID),
)
async def cb_send_price(message: Message, state: FSMContext, bot: Bot):
    price = (message.text or "").strip()
    if not price.isdigit() or int(price) <= 0:
        await message.answer("❌ Введіть тільки позитивне ціле число (наприклад: <code>500</code>):")
        return

    data = await state.get_data()
    client_id = int(data["target_client_id"])
    order_id  = data["target_order_id"]

    try:
        await bot.send_message(
            client_id,
            f"💰 Вартість замовлення <b>{order_id}</b>: <b>{price} грн</b>.\n"
            f"Якщо погоджуєтесь — менеджер перейде до виконання.",
        )
        await message.answer(f"✅ Ціну {price} грн надіслано студенту.")
        await update_sheet_cell(order_id, COL_PRICE, f"{price} грн")
    except Exception as e:
        logging.error(f"❌ cb_send_price: {e}")
        await message.answer(f"❌ Помилка доставки ціни: {e}")

    await state.clear()


# =====================================================================
# 8. МОЇ ЗАМОВЛЕННЯ
# =====================================================================
@router.message(Command("status"))
@router.message(F.text == "📊 Мої замовлення")
async def cmd_status(message: Message):
    await message.answer("🔍 Шукаю ваші замовлення...")
    orders = await get_student_orders(str(message.from_user.id))

    if not orders:
        await message.answer(
            "У вас поки немає замовлень.\n"
            "Натисніть <b>📝 Створити заявку</b>, щоб оформити перше.",
            reply_markup=kb_main(),
        )
        return

    lines = "📋 <b>Ваші замовлення:</b>\n\n"
    for o in orders:
        lines += (
            f"🔹 <b>Код:</b> <code>{o['id']}</code>\n"
            f"📚 <b>Дисципліна:</b> {o['subject']} ({o['type']})\n"
            f"📊 <b>Статус:</b> {o['status']}\n"
            f"💵 <b>Ціна:</b> {o['price']}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
        )
    await message.answer(lines, reply_markup=kb_main())


# =====================================================================
# 9. ДОВІДКА
# =====================================================================
@router.message(Command("help"))
@router.message(F.text == "ℹ️ Довідка")
async def cmd_help(message: Message):
    await message.answer(
        "<b>Як користуватися ботом:</b>\n\n"
        "📝 <b>Створити заявку</b> або /start — оформити нову роботу\n"
        "📊 <b>Мої замовлення</b> або /status — статус і ціна\n"
        "❌ <b>Скасувати замовлення</b> або /cancel — зупинити анкетування\n"
        "ℹ️ <b>Довідка</b> або /help — це меню\n\n"
        "Після оформлення менеджер зв'яжеться з вами особисто.",
        reply_markup=kb_main(),
    )


# =====================================================================
# 10. FALLBACK
#
# ВИПРАВЛЕНО: додано F.text — медіа (фото, голос, стікери)
# не отримують текстову відповідь "не зрозумів".
# StateFilter(None) — тільки коли немає активного стану.
# =====================================================================
@router.message(StateFilter(None), F.text)
async def fallback(message: Message):
    await message.answer(
        "Не зовсім зрозумів вас. 🤔\n"
        "Скористайтеся кнопками меню.",
        reply_markup=kb_main(),
    )


# =====================================================================
# 11. ЗАПУСК
# =====================================================================
async def main():
    from aiogram import Dispatcher

    # Перевірка Redis через реальний ping — тільки тут, асинхронно
    storage = await _build_storage()

    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    logging.info("🚀 EduExpert Bot запущено.")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        logging.info("🛑 Бот зупинено.")


if __name__ == "__main__":
    asyncio.run(main())
