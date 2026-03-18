import asyncio
import json
import logging
import os
import re
import ssl
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import aiohttp_socks
import pytz
import requests
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiohttp import web
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "45"))
PORT = int(os.getenv("PORT", "8080"))
BASE_DIR = Path(__file__).resolve().parent

APQE_PQFRTY = os.getenv("APQE_PQFRTY")
APSRC_PFRTY = os.getenv("APSRC_PFRTY")

PROXY_URL = os.getenv("PROXY_URL")

LVIV_API_URL = os.getenv("APQE_LOE")
LVIV_POWER_API_URL = os.getenv("APWR_LOE")

# Регіони
REGION_IF = "if"
REGION_LVIV = "lviv"

KYIV_TZ = ZoneInfo("Europe/Kyiv")

# Список черг для моніторингу
QUEUES = [
    "1.1",
    "1.2",
    "2.1",
    "2.2",
    "3.1",
    "3.2",
    "4.1",
    "4.2",
    "5.1",
    "5.2",
    "6.1",
    "6.2",
]

# Тексти кнопок
BTN_CHECK = "🔄 Перевірити графік"
BTN_MY_QUEUE = "📋 Мої підписки"
BTN_SET_QUEUE = "⚡ Обрати черги"
BTN_CHANGE_QUEUE = "⚙️ Налаштування"
BTN_HELP = "❓ Допомога"
BTN_DONATE = "💛 Підтримати проєкт"

ADMIN_ID = 1473999790

# Посилання на донат
DONATE_URL = "https://send.monobank.ua/jar/5N86nkGZ1R"
DONATE_PRIV_URL = "https://www.privat24.ua/send/i7yrx"
DONATE_TEXT = "[💛 Підтримай розвиток проєкту]({url})"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MongoDB ---
mongo_client: AsyncIOMotorClient = None
db = None


async def init_db():
    """Ініціалізація підключення до MongoDB"""
    global mongo_client, db
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client[DB_NAME]

        # Перевірка з'єднання
        await mongo_client.admin.command("ping")

        # Перевірка колекцій
        collections = await db.list_collection_names()
        logging.info(f"✅ Connected to MongoDB. Collections: {collections}")

        # Міграція: встановити region для існуючих користувачів
        migrated = await db.users.update_many(
            {"region": {"$exists": False}}, {"$set": {"region": REGION_IF}}
        )
        if migrated.modified_count > 0:
            logging.info(
                f"🔄 Migrated {migrated.modified_count} users → region '{REGION_IF}'"
            )

        # Рахуємо документи
        users_count = await db.users.count_documents({})
        states_count = await db.schedule_state.count_documents({})
        logging.info(f"📊 Users: {users_count}, Schedule states: {states_count}")

    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise


async def close_db():
    """Закриття підключення до MongoDB"""
    global mongo_client
    if mongo_client:
        mongo_client.close()
        logging.info("MongoDB connection closed")


# --- FSM СТАНИ ---
class AddressForm(StatesGroup):
    waiting_for_city = State()
    waiting_for_street = State()


class LvivAddressForm(StatesGroup):
    waiting_for_city_search = State()
    waiting_for_street_search = State()
    waiting_for_house = State()


class AdminBroadcast(StatesGroup):
    waiting_for_target = State()
    waiting_for_user_id = State()
    waiting_for_message = State()


# --- РОБОТА З БАЗОЮ ДАНИХ ---
async def get_user_data(user_id: int) -> dict | None:
    """Отримує дані користувача з MongoDB"""
    user = await db.users.find_one({"user_id": user_id})
    if user:
        # Підтримка старого формату (queue як str) та нового (queues як list)
        queues = user.get("queues", [])
        if not queues and user.get("queue"):
            queues = [user.get("queue")]  # Міграція старого формату
        return {
            "queues": queues,
            "address": user.get("address"),
            "reminders": user.get("reminders", False),
            "reminder_intervals": user.get(
                "reminder_intervals", DEFAULT_REMINDER_INTERVALS
            ),
            "region": user.get("region", REGION_IF),
        }
    return None


async def set_user_data(user_id: int, queues: list[str], address: str = None):
    """Зберігає дані користувача в MongoDB"""
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "queues": queues,
                "address": address,
                "updated_at": datetime.now(KYIV_TZ),
            },
            "$unset": {"queue": ""},
        },
        upsert=True,
    )


async def add_queue_to_user(user_id: int, queue: str, address: str = None):
    """Додає чергу до підписок користувача"""
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue not in queues:
            queues.append(queue)
        # Зберігаємо адресу тільки якщо передана
        addr = address if address else user_data.get("address")
        await set_user_data(user_id, queues, addr)
    else:
        await set_user_data(user_id, [queue], address)


async def remove_queue_from_user(user_id: int, queue: str):
    """Видаляє конкретну чергу з підписок користувача"""
    user_data = await get_user_data(user_id)
    if user_data:
        queues = user_data.get("queues", [])
        if queue in queues:
            queues.remove(queue)
        await set_user_data(user_id, queues, user_data.get("address"))


async def get_user_queues(user_id: int) -> list[str]:
    """Отримує список черг користувача"""
    data = await get_user_data(user_id)
    if data:
        return data.get("queues", [])
    return []


async def get_user_region(user_id: int) -> str | None:
    """Повертає регіон користувача або None"""
    user = await db.users.find_one({"user_id": user_id})
    if user:
        return user.get("region", REGION_IF)
    return None


async def set_user_region(user_id: int, region: str):
    """Встановлює регіон користувача"""
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"region": region, "updated_at": datetime.now(KYIV_TZ)}},
        upsert=True,
    )


async def remove_user_queue(user_id: int):
    """Видаляє всі підписки користувача"""
    await db.users.delete_one({"user_id": user_id})


async def get_users_by_queue(queue: str, region: str = None) -> list[int]:
    """Повертає список user_id підписаних на певну чергу (з фільтром за регіоном)"""
    query = {"$or": [{"queues": queue}, {"queue": queue}]}
    if region:
        query["region"] = region
    cursor = db.users.find(query)
    users = await cursor.to_list(length=None)
    return [user["user_id"] for user in users]


async def toggle_user_reminders(user_id: int) -> bool:
    """Перемикає стан нагадувань користувача, повертає новий стан"""
    user = await db.users.find_one({"user_id": user_id})
    current_state = user.get("reminders", False) if user else False
    new_state = not current_state

    await db.users.update_one(
        {"user_id": user_id}, {"$set": {"reminders": new_state}}, upsert=True
    )
    return new_state


async def get_user_reminders_state(user_id: int) -> bool:
    """Повертає стан нагадувань користувача"""
    user = await db.users.find_one({"user_id": user_id})
    return user.get("reminders", True) if user else True


async def get_user_reminder_intervals(user_id: int) -> list[int]:
    """Повертає обрані інтервали нагадувань користувача"""
    user = await db.users.find_one({"user_id": user_id})
    return (
        user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS)
        if user
        else DEFAULT_REMINDER_INTERVALS
    )


async def toggle_reminder_interval(user_id: int, interval: int) -> list[int]:
    """Перемикає інтервал нагадувань, повертає новий список"""
    user = await db.users.find_one({"user_id": user_id})
    intervals = (
        user.get("reminder_intervals", DEFAULT_REMINDER_INTERVALS.copy())
        if user
        else DEFAULT_REMINDER_INTERVALS.copy()
    )

    if interval in intervals:
        intervals.remove(interval)
    else:
        intervals.append(interval)
        intervals.sort(reverse=True)  # Сортуємо від більшого до меншого

    await db.users.update_one(
        {"user_id": user_id}, {"$set": {"reminder_intervals": intervals}}, upsert=True
    )
    return intervals


async def get_schedule_state(queue_id: str) -> str | None:
    """Отримує збережений стан графіку для черги"""
    try:
        state = await db.schedule_state.find_one({"queue_id": queue_id})
        if state:
            return state.get("data_hash")
        return None
    except Exception as e:
        logging.error(f"Error getting schedule state for {queue_id}: {e}")
        return None


async def save_schedule_state(queue_id: str, data_hash: str):
    """Зберігає стан графіку для черги"""
    try:
        await db.schedule_state.update_one(
            {"queue_id": queue_id},
            {"$set": {"data_hash": data_hash, "updated_at": datetime.now(KYIV_TZ)}},
            upsert=True,
        )
    except Exception as e:
        logging.error(f"Error saving schedule state for {queue_id}: {e}")


# --- НАГАДУВАННЯ ---
# Доступні інтервали нагадувань (хвилини)
AVAILABLE_REMINDER_INTERVALS = {
    5: "5 хв",
    10: "10 хв",
    15: "15 хв",
    30: "30 хв",
    60: "1 год",
    120: "2 год",
}
DEFAULT_REMINDER_INTERVALS = [60, 30, 15, 5]  # За замовчуванням


async def get_sent_reminder(
    user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int
) -> bool:
    """Перевіряє чи було відправлено нагадування"""
    reminder = await db.reminders.find_one(
        {
            "user_id": user_id,
            "queue_id": queue_id,
            "event_time": event_time,
            "event_type": event_type,
            "minutes": minutes,
        }
    )
    return reminder is not None


async def mark_reminder_sent(
    user_id: int, queue_id: str, event_time: str, event_type: str, minutes: int
):
    """Позначає нагадування як відправлене"""
    await db.reminders.update_one(
        {
            "user_id": user_id,
            "queue_id": queue_id,
            "event_time": event_time,
            "event_type": event_type,
            "minutes": minutes,
        },
        {"$set": {"sent_at": datetime.now(KYIV_TZ)}},
        upsert=True,
    )


async def cleanup_old_reminders():
    """Видаляє старі нагадування (старші 2 днів)"""
    try:
        cutoff = datetime.now(KYIV_TZ) - timedelta(days=2)
        result = await db.reminders.delete_many({"sent_at": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logging.info(f"Cleaned {result.deleted_count} old reminders")
    except Exception as e:
        logging.error(f"Error cleaning reminders: {e}")


# --- КЛАВІАТУРИ ---
def get_main_keyboard(has_queue: bool = False) -> ReplyKeyboardMarkup:
    """Головна клавіатура (Reply Keyboard)"""
    queue_btn = BTN_CHANGE_QUEUE if has_queue else BTN_SET_QUEUE

    buttons = [
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_MY_QUEUE)],
        [KeyboardButton(text=queue_btn), KeyboardButton(text=BTN_HELP)],
        [KeyboardButton(text=BTN_DONATE)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_queue_choice_keyboard(reminders_on: bool = True) -> InlineKeyboardMarkup:
    """Вибір способу встановлення черги"""
    reminder_icon = "🔔" if reminders_on else "🔕"
    buttons = [
        [
            InlineKeyboardButton(
                text="🏠 Додати за адресою", callback_data="enter_address"
            )
        ],
        [
            InlineKeyboardButton(
                text="🔢 Обрати зі списку", callback_data="select_queue"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{reminder_icon} Нагадування", callback_data="reminder_settings"
            )
        ],
        [
            InlineKeyboardButton(
                text="🗑 Скасувати всі підписки", callback_data="unsubscribe"
            )
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_reminder_intervals_keyboard(
    selected_intervals: list[int], reminders_on: bool = True
) -> InlineKeyboardMarkup:
    """Клавіатура вибору інтервалів нагадувань з кнопкою увімк/вимк"""
    buttons = []

    # Кнопка увімкнення/вимкнення нагадувань
    if reminders_on:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🔔 Нагадування УВІМКНЕНО — натисни щоб вимкнути",
                    callback_data="toggle_reminders",
                )
            ]
        )
    else:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🔕 Нагадування ВИМКНЕНО — натисни щоб увімкнути",
                    callback_data="toggle_reminders",
                )
            ]
        )

    # Інтервали
    row = []
    for interval, label in AVAILABLE_REMINDER_INTERVALS.items():
        text = f"✅ {label}" if interval in selected_intervals else f"⬜ {label}"
        row.append(
            InlineKeyboardButton(text=text, callback_data=f"reminder_int_{interval}")
        )
        if len(row) == 3:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_queue_list_keyboard(
    subscribed_queues: list[str] = None,
) -> InlineKeyboardMarkup:
    """Клавіатура вибору черги зі списку (з позначенням підписаних)"""
    if subscribed_queues is None:
        subscribed_queues = []

    buttons = []
    row = []
    for queue in QUEUES:
        # Позначаємо підписані черги галочкою
        text = f"✅ {queue}" if queue in subscribed_queues else f"{queue}"
        row.append(InlineKeyboardButton(text=text, callback_data=f"queue_{queue}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="✔️ Готово", callback_data="done_select")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_choice")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура скасування"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]
        ]
    )


def get_donate_keyboard() -> InlineKeyboardMarkup:
    """Кнопка підтримки під повідомленнями"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💛 Підтримати проєкт", callback_data="show_donate"
                )
            ]
        ]
    )


def get_region_keyboard() -> InlineKeyboardMarkup:
    """Клавіатура вибору регіону"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🏔 Івано-Франківська обл.", callback_data="region_if"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🦁 Львівська обл.", callback_data="region_lviv"
                )
            ],
        ]
    )


# --- ОТРИМАННЯ ДАНИХ ---
def get_ssl_context():
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=1")
    return ssl_context


async def fetch_schedule(session, queue_id):
    if not APQE_PQFRTY:
        logging.error("APQE_PQFRTY not set!")
        return None

    params = {"queue": queue_id}
    # Видаляємо старі заголовки, curl_cffi сформує їх автоматично

    try:
        # impersonate="chrome120" робить вигляд, що це браузер Chrome
        # proxy=PROXY_URL передає твій socks5
        async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
            response = await session.get(APQE_PQFRTY, params=params)

            if response.status_code == 200:
                try:
                    return response.json()
                except Exception:
                    # Якщо 200 ОК, але не JSON (наприклад "Інформація відсутня"), 
                    # вважаємо, що відключень немає
                    return []
            else:
                logging.error(
                    f"[ІФ] API returned {response.status_code} for queue {queue_id}"
                )
                return None
    except Exception as e:
        logging.error(f"[ІФ] Error fetching {queue_id}: {e}")
        return None


async def fetch_schedule_by_address(city: str, street: str, house: str) -> dict | None:
    if not APSRC_PFRTY:
        logging.error("APSRC_PFRTY not set!")
        return None

    address = f"{city},{street},{house}"
    payload = {"accountNumber": "", "userSearchChoice": "pob", "address": address}

    try:
        async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
            response = await session.post(APSRC_PFRTY, data=payload)

            if response.status_code == 200:
                data = response.json()
                logging.info(f"Address search result for '{address}': {data}")
                return data
            else:
                logging.error(f"Address search failed: {response.status_code}")
                return None
    except Exception as e:
        logging.error(f"Error searching by address: {e}")
        return None


def extract_queue_from_response(data) -> tuple[str | None, list | None]:
    if not data or not isinstance(data, dict):
        return None, None

    current = data.get("current", {})
    schedule = data.get("schedule", [])

    if current.get("hasQueue") != "yes":
        return None, None

    queue_num = current.get("queue")
    sub_queue = current.get("subQueue")

    if queue_num is not None and sub_queue is not None:
        queue_id = f"{queue_num}.{sub_queue}"
        if queue_id in QUEUES:
            return queue_id, schedule

    return None, None


# --- ЛЬВІВСЬКА ОБЛАСТЬ (API ЛОЕ) ---
def _parse_lviv_html(html: str) -> tuple[str | None, dict]:
    """Парсить HTML одного дня. Повертає (date_str, {group: [(from,to),...]})"""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # Витягуємо дату з заголовка: "Графік ... на 09.02.2026"
    date_match = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
    date_str = date_match.group(1) if date_match else None

    result = {}
    for g in QUEUES:
        pattern = rf"Група\s*{re.escape(g)}\b(.*?)(?=Група|$)"
        m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        group_text = m.group(1)
        times = re.findall(r"(\d{2}:\d{2})\s*(?:-|–|до|to)\s*(\d{2}:\d{2})", group_text)
        if not times:
            single = re.findall(r"(\d{2}:\d{2})", group_text)
            if len(single) >= 2:
                times = [
                    (single[i], single[i + 1]) for i in range(0, len(single) - 1, 2)
                ]
        result[g] = times
    return date_str, result


def _fetch_lviv_schedule_sync() -> dict | None:
    """Завантажує графіки з ЛОЕ API (всі дні: Today, Tomorrow, ...).
    Повертає {date_str: {group: [(from, to), ...], ...}, ...} або None."""
    try:
        resp = requests.get(LVIV_API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        member = data.get("hydra:member") or []
        if not member:
            return {}  # Повертаємо порожній словник = немає відключень

        menu_items = member[0].get("menuItems", [])

        all_schedules = {}  # {"09.02.2026": {"1.1": [(...), ...], ...}, "10.02.2026": {...}}

        for item in menu_items:
            name = item.get("name", "")
            html = item.get("rawHtml")
            if not html:
                continue
            # Парсимо Today, Tomorrow та будь-які інші з rawHtml
            if (
                name in ("Today", "Tomorrow")
                or "grafic" in name.lower()
                or "графік" in name.lower()
            ):
                date_str, groups = _parse_lviv_html(html)
                if date_str and groups:
                    all_schedules[date_str] = groups

        return all_schedules if all_schedules else {}
    except Exception as e:
        logging.error(f"[ЛОЕ] Error fetching Lviv schedule: {e}")
        return None


def _search_lviv_cities_sync(name_part: str) -> list[dict]:
    """Шукає населені пункти Львівської обл. за назвою"""
    try:
        resp = requests.get(
            f"{LVIV_POWER_API_URL}/pw_cities",
            params={"name": name_part, "pagination": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        results = []
        for item in resp.json().get("hydra:member", []):
            otg = item.get("otg", {}).get("name", "")
            results.append({"id": item["id"], "name": item["name"], "otg": otg})
        return results
    except Exception as e:
        logging.error(f"[ЛОЕ] Error searching Lviv cities: {e}")
        return []


def _search_lviv_streets_sync(city_id: int, name_part: str) -> list[dict]:
    """Шукає вулиці у населеному пункті Львівської обл."""
    try:
        resp = requests.get(
            f"{LVIV_POWER_API_URL}/pw_streets",
            params={"city.id": city_id, "name": name_part, "pagination": "false"},
            timeout=10,
        )
        resp.raise_for_status()
        results = []
        for item in resp.json().get("hydra:member", []):
            results.append({"id": item["id"], "name": item["name"]})
        return results
    except Exception as e:
        logging.error(f"[ЛОЕ] Error searching Lviv streets: {e}")
        return []


def _find_lviv_group_sync(city_id: int, street_id: int, house: str) -> str | None:
    """Знаходить групу ГПВ за адресою (Львівська обл.)"""
    try:
        resp = requests.get(
            f"{LVIV_POWER_API_URL}/pw_accounts",
            params={
                "city.id": city_id,
                "street.id": street_id,
                "buildingName": house,
                "pagination": "false",
            },
            timeout=10,
        )
        resp.raise_for_status()
        members = resp.json().get("hydra:member", [])
        if not members:
            return None
        raw = members[0].get("chergGpv")
        if raw and len(raw) == 2 and raw.isdigit():
            return f"{raw[0]}.{raw[1]}"
        return raw or None
    except Exception as e:
        logging.error(f"[ЛОЕ] Error finding Lviv group: {e}")
        return None


def format_lviv_notification(
    queue_id: str,
    slots: list[tuple[str, str]],
    is_update: bool = False,
    address: str = None,
    date_str: str = None,
) -> str:
    """Форматує графік Львівської області для однієї групи"""
    days_names = [
        "Понеділок",
        "Вівторок",
        "Середа",
        "Четвер",
        "П'ятниця",
        "Субота",
        "Неділя",
    ]
    now = datetime.now(KYIV_TZ)

    if not date_str:
        date_str = now.strftime("%d.%m.%Y")

    day_name = ""
    try:
        d, m, y = date_str.split(".")
        dt = datetime(int(y), int(m), int(d))
        day_name = days_names[dt.weekday()]
    except:
        pass

    header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
    address_line = f"📍 *Адреса:* {address}\n" if address else ""

    lines = []
    total_minutes = 0
    if slots:
        for start, end in slots:
            duration_str = ""
            try:
                sh, sm = map(int, start.split(":"))
                eh, em = map(int, end.split(":"))
                s_min = sh * 60 + sm
                e_min = eh * 60 + em
                if e_min == 0:
                    e_min = 24 * 60
                diff = e_min - s_min
                if diff > 0:
                    total_minutes += diff
                    h, m = divmod(diff, 60)
                    duration_str = f" ({h} год)" if m == 0 else f" ({h} год {m} хв)"
            except Exception:
                pass
            lines.append(f"  🔴 {start} - {end}{duration_str}")
        schedule_str = "\n".join(lines)
    else:
        schedule_str = "  ✅ Відключень не заплановано"

    total_str = ""
    if total_minutes > 0:
        th, tm = divmod(total_minutes, 60)
        total_str = f" ({th} год)" if tm == 0 else f" ({th} год {tm} хв)"

    text = (
        f"{header}\n\n"
        f"{address_line}"
        f"🔢 *Група:* {queue_id}\n"
        f"📅 *{date_str}* _{day_name}_ {total_str}\n\n"
        f"{schedule_str}"
    )
    return text


# --- ФОРМАТУВАННЯ ПОВІДОМЛЕННЯ ---
def format_notification(queue_id, data, is_update=True, address=None):
    """Форматує повідомлення з графіками на ВСІ доступні дати"""
    if data is None or not isinstance(data, list):
        # Якщо пустий список - це ОК (немає відключень)
        return f"⚠️ Отримано некоректні дані для черги {queue_id}"

    if not data:
        # Пустий список = світло є
        header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
        address_line = f"📍 *Адреса:* {address}\n" if address else ""
        return (
            f"{header}\n\n{address_line}"
            f"🔢 *Черга:* {queue_id}\n\n"
            "✅ Відключень не заплановано"
        )

    days_names = [
        "Понеділок",
        "Вівторок",
        "Середа",
        "Четвер",
        "П'ятниця",
        "Субота",
        "Неділя",
    ]

    header = "⚡️ *Оновлення ГПВ!*" if is_update else "📊 *Поточний графік*"
    address_line = f"📍 *Адреса:* {address}\n" if address else ""

    text = f"{header}\n\n{address_line}🔢 *Черга:* {queue_id}\n"

    # Обробляємо кожну дату
    for record in data:
        event_date = record.get("eventDate", "Невідомо")
        approved_since = record.get("scheduleApprovedSince", "")

        # День тижня
        day_name = ""
        try:
            day, month, year = event_date.split(".")
            dt = datetime(int(year), int(month), int(day))
            day_name = days_names[dt.weekday()]
        except:
            pass

        queue_data = record.get("queues", {}).get(queue_id, [])

        schedule_lines = []
        total_minutes = 0
        if queue_data:
            for slot in queue_data:
                start = slot.get("from", "??")
                end = slot.get("to", "??")

                # Тривалість
                duration_str = ""
                try:
                    start_h, start_m = map(int, start.split(":"))
                    end_h, end_m = map(int, end.split(":"))
                    start_minutes = start_h * 60 + start_m
                    end_minutes = end_h * 60 + end_m
                    if end_minutes == 0:
                        end_minutes = 24 * 60
                    diff_minutes = end_minutes - start_minutes
                    if diff_minutes > 0:
                        total_minutes += diff_minutes
                        h = diff_minutes // 60
                        m = diff_minutes % 60
                        duration_str = f" ({h} год)" if m == 0 else f" ({h} год {m} хв)"
                except:
                    pass

                schedule_lines.append(f"  🔴 {start} - {end}{duration_str}")

            schedule_str = "\n".join(schedule_lines)
        else:
            schedule_str = "  ✅ Відключень не заплановано"

        # Додаємо загальну тривалість
        total_str = ""
        if total_minutes > 0:
            total_hours = total_minutes // 60
            total_mins = total_minutes % 60
            if total_mins == 0:
                total_str = f"({total_hours} год)"
            else:
                total_str = f"({total_hours} год {total_mins} хв)"

        text += f"\n📅 *{event_date}* _{day_name}_ {total_str}\n{schedule_str}\n"
        last_approved = data[-1].get("scheduleApprovedSince", "")
        if last_approved:
            text += f"\n🕒 _Затверджено: {last_approved}_"

    return text


def format_user_status(user_data) -> str:
    """Форматує статус користувача"""
    if user_data:
        if isinstance(user_data, dict):
            queues = user_data.get("queues", [])
            # Підтримка старого формату
            if not queues and user_data.get("queue"):
                queues = [user_data.get("queue")]

            address = user_data.get("address")
            reminders = user_data.get("reminders", True)
            reminder_intervals = user_data.get(
                "reminder_intervals", DEFAULT_REMINDER_INTERVALS
            )

            if not queues:
                return "⚠️ Підписку не налаштовано"

            queues_str = ", ".join(sorted(queues))
            queues_count = len(queues)
            queues_label = "Черги" if queues_count > 1 else "Черга"

            if reminders and reminder_intervals:
                intervals_labels = [
                    AVAILABLE_REMINDER_INTERVALS.get(i, f"{i} хв")
                    for i in sorted(reminder_intervals, reverse=True)
                ]
                reminders_str = ", ".join(intervals_labels)
            elif reminders:
                reminders_str = "ВКЛ (без інтервалів)"
            else:
                reminders_str = "ВИКЛ"

            region = user_data.get("region", REGION_IF)
            region_name = (
                "🏔 Івано-Франківська" if region == REGION_IF else "🦁 Львівська"
            )

            lines = []
            lines.append(f"🗺 *Регіон:* {region_name}")
            if address:
                lines.append(f"📍 *Адреса:* {address}")
            lines.append(f"🔢 *{queues_label}:* {queues_str}")
            lines.append(f"⏰ *Нагадування:* {reminders_str}")

            return "\n".join(lines)
        else:
            return f"🔢 *Черга:* {user_data}"
    return "⚠️ Підписку не налаштовано"


# --- ХЕНДЛЕРИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)

    # Новий користувач — пропонуємо обрати регіон
    if user_data is None:
        text = (
            f"💡 *Привіт, {message.from_user.first_name}!*\n\n"
            f"Я *Люмос* — допоможу тобі дізнаватись про відключення першим!\n\n"
            f"🗺 *Спочатку обери свій регіон:*"
        )
        await message.answer(
            text, reply_markup=get_region_keyboard(), parse_mode=ParseMode.MARKDOWN
        )
        return

    queues = user_data.get("queues", [])
    has_queue = len(queues) > 0

    if has_queue:
        status = format_user_status(user_data)
        text = (
            f"💡 *З поверненням, {message.from_user.first_name}!*\n\n"
            f"{status}\n\n"
            f"Я повідомлю тебе, якщо графік зміниться ⚡"
        )
    else:
        text = (
            f"💡 *Привіт, {message.from_user.first_name}!*\n\n"
            f"Я *Люмос* — допоможу тобі дізнаватись про відключення першим!\n\n"
            f"⚡ Обирай свою чергу і будь готовим до відключень.\n\n"
            f"🤔 Не знаєш чергу? Натискай кнопку «⚡ Обрати чергу» — я допоможу все знайти!"
        )

    await message.answer(
        text, reply_markup=get_main_keyboard(has_queue), parse_mode=ParseMode.MARKDOWN
    )


@dp.callback_query(F.data == "region_if")
async def cb_region_if(callback: CallbackQuery):
    await set_user_region(callback.from_user.id, REGION_IF)
    text = (
        "🏔 *Обрано: Івано-Франківська область*\n\n"
        "⚡ Тепер обирай свою чергу і будь готовим до відключень!\n\n"
        "🤔 Не знаєш чергу? Натискай «⚡ Обрати чергу» \u2014 я допоможу все знайти!"
    )
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        pass
    await callback.message.answer(
        "Меню:", reply_markup=get_main_keyboard(has_queue=False)
    )
    await callback.answer("✅ Регіон обрано!")


@dp.callback_query(F.data == "region_lviv")
async def cb_region_lviv(callback: CallbackQuery):
    await set_user_region(callback.from_user.id, REGION_LVIV)
    text = (
        "🦁 *Обрано: Львівська область*\n\n"
        "⚡ Тепер обирай свою групу і будь готовим до відключень!\n\n"
        "🤔 Не знаєш групу? Натискай «⚡ Обрати чергу» \u2014 я допоможу все знайти!"
    )
    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except TelegramBadRequest:
        pass
    await callback.message.answer(
        "Меню:", reply_markup=get_main_keyboard(has_queue=False)
    )
    await callback.answer("✅ Регіон обрано!")


@dp.message(Command("time"))
async def cmd_time(message: Message):
    # Отримуємо час у зоні KYIV_TZ (яку ми визначили раніше)
    now = datetime.now(KYIV_TZ)

    text = (
        f"🕒 *Поточний час (Київ):*\n"
        f"`{now.strftime('%H:%M:%S')}`\n\n"
        f"📅 *Дата:* `{now.strftime('%d.%m.%Y')}`"
    )

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "📚 *Як користуватися ботом:*\n\n"
        f"*{BTN_CHECK}* - переглянути графіки ваших черг\n"
        f"*{BTN_MY_QUEUE}* - інформація про ваші підписки\n"
        f"*{BTN_SET_QUEUE}/{BTN_CHANGE_QUEUE}* - налаштування підписок\n\n"
        "🔔 *Як це працює:*\n"
        "1. Введіть адресу або оберіть черги зі списку\n"
        "2. Можна відслідковувати кілька черг одночасно\n"
        "3. Бот автоматично перевіряє графіки\n"
        "4. При змінах вам прийде сповіщення\n\n"
        "⏰ *Нагадування:*\n"
        "Обирайте інтервали: 5, 10, 15, 30 хв, 1 або 2 год\n"
        "Налаштувати можна в меню керування чергами\n\n"
        "💬 *Підтримка та питання:*\n"
        "@vaysed\\_manager"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# --- ХЕНДЛЕРИ КНОПОК КЛАВІАТУРИ ---
@dp.message(F.text == BTN_CHECK)
async def btn_check(message: Message):
    user_data = await get_user_data(message.from_user.id)
    region = user_data.get("region", REGION_IF) if user_data else None

    # Немає регіону — пропонуємо обрати
    if not region:
        await message.answer(
            "🗺 *Спочатку оберіть регіон:*",
            reply_markup=get_region_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    user_queues = user_data.get("queues", []) if user_data else []

    if not user_queues:
        reminders_on = await get_user_reminders_state(message.from_user.id)
        await message.answer(
            "⚠️ Спочатку оберіть чергу!",
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    loading_msg = await message.answer("⏳ Завантажую графіки...")
    address = user_data.get("address") if isinstance(user_data, dict) else None

    if region == REGION_LVIV:
        # Львівська область
        all_schedules = await asyncio.to_thread(_fetch_lviv_schedule_sync)
        await loading_msg.delete()

        if all_schedules is None:
            await message.answer(
                "❌ Не вдалося отримати дані з API ЛОЕ. Спробуйте пізніше."
            )
            return

        for queue in sorted(user_queues):
            # Збираємо всі дати для цієї черги
            queue_days = []
            for date_str in sorted(all_schedules.keys()):
                day_data = all_schedules[date_str]
                slots = day_data.get(queue, [])
                queue_days.append((date_str, slots))
            # Формуємо зведене повідомлення як в ІФ
            if queue_days:
                days_names = [
                    "Понеділок",
                    "Вівторок",
                    "Середа",
                    "Четвер",
                    "П'ятниця",
                    "Субота",
                    "Неділя",
                ]
                header = "📊 *Поточний графік*"
                address_line = (
                    f"📍 *Адреса:* {address if len(user_queues) == 1 else ''}\n"
                    if address
                    else ""
                )
                text = f"{header}\n\n{address_line}🔢 *Група:* {queue}\n"
                for date_str, slots in queue_days:
                    # День тижня
                    day_name = ""
                    try:
                        d, m, y = date_str.split(".")
                        dt = datetime(int(y), int(m), int(d))
                        day_name = days_names[dt.weekday()]
                    except:
                        pass
                    # Тривалість
                    total_minutes = 0
                    lines = []
                    if slots:
                        for start, end in slots:
                            duration_str = ""
                            try:
                                sh, sm = map(int, start.split(":"))
                                eh, em = map(int, end.split(":"))
                                s_min = sh * 60 + sm
                                e_min = eh * 60 + em
                                if e_min == 0:
                                    e_min = 24 * 60
                                diff = e_min - s_min
                                if diff > 0:
                                    total_minutes += diff
                                    h, m = divmod(diff, 60)
                                    duration_str = (
                                        f" ({h} год)"
                                        if m == 0
                                        else f" ({h} год {m} хв)"
                                    )
                            except Exception:
                                pass
                            lines.append(f"  🔴 {start} - {end}{duration_str}")
                        schedule_str = "\n".join(lines)
                    else:
                        schedule_str = "  ✅ Відключень не заплановано"
                    # Загальна тривалість
                    total_str = ""
                    if total_minutes > 0:
                        th, tm = divmod(total_minutes, 60)
                        total_str = (
                            f" ({th} год)" if tm == 0 else f" ({th} год {tm} хв)"
                        )
                    text += (
                        f"\n📅 {date_str} _{day_name}_ {total_str}\n{schedule_str}\n"
                    )
                await message.answer(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_donate_keyboard(),
                )
                await asyncio.sleep(0.3)
            else:
                # Якщо немає даних про відключення для цієї черги - значить світло є
                header = "📊 *Поточний графік*"
                address_line = (
                    f"📍 *Адреса:* {address if len(user_queues) == 1 else ''}\n"
                    if address
                    else ""
                )
                text = (
                    f"{header}\n\n{address_line}"
                    f"🔢 *Група:* {queue}\n\n"
                    "✅ Відключень не заплановано"
                )
                await message.answer(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_donate_keyboard(),
                )
                await asyncio.sleep(0.3)
    else:
        # Івано-Франківська область
        ssl_context = get_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=connector) as session:
            results = []
            for queue in sorted(user_queues):
                data = await fetch_schedule(session, queue)
                if data is not None:
                    msg = format_notification(
                        queue,
                        data,
                        is_update=False,
                        address=address if len(user_queues) == 1 else None,
                    )
                    results.append(msg)

            await loading_msg.delete()

            if results:
                for i, msg in enumerate(results):
                    if i == len(results) - 1:
                        await message.answer(
                            msg,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=get_donate_keyboard(),
                        )
                    else:
                        await message.answer(msg, parse_mode=ParseMode.MARKDOWN)
                    await asyncio.sleep(0.3)
            else:
                await message.answer("❌ Не вдалося отримати дані. Спробуйте пізніше.")


@dp.message(F.text == BTN_MY_QUEUE)
async def btn_my_queue(message: Message):
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    status = format_user_status(user_data)

    if queues:
        count = len(queues)
        plural = "черг" if count > 1 else "чергу"
        text = f"✅ *Ваші підписки:*\n\n{status}\n\n🔔 Ви відслідковуєте {count} {plural}.\nПри змінах прийде сповіщення."
    else:
        text = (
            f"⚠️ *Підписку не налаштовано*\n\nОберіть чергу, щоб отримувати сповіщення."
        )

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@dp.message(F.text.in_({BTN_SET_QUEUE, BTN_CHANGE_QUEUE}))
async def btn_set_queue(message: Message, state: FSMContext):
    await state.clear()
    user_data = await get_user_data(message.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    reminders_on = user_data.get("reminders", True) if user_data else True

    if queues:
        status = format_user_status(user_data)
        text = f"✏️ *Керування підписками*\n\n*Поточні підписки:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"

    await message.answer(
        text,
        reply_markup=get_queue_choice_keyboard(reminders_on),
        parse_mode=ParseMode.MARKDOWN,
    )


@dp.message(F.text == BTN_HELP)
async def btn_help(message: Message):
    await cmd_help(message)


def get_donate_text() -> str:
    """Повертає текст про донати"""
    return (
        "💛 *Підтримай розвиток проєкту!*\n\n"
        "🆓 *Люмос — повністю безкоштовний* і таким залишиться назавжди.\n\n"
        "Кожен донат — добровільний, але саме ваша підтримка допомагає "
        "робити бота кращим: додавати нові функції, покращувати стабільність "
        "та забезпечувати безперебійну роботу. 🙏\n\n"
        f"🐱 {DONATE_URL}\n"
        f"💚 {DONATE_PRIV_URL}"
    )


@dp.message(F.text == BTN_DONATE)
async def btn_donate(message: Message):
    await message.answer(get_donate_text(), parse_mode=ParseMode.MARKDOWN)


@dp.callback_query(F.data == "show_donate")
async def cb_show_donate(callback: CallbackQuery):
    """Показує повідомлення про донати"""
    await callback.message.answer(get_donate_text(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


# --- FSM ХЕНДЛЕРИ ДЛЯ АДРЕСИ ---
@dp.message(AddressForm.waiting_for_city)
async def process_city(message: Message, state: FSMContext):
    city = message.text.strip()
    await state.update_data(city=city)
    await state.set_state(AddressForm.waiting_for_street)

    text = (
        f"🏙 Місто: *{city}*\n\n"
        f"🏠 *Тепер введіть вулицю та номер будинку:*\n\n"
        f"Формат: `Вулиця, Номер`\n"
        f"Наприклад: `Паркова, 7    `"
    )
    await message.answer(
        text, reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN
    )


@dp.message(AddressForm.waiting_for_street)
async def process_street(message: Message, state: FSMContext):
    input_text = message.text.strip()

    if "," in input_text:
        parts = input_text.split(",", 1)
        street = parts[0].strip()
        house = parts[1].strip()
    else:
        parts = input_text.rsplit(" ", 1)
        if len(parts) == 2:
            street = parts[0].strip()
            house = parts[1].strip()
        else:
            await message.answer(
                "⚠️ Не вдалося розпізнати формат.\n\n"
                "Введіть у форматі: `Вулиця, Номер`\n"
                "Наприклад: `Бельведерська, 65`",
                reply_markup=get_cancel_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

    data = await state.get_data()
    city = data.get("city")

    full_address = f"{city}, {street}, {house}"

    loading_msg = await message.answer(
        f"⏳ Шукаю чергу для адреси:\n*{full_address}*...",
        parse_mode=ParseMode.MARKDOWN,
    )

    result = await fetch_schedule_by_address(city, street, house)

    await loading_msg.delete()

    if result:
        queue, schedule = extract_queue_from_response(result)

        if queue and schedule:
            await add_queue_to_user(message.from_user.id, queue, full_address)
            await state.clear()

            user_queues = await get_user_queues(message.from_user.id)
            queues_str = ", ".join(sorted(user_queues))

            text = (
                f"✅ *Адресу знайдено!*\n\n"
                f"📍 *Адреса:* {full_address}\n"
                f"🔢 *Черга:* {queue}\n\n"
                f"📋 *Всі ваші черги:* {queues_str}\n"
                f"🔔 Тепер ви отримуватимете сповіщення про зміни в графіку."
            )
            await message.answer(
                text,
                reply_markup=get_main_keyboard(has_queue=True),
                parse_mode=ParseMode.MARKDOWN,
            )

            msg = format_notification(
                queue, schedule, is_update=False, address=full_address
            )
            await message.answer(
                msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_donate_keyboard()
            )
        else:
            await state.clear()
            reminders_on = await get_user_reminders_state(message.from_user.id)
            await message.answer(
                "⚠️ Не вдалося визначити чергу для цієї адреси.\n\n"
                "Спробуйте ввести адресу ще раз або оберіть чергу вручну.",
                reply_markup=get_queue_choice_keyboard(reminders_on),
                parse_mode=ParseMode.MARKDOWN,
            )
    else:
        await state.clear()
        reminders_on = await get_user_reminders_state(message.from_user.id)
        await message.answer(
            "❌ Адресу не знайдено.\n\n"
            "Перевірте правильність написання та спробуйте ще раз.",
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN,
        )


# --- FSM ХЕНДЛЕРИ ДЛЯ АДРЕСИ (ЛЬВІВ) ---
@dp.message(LvivAddressForm.waiting_for_city_search)
async def lviv_city_search(message: Message, state: FSMContext):
    query = message.text.strip()
    if len(query) < 2:
        await message.answer(
            "⚠️ Введіть хоча б 2 символи.", reply_markup=get_cancel_keyboard()
        )
        return

    loading = await message.answer("🔍 Шукаю...")
    cities = await asyncio.to_thread(_search_lviv_cities_sync, query)
    await loading.delete()

    if not cities:
        await message.answer(
            "❌ Населений пункт не знайдено. Спробуйте ще раз:",
            reply_markup=get_cancel_keyboard(),
        )
        return

    cities_map = {str(c["id"]): c["name"] for c in cities[:20]}
    await state.update_data(cities_map=cities_map)

    buttons = []
    for c in cities[:20]:
        label = f"{c['name']} ({c['otg']})" if c["otg"] else c["name"]
        buttons.append(
            [InlineKeyboardButton(text=label, callback_data=f"lcity|{c['id']}")]
        )
    buttons.append(
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]
    )
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    count = len(cities)
    more = f"\n_(показано перші 20 з {count})_" if count > 20 else ""
    await message.answer(
        f"🏠 Знайдено *{count}* варіантів. Оберіть свій:{more}",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


@dp.callback_query(F.data.startswith("lcity|"))
async def cb_lviv_city_select(callback: CallbackQuery, state: FSMContext):
    city_id = callback.data.split("|", 1)[1]
    data = await state.get_data()
    cities_map = data.get("cities_map", {})
    city_name = cities_map.get(city_id, f"ID:{city_id}")

    await state.update_data(city_id=city_id, city_name=city_name)
    await state.set_state(LvivAddressForm.waiting_for_street_search)

    await callback.answer()
    try:
        await callback.message.edit_text(
            f"✅ *Обрано:* {city_name}\n\n🏠 *Тепер введіть назву вулиці:*\n\nНаприклад: `Шевченка`",
            reply_markup=get_cancel_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except TelegramBadRequest:
        pass


@dp.message(LvivAddressForm.waiting_for_street_search)
async def lviv_street_search(message: Message, state: FSMContext):
    query = message.text.strip()
    if len(query) < 2:
        await message.answer(
            "⚠️ Введіть хоча б 2 символи.", reply_markup=get_cancel_keyboard()
        )
        return

    data = await state.get_data()
    city_id = int(data["city_id"])

    loading = await message.answer("🔍 Шукаю...")
    streets = await asyncio.to_thread(_search_lviv_streets_sync, city_id, query)
    await loading.delete()

    if not streets:
        await message.answer(
            "❌ Вулицю не знайдено. Спробуйте ще:", reply_markup=get_cancel_keyboard()
        )
        return

    streets_map = {str(s["id"]): s["name"] for s in streets[:20]}
    await state.update_data(streets_map=streets_map)

    buttons = []
    for s in streets[:20]:
        buttons.append(
            [InlineKeyboardButton(text=s["name"], callback_data=f"lstreet|{s['id']}")]
        )
    buttons.append(
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]
    )
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    count = len(streets)
    more = f"\n_(показано перші 20 з {count})_" if count > 20 else ""
    await message.answer(
        f"🏠 Знайдено *{count}* вулиць. Оберіть:{more}",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


@dp.callback_query(F.data.startswith("lstreet|"))
async def cb_lviv_street_select(callback: CallbackQuery, state: FSMContext):
    street_id = callback.data.split("|", 1)[1]
    data = await state.get_data()
    streets_map = data.get("streets_map", {})
    street_name = streets_map.get(street_id, f"ID:{street_id}")
    city_name = data.get("city_name", "")

    await state.update_data(street_id=street_id, street_name=street_name)
    await state.set_state(LvivAddressForm.waiting_for_house)

    await callback.answer()
    try:
        await callback.message.edit_text(
            f"✅ *Обрано:* {city_name}, {street_name}\n\n"
            f"🔢 *Введіть номер будинку:*\n\nНаприклад: `7` або `12А`",
            reply_markup=get_cancel_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except TelegramBadRequest:
        pass


@dp.message(LvivAddressForm.waiting_for_house)
async def lviv_house_input(message: Message, state: FSMContext):
    house = message.text.strip()
    if not house:
        await message.answer(
            "⚠️ Введіть номер будинку:", reply_markup=get_cancel_keyboard()
        )
        return

    data = await state.get_data()
    city_id = int(data["city_id"])
    city_name = data.get("city_name", "")
    street_id = int(data["street_id"])
    street_name = data.get("street_name", "")

    loading = await message.answer("🔍 Шукаю групу...")
    group = await asyncio.to_thread(_find_lviv_group_sync, city_id, street_id, house)
    await loading.delete()
    await state.clear()

    full_address = f"{city_name}, {street_name}, {house}"

    if group:
        await add_queue_to_user(message.from_user.id, group, full_address)
        user_queues = await get_user_queues(message.from_user.id)
        queues_str = ", ".join(sorted(user_queues))

        text = (
            f"✅ *Адресу знайдено!*\n\n"
            f"📍 *Адреса:* {full_address}\n"
            f"🔢 *Група:* {group}\n\n"
            f"📋 *Всі ваші групи:* {queues_str}\n"
            f"🔔 Натисніть \u00ab{BTN_CHECK}\u00bb щоб переглянути графік."
        )
        await message.answer(
            text,
            reply_markup=get_main_keyboard(has_queue=True),
            parse_mode=ParseMode.MARKDOWN,
        )

        # Показуємо графік одразу
        loading2 = await message.answer("⏳ Завантажую графік...")
        schedules = await asyncio.to_thread(_fetch_lviv_schedule_sync)
        await loading2.delete()

        if schedules is not None:
            date_keys = list(schedules.keys())
            for i, date_str in enumerate(date_keys):
                slots = schedules[date_str].get(group, [])
                msg = format_lviv_notification(
                    group, slots, address=full_address, date_str=date_str
                )
                if i == len(date_keys) - 1:
                    await message.answer(
                        msg,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=get_donate_keyboard(),
                    )
                else:
                    await message.answer(msg, parse_mode=ParseMode.MARKDOWN)
                await asyncio.sleep(0.3)
    else:
        reminders_on = await get_user_reminders_state(message.from_user.id)
        await message.answer(
            f"❌ *Будинок не знайдено*\n\n📍 {full_address}\n\n"
            "Перевірте правильність номера або оберіть групу вручну.",
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN,
        )


# --- CALLBACK ХЕНДЛЕРИ ---
@dp.callback_query(F.data == "enter_address")
async def cb_enter_address(callback: CallbackQuery, state: FSMContext):
    region = await get_user_region(callback.from_user.id)

    if region == REGION_LVIV:
        # Львів — покрокова адреса через power-api
        await state.set_state(LvivAddressForm.waiting_for_city_search)
        text = "🏙 *Введіть назву населеного пункту:*\n\nНаприклад: `Львів` або `Броди`"
    else:
        # ІФ — стандартний пошук
        await state.set_state(AddressForm.waiting_for_city)
        text = "🏙 *Введіть назву міста/села:*\n\nНаприклад: `Івано-Франківськ`"
    await callback.message.edit_text(
        text, reply_markup=get_cancel_keyboard(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@dp.callback_query(F.data == "cancel_input")
async def cb_cancel_input(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_queues = await get_user_queues(callback.from_user.id)
    has_queue = len(user_queues) > 0

    await callback.message.edit_text(
        "❌ *Введення скасовано*", parse_mode=ParseMode.MARKDOWN
    )
    await callback.message.answer(
        "Оберіть дію:", reply_markup=get_main_keyboard(has_queue)
    )
    await callback.answer()


@dp.callback_query(F.data == "select_queue")
async def cb_select_queue(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_queues = await get_user_queues(callback.from_user.id)
    text = "🔢 *Оберіть черги для відслідковування:*\n\n✅ — підписані\nНатисніть на чергу щоб додати/видалити"
    await callback.message.edit_text(
        text,
        reply_markup=get_queue_list_keyboard(user_queues),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "back_choice")
async def cb_back_choice(callback: CallbackQuery):
    user_data = await get_user_data(callback.from_user.id)
    queues = user_data.get("queues", []) if user_data else []
    reminders_on = user_data.get("reminders", True) if user_data else True

    if queues:
        status = format_user_status(user_data)
        text = f"✏️ *Керування підписками*\n\n*Поточні підписки:*\n{status}\n\nОберіть спосіб:"
    else:
        text = "⚡ *Оберіть спосіб налаштування:*"

    await callback.message.edit_text(
        text,
        reply_markup=get_queue_choice_keyboard(reminders_on),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("queue_"))
async def cb_queue_select(callback: CallbackQuery):
    queue = callback.data.replace("queue_", "")

    if queue not in QUEUES:
        await callback.answer("❌ Невідома черга!", show_alert=True)
        return

    user_queues = await get_user_queues(callback.from_user.id)

    # Тогл - якщо є, видаляємо, якщо немає - додаємо
    if queue in user_queues:
        await remove_queue_from_user(callback.from_user.id, queue)
        await callback.answer(f"➖ Черга {queue} видалена")
    else:
        await add_queue_to_user(callback.from_user.id, queue)
        await callback.answer(f"➕ Черга {queue} додана")

    # Оновлюємо клавіатуру
    user_queues = await get_user_queues(callback.from_user.id)
    text = "🔢 *Оберіть черги для відслідковування:*\n\n✅ — підписані\nНатисніть на чергу щоб додати/видалити"
    await callback.message.edit_text(
        text,
        reply_markup=get_queue_list_keyboard(user_queues),
        parse_mode=ParseMode.MARKDOWN,
    )


@dp.callback_query(F.data == "done_select")
async def cb_done_select(callback: CallbackQuery):
    """Завершення вибору черг"""
    user_queues = await get_user_queues(callback.from_user.id)
    has_queue = len(user_queues) > 0

    if has_queue:
        queues_str = ", ".join(sorted(user_queues))
        count = len(user_queues)
        plural = "черг" if count > 1 else "чергу"

        text = (
            f"✅ *Підписки оновлено!*\n\n"
            f"🔢 *Ви відслідковуєте {count} {plural}:* {queues_str}\n\n"
            f"🔔 Тепер ви отримуватимете сповіщення про зміни в графіках."
        )
        await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        await callback.message.answer(
            "Меню оновлено:", reply_markup=get_main_keyboard(has_queue=True)
        )

        # Показуємо поточні графіки для обраних черг
        region = await get_user_region(callback.from_user.id)
        sorted_queues = sorted(user_queues)

        if region == REGION_LVIV:
            all_schedules = await asyncio.to_thread(_fetch_lviv_schedule_sync)
            if all_schedules:
                messages = []
                for queue in sorted_queues:
                    for date_str, day_data in all_schedules.items():
                        slots = day_data.get(queue, [])
                        messages.append((queue, slots, date_str))
                for i, (queue, slots, date_str) in enumerate(messages):
                    msg = format_lviv_notification(queue, slots, date_str=date_str)
                    if i == len(messages) - 1:
                        await callback.message.answer(
                            msg,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=get_donate_keyboard(),
                        )
                    else:
                        await callback.message.answer(
                            msg, parse_mode=ParseMode.MARKDOWN
                        )
                    await asyncio.sleep(0.3)
        else:
            ssl_context = get_ssl_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                for i, queue in enumerate(sorted_queues):
                    data = await fetch_schedule(session, queue)
                    if data:
                        msg = format_notification(queue, data, is_update=False)
                        if i == len(sorted_queues) - 1:
                            await callback.message.answer(
                                msg,
                                parse_mode=ParseMode.MARKDOWN,
                                reply_markup=get_donate_keyboard(),
                            )
                        else:
                            await callback.message.answer(
                                msg, parse_mode=ParseMode.MARKDOWN
                            )
                    await asyncio.sleep(0.3)
    else:
        reminders_on = await get_user_reminders_state(callback.from_user.id)
        text = "⚠️ *Ви не обрали жодної черги*\n\nОберіть хоча б одну чергу для відслідковування."
        await callback.message.edit_text(
            text,
            reply_markup=get_queue_choice_keyboard(reminders_on),
            parse_mode=ParseMode.MARKDOWN,
        )

    await callback.answer()


@dp.callback_query(F.data == "toggle_reminders")
async def cb_toggle_reminders(callback: CallbackQuery):
    """Перемикає стан нагадувань і оновлює екран налаштувань"""
    new_state = await toggle_user_reminders(callback.from_user.id)
    intervals = await get_user_reminder_intervals(callback.from_user.id)

    if intervals:
        selected = [
            AVAILABLE_REMINDER_INTERVALS[i]
            for i in sorted(intervals, reverse=True)
            if i in AVAILABLE_REMINDER_INTERVALS
        ]
        selected_text = ", ".join(selected)
    else:
        selected_text = "не обрано"

    status_text = "увімкнено ✅" if new_state else "вимкнено ❌"

    text = (
        f"⏰ *Налаштування нагадувань*\n\n"
        f"*Стан:* {status_text}\n"
        f"*Обрані інтервали:* {selected_text}\n\n"
        "Натисніть на інтервал щоб додати/видалити:"
    )

    try:
        await callback.message.edit_text(
            text,
            reply_markup=get_reminder_intervals_keyboard(intervals, new_state),
            parse_mode=ParseMode.MARKDOWN,
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise e

    state_text = "увімкнено" if new_state else "вимкнено"
    await callback.answer(f"🔔 Нагадування {state_text}!")


@dp.callback_query(F.data == "reminder_settings")
async def cb_reminder_settings(callback: CallbackQuery):
    """Показує налаштування інтервалів нагадувань"""
    intervals = await get_user_reminder_intervals(callback.from_user.id)
    reminders_on = await get_user_reminders_state(callback.from_user.id)

    if intervals:
        selected = [
            AVAILABLE_REMINDER_INTERVALS[i]
            for i in sorted(intervals, reverse=True)
            if i in AVAILABLE_REMINDER_INTERVALS
        ]
        selected_text = ", ".join(selected)
    else:
        selected_text = "не обрано"

    status_text = "увімкнено ✅" if reminders_on else "вимкнено ❌"

    text = (
        "⏰ *Налаштування нагадувань*\n\n"
        f"*Стан:* {status_text}\n"
        f"*Обрані інтервали:* {selected_text}\n\n"
        "Натисніть на інтервал щоб додати/видалити:"
    )

    await callback.message.edit_text(
        text,
        reply_markup=get_reminder_intervals_keyboard(intervals, reminders_on),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("reminder_int_"))
async def cb_toggle_reminder_interval(callback: CallbackQuery):
    """Перемикає інтервал нагадування"""
    interval = int(callback.data.replace("reminder_int_", ""))

    if interval not in AVAILABLE_REMINDER_INTERVALS:
        await callback.answer("❌ Невідомий інтервал!", show_alert=True)
        return

    new_intervals = await toggle_reminder_interval(callback.from_user.id, interval)
    reminders_on = await get_user_reminders_state(callback.from_user.id)

    if new_intervals:
        selected = [
            AVAILABLE_REMINDER_INTERVALS[i]
            for i in sorted(new_intervals, reverse=True)
            if i in AVAILABLE_REMINDER_INTERVALS
        ]
        selected_text = ", ".join(selected)
    else:
        selected_text = "не обрано"

    status_text = "увімкнено ✅" if reminders_on else "вимкнено ❌"

    text = (
        "⏰ *Налаштування нагадувань*\n\n"
        f"*Стан:* {status_text}\n"
        f"*Обрані інтервали:* {selected_text}\n\n"
        "Натисніть на інтервал щоб додати/видалити:"
    )

    await callback.message.edit_text(
        text,
        reply_markup=get_reminder_intervals_keyboard(new_intervals, reminders_on),
        parse_mode=ParseMode.MARKDOWN,
    )

    label = AVAILABLE_REMINDER_INTERVALS[interval]
    if interval in new_intervals:
        await callback.answer(f"✅ {label} додано")
    else:
        await callback.answer(f"➖ {label} видалено")


@dp.callback_query(F.data == "unsubscribe")
async def cb_unsubscribe(callback: CallbackQuery):
    user_queues = await get_user_queues(callback.from_user.id)

    if not user_queues:
        await callback.answer("ℹ️ У вас немає активних підписок", show_alert=True)
        return

    await remove_user_queue(callback.from_user.id)

    text = "🔕 *Всі підписки скасовано*\n\nВи більше не отримуватимете сповіщення."
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    await callback.message.answer(
        "Меню оновлено:", reply_markup=get_main_keyboard(has_queue=False)
    )
    await callback.answer("✅ Підписки скасовано")


# --- ОСНОВНИЙ ЦИКЛ ПЕРЕВІРКИ ---
def extract_all_schedules(data, queue_id: str) -> dict:
    """
    Витягує графіки для ВСІХ дат.
    Повертає: {"20.01.2026": [...hours...], "21.01.2026": [...hours...]}
    """
    result = {}

    if not data or not isinstance(data, list):
        return result

    for record in data:
        event_date = record.get("eventDate")
        if not event_date:
            continue

        queue_hours = record.get("queues", {}).get(queue_id, [])

        simplified_hours = []
        for slot in queue_hours:
            simplified_hours.append(
                {
                    "from": slot.get("from"),
                    "to": slot.get("to"),
                    "status": slot.get("status"),
                }
            )

        result[event_date] = simplified_hours

    return result


def format_schedule_notification(
    queue_id: str, date: str, hours: list, change_type: str, address: str = None
) -> str:
    """
    Форматує сповіщення про зміну графіку.
    change_type: "new" | "updated"
    """
    # День тижня
    day_name = ""
    try:
        day, month, year = date.split(".")
        dt = datetime(int(year), int(month), int(day))
        days = [
            "Понеділок",
            "Вівторок",
            "Середа",
            "Четвер",
            "П'ятниця",
            "Субота",
            "Неділя",
        ]
        day_name = days[dt.weekday()]
    except:
        pass

    # Заголовок
    if change_type == "new":
        header = f"📅 *Додано новий графік на {date}*"
    else:
        header = f"🔄 *Оновлено графік на {date}*"

    # Години
    schedule_lines = []
    total_minutes = 0
    if hours:
        for slot in hours:
            start = slot.get("from", "??")
            end = slot.get("to", "??")

            # Тривалість
            duration_str = ""
            try:
                start_h, start_m = map(int, start.split(":"))
                end_h, end_m = map(int, end.split(":"))
                start_minutes = start_h * 60 + start_m
                end_minutes = end_h * 60 + end_m
                if end_minutes == 0:
                    end_minutes = 24 * 60
                diff_minutes = end_minutes - start_minutes
                if diff_minutes > 0:
                    total_minutes += diff_minutes
                    h = diff_minutes // 60
                    m = diff_minutes % 60
                    duration_str = f" ({h} год)" if m == 0 else f" ({h} год {m} хв)"
            except:
                pass

            schedule_lines.append(f"🔴 {start} - {end}{duration_str}")

        schedule_str = "\n".join(schedule_lines)
    else:
        schedule_str = "✅ Відключень не заплановано"

    # Додаємо загальну тривалість до заголовка
    if total_minutes > 0:
        total_hours = total_minutes // 60
        total_mins = total_minutes % 60
        if total_mins == 0:
            total_str = f" ({total_hours} год)"
        else:
            total_str = f" ({total_hours} год {total_mins} хв)"
        if change_type == "new":
            header = f"📅 *Додано новий графік на {date}{total_str}*"
        else:
            header = f"🔄 *Оновлено графік на {date}{total_str}*"

    address_line = f"📍 {address}\n" if address else ""

    text = (
        f"{header}\n"
        f"_{day_name}_\n\n"
        f"{address_line}"
        f"🔢 Черга: *{queue_id}*\n\n"
        f"{schedule_str}"
    )
    return text


async def lviv_scheduled_checker():
    """Моніторинг графіків Львівської області (ЛОЕ)"""
    logging.info("🚀 [ЛОЕ] Monitor started")
    await asyncio.sleep(15)

    while True:
        try:
            # Визначаємо "Сьогодні" по Києву
            kiev_tz = pytz.timezone("Europe/Kiev")
            today = datetime.now(kiev_tz).date()

            logging.info(f"[ЛОЕ] Starting check. Today: {today}")

            all_schedules = await asyncio.to_thread(_fetch_lviv_schedule_sync)
            if all_schedules is None:
                logging.warning("[ЛОЕ] No schedule data received")
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            for queue_id in QUEUES:
                state_key = f"lviv_{queue_id}"

                # Завантажуємо збережений стан
                saved_state_json = await get_schedule_state(state_key)
                saved_data = {}
                if saved_state_json:
                    try:
                        saved_data = json.loads(saved_state_json)
                    except:
                        saved_data = {}

                # Очищення старих дат (видаляємо тільки те, що СУВОРО менше за сьогодні)
                cleaned_dates = []
                for ds in list(saved_data.keys()):
                    try:
                        d, m, y = ds.split(".")
                        record_date = datetime(int(y), int(m), int(d)).date()
                        # Видаляємо тільки якщо дата СУВОРО менша за сьогодні
                        if record_date < today:
                            del saved_data[ds]
                            cleaned_dates.append(ds)
                    except Exception as e:
                        logging.error(f"[ЛОЕ] Error parsing date {ds}: {e}")
                        pass

                if cleaned_dates:
                    logging.info(
                        f"[ЛОЕ] Cleaned old dates for {queue_id}: {cleaned_dates}"
                    )
                    await save_schedule_state(state_key, json.dumps(saved_data))

                # Порівнюємо кожну дату
                changes = []  # [(date_str, slots, "new"|"updated")]

                for date_str, day_data in all_schedules.items():
                    # --- ГОЛОВНИЙ ФІКС ---
                    # Якщо дата з графіку менша за сьогодні -> ЦЕ СМІТТЯ.
                    # Навіть якщо API каже, що це "Today".
                    try:
                        d, m, y = date_str.split(".")
                        schedule_date = datetime(int(y), int(m), int(d)).date()

                        if schedule_date < today:
                            logging.info(
                                f"[ЛОЕ] Skipping outdated schedule for {queue_id} on {date_str} (today is {today})"
                            )
                            continue
                    except Exception as e:
                        logging.error(f"[ЛОЕ] Error parsing date {date_str}: {e}")
                        continue
                    # ---------------------

                    slots = day_data.get(queue_id)
                    if slots is None:
                        continue

                    current_hash = json.dumps(slots, sort_keys=True)
                    old_hash = saved_data.get(date_str)

                    if old_hash is None:
                        changes.append((date_str, slots, "new"))
                        logging.info(f"[ЛОЕ] New schedule for {queue_id} on {date_str}")
                    elif old_hash != current_hash:
                        changes.append((date_str, slots, "updated"))
                        logging.info(
                            f"[ЛОЕ] Updated schedule for {queue_id} on {date_str}"
                        )

                    saved_data[date_str] = current_hash

                if changes:
                    subscribers = await get_users_by_queue(queue_id, REGION_LVIV)
                    if subscribers:
                        for user_id in subscribers:
                            try:
                                user_data = await get_user_data(user_id)
                                address = (
                                    user_data.get("address")
                                    if isinstance(user_data, dict)
                                    else None
                                )

                                for i, (date_str, slots, change_type) in enumerate(
                                    changes
                                ):
                                    msg = format_schedule_notification(
                                        queue_id,
                                        date_str,
                                        [{"from": s, "to": e} for s, e in slots],
                                        change_type,
                                        address,
                                    )
                                    if i == len(changes) - 1:
                                        await bot.send_message(
                                            user_id,
                                            msg,
                                            parse_mode=ParseMode.MARKDOWN,
                                            reply_markup=get_donate_keyboard(),
                                        )
                                    else:
                                        await bot.send_message(
                                            user_id, msg, parse_mode=ParseMode.MARKDOWN
                                        )
                                    await asyncio.sleep(0.3)

                                logging.info(
                                    f"[ЛОЕ] Notifications sent to {user_id} for {queue_id}"
                                )
                            except Exception as e:
                                logging.error(f"[ЛОЕ] Failed to send to {user_id}: {e}")
                            await asyncio.sleep(0.5)

                    await save_schedule_state(state_key, json.dumps(saved_data))

                await asyncio.sleep(0.5)

            logging.info(
                f"[ЛОЕ] Check completed. Next check in {CHECK_INTERVAL} seconds"
            )
        except Exception as e:
            logging.error(f"[ЛОЕ] Checker error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


async def scheduled_checker():
    logging.info("🚀 [ІФ] Monitor started")
    await asyncio.sleep(10)

    while True:
        for queue_id in QUEUES:
            data = await fetch_schedule(None, queue_id)
            if data is None:
                continue

            # Витягуємо графіки для всіх дат
            current_schedules = extract_all_schedules(data, queue_id)
            # Прибираємо перевірку if not current_schedules: continue
            # Бо пустий графік теж треба обробити (зняти збережені)

            # Завантажуємо збережений стан
            saved_state_json = await get_schedule_state(queue_id)
            saved_schedules = {}
            if saved_state_json:
                try:
                    saved_schedules = json.loads(saved_state_json)
                except:
                    saved_schedules = {}

            # Очищення старих дат (до сьогодні)
            today = datetime.now(KYIV_TZ).date()
            old_dates = []
            for date_str in list(saved_schedules.keys()):
                try:
                    day, month, year = date_str.split(".")
                    date_obj = datetime(int(year), int(month), int(day)).date()
                    if date_obj < today:
                        old_dates.append(date_str)
                        del saved_schedules[date_str]
                except:
                    pass

            if old_dates:
                logging.info(f"[ІФ] Cleaned old dates for {queue_id}: {old_dates}")
                await save_schedule_state(queue_id, json.dumps(saved_schedules))

            # Порівнюємо кожну дату окремо
            changes = []  # [(date, hours, "new"|"updated"), ...]

            for date, hours in current_schedules.items():
                current_hash = json.dumps(hours, sort_keys=True)

                if date not in saved_schedules:
                    # Нова дата - новий графік
                    changes.append((date, hours, "new"))
                    logging.info(f"[ІФ] New schedule for {queue_id} on {date}")
                elif saved_schedules[date] != current_hash:
                    # Дата є, але графік змінився
                    changes.append((date, hours, "updated"))
                    logging.info(f"[ІФ] Updated schedule for {queue_id} on {date}")

                # Оновлюємо збережений стан
                saved_schedules[date] = current_hash

            # Якщо є зміни - надсилаємо сповіщення
            if changes:
                subscribers = await get_users_by_queue(queue_id, REGION_IF)

                if subscribers:
                    for user_id in subscribers:
                        try:
                            user_data = await get_user_data(user_id)
                            address = (
                                user_data.get("address")
                                if isinstance(user_data, dict)
                                else None
                            )

                            # Надсилаємо окреме повідомлення для кожної зміненої дати
                            for i, (date, hours, change_type) in enumerate(changes):
                                msg = format_schedule_notification(
                                    queue_id, date, hours, change_type, address
                                )
                                # Додаємо кнопку донату до останнього повідомлення
                                if i == len(changes) - 1:
                                    await bot.send_message(
                                        user_id,
                                        msg,
                                        parse_mode=ParseMode.MARKDOWN,
                                        reply_markup=get_donate_keyboard(),
                                    )
                                else:
                                    await bot.send_message(
                                        user_id, msg, parse_mode=ParseMode.MARKDOWN
                                    )
                                await asyncio.sleep(0.3)

                            logging.info(
                                f"Notifications sent to {user_id} for queue {queue_id}"
                            )
                        except Exception as e:
                            logging.error(f"Failed to send to {user_id}: {e}")

                        await asyncio.sleep(0.5)

                # Зберігаємо оновлений стан
                await save_schedule_state(queue_id, json.dumps(saved_schedules))

            await asyncio.sleep(1)

        logging.info(f"[ІФ] Check completed. Next check in {CHECK_INTERVAL} seconds")
        await asyncio.sleep(CHECK_INTERVAL)


async def reminder_checker():
    """Перевіряє та надсилає нагадування про наближення подій"""
    logging.info("⏰ Reminder checker started")
    await asyncio.sleep(30)  # Початкова затримка

    while True:
        try:
            now = datetime.now(KYIV_TZ)
            today_str = now.strftime("%d.%m.%Y")

            # Очищення старих нагадувань раз на добу (о 3:00)
            if now.hour == 3 and now.minute < 2:
                await cleanup_old_reminders()

            # Завантажуємо графіки для ВСІХ черг один раз (кеш) — ІФ
            schedules_cache_if = {}
            for queue_id in QUEUES:
                data = await fetch_schedule(None, queue_id)
                if data:
                    schedule_data = (
                        data if isinstance(data, list) else data.get("schedule", [])
                    )
                    for record in schedule_data:
                        if record.get("eventDate") == today_str:
                            schedules_cache_if[queue_id] = record.get("queues", {}).get(
                                queue_id, []
                            )
                            break
                await asyncio.sleep(0.2)

            # Завантажуємо графіки Львів
            schedules_cache_lviv = {}
            lviv_data = await asyncio.to_thread(_fetch_lviv_schedule_sync)
            if lviv_data:
                lviv_today = lviv_data.get(today_str, {})
                for queue_id, slots in lviv_today.items():
                    schedules_cache_lviv[queue_id] = [
                        {"from": s, "to": e} for s, e in slots
                    ]

            # Отримуємо всіх користувачів з підписками та увімкненими нагадуваннями
            cursor = db.users.find(
                {"queues": {"$exists": True, "$ne": []}, "reminders": True}
            )
            users = await cursor.to_list(length=None)

            for user in users:
                user_id = user["user_id"]
                queues = user.get("queues", [])
                user_intervals = user.get(
                    "reminder_intervals", DEFAULT_REMINDER_INTERVALS
                )
                region = user.get("region", REGION_IF)

                # Пропускаємо якщо користувач не обрав жодного інтервалу
                if not user_intervals:
                    continue

                # Оновлюємо now для кожного користувача
                now = datetime.now(KYIV_TZ)

                # Обираємо кеш відповідно до регіону
                schedules_cache = (
                    schedules_cache_lviv
                    if region == REGION_LVIV
                    else schedules_cache_if
                )

                for queue_id in queues:
                    queue_data = schedules_cache.get(queue_id, [])

                    for slot in queue_data:
                        from_time = slot.get("from", "")
                        to_time = slot.get("to", "")

                        if not from_time or not to_time:
                            continue

                        # Перевіряємо нагадування для ВИМКНЕННЯ (from_time)
                        await check_and_send_reminder(
                            user_id,
                            queue_id,
                            today_str,
                            from_time,
                            "off",
                            now,
                            user_intervals,
                        )

                        # Перевіряємо нагадування для УВІМКНЕННЯ (to_time)
                        await check_and_send_reminder(
                            user_id,
                            queue_id,
                            today_str,
                            to_time,
                            "on",
                            now,
                            user_intervals,
                        )

        except Exception as e:
            logging.error(f"Reminder checker error: {e}")

        # Перевіряємо кожну хвилину
        await asyncio.sleep(60)


async def check_and_send_reminder(
    user_id: int,
    queue_id: str,
    date_str: str,
    time_str: str,
    event_type: str,
    now: datetime,
    user_intervals: list[int],
):
    """Перевіряє та надсилає нагадування якщо потрібно"""
    try:
        # Парсимо час події
        day, month, year = date_str.split(".")
        hour, minute = time_str.split(":")
        event_time = datetime(
            int(year), int(month), int(day), int(hour), int(minute), tzinfo=KYIV_TZ
        )

        # Різниця в хвилинах
        diff = (event_time - now).total_seconds() / 60

        # Перевіряємо кожен інтервал нагадування (тільки ті, що обрав користувач)
        for minutes in user_intervals:
            # Нагадування актуальне якщо залишилось від (minutes-1) до (minutes+1) хвилин
            if minutes - 1 <= diff <= minutes + 1:
                # Перевіряємо чи вже відправлено
                event_key = f"{date_str}_{time_str}"
                already_sent = await get_sent_reminder(
                    user_id, queue_id, event_key, event_type, minutes
                )

                if not already_sent:
                    # Форматуємо повідомлення
                    if event_type == "off":
                        emoji = "⚡🔴"
                        action = "вимкнення"
                    else:
                        emoji = "💡🟢"
                        action = "увімкнення"

                    if minutes >= 60:
                        hours = minutes // 60
                        time_text = f"{hours} год" if hours == 1 else f"{hours} год"
                    else:
                        time_text = f"{minutes} хв"

                    msg = (
                        f"{emoji} *Нагадування!*\n\n"
                        f"Через *{time_text}* о *{time_str}* — {action} світла\n"
                        f"🔢 Черга: *{queue_id}*"
                    )

                    try:
                        await bot.send_message(
                            user_id, msg, parse_mode=ParseMode.MARKDOWN
                        )
                        await mark_reminder_sent(
                            user_id, queue_id, event_key, event_type, minutes
                        )
                        logging.info(
                            f"Reminder sent: {user_id}, {queue_id}, {event_type} in {minutes}min at {time_str}"
                        )
                    except Exception as e:
                        logging.error(f"Failed to send reminder to {user_id}: {e}")

                break  # Відправляємо тільки одне нагадування за раз

    except Exception as e:
        logging.error(f"Error in check_and_send_reminder: {e}")


# --- АДМІН-ПАНЕЛЬ ---
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Розсилка всім", callback_data="admin_broadcast_all"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🏔 Розсилка ІФ", callback_data="admin_broadcast_if"
                ),
                InlineKeyboardButton(
                    text="🦁 Розсилка Львів", callback_data="admin_broadcast_lviv"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✉️ Надіслати одному", callback_data="admin_send_one"
                )
            ],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        ]
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    await state.clear()
    users_count = await db.users.count_documents({})
    active_count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}}
    )

    text = (
        "🔐 *Адмін-панель*\n\n"
        f"👥 Всього користувачів: *{users_count}*\n"
        f"✅ Активних (з чергами): *{active_count}*\n\n"
        "Оберіть дію:"
    )
    await message.answer(
        text, reply_markup=get_admin_keyboard(), parse_mode=ParseMode.MARKDOWN
    )


@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    users_count = await db.users.count_documents({})
    active_count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}}
    )
    reminders_on = await db.users.count_documents({"reminders": True})
    if_count = await db.users.count_documents({"region": REGION_IF})
    lviv_count = await db.users.count_documents({"region": REGION_LVIV})

    # Топ черг
    pipeline = [
        {"$unwind": "$queues"},
        {
            "$group": {
                "_id": {"queue": "$queues", "region": "$region"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]
    queue_stats = await db.users.aggregate(pipeline).to_list(length=40)

    if_lines = []
    lviv_lines = []
    for q in queue_stats:
        region = q["_id"].get("region", REGION_IF)
        queue_id = q["_id"]["queue"]
        if region == REGION_LVIV:
            lviv_lines.append(f"  `{queue_id}` — {q['count']}")
        else:
            if_lines.append(f"  `{queue_id}` — {q['count']}")

    if_str = "\n".join(if_lines) or "  немає"
    lviv_str = "\n".join(lviv_lines) or "  немає"

    text = (
        "📊 *Статистика бота*\n\n"
        f"👥 Всього користувачів: *{users_count}*\n"
        f"✅ Активних (з чергами): *{active_count}*\n"
        f"🔔 Нагадування увімкнено: *{reminders_on}*\n\n"
        f"🗺 *По регіонах:*\n"
        f"  🏔 ІФ: *{if_count}*\n"
        f"  🦁 Львів: *{lviv_count}*\n\n"
        f"📋 *Черги (ІФ):*\n{if_str}\n\n"
        f"📋 *Черги (Львів):*\n{lviv_str}"
    )

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_back")
async def cb_admin_back(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()

    users_count = await db.users.count_documents({})
    active_count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}}
    )

    text = (
        "🔐 *Адмін-панель*\n\n"
        f"👥 Всього користувачів: *{users_count}*\n"
        f"✅ Активних (з чергами): *{active_count}*\n\n"
        "Оберіть дію:"
    )
    await callback.message.edit_text(
        text, reply_markup=get_admin_keyboard(), parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast_all")
async def cb_admin_broadcast_all(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.set_state(AdminBroadcast.waiting_for_message)
    await state.update_data(target="all")

    active_count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}}
    )

    text = (
        f"📢 *Розсилка всім ({active_count} користувачів)*\n\n"
        "Надішліть повідомлення для розсилки.\n"
        "Підтримується: текст, фото, відео, документ, голосове, стікер — з підписом або без."
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="❌ Скасувати", callback_data="admin_cancel"
                    )
                ]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast_if")
async def cb_admin_broadcast_if(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.set_state(AdminBroadcast.waiting_for_message)
    await state.update_data(target="region", region=REGION_IF)

    count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}, "region": REGION_IF}
    )

    text = (
        f"🏔 *Розсилка ІФ ({count} користувачів)*\n\n"
        "Надішліть повідомлення для розсилки."
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="❌ Скасувати", callback_data="admin_cancel"
                    )
                ]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast_lviv")
async def cb_admin_broadcast_lviv(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.set_state(AdminBroadcast.waiting_for_message)
    await state.update_data(target="region", region=REGION_LVIV)

    count = await db.users.count_documents(
        {"queues": {"$exists": True, "$ne": []}, "region": REGION_LVIV}
    )

    text = (
        f"🦁 *Розсилка Львів ({count} користувачів)*\n\n"
        "Надішліть повідомлення для розсилки."
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="❌ Скасувати", callback_data="admin_cancel"
                    )
                ]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_send_one")
async def cb_admin_send_one(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    await state.set_state(AdminBroadcast.waiting_for_user_id)

    text = "✉️ *Надіслати одному користувачу*\n\nВведіть user\\_id користувача:"
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="❌ Скасувати", callback_data="admin_cancel"
                    )
                ]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text("❌ *Скасовано*", parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


@dp.message(AdminBroadcast.waiting_for_user_id)
async def admin_process_user_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    try:
        target_id = int(message.text.strip())
    except (ValueError, AttributeError):
        await message.answer(
            "❌ Невірний формат. Введіть числовий user\\_id:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Перевіряємо чи існує користувач в базі
    user = await db.users.find_one({"user_id": target_id})
    if not user:
        await message.answer(
            f"⚠️ Користувача `{target_id}` не знайдено в базі. Надішліть повідомлення все одно?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ Так, надіслати",
                            callback_data=f"admin_force_{target_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text="❌ Скасувати", callback_data="admin_cancel"
                        )
                    ],
                ]
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        await state.update_data(target_id=target_id)
        return

    await state.set_state(AdminBroadcast.waiting_for_message)
    await state.update_data(target="one", target_id=target_id)

    queues = user.get("queues", [])
    queues_str = ", ".join(queues) if queues else "немає"

    await message.answer(
        f"✉️ *Надсилаємо користувачу* `{target_id}`\n"
        f"📋 Черги: {queues_str}\n\n"
        "Надішліть повідомлення:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="❌ Скасувати", callback_data="admin_cancel"
                    )
                ]
            ]
        ),
        parse_mode=ParseMode.MARKDOWN,
    )


@dp.callback_query(F.data.startswith("admin_force_"))
async def cb_admin_force_send(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return

    target_id = int(callback.data.replace("admin_force_", ""))
    await state.set_state(AdminBroadcast.waiting_for_message)
    await state.update_data(target="one", target_id=target_id)

    await callback.message.edit_text(
        f"✉️ *Надсилаємо користувачу* `{target_id}`\n\nНадішліть повідомлення:",
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


@dp.message(AdminBroadcast.waiting_for_message)
async def admin_process_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    target = data.get("target")
    await state.clear()

    success = 0
    failed = 0

    if target == "all":
        # Розсилка всім активним користувачам
        cursor = db.users.find({"queues": {"$exists": True, "$ne": []}})
        users = await cursor.to_list(length=None)

        progress_msg = await message.answer(f"📢 Розсилка... 0/{len(users)}")

        for i, user in enumerate(users):
            uid = user["user_id"]
            try:
                await forward_admin_message(message, uid)
                success += 1
            except Exception as e:
                logging.error(f"Broadcast failed for {uid}: {e}")
                failed += 1

            await asyncio.sleep(0.05)

            # Оновлюємо прогрес кожні 20 користувачів
            if (i + 1) % 20 == 0:
                try:
                    await progress_msg.edit_text(f"📢 Розсилка... {i + 1}/{len(users)}")
                except:
                    pass

        try:
            await progress_msg.delete()
        except:
            pass

        await message.answer(
            f"✅ *Розсилка завершена!*\n\n"
            f"📤 Надіслано: *{success}*\n"
            f"❌ Помилок: *{failed}*",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif target == "region":
        region = data.get("region", REGION_IF)
        region_name = "🏔 ІФ" if region == REGION_IF else "🦁 Львів"
        cursor = db.users.find(
            {"queues": {"$exists": True, "$ne": []}, "region": region}
        )
        users = await cursor.to_list(length=None)

        progress_msg = await message.answer(
            f"📢 Розсилка {region_name}... 0/{len(users)}"
        )

        for i, user in enumerate(users):
            uid = user["user_id"]
            try:
                await forward_admin_message(message, uid)
                success += 1
            except Exception as e:
                logging.error(f"Broadcast ({region}) failed for {uid}: {e}")
                failed += 1

            await asyncio.sleep(0.05)

            if (i + 1) % 20 == 0:
                try:
                    await progress_msg.edit_text(
                        f"📢 Розсилка {region_name}... {i + 1}/{len(users)}"
                    )
                except:
                    pass

        try:
            await progress_msg.delete()
        except:
            pass

        await message.answer(
            f"✅ *Розсилка {region_name} завершена!*\n\n"
            f"📤 Надіслано: *{success}*\n"
            f"❌ Помилок: *{failed}*",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif target == "one":
        target_id = data.get("target_id")
        try:
            await forward_admin_message(message, target_id)
            await message.answer(
                f"✅ Повідомлення надіслано користувачу `{target_id}`",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            await message.answer(f"❌ Не вдалося надіслати: {e}")


async def forward_admin_message(message: Message, target_id: int):
    """Пересилає повідомлення адміна користувачу, зберігаючи формат"""
    if message.photo:
        await bot.send_photo(
            target_id,
            photo=message.photo[-1].file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
        )
    elif message.video:
        await bot.send_video(
            target_id,
            video=message.video.file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
        )
    elif message.animation:
        await bot.send_animation(
            target_id,
            animation=message.animation.file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
        )
    elif message.document:
        await bot.send_document(
            target_id,
            document=message.document.file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
        )
    elif message.voice:
        await bot.send_voice(
            target_id,
            voice=message.voice.file_id,
            caption=message.caption,
            caption_entities=message.caption_entities,
        )
    elif message.video_note:
        await bot.send_video_note(target_id, video_note=message.video_note.file_id)
    elif message.sticker:
        await bot.send_sticker(target_id, sticker=message.sticker.file_id)
    elif message.text:
        await bot.send_message(target_id, text=message.text, entities=message.entities)
    else:
        # Фолбек — просто копіюємо
        await message.copy_to(target_id)


# --- ВЕБ-СЕРВЕР ---
async def get_users_count() -> int:
    """Повертає кількість користувачів"""
    try:
        count = await db.users.count_documents({})
        return count
    except:
        return 0


async def handle_index(request):
    """Головна сторінка"""
    template_path = BASE_DIR / "templates" / "index.html"

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read()

        users_count = await get_users_count()
        html = html.replace("{{users_count}}", str(users_count))
        html = html.replace("{{check_interval}}", str(CHECK_INTERVAL))

        return web.Response(text=html, content_type="text/html")
    except Exception as e:
        logging.error(f"Error loading template: {e}")
        return web.Response(text="Lumos Bot is running!", content_type="text/plain")


async def handle_health(request):
    """Health check для Render"""
    return web.json_response(
        {
            "status": "ok",
            "service": "lumos-bot",
            "timestamp": datetime.now(KYIV_TZ).isoformat(),
        }
    )


async def start_web_server():
    """Запуск веб-сервера"""
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Web server started on port {PORT}")


async def main():
    logging.info("🤖 Bot starting...")
    logging.info(
        f"📋 Config: APQE_PQFRTY={'SET' if APQE_PQFRTY else 'NOT SET'}, APSRC_PFRTY={'SET' if APSRC_PFRTY else 'NOT SET'}"
    )
    logging.info(
        f"📋 Config: APQE_LOE={'SET' if LVIV_API_URL else 'NOT SET'}, APWR_LOE={'SET' if LVIV_POWER_API_URL else 'NOT SET'}"
    )
    logging.info(f"📋 MongoDB: {MONGO_URI[:20]}...")
    await init_db()

    try:
        # Запускаємо веб-сервер
        await start_web_server()

        # Запускаємо моніторинг графіків (ІФ)
        asyncio.create_task(scheduled_checker())

        # Запускаємо моніторинг графіків (Львів)
        asyncio.create_task(lviv_scheduled_checker())

        # Запускаємо нагадування
        asyncio.create_task(reminder_checker())

        # Запускаємо бота
        await dp.start_polling(bot)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
