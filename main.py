import asyncio
import logging
import os
import re
import requests
from bs4 import BeautifulSoup

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
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
    ReplyKeyboardRemove
)
from aiohttp import web
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "45"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

APQE_PQFRTY = os.getenv("APQE_PQFRTY")
APSRC_PFRTY = os.getenv("APSRC_PFRTY")
PROXY_URL = os.getenv("PROXY_URL")

LVIV_API_URL = os.getenv("APQE_LOE")
LVIV_POWER_API_URL = os.getenv("APWR_LOE")

QUEUES = [
    "1.1", "1.2", "2.1", "2.2", "3.1", "3.2",
    "4.1", "4.2", "5.1", "5.2", "6.1", "6.2"
]

BTN_CHECK = "🔄 Перевірити графік"
BTN_MY_QUEUE = "📋 Мої підписки"
BTN_SET_QUEUE = "⚡ Обрати черги"
BTN_CHANGE_QUEUE = "⚙️ Налаштування"
BTN_HELP = "❓ Допомога"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- MongoDB ---
mongo_client: AsyncIOMotorClient = None
db = None

async def init_db():
    global mongo_client, db
    try:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        await mongo_client.admin.command("ping")
        logging.info("✅ Connected to MongoDB")
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

async def close_db():
    global mongo_client
    if mongo_client:
        mongo_client.close()

async def get_user_data(user_id: int) -> dict | None:
    return await db.users.find_one({"user_id": user_id})

async def set_user_data(user_id: int, queues: list[str], address: str = None):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"queues": queues, "address": address, "region": "if"}},
        upsert=True,
    )

async def get_users_count() -> int:
    try:
        return await db.users.count_documents({})
    except:
        return 0

# --- FSM СТАНИ ---
class AddressForm(StatesGroup):
    waiting_for_city = State()
    waiting_for_street = State()
    waiting_for_house = State()

# --- КЛАВІАТУРИ ---
def get_main_keyboard(has_queue: bool = False) -> ReplyKeyboardMarkup:
    queue_btn = BTN_CHANGE_QUEUE if has_queue else BTN_SET_QUEUE
    buttons = [
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_MY_QUEUE)],
        [KeyboardButton(text=queue_btn), KeyboardButton(text=BTN_HELP)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_queue_choice_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🏠 Додати за адресою", callback_data="enter_address")],
        [InlineKeyboardButton(text="🔢 Обрати зі списку", callback_data="select_queue")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_input")]]
    )

# --- АПІ ПАРСИНГ ---
async def fetch_schedule(session: AsyncSession, queue_id):
    if not APQE_PQFRTY:
        return None
    params = {"queue": queue_id}
    try:
        response = await session.get(APQE_PQFRTY, params=params)
        if response.status_code == 200:
            try:
                return response.json()
            except:
                return []
        return None
    except Exception as e:
        logging.error(f"[ІФ] Error fetching {queue_id}: {e}")
        return None

async def fetch_schedule_by_address(session: AsyncSession, city: str, street: str, house: str) -> dict | None:
    if not APSRC_PFRTY:
        return None
    address = f"{city},{street},{house}"
    payload = {"accountNumber": "", "userSearchChoice": "pob", "address": address}
    try:
        response = await session.post(APSRC_PFRTY, data=payload)
        if response.status_code == 200:
            return response.json()
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

# --- ЛЬВІВСЬКА ОБЛАСТЬ (LUM-6) ---
def _parse_lviv_html(html: str) -> tuple[str | None, dict]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
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
        result[g] = times
    return date_str, result

async def fetch_lviv_schedule() -> dict | None:
    """Використовує requests для отримання даних з ЛОЕ"""
    if not LVIV_API_URL:
        return None
    try:
        # requests є синхронним, тому запускаємо в окремому потоці для асинхронності
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: requests.get(LVIV_API_URL, timeout=15))
        resp.raise_for_status()
        data = resp.json()
        member = data.get("hydra:member") or []
        if not member:
            return {}
        menu_items = member[0].get("menuItems", [])
        all_schedules = {}
        for item in menu_items:
            name = item.get("name", "")
            html = item.get("rawHtml")
            if not html:
                continue
            if name in ("Today", "Tomorrow") or "графік" in name.lower():
                date_str, groups = _parse_lviv_html(html)
                if date_str and groups:
                    all_schedules[date_str] = groups
        return all_schedules
    except Exception as e:
        logging.error(f"[ЛОЕ] Error fetching Lviv schedule: {e}")
        return None

# --- ХЕНДЛЕРИ ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_data = await get_user_data(message.from_user.id)
    if not user_data:
        await set_user_data(message.from_user.id, [])
        has_queue = False
    else:
        has_queue = len(user_data.get("queues", [])) > 0
    text = f"💡 *Привіт, {message.from_user.first_name}!*\n\nТепер додано підтримку Львівської області (парсинг)."
    await message.answer(text, reply_markup=get_main_keyboard(has_queue), parse_mode=ParseMode.MARKDOWN)

# --- ВЕБ-СЕРВЕР ---
async def handle_index(request):
    template_path = os.path.join(BASE_DIR, "templates", "index.html")
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

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_index)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Web server started on port {PORT}")

async def main():
    logging.info("🤖 Bot starting...")
    await init_db()
    try:
        await start_web_server()
        await dp.start_polling(bot)
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
