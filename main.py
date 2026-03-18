import asyncio
import hashlib
import json
import logging
import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

REGION_IF = "if"
REGION_LVIV = "lviv"
KYIV_TZ = ZoneInfo("Europe/Kyiv")

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

# --- ФОРМАТУВАННЯ (LUM-11) ---
def format_notification(queue_id: str, schedule: list, is_update: bool = False) -> str:
    """Форматування для Івано-Франківська"""
    header = "⚡️ *Оновлення графіку!*" if is_update else "📊 *Поточний графік*"
    lines = [f"{header}\n", f"📍 *Черга:* {queue_id}\n"]
    
    if not schedule:
        lines.append("✅ Відключень не заплановано.")
        return "\n".join(lines)
    
    for day in schedule:
        date = day.get("date", "")
        hours = day.get("hours", [])
        if not hours: continue
        
        lines.append(f"📅 *{date}:*")
        for h in hours:
            lines.append(f"• {h}")
        lines.append("")
        
    return "\n".join(lines).strip()

def format_lviv_notification(queue_id: str, slots: dict, is_update: bool = False) -> str:
    """Форматування для Львова"""
    header = "⚡️ *Оновлення графіку (Львів)!*" if is_update else "📊 *Поточний графік (Львів)*"
    lines = [f"{header}\n", f"📍 *Черга:* {queue_id}\n"]
    
    if not slots:
        lines.append("✅ Відключень не заплановано.")
        return "\n".join(lines)
        
    for date_str, groups in slots.items():
        times = groups.get(queue_id, [])
        if not times: continue
        
        lines.append(f"📅 *{date_str}:*")
        for start, end in times:
            lines.append(f"• {start} — {end}")
        lines.append("")
        
    return "\n".join(lines).strip()

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

async def set_user_data(user_id: int, queues: list[str], address: str = None, region: str = REGION_IF):
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": {"queues": queues, "address": address, "region": region, "updated_at": datetime.now(KYIV_TZ)}},
        upsert=True,
    )

async def get_users_by_queue(queue: str, region: str = None) -> list[int]:
    query = {"queues": queue}
    if region: query["region"] = region
    cursor = db.users.find(query)
    users = await cursor.to_list(length=None)
    return [user["user_id"] for user in users]

async def get_schedule_state(queue_id: str) -> str | None:
    state = await db.schedule_state.find_one({"queue_id": queue_id})
    return state.get("data_hash") if state else None

async def save_schedule_state(queue_id: str, data_hash: str):
    await db.schedule_state.update_one(
        {"queue_id": queue_id},
        {"$set": {"data_hash": data_hash, "updated_at": datetime.now(KYIV_TZ)}},
        upsert=True,
    )

# --- МОНІТОРИНГ ---
async def scheduled_checker():
    """Моніторинг Івано-Франківська"""
    while True:
        try:
            async with AsyncSession(impersonate="chrome120", proxy=PROXY_URL) as session:
                for q in QUEUES:
                    data = await fetch_schedule(session, q)
                    if data is None: continue
                    queue_id, schedule = extract_queue_from_response(data)
                    if not queue_id: continue
                    
                    data_hash = hashlib.md5(json.dumps(schedule, sort_keys=True).encode()).hexdigest()
                    old_hash = await get_schedule_state(queue_id)
                    
                    if old_hash and old_hash != data_hash:
                        text = format_notification(queue_id, schedule, is_update=True)
                        users = await get_users_by_queue(queue_id, region=REGION_IF)
                        for user_id in users:
                            try: await bot.send_message(user_id, text, parse_mode=ParseMode.MARKDOWN)
                            except: pass
                    await save_schedule_state(queue_id, data_hash)
                    await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in scheduled_checker: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

async def lviv_scheduled_checker():
    """Моніторинг Львова"""
    while True:
        try:
            all_schedules = await fetch_lviv_schedule()
            if all_schedules:
                for q in QUEUES:
                    q_data = {d: all_schedules[d].get(q, []) for d in all_schedules}
                    data_hash = hashlib.md5(json.dumps(q_data, sort_keys=True).encode()).hexdigest()
                    lviv_q_id = f"lviv_{q}"
                    old_hash = await get_schedule_state(lviv_q_id)
                    
                    if old_hash and old_hash != data_hash:
                        text = format_lviv_notification(q, all_schedules, is_update=True)
                        users = await get_users_by_queue(q, region=REGION_LVIV)
                        for user_id in users:
                            try: await bot.send_message(user_id, text, parse_mode=ParseMode.MARKDOWN)
                            except: pass
                    await save_schedule_state(lviv_q_id, data_hash)
        except Exception as e:
            logging.error(f"Error in lviv_scheduled_checker: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# --- АПІ ПАРСИНГ ---
async def fetch_schedule(session, queue_id):
    if not APQE_PQFRTY: return None
    params = {"queue": queue_id}
    try:
        response = await session.get(APQE_PQFRTY, params=params)
        if response.status_code == 200:
            try: return response.json()
            except: return []
        return None
    except Exception as e:
        logging.error(f"[ІФ] Error fetching {queue_id}: {e}")
        return None

def extract_queue_from_response(data) -> tuple[str | None, list | None]:
    if not data or not isinstance(data, dict): return None, None
    current = data.get("current", {})
    schedule = data.get("schedule", [])
    if current.get("hasQueue") != "yes": return None, None
    queue_num = current.get("queue")
    sub_queue = current.get("subQueue")
    if queue_num is not None and sub_queue is not None:
        queue_id = f"{queue_num}.{sub_queue}"
        if queue_id in QUEUES: return queue_id, schedule
    return None, None

async def fetch_lviv_schedule() -> dict | None:
    if not LVIV_API_URL: return None
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: requests.get(LVIV_API_URL, timeout=15))
        resp.raise_for_status()
        data = resp.json()
        member = data.get("hydra:member") or []
        if not member: return {}
        menu_items = member[0].get("menuItems", [])
        all_schedules = {}
        for item in menu_items:
            name = item.get("name", "")
            html = item.get("rawHtml")
            if not html: continue
            if name in ("Today", "Tomorrow") or "графік" in name.lower():
                date_str, groups = _parse_lviv_html(html)
                if date_str and groups: all_schedules[date_str] = groups
        return all_schedules
    except Exception as e:
        logging.error(f"[ЛОЕ] Error fetching Lviv schedule: {e}")
        return None

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
        if not m: continue
        group_text = m.group(1)
        times = re.findall(r"(\d{2}:\d{2})\s*(?:-|–|до|to)\s*(\d{2}:\d{2})", group_text)
        result[g] = times
    return date_str, result

# --- ОСНОВНИЙ ЦИКЛ ---
async def main():
    logging.info("🤖 Bot starting...")
    await init_db()
    asyncio.create_task(scheduled_checker())
    asyncio.create_task(lviv_scheduled_checker())
    try: await dp.start_polling(bot)
    finally: await close_db()

if __name__ == "__main__":
    asyncio.run(main())
