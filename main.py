import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# --- КОНФІГУРАЦІЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "lumos_bot")

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
        await mongo_client.admin.command("ping")
        logging.info("✅ Connected to MongoDB")
    except Exception as e:
        logging.error(f"❌ MongoDB connection failed: {e}")
        raise

async def close_db():
    """Закриття підключення до MongoDB"""
    global mongo_client
    if mongo_client:
        mongo_client.close()
        logging.info("MongoDB connection closed")

# --- РОБОТА З БАЗОЮ ДАНИХ ---
async def get_user_data(user_id: int) -> dict | None:
    """Отримує дані користувача з MongoDB"""
    user = await db.users.find_one({"user_id": user_id})
    return user

async def set_user_data(user_id: int, queues: list[str], address: str = None):
    """Зберігає дані користувача в MongoDB"""
    await db.users.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "queues": queues,
                "address": address,
            }
        },
        upsert=True,
    )

# --- ХЕНДЛЕРИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    # Приклад використання БД: ініціалізуємо користувача з порожнім списком черг, якщо його немає
    user_data = await get_user_data(message.from_user.id)
    if not user_data:
        await set_user_data(message.from_user.id, [])
        
    text = (
        f"💡 *Привіт, {message.from_user.first_name}!*\n\n"
        f"Я *Люмос* — допоможу тобі дізнаватись про відключення першим!\n\n"
        f"⚡ Бот у розробці (додано MongoDB). Слідкуй за оновленнями!"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


async def main():
    logging.info("🤖 Bot starting...")
    await init_db()
    try:
        await dp.start_polling(bot)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
