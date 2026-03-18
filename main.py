import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

# --- ЛОГУВАННЯ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Error Handling (LUM-19) ---
async def safe_db_call(func, *args, **kwargs):
    """Огортка для безпечних викликів до БД"""
    try:
        return await func(*args, **kwargs)
    except Exception as e:
        logging.error(f"❌ Database error in {func.__name__}: {e}")
        return None

# Приклад використання у хендлерах:
@dp.message(F.text == "Check")
async def handler(message: Message):
    try:
        # Логіка...
        pass
    except TelegramBadRequest as e:
        logging.warning(f"Telegram API warning: {e.message}")
    except Exception as e:
        logging.error(f"Unexpected error in handler: {e}")
        await message.answer("⚠️ Сталася неочікувана помилка. Спробуйте пізніше.")

# ... (rest of the bot logic with try/except) ...
