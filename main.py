import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

# --- КОНФІГУРАЦІЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# --- ХЕНДЛЕРИ КОМАНД ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        f"💡 *Привіт, {message.from_user.first_name}!*\n\n"
        f"Я *Люмос* — допоможу тобі дізнаватись про відключення першим!\n\n"
        f"⚡ Бот у розробці. Слідкуй за оновленнями!"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


async def main():
    logging.info("🤖 Bot starting...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
