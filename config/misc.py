from aiogram import Bot, Dispatcher
from aiogram.types import ParseMode

from .settings import BotSettings
from aiogram.contrib.fsm_storage.memory import MemoryStorage

poster_bot = Bot(token=BotSettings.API_TOKEN, parse_mode=ParseMode.HTML)

dp = Dispatcher(poster_bot, storage=MemoryStorage())
