from aiogram import types

from utils import strings


async def main_menu():
    keyboard = types.ReplyKeyboardMarkup(row_width=3, resize_keyboard=True)
    keyboard.add(strings.COMMANDS)
    # keyboard.add(strings.RECOMMEND)
    # keyboard.row(strings.VOICE_PRACTICE, strings.TEXT_PRACTICE)
    # keyboard.add(strings.RANDOM_SHOW)
    return keyboard
