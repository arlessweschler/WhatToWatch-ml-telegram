from aiogram import types


async def commands_for_shows():
    keyboard = types.InlineKeyboardMarkup(row_width=1)

    keyboard.add(types.InlineKeyboardButton("popular", callback_data="command::0"))
    keyboard.add(types.InlineKeyboardButton("watching now", callback_data="command::1"))
    keyboard.add(types.InlineKeyboardButton("recommendation from other users", callback_data="command::2"))
    keyboard.add(types.InlineKeyboardButton("random movie", callback_data="command::3"))
    keyboard.add(types.InlineKeyboardButton("personal recommendations", callback_data="command::4"))
    # keyboard.add(types.InlineKeyboardButton("popular", callback_data="command::3"))

    return keyboard
