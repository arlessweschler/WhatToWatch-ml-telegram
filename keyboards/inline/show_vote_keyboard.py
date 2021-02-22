from aiogram import types


async def show_vote_keyboard(show_id, stage: int = 0):
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.row(
        types.InlineKeyboardButton(f"❤", callback_data=f"card::+{show_id}"),
        types.InlineKeyboardButton(f"💔", callback_data=f"card::-{show_id}")
    )
    if stage == 0:
        keyboard.add(
            types.InlineKeyboardButton(f"show similar", callback_data=f"card::{show_id}")
        )
    elif stage == 1:
        keyboard.add(
            types.InlineKeyboardButton(f"loading", callback_data=f"card::{None}")
        )

    return keyboard
