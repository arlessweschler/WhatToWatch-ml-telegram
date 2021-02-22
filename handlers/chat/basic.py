from typing import Optional

from aiogram import types
from aiogram.dispatcher import FSMContext

import keyboards
from config.misc import poster_bot
from database.DBService import ShowFilmDB, GenreDB, ClientDB
from keyboards.default import main_keyboards as mk
from keyboards.inline.commands_for_shows import commands_for_shows
from keyboards.inline.show_vote_keyboard import show_vote_keyboard
from models.Client import Client
from states.RecommendationStates import RecommendationStates
from utils import constants, strings
from utils.bot_utils import post_with_image


async def bot_start(message: types.Message, state: FSMContext):
    client: Optional[Client] = await ClientDB.get_by_pk(message.from_user.id)
    if client is None:
        await ClientDB.create(message.from_user.id)
        await message.answer("menu", reply_markup=await mk.main_menu())

        # todo give about bot
        pass
    else:
        pass
    await poster_bot.send_message(message.from_user.id, strings.MAIN_COMMANDS, reply_markup=await commands_for_shows())

