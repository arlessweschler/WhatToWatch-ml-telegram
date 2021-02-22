from aiogram import types
from aiogram.dispatcher import FSMContext

from config.misc import poster_bot
from database.DBService import ShowFilmDB, GenreDB
from keyboards.default.main_keyboards import main_menu
from keyboards.inline.commands_for_shows import commands_for_shows
from keyboards.inline.show_vote_keyboard import show_vote_keyboard
from ml_utils.CollaborativeFilter import CollaborativeFilter
from models.ShowCardBot import ShowCardBot
from states.RecommendationStates import RecommendationStates
from utils import constants, strings
from utils.bot_utils import post_with_image

async def menu_movie_commands(message: types.Message, state: FSMContext):
    await poster_bot.send_message(message.from_user.id, strings.MAIN_COMMANDS, reply_markup=await commands_for_shows())

async def menu_recommend(message: types.Message, state: FSMContext):
    if state == RecommendationStates.callback_title:
        await poster_bot.send_message(message.from_user.id, strings.WAIT_FOR_FINISH, reply_markup=await main_menu())
        return
    try:
        await RecommendationStates.callback_title.set()
        collaborative = CollaborativeFilter(message.from_user.id)
        data = await collaborative.get_movies_id_after()
        if data:
            for show in data:
                show_card_temp = ShowCardBot(show)
                await show_card_temp.create_card()
                await show_card_temp.send_via_bot(message.from_user.id, poster_bot)
            await state.finish()


        else:
            await poster_bot.send_message(message.from_user.id, strings.YOUR_REMM_IS_EMPTY)
            await state.finish()
    except Exception as e:
        print(e)
        await poster_bot.send_message(message.from_user.id,strings.REM_ERROR)
        await state.finish()

    pass


async def menu_voice_practice(message: types.Message, state: FSMContext):
    pass


async def menu_text_practice(message: types.Message, state: FSMContext):
    pass


async def menu_random_show(message: types.Message, state: FSMContext):
    show_card_bot = ShowCardBot()

    await show_card_bot.create_random_card()
    await show_card_bot.send_via_bot(message.from_user.id, poster_bot)
