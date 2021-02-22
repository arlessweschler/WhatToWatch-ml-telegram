import datetime

from aiogram import types
from aiogram.dispatcher import FSMContext

from config.misc import poster_bot
from database.DBService import ShowFilmDB
from keyboards.default.main_keyboards import main_menu
from ml_utils.CollaborativeFilter import CollaborativeFilter
from models.ShowCardBot import ShowCardBot
from states.RecommendationStates import RecommendationStates
from utils import strings
import tmdbsimple as tmdb

from utils.bot_utils import parse_dict_to_showfilm


async def distribution(callback: types.CallbackQuery, state: FSMContext):
    def_number = int(callback.data.split("::")[1])
    if def_number == 0:
        await popular(callback, state)
    elif def_number == 1:
        await watching_now(callback, state)
    elif def_number == 2:
        await recommendation_from_other(callback, state)
    elif def_number == 3:
        await random_movie(callback, state)
    elif def_number == 4:
        await personal_recommendation(callback, state)


async def popular(callback: types.CallbackQuery, state: FSMContext):
    shows = await ShowFilmDB.get_popular_films_by_date(datetime.datetime.now() - datetime.timedelta(weeks=8))
    for show in shows:
        show_card_temp = ShowCardBot()
        await show_card_temp.set_show(show)
        await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)
    pass


async def watching_now(callback: types.CallbackQuery, state: FSMContext):
    shows = await ShowFilmDB.get_popular_films_by_date(datetime.datetime.now() - datetime.timedelta(weeks=4))
    if len(shows) < 1:
        movies = tmdb.Movies().now_playing()['results'][:5]
        for movie in movies:
            show_film = parse_dict_to_showfilm(movie)
            show_card_temp = ShowCardBot()
            await show_card_temp.set_show_with_genres(show_film)
            await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)

        pass
    else:
        for show in shows:
            show_card_temp = ShowCardBot()
            await show_card_temp.set_show(show)
            await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)
    pass


async def recommendation_from_other(callback: types.CallbackQuery, state: FSMContext):
    date = datetime.datetime.now()
    date = date.replace(year=date.year - 2)
    show_ids = await ShowFilmDB.get_shows_recommended_by_clients(date, 5, 0)
    # todo next pagination
    for show_id in show_ids:
        show_card_temp = ShowCardBot(show_id)
        await show_card_temp.create_card()
        await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)


async def random_movie(callback: types.CallbackQuery, state: FSMContext):
    show_card_bot = ShowCardBot()

    await show_card_bot.create_random_card()
    await show_card_bot.send_via_bot(callback.from_user.id, poster_bot)


async def personal_recommendation(callback: types.CallbackQuery, state: FSMContext):
    if state == RecommendationStates.callback_title:
        await poster_bot.send_message(callback.from_user.id, strings.WAIT_FOR_FINISH, reply_markup=await main_menu())
        return
    try:
        await RecommendationStates.callback_title.set()
        await poster_bot.send_message(callback.from_user.id, "wait for recommendations")
        collaborative = CollaborativeFilter(callback.from_user.id)
        data = await collaborative.get_movies_id_after()
        if data:
            for show in data:
                show_card_temp = ShowCardBot(show)
                await show_card_temp.create_card()
                await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)
            await state.finish()


        else:
            await poster_bot.send_message(callback.from_user.id, strings.YOUR_REMM_IS_EMPTY)
            await state.finish()
    except Exception as e:
        print(e)
        await poster_bot.send_message(callback.from_user.id, strings.REM_ERROR)
        await state.finish()
    pass
