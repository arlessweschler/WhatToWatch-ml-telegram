from aiogram import types
from aiogram.dispatcher import FSMContext

from config.misc import poster_bot
from database.DBService import FavoriteDB
from ml_utils.ViaTitleRecommendationSystem import ViaTitleRecommendationSystem
from models.ShowCardBot import ShowCardBot


async def card_buttons_handler(callback: types.CallbackQuery, state: FSMContext):
    show_data: str = callback.data.split("::")[1]
    show_id = abs(int(show_data))

    if show_data.startswith('+'):
        try:
            await FavoriteDB.create(callback.from_user.id, show_id)
            await poster_bot.answer_callback_query(callback.id,text="liked")
        except Exception as e:
            print(e)
            pass
        return

    elif show_data.startswith('-'):
        try:
            await FavoriteDB.delete(callback.from_user.id, show_id)
            await poster_bot.answer_callback_query(callback.id, text="disliked")
        except Exception as e:
            print(e)
            pass
        return

    show_card_bot = ShowCardBot(show_id)
    await show_card_bot.create_card()
    await show_card_bot.change_state_card(poster_bot, callback, 1)

    via_title_recommend = ViaTitleRecommendationSystem(movie=show_card_bot.show)
    await via_title_recommend.prepare_predict()
    shows = await via_title_recommend.predict()

    for i in shows:
        show_card_temp = ShowCardBot(i)
        await show_card_temp.create_card()
        await show_card_temp.send_via_bot(callback.from_user.id, poster_bot)

    await show_card_bot.change_state_card(poster_bot, callback, 2)

    await state.finish()
    # await poster_bot.answer_callback_query(callback.id)
