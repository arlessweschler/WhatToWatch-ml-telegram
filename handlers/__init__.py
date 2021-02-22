from aiogram import Dispatcher
from aiogram.dispatcher.filters import CommandStart

import handlers.chat
from handlers.chat.basic import bot_start
from handlers.recommendations.film_card_handlers import card_buttons_handler
import handlers.inline_commands

from states.RecommendationStates import RecommendationStates as rstates
from utils import strings


def setup(dp: Dispatcher):
    dp.register_message_handler(bot_start, CommandStart(), state='*')

    chat.setup(dp)
    inline_commands.setup(dp)


    dp.register_callback_query_handler(card_buttons_handler, lambda callback: callback.data.split("::")[0] == "card",
                                       state='*')
